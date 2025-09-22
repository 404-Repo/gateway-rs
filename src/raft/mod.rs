pub mod client;
pub mod gateway_state;
pub mod memstore;
pub mod network;
pub mod server;
pub mod store;
mod tests;

use anyhow::bail;
use anyhow::Result;
use backon::BackoffBuilder;
use backon::ConstantBuilder;
use backon::Retryable;
use bytes::Bytes;
use foldhash::fast::RandomState;
use futures_util::future::try_join_all;
use gateway_state::GatewayState;
use http::StatusCode;
use network::Network;
use openraft::BasicNode;
use salvo::conn::rustls::Keycert;
use salvo::conn::rustls::RustlsConfig;
use server::RServer;
use std::collections::BTreeMap;
use std::io::Cursor;
use std::net::IpAddr;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tracing::error;
use tracing::info;
use tracing::warn;
use uuid::Uuid;

use crate::api::request::GatewayInfoExt;
use crate::api::response::GatewayInfo;
use crate::api::Task;
use crate::common::cert::generate_and_create_keycert;
use crate::common::cert::load_certificates;
use crate::common::cert::load_private_key;
use crate::common::crypto_provider::init_crypto_provider;
use crate::common::queue::DupQueue;
use crate::common::resolve::lookup_hosts_ips;
use crate::common::task::TaskManager;
use crate::config::NodeConfig;
use crate::db::ApiKeyValidator;
use crate::db::DatabaseBuilder;
use crate::http3::client::Http3Client;
use crate::http3::client::Http3ClientBuilder;
use crate::http3::server::Http3Server;
use crate::metrics::Metrics;
use crate::raft::client::RClient;
use crate::raft::client::RClientBuilder;
use crate::raft::store::Request;
use crate::raft::store::Response;

pub type NodeId = u64;
pub type LogStore = store::LogStore;
pub type StateMachineStore = store::StateMachineStore;
pub type Raft = openraft::Raft<TypeConfig>;

#[derive(Debug, PartialEq)]
pub enum GatewayMode {
    Bootstrap,
    Vote,
    Single,
}

openraft::declare_raft_types!(
    pub TypeConfig:
        D = Request,
        R = Response,
        NodeId = NodeId,
);

pub mod typ {
    use openraft::BasicNode;

    use crate::raft::NodeId;

    pub type RaftError<E = openraft::error::Infallible> = openraft::error::RaftError<NodeId, E>;
    pub type RPCError<E = openraft::error::Infallible> =
        openraft::error::RPCError<NodeId, BasicNode, RaftError<E>>;
}

pub struct Gateway {
    pub _id: NodeId,
    pub _config: Arc<NodeConfig>,
    pub server: RServer,
    pub _gateway_state: GatewayState,
    pub gateway_info_updater: JoinHandle<()>,
    pub gateway_leader_change: JoinHandle<()>,
    pub api_key_validator_updater: JoinHandle<()>,
    pub task_manager: TaskManager,
    pub _log_store: LogStore,
    pub http_server: Http3Server,
    pub _task_queue: DupQueue<Task>,
}

async fn get_id_for_endpoint(
    timeout: Duration,
    ip: IpAddr,
    dns_name: &str,
    sleep_timeout: Duration,
    skip_verification: bool,
    retries: usize,
) -> Result<u64> {
    let url = format!("https://{}:4443/id", dns_name);
    let connection_addr = format!("{}:4443", ip);

    let backoff = ConstantBuilder::new()
        .with_delay(sleep_timeout)
        .with_max_times(retries)
        .build();

    let id = (|| async {
        if let Ok(client) = Http3ClientBuilder::new()
            .server_domain(dns_name)
            .server_ip(&connection_addr)
            .dangerous_skip_verification(skip_verification)
            .build()
            .await
        {
            if let Ok((status, body)) = client.get(&url, Some(timeout)).await {
                if status == StatusCode::OK {
                    let body_str = std::str::from_utf8(&body).map_err(|e| {
                        anyhow::anyhow!("Failed to convert response body to UTF-8: {}", e)
                    })?;
                    let id = body_str
                        .trim()
                        .parse::<u64>()
                        .map_err(|e| anyhow::anyhow!("Failed to parse ID as u64: {}", e))?;
                    return Ok(id);
                }
            }
        }
        Err(anyhow::anyhow!(format!(
            "Failed to get ID from {}, {}",
            connection_addr, dns_name
        )))
    })
    .retry(backoff)
    .await?;

    Ok(id)
}

pub async fn get_node_ids(
    timeout: Duration,
    ips: &[IpAddr],
    dns_names: &[impl AsRef<str>],
    sleep_timeout: Duration,
    skip_verification: bool,
    retries: usize,
) -> Result<Vec<u64>> {
    if ips.len() != dns_names.len() {
        return Err(anyhow::anyhow!(
            "The number of endpoints and DNS names must be equal"
        ));
    }
    let futures = ips.iter().zip(dns_names.iter()).map(|(ip, dns_name)| {
        get_id_for_endpoint(
            timeout,
            *ip,
            dns_name.as_ref(),
            sleep_timeout,
            skip_verification,
            retries,
        )
    });
    let ids = try_join_all(futures).await?;
    Ok(ids)
}

impl Gateway {
    pub async fn gateway_info_updater(
        config: Arc<NodeConfig>,
        gateway_state: GatewayState,
        task_queue: DupQueue<Task>,
        last_task_acquisition: Arc<AtomicU64>,
    ) {
        let mut last_leader: Option<u64> = None;
        let mut client: Option<Http3Client> = None;
        let max_idle_timeout_sec = config.http.max_idle_timeout_sec;
        let keep_alive_interval_sec = config.http.keep_alive_interval_sec;

        loop {
            sleep(Duration::from_millis(config.basic.update_gateway_info_ms)).await;

            let leader = match gateway_state.leader().await {
                Some(leader) => leader,
                None => {
                    warn!("No leader elected!");
                    continue;
                }
            };

            let last_update = match SystemTime::now().duration_since(UNIX_EPOCH) {
                Ok(duration) => duration.as_secs(),
                Err(e) => {
                    error!("SystemTime before UNIX EPOCH: {:?}", e);
                    0
                }
            };

            let info = GatewayInfo {
                node_id: config.network.node_id,
                domain: config.network.domain.clone(),
                ip: config.network.external_ip.clone(),
                name: config.network.name.clone(),
                http_port: config.http.port,
                available_tasks: task_queue.len(),
                last_task_acquisition: last_task_acquisition.load(Ordering::Relaxed),
                last_update,
            };

            if leader == config.network.node_id {
                if let Err(e) = gateway_state.set_gateway_info(info).await {
                    error!("set_gateway_info update error: {:?}", e);
                }
                last_leader = Some(leader);
                client = None;
                continue;
            }

            let leader_info = match gateway_state.gateway(leader).await {
                Ok(info) => info,
                Err(e) => {
                    warn!("Failed to get leader info: {:?}", e);
                    continue;
                }
            };

            if last_leader != Some(leader) || client.is_none() {
                client = None;

                let server_ip = format!("{}:{}", leader_info.ip, leader_info.http_port);
                match Http3ClientBuilder::new()
                    .server_domain(&leader_info.domain)
                    .server_ip(&server_ip)
                    .max_idle_timeout_sec(max_idle_timeout_sec)
                    .keep_alive_interval(keep_alive_interval_sec)
                    .dangerous_skip_verification(config.cert.dangerous_skip_verification)
                    .build()
                    .await
                {
                    Ok(c) => {
                        client = Some(c);
                        last_leader = Some(leader);
                    }
                    Err(e) => {
                        error!(
                            "Failed to create HTTP3 client: {:?} with params: {} {}",
                            e, &leader_info.domain, server_ip,
                        );
                        continue;
                    }
                }
            }

            let info_ext = GatewayInfoExt {
                node_id: info.node_id,
                domain: info.domain,
                ip: info.ip,
                name: info.name,
                http_port: info.http_port,
                available_tasks: info.available_tasks,
                cluster_name: config.raft.cluster_name.clone(),
                last_task_acquisition: info.last_task_acquisition,
                last_update: info.last_update,
            };

            let payload = match rmp_serde::to_vec(&info_ext) {
                Ok(p) => p,
                Err(e) => {
                    error!("Serialization error: {:?}", e);
                    continue;
                }
            };

            let url = format!(
                "https://{}:{}/write",
                leader_info.domain, leader_info.http_port
            );

            if let Some(client) = client.as_ref() {
                match client
                    .post(
                        &url,
                        Bytes::from(payload),
                        Some(Duration::from_secs(config.http.post_timeout_sec)),
                    )
                    .await
                {
                    Ok((status, response)) => {
                        if !status.is_success() {
                            error!(
                                "Gateway info update failed with status: {}, response: {:?}",
                                status,
                                String::from_utf8_lossy(&response)
                            );
                        }
                    }
                    Err(e) => {
                        error!("Gateway info update failed: {:?}", e);
                    }
                }
            }
        }
    }

    pub async fn gateway_generic_key_update(
        gateway_state: &GatewayState,
        current_node_id: u64,
        configured_key: Option<Uuid>,
    ) -> Result<()> {
        gateway_state
            .update_gateway_generic_key(current_node_id, configured_key, None)
            .await
    }

    pub async fn gateway_leader_change(gateway_state: GatewayState, current_node_id: u64) {
        let mut metrics_rx = gateway_state.metrics().await;
        let mut last_leader = None;
        loop {
            metrics_rx
                .changed()
                .await
                .expect("Failed to listen for metric changes");
            let current_leader = metrics_rx.borrow().current_leader;
            if current_leader != last_leader {
                if let Some(leader_id) = current_leader {
                    info!("Leader changed to node {}", leader_id);
                    if leader_id == current_node_id {
                        let _ = Gateway::gateway_generic_key_update(
                            &gateway_state,
                            current_node_id,
                            gateway_state.preconfigured_generic_key(),
                        )
                        .await;
                    }
                } else {
                    info!("Leadership changed, but no leader is elected yet");
                }
                last_leader = current_leader;
            }
        }
    }
}

impl Drop for Gateway {
    fn drop(&mut self) {
        self.http_server.abort();
        self.gateway_info_updater.abort();
        self.gateway_leader_change.abort();
        self.api_key_validator_updater.abort();
        self.server.abort();
        self.task_manager.abort();
    }
}

fn build_task_queue(cfg: &NodeConfig) -> DupQueue<Task> {
    DupQueue::<Task>::builder()
        .dup(cfg.basic.unique_validators_per_task)
        .ttl(cfg.basic.taskqueue_task_ttl)
        .cleanup_interval(cfg.basic.taskqueue_cleanup_interval)
        .build()
}

fn build_raft_config(cfg: &NodeConfig) -> Result<Arc<openraft::Config>> {
    Ok(Arc::new(
        openraft::Config {
            cluster_name: cfg.raft.cluster_name.clone(),
            heartbeat_interval: cfg.raft.heartbeat_interval,
            election_timeout_min: cfg.raft.election_timeout_min,
            election_timeout_max: cfg.raft.election_timeout_max,
            max_payload_entries: cfg.raft.max_payload_entries,
            replication_lag_threshold: cfg.raft.replication_lag_threshold,
            snapshot_max_chunk_size: cfg.raft.snapshot_max_chunk_size,
            max_in_snapshot_log_to_keep: cfg.raft.max_in_snapshot_log_to_keep,
            snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(
                cfg.raft.snapshot_logs_since_last,
            ),
            ..Default::default()
        }
        .validate()?,
    ))
}

async fn setup_remote_clients(
    cfg: &NodeConfig,
    node_ips: &[IpAddr],
    peer_dns_names: &[String],
    clients_map: Arc<scc::HashMap<String, RClient, RandomState>>,
) -> Result<()> {
    let create_client_futs = node_ips
        .iter()
        .zip(peer_dns_names.iter())
        .map(|(ip, dns_name)| {
            let endpoint = format!("{}:{}", ip, cfg.network.server_port);
            let clients_map = clients_map.clone();
            async move {
                let client = RClientBuilder::new()
                    .remote_addr(endpoint.clone())
                    .server_name(dns_name.clone())
                    .local_bind_addr(format!("{}:{}", cfg.network.bind_ip, 0))
                    .dangerous_skip_verification(cfg.cert.dangerous_skip_verification)
                    .max_idle_timeout_sec(cfg.rclient.max_idle_timeout_sec)
                    .keep_alive_interval(cfg.rclient.keep_alive_interval_sec)
                    .protocol_cfg(cfg.rserver.clone())
                    .build()
                    .await?;
                clients_map
                    .insert_async(endpoint, client)
                    .await
                    .map_err(|e| anyhow::anyhow!("{:?}", e))?;
                Ok::<(), anyhow::Error>(())
            }
        });

    try_join_all(create_client_futs).await?;
    Ok(())
}

async fn init_membership(
    mode: GatewayMode,
    node_id: u64,
    node_ids: &[u64],
    node_ips: &[IpAddr],
    cfg: &NodeConfig,
    raft: &Raft,
    server_addr: &str,
) -> Result<()> {
    match mode {
        GatewayMode::Bootstrap if node_id == 1 => {
            info!("Initializing the cluster with node {}", node_id);
            raft.initialize(BTreeMap::from([(
                1,
                BasicNode {
                    addr: server_addr.to_string(),
                },
            )]))
            .await?;

            let base_raft = raft.clone();
            let futs = node_ids.iter().zip(node_ips.iter()).map(|(id, ip)| {
                let raft = base_raft.clone();
                let endpoint = format!("{}:{}", ip, cfg.network.server_port);
                async move {
                    info!(
                        "Adding node {} as a learner with endpoint: {}",
                        id, endpoint
                    );
                    raft.add_learner(
                        *id,
                        BasicNode {
                            addr: endpoint.to_string(),
                        },
                        true,
                    )
                    .await?;
                    Ok::<(), anyhow::Error>(())
                }
            });
            try_join_all(futs).await?;
        }
        GatewayMode::Bootstrap => {
            info!(
                "Node {} started uninitialized; will be added by leader",
                node_id
            );
        }
        GatewayMode::Vote | GatewayMode::Single => {
            let mut members = BTreeMap::from([(
                node_id,
                BasicNode {
                    addr: format!("{}:{}", cfg.network.external_ip, cfg.network.server_port),
                },
            )]);
            if mode == GatewayMode::Vote {
                members.extend(node_ids.iter().zip(node_ips.iter()).map(|(&id, ip)| {
                    (
                        id,
                        BasicNode {
                            addr: format!("{}:{}", ip, cfg.network.server_port),
                        },
                    )
                }));
            }

            match raft.initialize(members).await {
                Ok(_) => info!(
                    "Node {} successfully initialized in {} mode",
                    node_id,
                    if mode == GatewayMode::Vote {
                        "vote"
                    } else {
                        "single"
                    }
                ),
                Err(e) => info!(
                    "Warning: Node {} not initialized (possibly already): {:?}",
                    node_id, e
                ),
            }
        }
    }

    Ok(())
}

pub async fn start_gateway(mode: GatewayMode, config: Arc<NodeConfig>) -> Result<Gateway> {
    init_crypto_provider()?;

    let task_queue = build_task_queue(&config);
    let raft_config = build_raft_config(&config)?;

    let log_store = LogStore::default();
    let state_machine_store = Arc::new(StateMachineStore::default());
    let clients_map = Arc::new(scc::HashMap::with_capacity_and_hasher(
        config.network.node_dns_names.len(),
        RandomState::default(),
    ));
    let raft = Raft::new(
        config.network.node_id,
        Arc::clone(&raft_config),
        Network::new(clients_map.clone()),
        log_store.clone(),
        Arc::clone(&state_machine_store),
    )
    .await?;
    let server_addr = format!("{}:{}", config.network.bind_ip, config.network.server_port);

    let use_cert_files =
        !config.cert.cert_file_path.is_empty() && !config.cert.key_file_path.is_empty();
    let cert_tuple = if use_cert_files {
        let cert = load_certificates(&config.cert.cert_file_path).await?;
        let key = load_private_key(&config.cert.key_file_path).await?;
        Some((cert, key))
    } else {
        None
    };

    info!(
        "Certificate files {}.",
        if use_cert_files {
            "have been provided and will be used"
        } else {
            "haven't been provided; self-signed certificates will be used instead"
        }
    );

    let server = RServer::new(
        &server_addr,
        cert_tuple,
        raft.clone(),
        config.rserver.clone(),
    )
    .await?;

    let last_task_acquisition = Arc::new(AtomicU64::from(
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
    ));

    let db = DatabaseBuilder::new()
        .host(&config.db.host)
        .port(config.db.port)
        .user(&config.db.user)
        .password(&config.db.password)
        .dbname(&config.db.db)
        .sslcert_path(&config.db.sslcert)
        .sslkey_path(&config.db.sslkey)
        .sslrootcert_path(&config.db.sslrootcert)
        .build()
        .await?;
    let api_key_validator = ApiKeyValidator::new(
        Arc::new(db),
        Duration::from_secs(config.db.api_keys_update_interval),
        config.db.keys_cache_ttl_sec,
        config.db.keys_cache_max_capacity,
    )?;
    let api_key_validator = Arc::new(api_key_validator);

    let api_key_validator_updater =
        tokio::spawn(ApiKeyValidator::run(Arc::clone(&api_key_validator)));
    let metrics = Metrics::new(0.05).map_err(|e| anyhow::anyhow!(e))?;

    let task_manager = TaskManager::new(
        config.basic.taskmanager_initial_capacity,
        config.basic.unique_validators_per_task,
        Duration::from_secs(config.basic.taskmanager_cleanup_interval),
        Duration::from_secs(config.basic.taskmanager_result_lifetime),
        metrics.clone(),
    )
    .await;

    let gateway_state = GatewayState::new(
        state_machine_store,
        raft.clone(),
        last_task_acquisition.clone(),
        api_key_validator,
        task_manager.clone(),
        Arc::clone(&config),
    );

    let key_cert = if use_cert_files {
        Keycert::new()
            .cert_from_path(&config.cert.cert_file_path)?
            .key_from_path(&config.cert.key_file_path)?
    } else {
        generate_and_create_keycert(vec!["localhost".to_string()])?
    };

    let http_server = match Http3Server::run(
        Arc::clone(&config),
        RustlsConfig::new(key_cert),
        gateway_state.clone(),
        task_queue.clone(),
        metrics.clone(),
    )
    .await
    {
        Ok(srv) => srv,
        Err(e) => bail!("Failed to start HTTP3 server: {:?}", e),
    };

    let peer_dns_names: Vec<_> = {
        let mut names = config
            .network
            .node_dns_names
            .iter()
            .filter(|&name| name != &config.network.domain)
            .cloned()
            .collect::<Vec<_>>();
        names.sort();
        names
    };

    let node_ips = if mode == GatewayMode::Single || peer_dns_names.is_empty() {
        vec![]
    } else {
        lookup_hosts_ips(&peer_dns_names).await?
    };

    let node_ids = get_node_ids(
        Duration::from_secs(config.http.get_timeout_sec),
        &node_ips,
        &peer_dns_names,
        Duration::from_secs(config.network.node_id_discovery_sleep),
        config.cert.dangerous_skip_verification,
        config.network.node_id_discovery_retries,
    )
    .await?;

    // If not in single-node mode, create remote client connections.
    if mode != GatewayMode::Single {
        setup_remote_clients(&config, &node_ips, &peer_dns_names, clients_map.clone()).await?;
    }

    let node_id = config.network.node_id;
    info!("Starting node {} with mode {:?}", node_id, mode);

    // Initialize raft membership based on the mode.
    init_membership(
        mode,
        node_id,
        &node_ids,
        &node_ips,
        &config,
        &raft,
        &server_addr,
    )
    .await?;

    let gateway_info_updater = tokio::spawn(Gateway::gateway_info_updater(
        Arc::clone(&config),
        gateway_state.clone(),
        task_queue.clone(),
        last_task_acquisition.clone(),
    ));
    let gateway_leader_change = tokio::spawn(Gateway::gateway_leader_change(
        gateway_state.clone(),
        node_id,
    ));

    Ok(Gateway {
        _config: config,
        _id: node_id,
        server,
        _gateway_state: gateway_state,
        gateway_info_updater,
        gateway_leader_change,
        api_key_validator_updater,
        task_manager,
        _log_store: log_store,
        http_server,
        _task_queue: task_queue,
    })
}
