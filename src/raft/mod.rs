pub mod archive;
pub mod client;
pub mod gateway_state;
pub mod memstore;
pub mod network;
pub mod server;
pub mod store;
mod tests;

#[cfg(test)]
pub(crate) mod test_utils;

use anyhow::bail;
use anyhow::Result;
use backon::BackoffBuilder;
use backon::ConstantBuilder;
use backon::Retryable;
use bytes::Bytes;
use foldhash::fast::RandomState;
use futures_util::future::try_join_all;
use gateway_state::{GatewayState, GatewayStateInit};
use http::StatusCode;
use network::Network;
use openraft::BasicNode;
use salvo::conn::rustls::Keycert;
use salvo::conn::rustls::RustlsConfig;
use server::RServer;
use std::collections::BTreeMap;
use std::io::Cursor;
use std::net::IpAddr;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;
use tokio::task::{JoinError, JoinHandle};
use tokio::time::sleep;
use tracing::error;
use tracing::info;
use tracing::warn;
use uuid::Uuid;

use crate::api::request::GatewayInfoExtRef;
use crate::api::response::GatewayInfoRef;
use crate::api::Task;
use crate::common::cert::generate_and_create_keycert;
use crate::common::cert::load_certificates;
use crate::common::cert::load_private_key;
use crate::common::company_usage::CompanyUsageRecorder;
use crate::common::crypto_provider::init_crypto_provider;
use crate::common::queue::DupQueue;
use crate::common::resolve::lookup_hosts_ips;
use crate::config::NodeConfig;
use crate::db::ApiKeyValidator;
use crate::db::DatabaseBuilder;
use crate::http3::client::Http3Client;
use crate::http3::client::Http3ClientBuilder;
use crate::http3::server::Http3Server;
use crate::metrics::Metrics;
use crate::raft::client::RClient;
use crate::raft::client::RClientBuilder;
use crate::raft::store::RateLimitDelta;
use crate::raft::store::Request;
use crate::raft::store::Response;
use crate::task::TaskManager;
use scc::Queue;
use tokio_util::sync::CancellationToken;

pub type NodeId = u64;
pub type LogStore = store::LogStore;
pub type StateMachineStore = store::StateMachineStore;
pub type Raft = openraft::Raft<TypeConfig>;

pub(crate) const SNAPSHOT_COMPRESSION_LVL: i32 = 1;
const MAX_DELTAS_IN_BATCH: usize = 8192;

#[derive(Debug, PartialEq, Clone, Copy)]
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
    server: RServer,
    gateway_info_updater: JoinHandle<()>,
    gateway_leader_change: JoinHandle<()>,
    api_key_validator_updater: JoinHandle<()>,
    task_manager: TaskManager,
    http_server: Http3Server,
    shutdown: CancellationToken,
}

pub enum GatewayExit {
    Shutdown,
    TaskStopped {
        task: &'static str,
        result: Result<(), JoinError>,
    },
}

impl Gateway {
    async fn flush_rate_limit_deltas(
        gateway_state: &GatewayState,
        deltas: Arc<Vec<RateLimitDelta>>,
        client: Option<&Http3Client>,
    ) {
        if deltas.is_empty() {
            return;
        }
        let _ = gateway_state.submit_rate_limit_deltas(deltas, client).await;
    }

    pub async fn gateway_info_updater(
        config: Arc<NodeConfig>,
        gateway_state: GatewayState,
        task_queue: DupQueue<Task>,
        last_task_acquisition: Arc<AtomicU64>,
        rate_limit_queue: Arc<Queue<RateLimitDelta>>,
        shutdown: CancellationToken,
    ) {
        let mut last_leader: Option<u64> = None;
        let mut client: Option<Http3Client> = None;
        let post_timeout = Duration::from_secs(config.http.post_timeout_sec);
        let update_interval = Duration::from_millis(config.basic.update_gateway_info_ms);

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = sleep(update_interval) => {}
            }

            let leader = match gateway_state.leader().await {
                Some(leader) => leader,
                None => {
                    warn!("No leader elected!");
                    continue;
                }
            };

            // Drain any pending rate limit deltas
            let mut deltas = Vec::new();
            while let Some(entry) = rate_limit_queue.pop() {
                deltas.push(**entry);
                if deltas.len() == MAX_DELTAS_IN_BATCH {
                    break;
                }
            }

            let last_update = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|duration| duration.as_secs())
                .unwrap_or_else(|e| {
                    error!("SystemTime before UNIX EPOCH: {:?}", e);
                    0
                });
            let available_tasks = task_queue.len();
            let last_task_acquisition = last_task_acquisition.load(Ordering::Relaxed);

            if leader == config.network.node_id {
                let _ = tokio::join!(
                    gateway_state.set_gateway_info(GatewayInfoRef {
                        node_id: config.network.node_id,
                        domain: &config.network.domain,
                        ip: &config.network.external_ip,
                        name: &config.network.name,
                        http_port: config.http.port,
                        available_tasks,
                        last_task_acquisition,
                        last_update,
                    }),
                    Gateway::flush_rate_limit_deltas(&gateway_state, Arc::new(deltas), None)
                );
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

                let server_ip = gateway_state.leader_server_addr(&leader_info);
                match gateway_state.build_leader_client(&leader_info).await {
                    Ok(new_client) => {
                        client = Some(new_client);
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

            let info_ext = GatewayInfoExtRef {
                node_id: config.network.node_id,
                domain: &config.network.domain,
                ip: &config.network.external_ip,
                name: &config.network.name,
                http_port: config.http.port,
                available_tasks,
                cluster_name: &config.raft.cluster_name,
                last_task_acquisition,
                last_update,
            };

            let payload = match rmp_serde::to_vec(&info_ext) {
                Ok(p) => p,
                Err(e) => {
                    error!("Serialization error: {:?}", e);
                    continue;
                }
            };

            let url = gateway_state.leader_write_url(&leader_info);

            if let Some(client) = client.as_ref() {
                let admin_key = gateway_state.admin_key();
                let headers = [("x-admin-key", admin_key.as_str())];
                let (info_res, _) = tokio::join!(
                    client.post(
                        &url,
                        Bytes::from(payload),
                        Some(&headers),
                        Some(post_timeout)
                    ),
                    Gateway::flush_rate_limit_deltas(
                        &gateway_state,
                        Arc::new(deltas),
                        Some(client)
                    )
                );
                match info_res {
                    Ok((status, response)) if !status.is_success() => {
                        error!(
                            "Gateway info update failed with status: {}, response: {:?}",
                            status,
                            String::from_utf8_lossy(&response)
                        );
                    }
                    Ok(_) => {}
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

    pub async fn gateway_leader_change(
        gateway_state: GatewayState,
        current_node_id: u64,
        shutdown: CancellationToken,
    ) {
        let mut metrics_rx = gateway_state.metrics().await;
        let mut last_leader = None;
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                res = metrics_rx.changed() => {
                    if res.is_err() {
                        error!("Failed to listen for metric changes");
                        break;
                    }
                }
            }
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

    pub async fn wait_for_exit(mut self) -> GatewayExit {
        let Gateway {
            server,
            gateway_info_updater,
            gateway_leader_change,
            api_key_validator_updater,
            http_server,
            shutdown,
            ..
        } = &mut self;

        tokio::select! {
            _ = shutdown.cancelled() => GatewayExit::Shutdown,
            res = server.wait() => GatewayExit::TaskStopped {
                task: "rserver_accept",
                result: res,
            },
            res = http_server.wait() => GatewayExit::TaskStopped {
                task: "http3_server",
                result: res,
            },
            res = gateway_info_updater => GatewayExit::TaskStopped {
                task: "gateway_info_updater",
                result: res,
            },
            res = gateway_leader_change => GatewayExit::TaskStopped {
                task: "gateway_leader_change",
                result: res,
            },
            res = api_key_validator_updater => GatewayExit::TaskStopped {
                task: "api_key_validator_updater",
                result: res,
            },
        }
    }

    #[allow(dead_code)]
    pub fn shutdown(&self) {
        self.shutdown.cancel();
        self.task_manager.abort();

        self.server.abort();
        self.gateway_info_updater.abort();
        self.gateway_leader_change.abort();
        self.api_key_validator_updater.abort();
        self.http_server.abort();
    }
}

impl Drop for Gateway {
    fn drop(&mut self) {
        self.shutdown.cancel();
        self.http_server.abort();
        self.gateway_info_updater.abort();
        self.gateway_leader_change.abort();
        self.api_key_validator_updater.abort();
        self.server.abort();
        self.task_manager.abort();
    }
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

pub async fn start_gateway(
    mode: GatewayMode,
    config: Arc<NodeConfig>,
    shutdown: CancellationToken,
) -> Result<Gateway> {
    init_crypto_provider()?;

    let clients_map = Arc::new(scc::HashMap::with_capacity_and_hasher(
        config.network.node_dns_names.len(),
        RandomState::default(),
    ));

    let task_queue = build_task_queue(&config);
    let raft_config = build_raft_config(&config)?;
    let gateway_shutdown = shutdown.child_token();

    let snapshot_dir = PathBuf::from(&config.raft.snapshot_dir);
    let log_store_path = snapshot_dir.join("log_store.bin");
    let log_store = LogStore::with_persistence_and_thresholds(
        &log_store_path,
        config.raft.compaction_threshold_bytes,
        config.raft.compaction_ops,
    )?;
    let state_machine_store = Arc::new(StateMachineStore::with_persistence(
        &snapshot_dir,
        config.raft.max_snapshots_to_keep,
        Some(log_store_path.clone()),
    )?);
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
        gateway_shutdown.clone(),
    )
    .await?;

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
        Vec::new()
    } else {
        lookup_hosts_ips(&peer_dns_names).await?
    };

    if mode != GatewayMode::Single && !node_ips.is_empty() {
        setup_remote_clients(
            config.as_ref(),
            &node_ips,
            &peer_dns_names,
            clients_map.clone(),
        )
        .await?;
    }

    let last_task_acquisition = Arc::new(AtomicU64::from(
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
    ));

    let db = Arc::new(DatabaseBuilder::from_config(&config.db).build().await?);
    let api_key_validator = ApiKeyValidator::new(
        Arc::clone(&db),
        Duration::from_secs(config.db.api_keys_update_interval),
        config.db.keys_cache_ttl_sec,
        config.db.keys_cache_initial_capacity,
        config.db.keys_cache_max_capacity,
        &config.http.api_key_secret,
        config.db.deleted_keys_ttl_minutes,
    )?;
    let api_key_validator = Arc::new(api_key_validator);
    let flush_interval = config.db.company_usage_flush_interval_sec.max(1);
    let company_usage_recorder = CompanyUsageRecorder::new(
        Arc::clone(&db),
        Duration::from_secs(flush_interval),
        gateway_shutdown.clone(),
    );

    let api_key_validator_updater = tokio::spawn(ApiKeyValidator::run(
        Arc::clone(&api_key_validator),
        gateway_shutdown.clone(),
    ));
    let metrics = Metrics::new(0.05).map_err(|e| anyhow::anyhow!(e))?;

    let task_manager = TaskManager::new(
        config.basic.taskmanager_initial_capacity,
        config.basic.unique_validators_per_task,
        Duration::from_secs(config.basic.taskmanager_cleanup_interval),
        Duration::from_secs(config.basic.taskmanager_result_lifetime),
        metrics.clone(),
    )
    .await;

    let rate_limit_queue = Arc::new(Queue::<RateLimitDelta>::default());

    let gateway_state = GatewayState::new(GatewayStateInit {
        state: state_machine_store,
        raft: raft.clone(),
        last_task_acquisition: last_task_acquisition.clone(),
        key_validator_updater: api_key_validator,
        task_manager: task_manager.clone(),
        config: Arc::clone(&config),
        rate_limit_queue: rate_limit_queue.clone(),
        usage_recorder: company_usage_recorder.clone(),
    });

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
        gateway_shutdown.clone(),
    )
    .await
    {
        Ok(srv) => srv,
        Err(e) => bail!("Failed to start HTTP3 server: {:?}", e),
    };

    let node_ids = if node_ips.is_empty() {
        Vec::new()
    } else {
        get_node_ids(
            Duration::from_secs(config.http.get_timeout_sec),
            &node_ips,
            &peer_dns_names,
            Duration::from_secs(config.network.node_id_discovery_sleep),
            config.cert.dangerous_skip_verification,
            config.network.node_id_discovery_retries,
        )
        .await?
    };

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
        task_queue,
        last_task_acquisition,
        rate_limit_queue,
        gateway_shutdown.clone(),
    ));
    let gateway_leader_change = tokio::spawn(Gateway::gateway_leader_change(
        gateway_state,
        node_id,
        gateway_shutdown.clone(),
    ));

    Ok(Gateway {
        server,
        gateway_info_updater,
        gateway_leader_change,
        api_key_validator_updater,
        task_manager,
        http_server,
        shutdown: gateway_shutdown,
    })
}
