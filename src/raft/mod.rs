pub mod archive;
pub mod client;
pub mod gateway_state;
pub mod memstore;
pub mod network;
pub mod rate_limit;
pub mod server;
pub mod store;
mod tests;

#[cfg(test)]
pub(crate) mod test_utils;

use crate::api::request::GatewayInfoExtRef;
use crate::api::response::GatewayInfoRef;
use crate::common::cert::generate_and_create_keycert;
use crate::common::cert::load_certificates;
use crate::common::cert::load_private_key;
use crate::common::queue::TaskQueue;
use crate::common::rate_limit_buffer::RateLimitMutationBuffer;
use crate::common::resolve::lookup_one_ip_per_host;
use crate::config::{NodeConfig, TransportMode};
use crate::config_runtime::RuntimeConfigStore;
use crate::crypto::crypto_provider::init_crypto_provider;
use crate::db::{
    ApiKeyValidator, ApiKeyValidatorConfig, DatabaseBuilder, EventRecorder, EventSinkHandle,
    GatewayRuntimeSettingsStore, TaskLifecycleStoreHandle, api_key_sync_interval,
};
use crate::http3::client::Http3Client;
use crate::http3::client::Http3ClientBuilder;
use crate::http3::server::Http3Server;
use crate::metrics::Metrics;
use crate::raft::client::RClient;
use crate::raft::client::RClientBuilder;
use crate::raft::store::RateLimitMutationBatch;
use crate::raft::store::Request;
use crate::raft::store::Response;
use crate::task::{TaskManager, TaskManagerInit};
use anyhow::Result;
use anyhow::bail;
use backon::BackoffBuilder;
use backon::ConstantBuilder;
use backon::Retryable;
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
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;
use tokio::task::{JoinError, JoinHandle};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::error;
use tracing::info;
use tracing::warn;

#[cfg(feature = "test-support")]
use crate::db::ApiKeyLookup;

pub type NodeId = u64;
pub type LogStore = store::LogStore;
pub type StateMachineStore = store::StateMachineStore;
pub type Raft = openraft::Raft<TypeConfig>;

pub(crate) const SNAPSHOT_COMPRESSION_LVL: i32 = 1;

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
    api_key_cache_updater: JoinHandle<()>,
    gateway_settings_updater: JoinHandle<()>,
    task_lifecycle_reconciler: JoinHandle<()>,
    _task_queue: TaskQueue,
    task_manager: TaskManager,
    _api_key_validator: Arc<ApiKeyValidator>,
    _event_recorder: EventRecorder,
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
    async fn collect_pending_rate_limit_mutations(
        rate_limit_queue: &RateLimitMutationBuffer,
        max_mutations_in_batch: usize,
        pending_batch: &mut Option<RateLimitMutationBatch>,
    ) {
        if pending_batch.is_some() {
            return;
        }
        *pending_batch = rate_limit_queue.drain_batch(max_mutations_in_batch).await;
    }

    async fn flush_pending_rate_limit_mutations(
        gateway_state: &GatewayState,
        pending_batch: &mut Option<RateLimitMutationBatch>,
        client: Option<&Http3Client>,
    ) {
        let Some(batch) = pending_batch.as_ref() else {
            return;
        };

        match gateway_state
            .submit_rate_limit_mutation_batch(batch, client)
            .await
        {
            Ok(_) => {
                *pending_batch = None;
            }
            Err(e) => {
                warn!(
                    "Rate limit mutation flush failed, retaining batch for retry (request_id: {}): {}",
                    batch.request_id, e
                );
            }
        }
    }

    pub async fn gateway_info_updater(
        config: Arc<RuntimeConfigStore>,
        gateway_state: GatewayState,
        task_queue: TaskQueue,
        last_task_acquisition: Arc<AtomicU64>,
        rate_limit_queue: RateLimitMutationBuffer,
        shutdown: CancellationToken,
    ) {
        let mut last_leader: Option<u64> = None;
        let mut client: Option<Http3Client> = None;
        let mut pending_batch: Option<RateLimitMutationBatch> = None;

        loop {
            let cfg = config.snapshot();
            let node_cfg = cfg.node();
            let update_interval = Duration::from_millis(node_cfg.basic.update_gateway_info_ms);
            let max_mutations_in_batch = node_cfg.basic.max_rate_limit_deltas_per_batch.max(1);

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

            Gateway::collect_pending_rate_limit_mutations(
                &rate_limit_queue,
                max_mutations_in_batch,
                &mut pending_batch,
            )
            .await;

            let last_update = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|duration| duration.as_secs())
                .unwrap_or_else(|e| {
                    error!("SystemTime before UNIX EPOCH: {:?}", e);
                    0
                });
            let available_tasks = task_queue.len();
            let last_task_acquisition = last_task_acquisition.load(Ordering::Relaxed);

            if leader == node_cfg.network.node_id {
                if let Err(e) = gateway_state
                    .set_gateway_info(GatewayInfoRef {
                        node_id: node_cfg.network.node_id,
                        domain: &node_cfg.network.domain,
                        ip: &node_cfg.network.external_ip,
                        name: &node_cfg.network.name,
                        http_port: node_cfg.http.port,
                        available_tasks,
                        last_task_acquisition,
                        last_update,
                    })
                    .await
                {
                    error!("Gateway info update failed (leader): {:?}", e);
                }
                Gateway::flush_pending_rate_limit_mutations(
                    &gateway_state,
                    &mut pending_batch,
                    None,
                )
                .await;
                Gateway::collect_pending_rate_limit_mutations(
                    &rate_limit_queue,
                    max_mutations_in_batch,
                    &mut pending_batch,
                )
                .await;
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
                node_id: node_cfg.network.node_id,
                domain: &node_cfg.network.domain,
                ip: &node_cfg.network.external_ip,
                name: &node_cfg.network.name,
                http_port: node_cfg.http.port,
                available_tasks,
                cluster_name: &node_cfg.raft.cluster_name,
                last_task_acquisition,
                last_update,
            };

            if let Some(client) = client.as_ref() {
                if let Err(e) = gateway_state
                    .submit_gateway_info_ext(info_ext, Some(client))
                    .await
                {
                    error!("Gateway info update failed: {:?}", e);
                }
                Gateway::flush_pending_rate_limit_mutations(
                    &gateway_state,
                    &mut pending_batch,
                    Some(client),
                )
                .await;
                Gateway::collect_pending_rate_limit_mutations(
                    &rate_limit_queue,
                    max_mutations_in_batch,
                    &mut pending_batch,
                )
                .await;
            }
        }
    }

    pub async fn gateway_leader_change(
        gateway_state: GatewayState,
        _current_node_id: u64,
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
                } else {
                    info!("Leadership changed, but no leader is elected yet");
                }
                last_leader = current_leader;
            }
        }
    }

    pub async fn task_lifecycle_reconciler(
        gateway_state: GatewayState,
        interval: Duration,
        batch_size: i32,
        terminal_task_retention: Duration,
        shutdown: CancellationToken,
    ) {
        let mut ticker = tokio::time::interval(interval);
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = ticker.tick() => {}
            }

            let now_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|duration| duration.as_millis() as i64)
                .unwrap_or_else(|err| {
                    error!(
                        "SystemTime before UNIX EPOCH during task lifecycle reconciliation: {:?}",
                        err
                    );
                    0
                });

            if gateway_state.leader().await != Some(gateway_state.current_node_id()) {
                continue;
            }

            let retention_ms = terminal_task_retention
                .as_millis()
                .clamp(1, i64::MAX as u128) as i64;
            let completed_before_ms = now_ms.saturating_sub(retention_ms);
            let reconcile_limit = batch_size.max(1);

            match gateway_state
                .expire_generation_tasks(reconcile_limit, now_ms)
                .await
            {
                Ok(expired_task_ids) => {
                    if !expired_task_ids.is_empty() {
                        info!(
                            "Expired {} overdue generation tasks in the billing database",
                            expired_task_ids.len()
                        );
                    }
                }
                Err(err) => {
                    error!(
                        "Failed to reconcile overdue generation tasks in the billing database: {:?}",
                        err
                    );
                }
            }

            match gateway_state
                .purge_terminal_generation_tasks(reconcile_limit, completed_before_ms)
                .await
            {
                Ok(purged_task_ids) => {
                    if !purged_task_ids.is_empty() {
                        info!(
                            "Purged {} terminal generation tasks from the billing database",
                            purged_task_ids.len()
                        );
                    }
                }
                Err(err) => {
                    error!(
                        "Failed to purge terminal generation tasks from the billing database: {:?}",
                        err
                    );
                }
            }
        }
    }

    pub async fn wait_for_exit(mut self) -> GatewayExit {
        let Gateway {
            server,
            gateway_info_updater,
            gateway_leader_change,
            api_key_cache_updater,
            gateway_settings_updater,
            task_lifecycle_reconciler,
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
            res = api_key_cache_updater => GatewayExit::TaskStopped {
                task: "api_key_cache_updater",
                result: res,
            },
            res = gateway_settings_updater => GatewayExit::TaskStopped {
                task: "gateway_settings_updater",
                result: res,
            },
            res = task_lifecycle_reconciler => GatewayExit::TaskStopped {
                task: "task_lifecycle_reconciler",
                result: res,
            },
        }
    }

    #[cfg(feature = "test-support")]
    pub fn task_queue(&self) -> TaskQueue {
        self._task_queue.clone()
    }

    #[cfg(feature = "test-support")]
    pub fn task_manager(&self) -> TaskManager {
        self.task_manager.clone()
    }

    #[cfg(feature = "test-support")]
    pub async fn sync_db_caches_for_test(&self) -> Result<()> {
        self._api_key_validator.sync_db_caches_for_test().await
    }

    #[cfg(feature = "test-support")]
    pub async fn lookup_api_key_for_test(&self, api_key: &str) -> ApiKeyLookup {
        self._api_key_validator.lookup(api_key).await
    }

    #[cfg(feature = "test-support")]
    pub async fn flush_events_for_test(&self) {
        self._event_recorder.flush_once_for_test().await;
    }
}

impl Drop for Gateway {
    fn drop(&mut self) {
        self.shutdown.cancel();
        self.http_server.abort();
        self.gateway_info_updater.abort();
        self.gateway_leader_change.abort();
        self.api_key_cache_updater.abort();
        self.gateway_settings_updater.abort();
        self.task_lifecycle_reconciler.abort();
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
            && let Ok((status, body)) = client.get(&url, Some(timeout)).await
            && status == StatusCode::OK
        {
            let body_str = std::str::from_utf8(&body)
                .map_err(|e| anyhow::anyhow!("Failed to convert response body to UTF-8: {}", e))?;
            let id = body_str
                .trim()
                .parse::<u64>()
                .map_err(|e| anyhow::anyhow!("Failed to parse ID as u64: {}", e))?;
            return Ok(id);
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

fn build_task_queue(cfg: &NodeConfig) -> TaskQueue {
    TaskQueue::builder()
        .dup(cfg.basic.unique_workers_per_task)
        .ttl(cfg.basic.taskqueue_task_ttl)
        .cleanup_interval(cfg.basic.taskqueue_cleanup_interval)
        .default_model(cfg.model_config.default_model.clone())
        .models(cfg.model_config.models.keys().cloned())
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
    config: Arc<RuntimeConfigStore>,
    shutdown: CancellationToken,
) -> Result<Gateway> {
    init_crypto_provider()?;

    let cfg_view = config.snapshot();
    let cfg = cfg_view.node();

    if cfg.http.transport == TransportMode::Plain && mode != GatewayMode::Single {
        bail!("Plain HTTP transport is currently only supported in single-node mode");
    }

    let clients_map = Arc::new(scc::HashMap::with_capacity_and_hasher(
        cfg.network.node_dns_names.len(),
        RandomState::default(),
    ));

    let task_queue = build_task_queue(cfg);
    let raft_config = build_raft_config(cfg)?;
    let gateway_shutdown = shutdown.child_token();

    let snapshot_dir = PathBuf::from(&cfg.raft.snapshot_dir);
    let log_store_path = snapshot_dir.join("log_store.bin");
    let log_store = LogStore::with_persistence_and_thresholds(
        &log_store_path,
        cfg.raft.compaction_threshold_bytes,
        cfg.raft.compaction_ops,
        cfg.raft.log_store_flush_interval_ms,
    )?;
    let state_machine_store = Arc::new(StateMachineStore::with_persistence(
        &snapshot_dir,
        cfg.raft.max_snapshots_to_keep,
        Some(log_store_path.clone()),
    )?);
    let raft = Raft::new(
        cfg.network.node_id,
        Arc::clone(&raft_config),
        Network::new(clients_map.clone()),
        log_store.clone(),
        Arc::clone(&state_machine_store),
    )
    .await?;
    let server_addr = format!("{}:{}", cfg.network.bind_ip, cfg.network.server_port);

    let use_cert_files = !cfg.cert.cert_file_path.is_empty() && !cfg.cert.key_file_path.is_empty();
    let cert_tuple = if use_cert_files {
        let cert = load_certificates(&cfg.cert.cert_file_path).await?;
        let key = load_private_key(&cfg.cert.key_file_path).await?;
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
        cfg.rserver.clone(),
        gateway_shutdown.clone(),
    )
    .await?;

    let peer_dns_names: Vec<_> = {
        let mut names = cfg
            .network
            .node_dns_names
            .iter()
            .filter(|&name| name != &cfg.network.domain)
            .cloned()
            .collect::<Vec<_>>();
        names.sort();
        names
    };

    let node_ips = if mode == GatewayMode::Single || peer_dns_names.is_empty() {
        Vec::new()
    } else {
        lookup_one_ip_per_host(&peer_dns_names).await?
    };

    if mode != GatewayMode::Single && !node_ips.is_empty() {
        setup_remote_clients(cfg, &node_ips, &peer_dns_names, clients_map.clone()).await?;
    }

    let last_task_acquisition = Arc::new(AtomicU64::from(
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
    ));

    info!("Initializing PostgreSQL gateway state");
    let db = Arc::new(
        DatabaseBuilder::from_config(&cfg.db)
            .shutdown_token(gateway_shutdown.clone())
            .build()
            .await?,
    );
    info!("PostgreSQL gateway state initialized");
    let api_key_validator = ApiKeyValidator::new(
        Arc::clone(&db),
        ApiKeyValidatorConfig {
            update_interval: api_key_sync_interval(cfg.db.api_keys_update_interval),
            cache_ttl_sec: cfg.db.keys_cache_ttl_sec,
            cache_initial_capacity: cfg.db.keys_cache_initial_capacity,
            cache_max_capacity: cfg.db.keys_cache_max_capacity,
            api_key_secret: &cfg.http.api_key_secret,
            negative_cache_ttl_sec: cfg.http.invalid_api_key_negative_cache_ttl_sec,
            unknown_key_ip_miss_ttl_sec: cfg.http.invalid_api_key_ip_miss_ttl_sec,
            unknown_key_ip_cooldown_ttl_sec: cfg.http.invalid_api_key_ip_cooldown_ttl_sec,
            unknown_key_ip_cache_capacity: cfg.http.invalid_api_key_ip_cache_capacity,
            unknown_key_ip_miss_limit: cfg.http.invalid_api_key_ip_miss_limit,
            deleted_keys_ttl_minutes: cfg.db.deleted_keys_ttl_minutes,
            fallback_generic_key: cfg.http.generic_key,
        },
    )?;
    let api_key_validator = Arc::new(api_key_validator);
    let events_flush_interval = cfg.db.events_flush_interval_sec.max(1);
    let events_queue_capacity = cfg.db.events_queue_capacity.max(1);
    let event_sink = Arc::new(EventSinkHandle::Database(Arc::clone(&db)));
    let event_recorder = EventRecorder::new(
        event_sink,
        Arc::from(cfg.network.name.as_str()),
        Duration::from_secs(events_flush_interval),
        events_queue_capacity,
        gateway_shutdown.clone(),
    );

    let api_key_cache_updater = tokio::spawn(ApiKeyValidator::run(
        Arc::clone(&api_key_validator),
        gateway_shutdown.clone(),
    ));
    let gateway_settings_updater = tokio::spawn(GatewayRuntimeSettingsStore::run(
        api_key_validator.gateway_settings(),
        gateway_shutdown.clone(),
    ));
    let metrics = Metrics::new(0.05).map_err(|e| anyhow::anyhow!(e))?;

    let rate_limit_queue = RateLimitMutationBuffer::default();
    let task_manager = TaskManager::new_with_rate_limit_mutation_queue(TaskManagerInit {
        initial_capacity: cfg.basic.taskmanager_initial_capacity,
        expected_results: cfg.basic.unique_workers_per_task,
        cleanup_interval: Duration::from_secs(cfg.basic.taskmanager_cleanup_interval),
        result_lifetime: Duration::from_secs(cfg.basic.taskmanager_result_lifetime),
        rate_limit_mutation_queue: rate_limit_queue.clone(),
        metrics: metrics.clone(),
        worker_event_recorder: Some(event_recorder.clone()),
    })
    .await;

    let gateway_state = GatewayState::new(GatewayStateInit {
        state: state_machine_store,
        raft: raft.clone(),
        last_task_acquisition: last_task_acquisition.clone(),
        task_lifecycle_store: TaskLifecycleStoreHandle::Database(Arc::clone(&db)),
        api_key_validator: Arc::clone(&api_key_validator),
        task_manager: task_manager.clone(),
        config: Arc::clone(&config),
        event_recorder: event_recorder.clone(),
    });

    let http_tls_config = if cfg.http.transport == TransportMode::Tls {
        let key_cert = if use_cert_files {
            Keycert::new()
                .cert_from_path(&cfg.cert.cert_file_path)?
                .key_from_path(&cfg.cert.key_file_path)?
        } else {
            generate_and_create_keycert(vec!["localhost".to_string()])?
        };
        Some(RustlsConfig::new(key_cert))
    } else {
        None
    };

    let http_server = match Http3Server::run(
        Arc::clone(&config),
        http_tls_config,
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
            Duration::from_secs(cfg.http.get_timeout_sec),
            &node_ips,
            &peer_dns_names,
            Duration::from_secs(cfg.network.node_id_discovery_sleep),
            cfg.cert.dangerous_skip_verification,
            cfg.network.node_id_discovery_retries,
        )
        .await?
    };

    let node_id = cfg.network.node_id;
    info!("Starting node {} with mode {:?}", node_id, mode);

    // Initialize raft membership based on the mode.
    init_membership(
        mode,
        node_id,
        &node_ids,
        &node_ips,
        cfg,
        &raft,
        &server_addr,
    )
    .await?;

    let gateway_info_updater = tokio::spawn(Gateway::gateway_info_updater(
        Arc::clone(&config),
        gateway_state.clone(),
        task_queue.clone(),
        last_task_acquisition,
        rate_limit_queue,
        gateway_shutdown.clone(),
    ));
    let gateway_leader_change = tokio::spawn(Gateway::gateway_leader_change(
        gateway_state.clone(),
        node_id,
        gateway_shutdown.clone(),
    ));
    let task_lifecycle_reconciler = tokio::spawn(Gateway::task_lifecycle_reconciler(
        gateway_state.clone(),
        Duration::from_secs(cfg.basic.taskmanager_cleanup_interval.max(1)),
        cfg.basic.taskmanager_initial_capacity.clamp(1, 512) as i32,
        Duration::from_secs(cfg.basic.generation_task_retention_sec.max(1)),
        gateway_shutdown.clone(),
    ));

    Ok(Gateway {
        server,
        gateway_info_updater,
        gateway_leader_change,
        api_key_cache_updater,
        gateway_settings_updater,
        task_lifecycle_reconciler,
        _task_queue: task_queue,
        task_manager,
        _api_key_validator: api_key_validator,
        _event_recorder: event_recorder,
        http_server,
        shutdown: gateway_shutdown,
    })
}
