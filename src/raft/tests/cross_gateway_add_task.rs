use super::*;
use crate::api::request::GatewayInfoExtRef;
use crate::api::response::GatewayInfo;
use crate::common::cert::generate_and_create_keycert;
use crate::common::queue::TaskQueue;
use crate::common::rate_limit_buffer::RateLimitMutationBuffer;
use crate::config::NodeConfig;
use crate::config_runtime::RuntimeConfigStore;
use crate::db::{
    ApiKeyValidator, ApiKeyValidatorConfig, CreateGenerationTaskInput, CreateGenerationTaskOutcome,
    EventRecorder, EventSinkHandle, FinalizeGenerationTaskAssignmentInput,
    FinalizeGenerationTaskAssignmentOutcome, GenerationTaskAccessOwner,
    GenerationTaskStatusSnapshot, RecordedGenerationTaskAssignment,
    RecordedGenerationTaskAssignmentAction, TaskLifecycleStore, TaskLifecycleStoreHandle,
    api_key_sync_interval,
};
use crate::http3::client::{Http3Client, Http3ClientBuilder};
use crate::http3::server::Http3Server;
use crate::metrics::Metrics;
use crate::raft::gateway_state::{GatewayState, GatewayStateInit};
use crate::raft::rate_limit::DistributedRateLimiter;
use crate::raft::store::Subject;
use crate::raft::test_utils::ensure_crypto_provider_for_tests;
use crate::task::{TaskManager, TaskManagerInit};
use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use http::StatusCode;
use salvo::conn::rustls::RustlsConfig;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;
use tempfile::NamedTempFile;
use tokio::sync::Notify;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const LOCALHOST: &str = "127.0.0.1";
const TEST_DOMAIN: &str = "localhost";
const PROMPT_JSON: &str = r#"{"prompt":"mechanic robot"}"#;
const GENERIC_WINDOW_MS: u64 = 86_400_000;

#[derive(Default)]
struct BlockingCreateStore {
    create_calls: AtomicUsize,
    started: Notify,
    release_first: Notify,
}

impl BlockingCreateStore {
    async fn wait_for_first_call(&self) {
        while self.create_calls.load(Ordering::SeqCst) == 0 {
            self.started.notified().await;
        }
    }

    fn create_calls(&self) -> usize {
        self.create_calls.load(Ordering::SeqCst)
    }

    fn release_first_call(&self) {
        self.release_first.notify_waiters();
    }
}

#[async_trait]
impl TaskLifecycleStore for BlockingCreateStore {
    async fn create_generation_task(
        &self,
        _input: &CreateGenerationTaskInput,
    ) -> Result<CreateGenerationTaskOutcome> {
        let call_index = self.create_calls.fetch_add(1, Ordering::SeqCst);
        self.started.notify_waiters();
        if call_index == 0 {
            self.release_first.notified().await;
        }
        Ok(CreateGenerationTaskOutcome::Created)
    }

    async fn record_generation_task_assignments(
        &self,
        task_ids: &[Uuid],
        _worker_hotkey: &str,
        _worker_id: &str,
        _assigned_at_ms: i64,
    ) -> Result<Vec<RecordedGenerationTaskAssignment>> {
        Ok(task_ids
            .iter()
            .copied()
            .map(|task_id| RecordedGenerationTaskAssignment {
                task_id,
                assignment_token: Some(Uuid::new_v4()),
                action: RecordedGenerationTaskAssignmentAction::Assigned,
            })
            .collect())
    }

    async fn finalize_generation_task_assignment(
        &self,
        _input: &FinalizeGenerationTaskAssignmentInput,
    ) -> Result<FinalizeGenerationTaskAssignmentOutcome> {
        Ok(FinalizeGenerationTaskAssignmentOutcome::Applied)
    }

    async fn expire_generation_tasks(&self, _limit: i32, _now_ms: i64) -> Result<Vec<Uuid>> {
        Ok(Vec::new())
    }

    async fn purge_terminal_generation_tasks(
        &self,
        _limit: i32,
        _completed_before_ms: i64,
    ) -> Result<Vec<Uuid>> {
        Ok(Vec::new())
    }

    async fn get_generation_task_access_owner(
        &self,
        _task_id: Uuid,
    ) -> Result<Option<GenerationTaskAccessOwner>> {
        Ok(None)
    }

    async fn get_generation_task_status_snapshot(
        &self,
        _task_id: Uuid,
    ) -> Result<Option<GenerationTaskStatusSnapshot>> {
        Ok(None)
    }

    async fn get_generation_task_generic_key_hash(
        &self,
        _task_id: Uuid,
    ) -> Result<Option<[u8; 16]>> {
        Ok(None)
    }
}

struct GatewayNodeHarness {
    node_id: u64,
    name: String,
    http_port: u16,
    gateway_state: GatewayState,
    key_validator: Arc<ApiKeyValidator>,
    client: Http3Client,
    shutdown: CancellationToken,
    http_server: Http3Server,
    _runtime_config: Arc<RuntimeConfigStore>,
    _config_file: NamedTempFile,
    _task_queue: TaskQueue,
    _task_manager: TaskManager,
}

struct CrossGatewayHarness {
    node_configs: Vec<OwnedNodeConfig>,
    raft_nodes: Vec<Raft>,
    raft_servers: Vec<RServer>,
    nodes: Vec<GatewayNodeHarness>,
    blocking_store: Arc<BlockingCreateStore>,
}

impl Drop for CrossGatewayHarness {
    fn drop(&mut self) {
        for node in &self.nodes {
            node.shutdown.cancel();
            node.http_server.abort();
        }
        for server in &self.raft_servers {
            server.abort();
        }
    }
}

impl CrossGatewayHarness {
    fn follower_index(&self, leader_index: usize) -> usize {
        (0..self.nodes.len())
            .find(|index| *index != leader_index)
            .expect("cluster must contain at least one follower")
    }
}

fn parse_socket_addr(addr: &str) -> Result<SocketAddr> {
    Ok(addr.parse::<SocketAddr>()?)
}

fn build_task_queue(config: &NodeConfig) -> TaskQueue {
    TaskQueue::builder()
        .dup(config.basic.unique_workers_per_task)
        .ttl(config.basic.taskqueue_task_ttl)
        .cleanup_interval(config.basic.taskqueue_cleanup_interval)
        .default_model(config.model_config.default_model.clone())
        .models(config.model_config.models.keys().cloned())
        .build()
}

fn ensure_crypto_provider() {
    ensure_crypto_provider_for_tests();
}

fn load_base_config() -> NodeConfig {
    let mut path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("dev-env/config/config-single.toml");
    let contents = std::fs::read_to_string(&path).expect("read config-single.toml");
    toml::from_str(&contents).expect("parse config-single.toml")
}

async fn build_http_client(http_port: u16) -> Result<Http3Client> {
    let server_ip = format!("{LOCALHOST}:{http_port}");
    let url = format!("https://{TEST_DOMAIN}:{http_port}/id");
    for attempt in 0..10 {
        match Http3ClientBuilder::new()
            .server_domain(TEST_DOMAIN)
            .server_ip(server_ip.clone())
            .dangerous_skip_verification(true)
            .build()
            .await
        {
            Ok(client) => {
                if client.get(&url, Some(Duration::from_secs(2))).await.is_ok() {
                    return Ok(client);
                }
            }
            Err(err) if attempt == 9 => return Err(err),
            Err(_) => {}
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
    }

    Err(anyhow::anyhow!("failed to connect to test HTTP3 gateway"))
}

async fn build_gateway_node(
    base_config: &NodeConfig,
    node_id: u64,
    raft_addr: &str,
    http_addr: &str,
    raft: Raft,
    state: Arc<StateMachineStore>,
    blocking_store: Arc<BlockingCreateStore>,
) -> Result<GatewayNodeHarness> {
    let raft_socket = parse_socket_addr(raft_addr)?;
    let http_socket = parse_socket_addr(http_addr)?;

    let mut config = base_config.clone();
    config.network.node_id = node_id;
    config.network.bind_ip = LOCALHOST.to_string();
    config.network.external_ip = LOCALHOST.to_string();
    config.network.domain = TEST_DOMAIN.to_string();
    config.network.server_port = raft_socket.port();
    config.network.name = format!("node-{node_id}");
    config.network.node_dns_names.clear();
    config.http.port = http_socket.port();
    config.cert.dangerous_skip_verification = true;
    config.cert.cert_file_path.clear();
    config.cert.key_file_path.clear();

    let config_file = tempfile::Builder::new()
        .prefix(&format!("gateway-http-node-{node_id}-"))
        .suffix(".toml")
        .tempfile()?;
    let config_path = config_file.path().to_path_buf();
    std::fs::write(&config_path, toml::to_string(&config)?)?;

    let runtime_config = Arc::new(RuntimeConfigStore::new(config_path, config.clone()).await?);
    let metrics = Metrics::new(0.05).map_err(|err| anyhow::anyhow!(err))?;
    let task_queue = build_task_queue(&config);
    let db = Arc::new(crate::db::Database::new_mock());
    let key_validator = Arc::new(ApiKeyValidator::new(
        Arc::clone(&db),
        ApiKeyValidatorConfig {
            update_interval: api_key_sync_interval(config.db.api_keys_update_interval),
            cache_ttl_sec: config.db.keys_cache_ttl_sec,
            cache_initial_capacity: config.db.keys_cache_initial_capacity,
            cache_max_capacity: config.db.keys_cache_max_capacity,
            api_key_secret: &config.http.api_key_secret,
            negative_cache_ttl_sec: config.http.invalid_api_key_negative_cache_ttl_sec,
            unknown_key_ip_miss_ttl_sec: config.http.invalid_api_key_ip_miss_ttl_sec,
            unknown_key_ip_cooldown_ttl_sec: config.http.invalid_api_key_ip_cooldown_ttl_sec,
            unknown_key_ip_cache_capacity: config.http.invalid_api_key_ip_cache_capacity,
            unknown_key_ip_miss_limit: config.http.invalid_api_key_ip_miss_limit,
            deleted_keys_ttl_minutes: config.db.deleted_keys_ttl_minutes,
            fallback_generic_key: config.http.generic_key,
        },
    )?);

    let event_sink = Arc::new(EventSinkHandle::Noop);
    let shutdown = CancellationToken::new();
    let event_recorder = EventRecorder::new(
        event_sink,
        Arc::from(config.network.name.as_str()),
        Duration::from_secs(config.db.events_flush_interval_sec.max(1)),
        config.db.events_queue_capacity.max(1),
        shutdown.clone(),
    );

    let rate_limit_queue = RateLimitMutationBuffer::default();
    let task_manager = TaskManager::new_with_rate_limit_mutation_queue(TaskManagerInit {
        initial_capacity: config.basic.taskmanager_initial_capacity,
        expected_results: config.basic.unique_workers_per_task,
        cleanup_interval: Duration::from_secs(config.basic.taskmanager_cleanup_interval),
        result_lifetime: Duration::from_secs(config.basic.taskmanager_result_lifetime),
        rate_limit_mutation_queue: rate_limit_queue.clone(),
        metrics: metrics.clone(),
        worker_event_recorder: Some(event_recorder.clone()),
    })
    .await;

    let gateway_state = GatewayState::new(GatewayStateInit {
        state,
        raft,
        last_task_acquisition: Arc::new(AtomicU64::new(0)),
        task_lifecycle_store: TaskLifecycleStoreHandle::Shared(blocking_store),
        api_key_validator: key_validator.clone(),
        task_manager: task_manager.clone(),
        config: runtime_config.clone(),
        event_recorder,
    });

    let http_server = Http3Server::run(
        runtime_config.clone(),
        Some(RustlsConfig::new(generate_and_create_keycert(vec![
            TEST_DOMAIN.to_string(),
        ])?)),
        gateway_state.clone(),
        task_queue.clone(),
        metrics,
        shutdown.clone(),
    )
    .await?;

    let client = build_http_client(http_socket.port()).await?;

    Ok(GatewayNodeHarness {
        node_id,
        name: config.network.name,
        http_port: http_socket.port(),
        gateway_state,
        key_validator,
        client,
        shutdown,
        http_server,
        _runtime_config: runtime_config,
        _config_file: config_file,
        _task_queue: task_queue,
        _task_manager: task_manager,
    })
}

async fn setup_cross_gateway_harness(
    generic_key_concurrent_limit: Option<usize>,
) -> Result<CrossGatewayHarness> {
    setup_cross_gateway_harness_inner(generic_key_concurrent_limit, None).await
}

async fn setup_cross_gateway_harness_with_admin_key_miss_limit(
    admin_key_miss_limit: u64,
) -> Result<CrossGatewayHarness> {
    setup_cross_gateway_harness_inner(None, Some(admin_key_miss_limit)).await
}

async fn setup_cross_gateway_harness_inner(
    generic_key_concurrent_limit: Option<usize>,
    admin_key_miss_limit: Option<u64>,
) -> Result<CrossGatewayHarness> {
    init_tracing();
    ensure_crypto_provider();

    let mut base_config = load_base_config();
    if let Some(limit) = generic_key_concurrent_limit {
        base_config.http.generic_key_concurrent_limit = limit;
    }
    if let Some(limit) = admin_key_miss_limit {
        base_config.http.invalid_api_key_ip_miss_limit = limit;
    }
    let (_network_guard, node_configs) = reserve_node_configs(3).await?;
    let node_clients = make_node_clients(node_configs.len());
    let (_config, _pcfg, raft_nodes, state_machines, raft_servers) =
        setup_connected_cluster(&node_configs, node_clients, None).await?;
    initialize_first_node_membership(&raft_nodes, &node_configs).await?;

    let (_leader_id, leader_index) =
        wait_for_consistent_leader_index(&raft_nodes, &node_configs, Duration::from_secs(10))
            .await?;

    let (http_addrs, http_reservations) = reserve_udp_addresses(node_configs.len())?;
    drop(http_reservations);

    let blocking_store = Arc::new(BlockingCreateStore::default());
    let mut nodes = Vec::with_capacity(node_configs.len());
    for ((node_id, raft_addr), (state, http_addr)) in node_configs
        .iter()
        .zip(state_machines.iter().cloned().zip(http_addrs.iter()))
    {
        nodes.push(
            build_gateway_node(
                &base_config,
                *node_id,
                raft_addr,
                http_addr.as_str(),
                raft_nodes[node_index(&node_configs, *node_id)].clone(),
                state,
                blocking_store.clone(),
            )
            .await?,
        );
    }

    for node in &nodes {
        let gateway_info = GatewayInfo {
            node_id: node.node_id,
            domain: TEST_DOMAIN.to_string(),
            ip: LOCALHOST.to_string(),
            name: node.name.clone(),
            http_port: node.http_port,
            available_tasks: 0,
            last_task_acquisition: 0,
            last_update: 0,
        };
        client_write_and_wait(
            &raft_nodes[leader_index],
            &raft_nodes,
            Request::Set {
                request_id: Uuid::new_v4().as_u128(),
                key: node.node_id.to_string(),
                value: rmp_serde::to_vec(&gateway_info)?,
            },
        )
        .await?;
    }

    Ok(CrossGatewayHarness {
        node_configs,
        raft_nodes,
        raft_servers,
        nodes,
        blocking_store,
    })
}

async fn post_add_task(
    client: &Http3Client,
    http_port: u16,
    api_key: &str,
) -> Result<(StatusCode, Bytes)> {
    let url = format!("https://{TEST_DOMAIN}:{http_port}/add_task");
    let headers = [("x-api-key", api_key)];
    client
        .post(
            &url,
            Bytes::from_static(PROMPT_JSON.as_bytes()),
            Some(&headers),
            Some(Duration::from_secs(5)),
        )
        .await
}

async fn post_gateway_write(
    client: &Http3Client,
    http_port: u16,
    payload: Bytes,
    admin_key: &str,
) -> Result<(StatusCode, Bytes)> {
    let url = format!("https://{TEST_DOMAIN}:{http_port}/write");
    let headers = [("x-admin-key", admin_key)];
    client
        .post(&url, payload, Some(&headers), Some(Duration::from_secs(5)))
        .await
}

fn random_non_admin_key(admin_key: Uuid) -> Uuid {
    loop {
        let candidate = Uuid::new_v4();
        if candidate != admin_key {
            return candidate;
        }
    }
}

async fn wait_for_active_replication(
    gateway_state: &GatewayState,
    subject: Subject,
    id: u128,
) -> Result<()> {
    let day_epoch = DistributedRateLimiter::current_day_epoch();
    timeout(Duration::from_secs(5), async {
        loop {
            let (active, _) = gateway_state
                .get_cluster_rate_usage(subject, id, day_epoch)
                .await;
            if active >= 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await?;
    Ok(())
}

async fn assert_cross_gateway_limit_rejection(
    harness: &CrossGatewayHarness,
    api_key: &str,
    subject: Subject,
    subject_id: u128,
    expected_message: Option<&str>,
) -> Result<()> {
    let (_leader_id, leader_index) = wait_for_consistent_leader_index(
        &harness.raft_nodes,
        &harness.node_configs,
        Duration::from_secs(10),
    )
    .await?;
    let first_index = leader_index;
    let second_index = harness.follower_index(leader_index);

    let first_client = harness.nodes[first_index].client.clone();
    let first_port = harness.nodes[first_index].http_port;
    let first_key = api_key.to_string();
    let first_request =
        tokio::spawn(async move { post_add_task(&first_client, first_port, &first_key).await });

    if timeout(
        Duration::from_secs(5),
        harness.blocking_store.wait_for_first_call(),
    )
    .await
    .is_err()
    {
        if first_request.is_finished() {
            let (status, body) = first_request.await??;
            return Err(anyhow::anyhow!(
                "first gateway returned before create_generation_task: status={}, body={}",
                status,
                String::from_utf8_lossy(body.as_ref()),
            ));
        }
        return Err(anyhow::anyhow!(
            "first gateway never reached create_generation_task within timeout"
        ));
    }
    assert_eq!(harness.blocking_store.create_calls(), 1);
    wait_for_active_replication(
        &harness.nodes[second_index].gateway_state,
        subject,
        subject_id,
    )
    .await?;

    let (status, body) = post_add_task(
        &harness.nodes[second_index].client,
        harness.nodes[second_index].http_port,
        api_key,
    )
    .await?;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    let payload: serde_json::Value = serde_json::from_slice(body.as_ref())?;
    assert_eq!(
        payload.get("error").and_then(|value| value.as_str()),
        Some("concurrent_limit")
    );
    if let Some(expected_message) = expected_message {
        assert_eq!(
            payload.get("message").and_then(|value| value.as_str()),
            Some(expected_message)
        );
    }
    assert_eq!(
        harness.blocking_store.create_calls(),
        1,
        "second gateway must reject before create_generation_task is reached",
    );

    harness.blocking_store.release_first_call();

    let (first_status, first_body) = first_request.await??;
    assert_eq!(first_status, StatusCode::OK);
    let first_payload: serde_json::Value = serde_json::from_slice(first_body.as_ref())?;
    assert!(first_payload.get("id").is_some());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn random_admin_key_cannot_write_fourth_node_into_cluster() -> Result<()> {
    let harness = setup_cross_gateway_harness(None).await?;
    let (_leader_id, leader_index) = wait_for_consistent_leader_index(
        &harness.raft_nodes,
        &harness.node_configs,
        Duration::from_secs(10),
    )
    .await?;
    let leader = &harness.nodes[leader_index];
    let cfg = leader._runtime_config.snapshot();
    let valid_admin_key = cfg.http().admin_key;
    let random_admin_key = random_non_admin_key(valid_admin_key);
    let rogue_node_id = 4;
    let info = GatewayInfoExtRef {
        node_id: rogue_node_id,
        domain: TEST_DOMAIN,
        ip: LOCALHOST,
        name: "rogue-node",
        http_port: leader.http_port,
        available_tasks: 0,
        cluster_name: cfg.node().raft.cluster_name.as_str(),
        last_task_acquisition: 0,
        last_update: 0,
    };
    let payload = Bytes::from(rmp_serde::to_vec(&info)?);
    let random_admin_key = random_admin_key.to_string();

    let (status, body) = post_gateway_write(
        &leader.client,
        leader.http_port,
        payload,
        random_admin_key.as_str(),
    )
    .await?;

    assert_eq!(
        status,
        StatusCode::UNAUTHORIZED,
        "body: {}",
        String::from_utf8_lossy(body.as_ref())
    );
    assert!(
        leader.gateway_state.gateway(rogue_node_id).await.is_err(),
        "rogue node info must not be committed with a random admin key",
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn repeated_random_admin_key_writes_are_rejected_before_body_parse() -> Result<()> {
    let harness = setup_cross_gateway_harness_with_admin_key_miss_limit(2).await?;
    let (_leader_id, leader_index) = wait_for_consistent_leader_index(
        &harness.raft_nodes,
        &harness.node_configs,
        Duration::from_secs(10),
    )
    .await?;
    let leader = &harness.nodes[leader_index];
    let cfg = leader._runtime_config.snapshot();
    let random_admin_key = random_non_admin_key(cfg.http().admin_key).to_string();

    let (first_status, first_body) = post_gateway_write(
        &leader.client,
        leader.http_port,
        Bytes::from_static(b"not-msgpack"),
        random_admin_key.as_str(),
    )
    .await?;
    assert_eq!(
        first_status,
        StatusCode::UNAUTHORIZED,
        "bad-key write should fail on admin key before body parsing; body: {}",
        String::from_utf8_lossy(first_body.as_ref())
    );

    let (second_status, second_body) = post_gateway_write(
        &leader.client,
        leader.http_port,
        Bytes::from_static(b"still-not-msgpack"),
        random_admin_key.as_str(),
    )
    .await?;
    assert_eq!(
        second_status,
        StatusCode::TOO_MANY_REQUESTS,
        "body: {}",
        String::from_utf8_lossy(second_body.as_ref())
    );
    let payload: serde_json::Value = serde_json::from_slice(second_body.as_ref())?;
    assert_eq!(
        payload.get("error").and_then(|value| value.as_str()),
        Some("invalid_admin_key_rate_limit")
    );

    let (third_status, _third_body) = post_gateway_write(
        &leader.client,
        leader.http_port,
        Bytes::from_static(b"still-not-msgpack-again"),
        random_admin_key.as_str(),
    )
    .await?;
    assert_eq!(third_status, StatusCode::TOO_MANY_REQUESTS);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn second_gateway_rejects_same_user_before_create_generation_task() -> Result<()> {
    let harness = setup_cross_gateway_harness(None).await?;
    let api_key = Uuid::new_v4().to_string();
    let user_id = 42i64;
    for node in &harness.nodes {
        node.key_validator
            .seed_user_key(&api_key, user_id, Some("user@example.com"))
            .await;
    }

    assert_cross_gateway_limit_rejection(
        &harness,
        &api_key,
        Subject::User,
        u128::from(user_id as u64),
        Some("User concurrent task limit exceeded."),
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn second_gateway_rejects_same_company_before_create_generation_task() -> Result<()> {
    let harness = setup_cross_gateway_harness(None).await?;
    let api_key = Uuid::new_v4().to_string();
    let company_id = Uuid::new_v4();
    for node in &harness.nodes {
        node.key_validator
            .seed_company_key(&api_key, company_id, "Acme", 1, 100)
            .await;
    }

    assert_cross_gateway_limit_rejection(
        &harness,
        &api_key,
        Subject::Company,
        company_id.as_u128(),
        Some("Acme concurrent task limit exceeded."),
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn second_gateway_rejects_same_generic_key_before_create_generation_task() -> Result<()> {
    let harness = setup_cross_gateway_harness(Some(1)).await?;
    let generic_key = harness.nodes[0]
        .key_validator
        .generic_key()
        .expect("generic key");
    let api_key = generic_key.to_string();
    for node in &harness.nodes {
        node.key_validator
            .gateway_settings()
            .seed_generic_key_limits(Some(generic_key), 10, 10, GENERIC_WINDOW_MS);
    }

    assert_cross_gateway_limit_rejection(
        &harness,
        &api_key,
        Subject::GenericGlobal,
        0,
        Some("Generic key concurrent task limit exceeded."),
    )
    .await
}
