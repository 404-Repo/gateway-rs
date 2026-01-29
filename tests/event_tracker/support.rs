use std::collections::BTreeMap;
use std::collections::HashSet as StdHashSet;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Once};
use std::time::Duration;

use base64_simd::STANDARD;
use foldhash::HashSet;
use foldhash::fast::RandomState;
use http::{HeaderMap, StatusCode};
use http_body_util::BodyExt;
use image::{ImageFormat, Rgba, RgbaImage};
use openraft::BasicNode;
use regex::Regex;
use salvo::http::request::SecureMaxSize;
use salvo::prelude::*;
use schnorrkel::{ExpansionMode, MiniSecretKey, context::signing_context};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use gateway::api::Task;
use gateway::api::request::AddTaskResultRequest;
use gateway::api::response::GatewayInfo;
use gateway::common::queue::DupQueue;
use gateway::config::{ModelConfigStore, NodeConfig};
use gateway::crypto::crypto_provider::init_crypto_provider;
use gateway::crypto::hotkey::Hotkey;
use gateway::db::{
    ActivityEventRow, EventRecorder, EventSinkHandle, InMemoryEventSink, WorkerEventRow,
};
use gateway::metrics::Metrics;
use gateway::raft::store::RateLimitDelta;
use gateway::raft::{LogStore, StateMachineStore};
use gateway::task::TaskManager;
use gateway::test_support::{
    AddTaskWhitelist, GatewayState, GatewayStateInit, HttpState, HttpStateInit, ImageUploadLimiter,
    Network, RateLimitContext, RateLimitService, add_result_handler, add_task_handler,
    api_or_generic_key_check, get_result_handler, get_status_handler, get_tasks_handler,
};
use gateway::test_support::{id_handler, version_handler};

static CRYPTO_INIT: Once = Once::new();

const SIGNING_CTX: &[u8] = b"substrate";
const GATEWAY_PREFIX: &str = "404_GATEWAY_";

fn ensure_crypto_provider() {
    CRYPTO_INIT.call_once(|| {
        init_crypto_provider().expect("crypto provider init must succeed");
    });
}

fn load_config() -> (NodeConfig, PathBuf) {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("dev-env/config/config-single.toml");
    let contents = std::fs::read_to_string(&path).expect("read config-single.toml");
    let config: NodeConfig = toml::from_str(&contents).expect("parse config-single.toml");
    (config, path)
}

pub(crate) fn tiny_png_bytes() -> Vec<u8> {
    let img = RgbaImage::from_pixel(1, 1, Rgba([255, 0, 0, 255]));
    let mut cursor = std::io::Cursor::new(Vec::new());
    image::DynamicImage::ImageRgba8(img)
        .write_to(&mut cursor, ImageFormat::Png)
        .expect("encode png");
    cursor.into_inner()
}

pub(crate) fn sign_worker(seed: [u8; 32]) -> (Hotkey, String, String) {
    let mini = MiniSecretKey::from_bytes(&seed).expect("seed");
    let keypair = mini.expand_to_keypair(ExpansionMode::Uniform);
    let timestamp = format!("{}{}", GATEWAY_PREFIX, current_timestamp());
    let ctx = signing_context(SIGNING_CTX);
    let signature = keypair.sign(ctx.bytes(timestamp.as_bytes()));
    let sig = STANDARD.encode_to_string(signature.to_bytes());
    let hotkey = Hotkey::from_bytes(&keypair.public.to_bytes());
    (hotkey, timestamp, sig)
}

pub(crate) fn sign_worker_with_timestamp(seed: [u8; 32], timestamp: &str) -> (Hotkey, String) {
    let mini = MiniSecretKey::from_bytes(&seed).expect("seed");
    let keypair = mini.expand_to_keypair(ExpansionMode::Uniform);
    let ctx = signing_context(SIGNING_CTX);
    let signature = keypair.sign(ctx.bytes(timestamp.as_bytes()));
    let sig = STANDARD.encode_to_string(signature.to_bytes());
    let hotkey = Hotkey::from_bytes(&keypair.public.to_bytes());
    (hotkey, sig)
}

pub(crate) fn current_timestamp() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_secs()
}

pub(crate) fn multipart_add_result(
    task_id: Uuid,
    worker_hotkey: &Hotkey,
    worker_id: &str,
    timestamp: &str,
    signature: &str,
    status: &str,
    asset: Option<&[u8]>,
    reason: Option<&str>,
) -> (String, Vec<u8>) {
    let boundary = "XBOUNDARY123";
    let mut body = Vec::new();

    fn push_text(body: &mut Vec<u8>, boundary: &str, name: &str, value: &str) {
        body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
        body.extend_from_slice(
            format!("Content-Disposition: form-data; name=\"{}\"\r\n\r\n", name).as_bytes(),
        );
        body.extend_from_slice(value.as_bytes());
        body.extend_from_slice(b"\r\n");
    }

    fn push_file(
        body: &mut Vec<u8>,
        boundary: &str,
        name: &str,
        filename: &str,
        content_type: &str,
        bytes: &[u8],
    ) {
        body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
        body.extend_from_slice(
            format!(
                "Content-Disposition: form-data; name=\"{}\"; filename=\"{}\"\r\n",
                name, filename
            )
            .as_bytes(),
        );
        body.extend_from_slice(format!("Content-Type: {}\r\n\r\n", content_type).as_bytes());
        body.extend_from_slice(bytes);
        body.extend_from_slice(b"\r\n");
    }

    push_text(&mut body, boundary, "id", task_id.to_string().as_str());
    push_text(&mut body, boundary, "signature", signature);
    push_text(&mut body, boundary, "timestamp", timestamp);
    push_text(&mut body, boundary, "worker_hotkey", worker_hotkey.as_ref());
    push_text(&mut body, boundary, "worker_id", worker_id);
    push_text(&mut body, boundary, "status", status);
    if let Some(reason) = reason {
        push_text(&mut body, boundary, "reason", reason);
    }
    if let Some(asset) = asset {
        push_file(
            &mut body,
            boundary,
            "asset",
            "result.spz",
            "application/octet-stream",
            asset,
        );
    }

    body.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());
    (boundary.to_string(), body)
}

pub(crate) fn multipart_add_task(prompt: Option<&str>, image: Option<&[u8]>) -> (String, Vec<u8>) {
    let boundary = "XBOUNDARY123";
    let mut body = Vec::new();

    fn push_text(body: &mut Vec<u8>, boundary: &str, name: &str, value: &str) {
        body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
        body.extend_from_slice(
            format!("Content-Disposition: form-data; name=\"{}\"\r\n\r\n", name).as_bytes(),
        );
        body.extend_from_slice(value.as_bytes());
        body.extend_from_slice(b"\r\n");
    }

    fn push_file(
        body: &mut Vec<u8>,
        boundary: &str,
        name: &str,
        filename: &str,
        content_type: &str,
        bytes: &[u8],
    ) {
        body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
        body.extend_from_slice(
            format!(
                "Content-Disposition: form-data; name=\"{}\"; filename=\"{}\"\r\n",
                name, filename
            )
            .as_bytes(),
        );
        body.extend_from_slice(format!("Content-Type: {}\r\n\r\n", content_type).as_bytes());
        body.extend_from_slice(bytes);
        body.extend_from_slice(b"\r\n");
    }

    if let Some(prompt) = prompt {
        push_text(&mut body, boundary, "prompt", prompt);
    }
    if let Some(image) = image {
        push_file(
            &mut body,
            boundary,
            "image",
            "image.png",
            "image/png",
            image,
        );
    }

    body.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());
    (boundary.to_string(), body)
}

pub(crate) fn multipart_custom(
    fields: Vec<(String, String)>,
    file: Option<(&str, &[u8])>,
) -> (String, Vec<u8>) {
    let boundary = "XBOUNDARY123";
    let mut body = Vec::new();

    fn push_text(body: &mut Vec<u8>, boundary: &str, name: &str, value: &str) {
        body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
        body.extend_from_slice(
            format!("Content-Disposition: form-data; name=\"{}\"\r\n\r\n", name).as_bytes(),
        );
        body.extend_from_slice(value.as_bytes());
        body.extend_from_slice(b"\r\n");
    }

    fn push_file(
        body: &mut Vec<u8>,
        boundary: &str,
        name: &str,
        filename: &str,
        content_type: &str,
        bytes: &[u8],
    ) {
        body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
        body.extend_from_slice(
            format!(
                "Content-Disposition: form-data; name=\"{}\"; filename=\"{}\"\r\n",
                name, filename
            )
            .as_bytes(),
        );
        body.extend_from_slice(format!("Content-Type: {}\r\n\r\n", content_type).as_bytes());
        body.extend_from_slice(bytes);
        body.extend_from_slice(b"\r\n");
    }

    for (name, value) in fields {
        push_text(&mut body, boundary, &name, &value);
    }
    if let Some((name, bytes)) = file {
        push_file(
            &mut body,
            boundary,
            name,
            "result.spz",
            "application/octet-stream",
            bytes,
        );
    }

    body.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());
    (boundary.to_string(), body)
}

pub(crate) struct EventHarness {
    pub(crate) service: Service,
    pub(crate) task_queue: DupQueue<Task>,
    pub(crate) task_manager: TaskManager,
    pub(crate) event_sink: InMemoryEventSink,
    pub(crate) key_validator: Arc<gateway::db::ApiKeyValidator>,
    pub(crate) event_recorder: EventRecorder,
    pub(crate) shutdown: CancellationToken,
    pub(crate) api_key: Uuid,
}

impl Drop for EventHarness {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

pub(crate) async fn build_harness(
    rate_ctx: RateLimitContext,
    worker_whitelist: Option<HashSet<Hotkey>>,
) -> EventHarness {
    ensure_crypto_provider();

    let (config, config_path) = load_config();
    let mut config = config;
    if let Some(whitelist) = worker_whitelist {
        config.http.worker_whitelist = whitelist;
    }
    let config = Arc::new(config);

    let generic_key = config.http.generic_key.unwrap_or_else(Uuid::new_v4);

    let model_store = Arc::new(
        ModelConfigStore::new(config_path, config.model_config.clone())
            .await
            .expect("model store"),
    );

    let metrics = Metrics::new(0.05).expect("metrics");

    let task_queue: DupQueue<Task> = DupQueue::<Task>::builder()
        .dup(config.basic.unique_workers_per_task)
        .ttl(config.basic.taskqueue_task_ttl)
        .cleanup_interval(config.basic.taskqueue_cleanup_interval)
        .build();

    let db = Arc::new(gateway::db::Database::new_mock());
    let shutdown = CancellationToken::new();

    let event_sink = InMemoryEventSink::default();
    let events_flush_interval = config.db.events_flush_interval_sec.max(1);
    let events_queue_capacity = config.db.events_queue_capacity.max(1);
    let event_recorder = EventRecorder::new(
        Arc::new(EventSinkHandle::InMemory(event_sink.clone())),
        Arc::from(config.network.name.as_str()),
        Duration::from_secs(events_flush_interval),
        events_queue_capacity,
        shutdown.clone(),
    );

    let key_validator = Arc::new(
        gateway::db::ApiKeyValidator::new(
            Arc::clone(&db),
            Duration::from_secs(config.db.api_keys_update_interval),
            config.db.keys_cache_ttl_sec,
            config.db.keys_cache_initial_capacity,
            config.db.keys_cache_max_capacity,
            &config.http.api_key_secret,
            config.db.deleted_keys_ttl_minutes,
        )
        .expect("api key validator"),
    );

    let state_machine_store = Arc::new(StateMachineStore::default());
    {
        let mut sm = state_machine_store.state_machine.write().await;
        let serialized_key = rmp_serde::to_vec(&generic_key).expect("serialize key");
        sm.data.insert("generic_key".to_string(), serialized_key);
    }

    let node_clients = scc::HashMap::with_capacity_and_hasher(1, RandomState::default());
    let network = Network::new(Arc::new(node_clients));
    let log_store = LogStore::default();
    let raft_config = Arc::new(
        openraft::Config {
            cluster_name: config.raft.cluster_name.clone(),
            heartbeat_interval: 100,
            election_timeout_min: 300,
            election_timeout_max: 600,
            ..Default::default()
        }
        .validate()
        .expect("raft config"),
    );
    let raft = gateway::raft::Raft::new(
        config.network.node_id,
        Arc::clone(&raft_config),
        network,
        log_store,
        Arc::clone(&state_machine_store),
    )
    .await
    .expect("raft");

    let mut members = BTreeMap::new();
    members.insert(
        config.network.node_id,
        BasicNode {
            addr: format!(
                "{}:{}",
                config.network.external_ip, config.network.server_port
            ),
        },
    );
    let _ = raft.initialize(members).await;

    let gateway_info = GatewayInfo {
        node_id: config.network.node_id,
        domain: config.network.domain.clone(),
        ip: config.network.external_ip.clone(),
        name: config.network.name.clone(),
        http_port: config.http.port,
        available_tasks: 0,
        last_task_acquisition: 0,
        last_update: 0,
    };
    let gateway_bytes = rmp_serde::to_vec(&gateway_info).expect("serialize gateway info");
    {
        let mut sm = state_machine_store.state_machine.write().await;
        sm.data
            .insert(config.network.node_id.to_string(), gateway_bytes);
    }

    let task_manager = TaskManager::new(
        config.basic.taskmanager_initial_capacity,
        config.basic.unique_workers_per_task,
        Duration::from_secs(config.basic.taskmanager_cleanup_interval),
        Duration::from_secs(config.basic.taskmanager_result_lifetime),
        metrics.clone(),
        Some(event_recorder.clone()),
    )
    .await;

    let rate_limit_queue = Arc::new(scc::Queue::<RateLimitDelta>::default());

    let gateway_state = GatewayState::new(GatewayStateInit {
        state: Arc::clone(&state_machine_store),
        raft,
        last_task_acquisition: Arc::new(AtomicU64::new(0)),
        key_validator_updater: key_validator.clone(),
        task_manager: task_manager.clone(),
        config: Arc::clone(&config),
        rate_limit_queue,
        event_recorder: event_recorder.clone(),
    });

    let prompt_regex = Regex::new(&config.prompt.allowed_pattern).expect("prompt regex");
    let add_task_whitelist = AddTaskWhitelist {
        ips: Arc::new(StdHashSet::new()),
    };
    let image_upload_limiter = ImageUploadLimiter::new(config.http.max_concurrent_image_uploads);
    let (rate_limit_service, _rate_limiters) = RateLimitService::new(&config.http);
    let state = HttpState::new(HttpStateInit {
        config: Arc::clone(&config),
        model_store,
        gateway_state: gateway_state.clone(),
        task_queue: task_queue.clone(),
        metrics: metrics.clone(),
        add_task_whitelist,
        cluster_ips: StdHashSet::<std::net::IpAddr>::new(),
        image_upload_limiter,
        prompt_regex,
        rate_limits: rate_limit_service,
    });

    let request_size_limit = config.http.request_size_limit as usize;
    let size_limit_handler = || SecureMaxSize(request_size_limit);
    let add_task_size_limit = config.http.add_task_size_limit as usize;
    let add_task_size_limit_handler = || SecureMaxSize(add_task_size_limit);

    let router = Router::new()
        .hoop(affix_state::inject(state))
        .hoop(affix_state::inject(rate_ctx))
        .push(
            Router::with_path("/add_task")
                .hoop(add_task_size_limit_handler())
                .hoop(api_or_generic_key_check)
                .post(add_task_handler),
        )
        .push(
            Router::with_path("/get_result")
                .hoop(size_limit_handler())
                .hoop(api_or_generic_key_check)
                .get(get_result_handler),
        )
        .push(
            Router::with_path("/get_status")
                .hoop(size_limit_handler())
                .hoop(api_or_generic_key_check)
                .get(get_status_handler),
        )
        .push(
            Router::with_path("/get_tasks")
                .hoop(size_limit_handler())
                .post(get_tasks_handler),
        )
        .push(
            Router::with_path("/add_result")
                .hoop(size_limit_handler())
                .post(add_result_handler),
        )
        .push(
            Router::with_path("/get_version")
                .hoop(size_limit_handler())
                .get(version_handler),
        )
        .push(
            Router::with_path("/id")
                .hoop(size_limit_handler())
                .get(id_handler),
        );

    let service = Service::new(router);

    EventHarness {
        service,
        task_queue,
        task_manager,
        event_sink,
        key_validator,
        event_recorder,
        shutdown,
        api_key: generic_key,
    }
}

pub(crate) async fn read_response(res: Response) -> (StatusCode, HeaderMap, Vec<u8>) {
    let hyper_res = res.into_hyper();
    let status = hyper_res.status();
    let headers = hyper_res.headers().clone();
    let body = hyper_res
        .into_body()
        .collect()
        .await
        .expect("read body")
        .to_bytes();
    (status, headers, body.to_vec())
}

pub(crate) fn default_rate_ctx() -> RateLimitContext {
    RateLimitContext {
        user_id: Some(Uuid::new_v4()),
        has_valid_api_key: true,
        key_is_uuid: true,
        ..RateLimitContext::default()
    }
}

pub(crate) async fn wait_for_activity(
    recorder: &EventRecorder,
    sink: &InMemoryEventSink,
    expected: usize,
) -> Vec<ActivityEventRow> {
    for _ in 0..50 {
        recorder.flush_once_for_test().await;
        let rows = sink.activity_rows().await;
        if rows.len() >= expected {
            return rows;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    sink.activity_rows().await
}

pub(crate) async fn wait_for_worker(
    recorder: &EventRecorder,
    sink: &InMemoryEventSink,
    expected: usize,
) -> Vec<WorkerEventRow> {
    for _ in 0..50 {
        recorder.flush_once_for_test().await;
        let rows = sink.worker_rows().await;
        if rows.len() >= expected {
            return rows;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    sink.worker_rows().await
}

pub(crate) async fn add_success_result(
    task_manager: &TaskManager,
    task_id: Uuid,
    worker: Hotkey,
    asset: Vec<u8>,
) {
    task_manager
        .record_assignment(task_id, worker.clone(), worker.to_string().into())
        .await;
    let result = AddTaskResultRequest {
        worker_hotkey: worker.clone(),
        worker_id: worker.to_string().into(),
        asset: Some(asset),
        reason: None,
        instant: std::time::Instant::now(),
    };
    task_manager
        .add_result(task_id, result)
        .await
        .expect("add result");
}
