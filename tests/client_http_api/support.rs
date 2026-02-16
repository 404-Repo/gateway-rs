use std::collections::{BTreeMap, HashSet as StdHashSet};
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Once};
use std::time::{Duration, Instant};

use base64_simd::STANDARD;
use foldhash::HashSet;
use foldhash::fast::RandomState;
use http::{HeaderMap, StatusCode};
use http_body_util::BodyExt;
use image::{ImageFormat, Rgba, RgbaImage};
use openraft::BasicNode;
use regex::Regex;
use salvo::catcher::Catcher;
use salvo::http::request::SecureMaxSize;
use salvo::prelude::*;
use salvo::test::TestClient;
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
use gateway::db::{ApiKeyValidator, Database, EventRecorder, EventSinkHandle};
use gateway::metrics::Metrics;
use gateway::raft::store::RateLimitDelta;
use gateway::raft::{LogStore, StateMachineStore};
use gateway::task::TaskManager;
use gateway::test_support::{
    GatewayState, GatewayStateInit, HttpState, HttpStateInit, ImageUploadLimiter, Network,
    RateLimitService, RateLimitWhitelist, add_task_handler, api_or_generic_key_check,
    custom_response, enforce_rate_limit, get_load_handler, get_result_handler, get_status_handler,
    get_tasks_handler, id_handler, prepare_rate_limit_context, version_handler,
};

static CRYPTO_INIT: Once = Once::new();

pub(crate) const HOTKEY_SEED_BASE: u8 = 42;
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

pub(crate) fn hotkey_from_seed(seed: u8) -> Hotkey {
    Hotkey::from_bytes(&[seed; 32])
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

pub(crate) fn current_timestamp() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_secs()
}

pub(crate) fn tiny_png_bytes() -> Vec<u8> {
    let img = RgbaImage::from_pixel(1, 1, Rgba([255, 0, 0, 255]));
    let mut cursor = std::io::Cursor::new(Vec::new());
    image::DynamicImage::ImageRgba8(img)
        .write_to(&mut cursor, ImageFormat::Png)
        .expect("encode png");
    cursor.into_inner()
}

fn minimal_ply_bytes() -> Vec<u8> {
    let mut data = Vec::new();
    data.extend_from_slice(b"ply\n");
    data.extend_from_slice(b"format binary_little_endian 1.0\n");
    data.extend_from_slice(b"element vertex 1\n");
    data.extend_from_slice(b"property float x\n");
    data.extend_from_slice(b"property float y\n");
    data.extend_from_slice(b"property float z\n");
    data.extend_from_slice(b"property float f_dc_0\n");
    data.extend_from_slice(b"property float f_dc_1\n");
    data.extend_from_slice(b"property float f_dc_2\n");
    data.extend_from_slice(b"property float opacity\n");
    data.extend_from_slice(b"property float scale_0\n");
    data.extend_from_slice(b"property float scale_1\n");
    data.extend_from_slice(b"property float scale_2\n");
    data.extend_from_slice(b"property float rot_0\n");
    data.extend_from_slice(b"property float rot_1\n");
    data.extend_from_slice(b"property float rot_2\n");
    data.extend_from_slice(b"property float rot_3\n");
    data.extend_from_slice(b"end_header\n");

    let values = [
        0.0f32, 0.0, 0.0, // position
        1.0, 0.0, 0.0, // color
        1.0, // opacity
        1.0, 1.0, 1.0, // scale
        1.0, 0.0, 0.0, 0.0, // rotation (w, x, y, z)
    ];
    for value in values {
        data.extend_from_slice(&value.to_le_bytes());
    }
    data
}

pub(crate) fn tiny_spz_bytes() -> Vec<u8> {
    let ply = minimal_ply_bytes();
    let mut output = Vec::new();
    spz_lib::compress(&ply, 1, 1, &mut output).expect("compress spz");
    output
}

pub(crate) fn multipart_body(
    prompt: Option<&str>,
    image: Option<&[u8]>,
    model: Option<&str>,
    seed: Option<&str>,
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

    if let Some(p) = prompt {
        push_text(&mut body, boundary, "prompt", p);
    }
    if let Some(m) = model {
        push_text(&mut body, boundary, "model", m);
    }
    if let Some(img) = image {
        push_file(&mut body, boundary, "image", "image.png", "image/png", img);
    }
    if let Some(s) = seed {
        push_text(&mut body, boundary, "seed", &s.to_string());
    }

    body.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());
    (boundary.to_string(), body)
}

pub(crate) struct TestHarness {
    pub(crate) service: Service,
    pub(crate) api_key: Uuid,
    pub(crate) task_queue: DupQueue<Task>,
    pub(crate) task_manager: TaskManager,
    pub(crate) config: Arc<NodeConfig>,
    pub(crate) shutdown: CancellationToken,
}

pub(crate) async fn build_harness() -> TestHarness {
    build_harness_inner(true, None).await
}

pub(crate) async fn build_harness_with_gateway_info(include_gateway: bool) -> TestHarness {
    build_harness_inner(include_gateway, None).await
}

pub(crate) async fn build_harness_with_worker_whitelist(
    worker_whitelist: HashSet<Hotkey>,
) -> TestHarness {
    build_harness_inner(true, Some(worker_whitelist)).await
}

async fn build_harness_inner(
    include_gateway: bool,
    worker_whitelist: Option<HashSet<Hotkey>>,
) -> TestHarness {
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

    let db = Arc::new(Database::new_mock());
    let shutdown = CancellationToken::new();

    let event_sink = Arc::new(EventSinkHandle::Noop);

    let events_flush_interval = config.db.events_flush_interval_sec.max(1);
    let events_queue_capacity = config.db.events_queue_capacity.max(1);

    let event_recorder = EventRecorder::new(
        Arc::clone(&event_sink),
        Arc::from(config.network.name.as_str()),
        Duration::from_secs(events_flush_interval),
        events_queue_capacity,
        shutdown.clone(),
    );

    let key_validator = Arc::new(
        ApiKeyValidator::new(
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

    if include_gateway {
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
        key_validator_updater: key_validator,
        task_manager: task_manager.clone(),
        config: Arc::clone(&config),
        rate_limit_queue,
        event_recorder,
    });

    let prompt_regex = Regex::new(&config.prompt.allowed_pattern).expect("prompt regex");
    let rate_limit_whitelist = RateLimitWhitelist {
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
        rate_limit_whitelist,
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
        .hoop(prepare_rate_limit_context)
        .push(
            Router::with_path("/add_task")
                .hoop(add_task_size_limit_handler())
                .hoop(api_or_generic_key_check)
                .hoop(enforce_rate_limit)
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
            Router::with_path("/get_load")
                .hoop(size_limit_handler())
                .get(get_load_handler),
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

    let service = Service::new(router).catcher(Catcher::default().hoop(custom_response));

    TestHarness {
        service,
        api_key: generic_key,
        task_queue,
        task_manager,
        config,
        shutdown,
    }
}

impl Drop for TestHarness {
    fn drop(&mut self) {
        self.shutdown.cancel();
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

pub(crate) async fn add_task_prompt(h: &TestHarness, prompt: &str, model: Option<&str>) -> Uuid {
    let mut payload = serde_json::json!({"prompt": prompt});
    if let Some(model) = model {
        payload["model"] = serde_json::Value::String(model.to_string());
    }
    let res = TestClient::post("http://localhost/add_task")
        .add_header("x-api-key", h.api_key.to_string(), true)
        .json(&payload)
        .send(&h.service)
        .await;
    let (status, _headers, body) = read_response(res).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "add_task body: {}",
        String::from_utf8_lossy(&body)
    );
    let payload: serde_json::Value = serde_json::from_slice(&body).expect("json response");
    let task_id = payload
        .get("id")
        .and_then(|v| v.as_str())
        .expect("id string");
    Uuid::parse_str(task_id).expect("uuid")
}

pub(crate) async fn add_task_image(h: &TestHarness, image: &[u8], model: Option<&str>) -> Uuid {
    let (boundary, body) = multipart_body(None, Some(image), model, None);
    let res = TestClient::post("http://localhost/add_task")
        .add_header("x-api-key", h.api_key.to_string(), true)
        .add_header(
            "content-type",
            format!("multipart/form-data; boundary={}", boundary),
            true,
        )
        .body(body)
        .send(&h.service)
        .await;
    let (status, _headers, body) = read_response(res).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "add_task body: {}",
        String::from_utf8_lossy(&body)
    );
    let payload: serde_json::Value = serde_json::from_slice(&body).expect("json response");
    let task_id = payload
        .get("id")
        .and_then(|v| v.as_str())
        .expect("id string");
    Uuid::parse_str(task_id).expect("uuid")
}

pub(crate) async fn add_success_result(
    task_manager: &TaskManager,
    task_id: Uuid,
    worker: Hotkey,
    asset: Vec<u8>,
) {
    let worker_id = worker.to_string();
    add_success_result_with_worker_id(task_manager, task_id, worker, worker_id, asset).await;
}

pub(crate) async fn add_success_result_with_worker_id(
    task_manager: &TaskManager,
    task_id: Uuid,
    worker: Hotkey,
    worker_id: String,
    asset: Vec<u8>,
) {
    task_manager
        .record_assignment(task_id, worker.clone(), worker.to_string().into())
        .await;
    let result = AddTaskResultRequest {
        worker_hotkey: worker.clone(),
        worker_id: Arc::<str>::from(worker_id),
        asset: Some(asset),
        reason: None,
        instant: Instant::now(),
    };
    task_manager
        .add_result(task_id, result)
        .await
        .expect("add result");
}

pub(crate) async fn add_failure_result(task_manager: &TaskManager, task_id: Uuid, worker: Hotkey) {
    task_manager
        .record_assignment(task_id, worker.clone(), worker.to_string().into())
        .await;
    let result = AddTaskResultRequest {
        worker_hotkey: worker.clone(),
        worker_id: worker.to_string().into(),
        asset: None,
        reason: Some(Arc::<str>::from("failed")),
        instant: Instant::now(),
    };
    task_manager
        .add_result(task_id, result)
        .await
        .expect("add result");
}
