use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use base64_simd::STANDARD;
use foldhash::HashSet;
use http::StatusCode;
use salvo::catcher::Catcher;
use salvo::prelude::*;
use salvo::test::TestClient;
use schnorrkel::{ExpansionMode, MiniSecretKey, context::signing_context};
use tempfile::NamedTempFile;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use gateway::api::Task;
use gateway::api::request::AddTaskResultRequest;
use gateway::common::queue::DupQueue;
use gateway::config::NodeConfig;
use gateway::config_runtime::RuntimeConfigStore;
use gateway::crypto::hotkey::Hotkey;
use gateway::db::{EventRecorder, EventSinkHandle};
use gateway::task::TaskManager;
use gateway::test_support::{
    MultipartFilePart, add_task_handler, api_or_generic_key_check, build_multipart_form,
    build_shared_harness_core, current_timestamp_secs, custom_response,
    dynamic_add_task_size_limit, dynamic_request_size_limit, enforce_rate_limit,
    ensure_test_crypto_provider, get_load_handler, get_result_handler, get_status_handler,
    get_tasks_handler, id_handler, load_test_single_node_config, prepare_rate_limit_context,
    version_handler,
};

pub(crate) use crate::common::{read_response, tiny_png_bytes};

pub(crate) const HOTKEY_SEED_BASE: u8 = 42;
const SIGNING_CTX: &[u8] = b"substrate";
const GATEWAY_PREFIX: &str = "404_GATEWAY_";

fn ensure_crypto_provider() {
    ensure_test_crypto_provider();
}

fn load_config() -> (NodeConfig, PathBuf) {
    load_test_single_node_config()
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
    current_timestamp_secs()
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
    model_params: Option<&str>,
) -> (String, Vec<u8>) {
    let mut fields = Vec::<(&str, &str)>::new();
    if let Some(p) = prompt {
        fields.push(("prompt", p));
    }
    if let Some(m) = model {
        fields.push(("model", m));
    }
    if let Some(s) = seed {
        fields.push(("seed", s));
    }
    if let Some(mp) = model_params {
        fields.push(("model_params", mp));
    }

    let mut files = Vec::new();
    if let Some(img) = image {
        files.push(MultipartFilePart {
            name: "image",
            filename: "image.png",
            content_type: "image/png",
            bytes: img,
        });
    }

    build_multipart_form(&fields, &files)
}

pub(crate) struct TestHarness {
    pub(crate) service: Service,
    pub(crate) api_key: Uuid,
    pub(crate) task_queue: DupQueue<Task>,
    pub(crate) task_manager: TaskManager,
    pub(crate) config: Arc<NodeConfig>,
    pub(crate) runtime_config: Arc<RuntimeConfigStore>,
    pub(crate) config_path: PathBuf,
    pub(crate) _config_file: NamedTempFile,
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

    let (config, _config_path) = load_config();
    let mut config = config;
    if let Some(whitelist) = worker_whitelist {
        config.http.worker_whitelist = whitelist;
    }
    let config = Arc::new(config);
    let config_file = tempfile::Builder::new()
        .suffix(".toml")
        .tempfile()
        .expect("temp config file");
    let config_toml = toml::to_string(config.as_ref()).expect("serialize test config");
    std::fs::write(config_file.path(), config_toml).expect("write temp config");
    let config_path = config_file.path().to_path_buf();

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

    let core = build_shared_harness_core(
        Arc::clone(&config),
        config_path.clone(),
        event_recorder,
        include_gateway,
    )
    .await;

    let state = core.state;

    let router = Router::new()
        .hoop(affix_state::inject(state))
        .hoop(prepare_rate_limit_context)
        .push(
            Router::with_path("/add_task")
                .hoop(dynamic_add_task_size_limit)
                .hoop(api_or_generic_key_check)
                .hoop(enforce_rate_limit)
                .post(add_task_handler),
        )
        .push(
            Router::with_path("/get_result")
                .hoop(dynamic_request_size_limit)
                .hoop(api_or_generic_key_check)
                .get(get_result_handler),
        )
        .push(
            Router::with_path("/get_status")
                .hoop(dynamic_request_size_limit)
                .hoop(api_or_generic_key_check)
                .get(get_status_handler),
        )
        .push(
            Router::with_path("/get_tasks")
                .hoop(dynamic_request_size_limit)
                .post(get_tasks_handler),
        )
        .push(
            Router::with_path("/get_load")
                .hoop(dynamic_request_size_limit)
                .get(get_load_handler),
        )
        .push(
            Router::with_path("/get_version")
                .hoop(dynamic_request_size_limit)
                .get(version_handler),
        )
        .push(
            Router::with_path("/id")
                .hoop(dynamic_request_size_limit)
                .get(id_handler),
        );

    let service = Service::new(router).catcher(Catcher::default().hoop(custom_response));

    TestHarness {
        service,
        api_key: core.generic_key,
        task_queue: core.task_queue,
        task_manager: core.task_manager,
        config,
        runtime_config: core.runtime_config,
        config_path,
        _config_file: config_file,
        shutdown,
    }
}

impl Drop for TestHarness {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
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
    let (boundary, body) = multipart_body(None, Some(image), model, None, None);
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
