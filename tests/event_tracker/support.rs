use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use base64_simd::STANDARD;
use foldhash::HashSet;
use salvo::http::request::SecureMaxSize;
use salvo::prelude::*;
use schnorrkel::{ExpansionMode, MiniSecretKey, context::signing_context};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use gateway::api::Task;
use gateway::api::request::AddTaskResultRequest;
use gateway::common::queue::DupQueue;
use gateway::config::NodeConfig;
use gateway::crypto::hotkey::Hotkey;
use gateway::db::{
    ActivityEventRow, EventRecorder, EventSinkHandle, InMemoryEventSink, WorkerEventRow,
};
use gateway::task::TaskManager;
use gateway::test_support::{
    MultipartFilePart, RateLimitContext, add_result_handler, add_task_handler,
    api_or_generic_key_check, build_multipart_form, build_shared_harness_core,
    current_timestamp_secs, ensure_test_crypto_provider, get_result_handler, get_status_handler,
    get_tasks_handler, load_test_single_node_config,
};
use gateway::test_support::{id_handler, version_handler};

pub(crate) use crate::common::{read_response, tiny_png_bytes};

const SIGNING_CTX: &[u8] = b"substrate";
const GATEWAY_PREFIX: &str = "404_GATEWAY_";

fn ensure_crypto_provider() {
    ensure_test_crypto_provider();
}

fn load_config() -> (NodeConfig, PathBuf) {
    load_test_single_node_config()
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
    current_timestamp_secs()
}

#[allow(clippy::too_many_arguments)]
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
    let task_id_str = task_id.to_string();
    let mut fields = vec![
        ("id", task_id_str.as_str()),
        ("signature", signature),
        ("timestamp", timestamp),
        ("worker_hotkey", worker_hotkey.as_ref()),
        ("worker_id", worker_id),
        ("status", status),
    ];
    if let Some(reason) = reason {
        fields.push(("reason", reason));
    }

    let mut files = Vec::new();
    if let Some(asset) = asset {
        files.push(MultipartFilePart {
            name: "asset",
            filename: "result.spz",
            content_type: "application/octet-stream",
            bytes: asset,
        });
    }

    build_multipart_form(&fields, &files)
}

pub(crate) fn multipart_add_task(prompt: Option<&str>, image: Option<&[u8]>) -> (String, Vec<u8>) {
    let mut fields = Vec::<(&str, &str)>::new();
    if let Some(prompt) = prompt {
        fields.push(("prompt", prompt));
    }
    let mut files = Vec::new();
    if let Some(image) = image {
        files.push(MultipartFilePart {
            name: "image",
            filename: "image.png",
            content_type: "image/png",
            bytes: image,
        });
    }

    build_multipart_form(&fields, &files)
}

pub(crate) fn multipart_custom(
    fields: Vec<(String, String)>,
    file: Option<(&str, &[u8])>,
) -> (String, Vec<u8>) {
    let fields_ref: Vec<(&str, &str)> = fields
        .iter()
        .map(|(name, value)| (name.as_str(), value.as_str()))
        .collect();

    let mut files = Vec::new();
    if let Some((name, bytes)) = file {
        files.push(MultipartFilePart {
            name,
            filename: "result.spz",
            content_type: "application/octet-stream",
            bytes,
        });
    }

    build_multipart_form(&fields_ref, &files)
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

    let core = build_shared_harness_core(
        Arc::clone(&config),
        config_path,
        event_recorder.clone(),
        true,
    )
    .await;

    let state = core.state;

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
        task_queue: core.task_queue,
        task_manager: core.task_manager,
        event_sink,
        key_validator: core.key_validator,
        event_recorder,
        shutdown,
        api_key: core.generic_key,
    }
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
