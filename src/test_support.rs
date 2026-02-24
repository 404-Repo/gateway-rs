pub use crate::http3::distributed_rate_limiter::DistributedRateLimiter;
pub use crate::http3::handlers::core::api_or_generic_key_check;
pub use crate::http3::handlers::core::{id_handler, version_handler};
pub use crate::http3::handlers::result::add_result_handler;
pub use crate::http3::handlers::result::{get_result_handler, get_status_handler};
pub use crate::http3::handlers::task::{add_task_handler, get_load_handler, get_tasks_handler};
pub use crate::http3::rate_limits::{CompanyRateLimit, RateLimitService};
pub use crate::http3::rate_limits::{
    RateLimitContext, RateLimiters, enforce_rate_limit, prepare_rate_limit_context,
};
pub use crate::http3::response::custom_response;
pub use crate::http3::state::{HttpState, HttpStateInit};
pub use crate::http3::upload_limiter::ImageUploadLimiter;
pub use crate::http3::whitelist::RateLimitWhitelist;
pub use crate::raft::gateway_state::{GatewayState, GatewayStateInit};
pub use crate::raft::network::Network;

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Once;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use openraft::BasicNode;
use uuid::Uuid;

use crate::api::Task;
use crate::api::response::GatewayInfo;
use crate::common::queue::DupQueue;
use crate::config::NodeConfig;
use crate::crypto::crypto_provider::init_crypto_provider;
use crate::db::{ApiKeyValidator, Database, EventRecorder};
use crate::metrics::Metrics;
use crate::raft::store::RateLimitDelta;
use crate::raft::{LogStore, StateMachineStore};
use crate::task::TaskManager;

static TEST_CRYPTO_INIT: Once = Once::new();

pub fn ensure_test_crypto_provider() {
    TEST_CRYPTO_INIT.call_once(|| {
        init_crypto_provider().expect("crypto provider init must succeed");
    });
}

pub fn load_test_single_node_config() -> (NodeConfig, PathBuf) {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("dev-env/config/config-single.toml");
    let contents = std::fs::read_to_string(&path).expect("read config-single.toml");
    let config: NodeConfig = toml::from_str(&contents).expect("parse config-single.toml");
    (config, path)
}

pub fn current_timestamp_secs() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_secs()
}

pub struct MultipartFilePart<'a> {
    pub name: &'a str,
    pub filename: &'a str,
    pub content_type: &'a str,
    pub bytes: &'a [u8],
}

pub struct SharedHarnessCore {
    pub generic_key: Uuid,
    pub task_queue: DupQueue<Task>,
    pub task_manager: TaskManager,
    pub key_validator: Arc<ApiKeyValidator>,
    pub state: HttpState,
}

pub async fn build_shared_harness_core(
    config: Arc<NodeConfig>,
    config_path: PathBuf,
    event_recorder: EventRecorder,
    include_gateway_info: bool,
) -> SharedHarnessCore {
    let generic_key = config.http.generic_key.unwrap_or_else(Uuid::new_v4);

    let model_store = Arc::new(
        crate::config::ModelConfigStore::new(config_path, config.model_config.clone())
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

    let node_clients =
        scc::HashMap::with_capacity_and_hasher(1, foldhash::fast::RandomState::default());
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
    let raft = crate::raft::Raft::new(
        config.network.node_id,
        Arc::clone(&raft_config),
        network,
        log_store,
        Arc::clone(&state_machine_store),
    )
    .await
    .expect("raft");

    let mut members = std::collections::BTreeMap::new();
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

    if include_gateway_info {
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
        key_validator_updater: key_validator.clone(),
        task_manager: task_manager.clone(),
        config: Arc::clone(&config),
        rate_limit_queue,
        event_recorder,
    });

    let prompt_regex = regex::Regex::new(&config.prompt.allowed_pattern).expect("prompt regex");
    let rate_limit_whitelist = RateLimitWhitelist {
        ips: Arc::new(std::collections::HashSet::new()),
    };
    let image_upload_limiter = ImageUploadLimiter::new(config.http.max_concurrent_image_uploads);
    let (rate_limit_service, _rate_limiters) = RateLimitService::new(&config.http);
    let state = HttpState::new(HttpStateInit {
        config,
        model_store,
        gateway_state,
        task_queue: task_queue.clone(),
        metrics,
        rate_limit_whitelist,
        cluster_ips: std::collections::HashSet::<std::net::IpAddr>::new(),
        image_upload_limiter,
        prompt_regex,
        rate_limits: rate_limit_service,
    });

    SharedHarnessCore {
        generic_key,
        task_queue,
        task_manager,
        key_validator,
        state,
    }
}

pub fn build_multipart_form(
    fields: &[(impl AsRef<str>, impl AsRef<str>)],
    files: &[MultipartFilePart<'_>],
) -> (String, Vec<u8>) {
    let boundary = "XBOUNDARY123";
    let mut body = Vec::new();

    for (name, value) in fields {
        body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
        body.extend_from_slice(
            format!(
                "Content-Disposition: form-data; name=\"{}\"\r\n\r\n",
                name.as_ref()
            )
            .as_bytes(),
        );
        body.extend_from_slice(value.as_ref().as_bytes());
        body.extend_from_slice(b"\r\n");
    }

    for file in files {
        body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
        body.extend_from_slice(
            format!(
                "Content-Disposition: form-data; name=\"{}\"; filename=\"{}\"\r\n",
                file.name, file.filename
            )
            .as_bytes(),
        );
        body.extend_from_slice(format!("Content-Type: {}\r\n\r\n", file.content_type).as_bytes());
        body.extend_from_slice(file.bytes);
        body.extend_from_slice(b"\r\n");
    }

    body.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());
    (boundary.to_string(), body)
}
