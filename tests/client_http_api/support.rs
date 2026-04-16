use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use base64_simd::STANDARD;
use foldhash::HashSet;
use http::StatusCode;
use schnorrkel::{ExpansionMode, MiniSecretKey, context::signing_context};
use uuid::Uuid;

use gateway::api::request::AddTaskResultRequest;
use gateway::common::queue::TaskQueue;
use gateway::config::NodeConfig;
use gateway::config_runtime::RuntimeConfigStore;
use gateway::crypto::crypto_provider::ApiKeyHasher;
use gateway::crypto::hotkey::Hotkey;
use gateway::db::ApiKeyLookup;
use gateway::raft::rate_limit::RateLimitPolicies;
use gateway::task::TaskManager;
use gateway::test_support::{
    MultipartFilePart, build_multipart_form, current_timestamp_secs, ensure_test_crypto_provider,
};

use crate::common::real_harness::{
    GatewayRuntimeAppSettingsUpdate, GatewayRuntimeStart, PersonalKeySeed, insert_personal_api_key,
    update_app_settings_gateway_runtime, update_app_settings_generic_key,
};
use crate::common::{GatewayHarnessOptions, GatewayRuntimeHarness};
pub(crate) use crate::common::{TestClient, TestService, read_response, tiny_png_bytes};

pub(crate) const HOTKEY_SEED_BASE: u8 = 42;
const SIGNING_CTX: &[u8] = b"substrate";
const GATEWAY_PREFIX: &str = "404_GATEWAY_";

pub(crate) struct TestHarness {
    pub(crate) service: TestService,
    pub(crate) api_key: Uuid,
    pub(crate) user_api_key: String,
    pub(crate) company_api_key: String,
    pub(crate) company_id: Uuid,
    pub(crate) admin_key: Uuid,
    pub(crate) task_queue: TaskQueue,
    pub(crate) task_manager: TaskManager,
    pub(crate) config: Arc<NodeConfig>,
    pub(crate) runtime_config: Arc<RuntimeConfigStore>,
    pub(crate) config_path: PathBuf,
    core: GatewayRuntimeHarness,
}

pub(crate) struct GenerationTaskLifecycleRow {
    pub(crate) billing_mode: String,
    pub(crate) reservation_status: String,
    pub(crate) task_status: String,
    pub(crate) account_id: Option<i64>,
    pub(crate) user_id: Option<i64>,
    pub(crate) company_id: Option<Uuid>,
    pub(crate) api_key_id: Option<i64>,
}

fn ensure_crypto_provider() {
    ensure_test_crypto_provider();
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
    data.extend_from_slice(
        b"ply
",
    );
    data.extend_from_slice(
        b"format binary_little_endian 1.0
",
    );
    data.extend_from_slice(
        b"element vertex 1
",
    );
    data.extend_from_slice(
        b"property float x
",
    );
    data.extend_from_slice(
        b"property float y
",
    );
    data.extend_from_slice(
        b"property float z
",
    );
    data.extend_from_slice(
        b"property float f_dc_0
",
    );
    data.extend_from_slice(
        b"property float f_dc_1
",
    );
    data.extend_from_slice(
        b"property float f_dc_2
",
    );
    data.extend_from_slice(
        b"property float opacity
",
    );
    data.extend_from_slice(
        b"property float scale_0
",
    );
    data.extend_from_slice(
        b"property float scale_1
",
    );
    data.extend_from_slice(
        b"property float scale_2
",
    );
    data.extend_from_slice(
        b"property float rot_0
",
    );
    data.extend_from_slice(
        b"property float rot_1
",
    );
    data.extend_from_slice(
        b"property float rot_2
",
    );
    data.extend_from_slice(
        b"property float rot_3
",
    );
    data.extend_from_slice(
        b"end_header
",
    );

    let values = [
        0.0f32, 0.0, 0.0, 1.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0,
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

pub(crate) async fn build_harness() -> TestHarness {
    build_harness_with_options(GatewayHarnessOptions::default()).await
}

pub(crate) async fn build_harness_with_gateway_info(include_gateway: bool) -> TestHarness {
    build_harness_with_options(GatewayHarnessOptions {
        include_gateway_info: include_gateway,
        ..GatewayHarnessOptions::default()
    })
    .await
}

pub(crate) async fn build_harness_with_worker_whitelist(
    worker_whitelist: HashSet<Hotkey>,
) -> TestHarness {
    build_harness_with_options(GatewayHarnessOptions {
        worker_whitelist: Some(worker_whitelist),
        ..GatewayHarnessOptions::default()
    })
    .await
}

pub(crate) async fn build_harness_with_options(options: GatewayHarnessOptions) -> TestHarness {
    ensure_crypto_provider();

    let GatewayRuntimeStart {
        harness: core,
        runtime_config,
        config_path,
        generic_key,
        admin_key,
        default_user_api_key,
        default_company_api_key,
        company_id,
    } = GatewayRuntimeHarness::start(options).await;

    TestHarness {
        service: core.service.clone(),
        api_key: generic_key,
        user_api_key: default_user_api_key.clone(),
        company_api_key: default_company_api_key.clone(),
        company_id,
        admin_key,
        task_queue: core.gateway.task_queue(),
        task_manager: core.gateway.task_manager(),
        config: Arc::clone(&core.config),
        runtime_config,
        config_path,
        core,
    }
}

pub(crate) async fn add_task_prompt(h: &TestHarness, prompt: &str, model: Option<&str>) -> Uuid {
    add_task_prompt_with_api_key(h, &h.api_key.to_string(), prompt, model).await
}

pub(crate) async fn add_task_prompt_with_api_key(
    h: &TestHarness,
    api_key: &str,
    prompt: &str,
    model: Option<&str>,
) -> Uuid {
    let mut payload = serde_json::json!({ "prompt": prompt });
    if let Some(model) = model {
        payload["model"] = serde_json::Value::String(model.to_string());
    }
    let res = TestClient::post("http://localhost/add_task")
        .add_header("x-api-key", api_key, true)
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
            format!("multipart/form-data; boundary={boundary}"),
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

pub(crate) async fn set_generic_rate_limit_policies(h: &TestHarness, policies: RateLimitPolicies) {
    update_app_settings_generic_key(
        &h.core.db_client,
        h.api_key,
        policies.generic_global_daily_limit as i32,
        policies.generic_per_ip_daily_limit as i32,
    )
    .await
    .expect("update generic key policies");
    h.core
        .gateway
        .sync_db_caches_for_test()
        .await
        .expect("sync db caches");
}

pub(crate) async fn set_gateway_runtime_settings(
    h: &TestHarness,
    generic_policies: RateLimitPolicies,
    unauthorized_per_ip_daily_limit: i32,
    rate_limit_whitelist: &[String],
    max_task_queue_len: i32,
    request_file_size_limit: i64,
) {
    update_app_settings_gateway_runtime(
        &h.core.db_client,
        GatewayRuntimeAppSettingsUpdate {
            generic_key: h.api_key,
            global_limit: generic_policies.generic_global_daily_limit as i32,
            per_ip_limit: generic_policies.generic_per_ip_daily_limit as i32,
            unauthorized_per_ip_limit: unauthorized_per_ip_daily_limit,
            rate_limit_whitelist: rate_limit_whitelist.to_vec(),
            max_task_queue_len,
            request_file_size_limit,
            guest_generation_limit: 1,
            guest_window_ms: 86_400_000,
            registered_generation_limit: 0,
            registered_window_ms: 86_400_000,
        },
    )
    .await
    .expect("update gateway runtime settings");
    h.core
        .gateway
        .sync_db_caches_for_test()
        .await
        .expect("sync db caches");
}

fn api_key_hash_bytes(h: &TestHarness, api_key: &str) -> [u8; 32] {
    let hasher = ApiKeyHasher::new(&h.core.config.http.api_key_secret).expect("api key hasher");
    hasher.compute_hash_array(api_key)
}

async fn sync_db_caches(h: &TestHarness) {
    h.core
        .gateway
        .sync_db_caches_for_test()
        .await
        .expect("sync db caches");
}

pub(crate) async fn update_personal_api_key_limits_without_timestamp_no_sync(
    h: &TestHarness,
    api_key: &str,
    concurrent_limit: i32,
    daily_limit: i32,
) {
    let key_hash = api_key_hash_bytes(h, api_key);
    let updated = h
        .core
        .db_client
        .execute(
            "UPDATE users
             SET task_limit_concurrent = $1,
                 task_limit_daily = $2
             WHERE id = (
               SELECT user_id
               FROM api_keys
               WHERE api_key_hash = $3
                 AND user_id IS NOT NULL
               LIMIT 1
             )",
            &[&concurrent_limit, &daily_limit, &&key_hash[..]],
        )
        .await
        .expect("update personal key limits without timestamp");
    assert_eq!(updated, 1, "expected exactly one user row to update");
}

pub(crate) async fn update_personal_api_key_limits_without_timestamp(
    h: &TestHarness,
    api_key: &str,
    concurrent_limit: i32,
    daily_limit: i32,
) {
    update_personal_api_key_limits_without_timestamp_no_sync(
        h,
        api_key,
        concurrent_limit,
        daily_limit,
    )
    .await;
    sync_db_caches(h).await;
}

pub(crate) async fn set_registered_free_settings_no_sync(
    h: &TestHarness,
    registered_generation_limit: i32,
    registered_window_ms: i64,
) {
    let updated = h
        .core
        .db_client
        .execute(
            "UPDATE app_settings
             SET registered_generation_limit = $1,
                 registered_window_ms = $2,
                 updated_at = FLOOR(EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT
             WHERE id = 1",
            &[&registered_generation_limit, &registered_window_ms],
        )
        .await
        .expect("update registered free settings without timestamp");
    assert_eq!(
        updated, 1,
        "expected exactly one app_settings row to update"
    );
}

pub(crate) async fn set_registered_free_settings(
    h: &TestHarness,
    registered_generation_limit: i32,
    registered_window_ms: i64,
) {
    set_registered_free_settings_no_sync(h, registered_generation_limit, registered_window_ms)
        .await;
    sync_db_caches(h).await;
}

pub(crate) async fn set_guest_free_settings_no_sync(
    h: &TestHarness,
    guest_generation_limit: i32,
    guest_window_ms: i64,
) {
    let updated = h
        .core
        .db_client
        .execute(
            "UPDATE app_settings
             SET guest_generation_limit = $1,
                 guest_window_ms = $2,
                 updated_at = FLOOR(EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT
             WHERE id = 1",
            &[&guest_generation_limit, &guest_window_ms],
        )
        .await
        .expect("update guest free settings without timestamp");
    assert_eq!(
        updated, 1,
        "expected exactly one app_settings row to update"
    );
}

pub(crate) async fn set_guest_free_settings(
    h: &TestHarness,
    guest_generation_limit: i32,
    guest_window_ms: i64,
) {
    set_guest_free_settings_no_sync(h, guest_generation_limit, guest_window_ms).await;
    sync_db_caches(h).await;
}

pub(crate) async fn update_company_api_key_limits_without_timestamp(
    h: &TestHarness,
    api_key: &str,
    concurrent_limit: i32,
    daily_limit: i32,
) {
    let key_hash = api_key_hash_bytes(h, api_key);
    let updated = h
        .core
        .db_client
        .execute(
            "UPDATE companies
             SET task_limit_concurrent = $1,
                 task_limit_daily = $2
             WHERE id = (
               SELECT company_id
               FROM api_keys
               WHERE api_key_hash = $3
                 AND company_id IS NOT NULL
               LIMIT 1
             )",
            &[&concurrent_limit, &daily_limit, &&key_hash[..]],
        )
        .await
        .expect("update company key limits without timestamp");
    assert_eq!(updated, 1, "expected exactly one company row to update");
    sync_db_caches(h).await;
}

pub(crate) async fn revoke_api_key_without_timestamp_no_sync(h: &TestHarness, api_key: &str) {
    let key_hash = api_key_hash_bytes(h, api_key);
    let updated = h
        .core
        .db_client
        .execute(
            "UPDATE api_keys
             SET revoked_at = FLOOR(EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT
             WHERE api_key_hash = $1
               AND revoked_at IS NULL",
            &[&&key_hash[..]],
        )
        .await
        .expect("revoke api key without timestamp");
    assert_eq!(updated, 1, "expected exactly one api_keys row to update");
}

pub(crate) async fn revoke_api_key_without_timestamp(h: &TestHarness, api_key: &str) {
    revoke_api_key_without_timestamp_no_sync(h, api_key).await;
    sync_db_caches(h).await;
}

pub(crate) async fn lookup_api_key(h: &TestHarness, api_key: &str) -> ApiKeyLookup {
    h.core.gateway.lookup_api_key_for_test(api_key).await
}

pub(crate) async fn create_personal_api_key(h: &TestHarness) -> String {
    let api_key = Uuid::new_v4().to_string();
    let hasher = ApiKeyHasher::new(&h.core.config.http.api_key_secret).expect("api key hasher");
    insert_personal_api_key(
        &h.core.db_client,
        &hasher,
        PersonalKeySeed {
            api_key: &api_key,
            user_email: None,
            concurrent_limit: 1,
            daily_limit: 10,
            balance_cents: 0,
        },
    )
    .await
    .expect("insert personal key");
    h.core
        .gateway
        .sync_db_caches_for_test()
        .await
        .expect("sync db caches");
    api_key
}

pub(crate) async fn top_up_personal_api_key_balance(
    h: &TestHarness,
    api_key: &str,
    amount_cents: i64,
) {
    let key_hash = api_key_hash_bytes(h, api_key);
    let account_row = h
        .core
        .db_client
        .query_one(
            "SELECT account_id
             FROM api_keys
             WHERE api_key_hash = $1
               AND user_id IS NOT NULL
             LIMIT 1",
            &[&&key_hash[..]],
        )
        .await
        .expect("select personal account for topup");
    let account_id: i64 = account_row.get("account_id");
    let now = current_timestamp() as i64 * 1000;
    let request_id = format!("gateway-test-topup-{}", Uuid::new_v4());
    h.core
        .db_client
        .query_one(
            "SELECT ledger_id
             FROM gen_apply_account_topup($1, $2, NULL, $3, $4, $5)",
            &[
                &account_id,
                &amount_cents,
                &request_id,
                &"gateway test topup",
                &now,
            ],
        )
        .await
        .expect("apply personal api key topup");
    sync_db_caches(h).await;
}

pub(crate) async fn timeout_generation_task_in_db(h: &TestHarness, task_id: Uuid) {
    let now = current_timestamp() as i64 * 1000;
    let updated = h
        .core
        .db_client
        .execute(
            "UPDATE generation_tasks
             SET deadline_at = $2
             WHERE id = $1",
            &[&task_id, &(now - 1)],
        )
        .await
        .expect("move generation task deadline into the past");
    assert_eq!(
        updated, 1,
        "expected exactly one generation task row to update"
    );

    let rows = h
        .core
        .db_client
        .query(
            "SELECT task_id
             FROM generation_expire_tasks($1, $2)",
            &[&100_i32, &now],
        )
        .await
        .expect("expire generation tasks");
    assert!(
        rows.iter()
            .any(|row| row.get::<_, Uuid>("task_id") == task_id),
        "expected generation_expire_tasks to expire task {task_id}"
    );
}

pub(crate) async fn purge_terminal_generation_task_in_db(h: &TestHarness, task_id: Uuid) {
    let completed_before_ms = current_timestamp() as i64 * 1000;
    let rows = h
        .core
        .db_client
        .query(
            "SELECT task_id
             FROM generation_purge_terminal_tasks($1, $2)",
            &[&100_i32, &completed_before_ms],
        )
        .await
        .expect("purge terminal generation tasks");
    assert!(
        rows.iter()
            .any(|row| row.get::<_, Uuid>("task_id") == task_id),
        "expected generation_purge_terminal_tasks to purge task {task_id}"
    );
}

pub(crate) async fn fetch_generation_task_lifecycle(
    h: &TestHarness,
    task_id: Uuid,
) -> Option<GenerationTaskLifecycleRow> {
    let row = h
        .core
        .db_client
        .query_opt(
            "SELECT
               reservation_row.billing_mode,
               reservation_row.status AS reservation_status,
               task_row.status AS task_status,
               task_row.account_id,
               task_row.user_id,
               task_row.company_id,
               task_row.api_key_id
             FROM generation_tasks AS task_row
             INNER JOIN generation_reservations AS reservation_row
               ON reservation_row.id = task_row.reservation_id
             WHERE task_row.id = $1
             LIMIT 1",
            &[&task_id],
        )
        .await
        .expect("fetch generation task lifecycle");
    row.map(|row| GenerationTaskLifecycleRow {
        billing_mode: row.get("billing_mode"),
        reservation_status: row.get("reservation_status"),
        task_status: row.get("task_status"),
        account_id: row.get("account_id"),
        user_id: row.get("user_id"),
        company_id: row.get("company_id"),
        api_key_id: row.get("api_key_id"),
    })
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
    let worker_id_ref: Arc<str> = worker_id.clone().into();
    task_manager
        .record_assignment(task_id, worker.clone(), worker_id_ref.clone())
        .await;
    let result = AddTaskResultRequest {
        worker_hotkey: worker.clone(),
        worker_id: worker_id_ref,
        assignment_token: Uuid::nil(),
        asset: Some(asset.into()),
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
        assignment_token: Uuid::nil(),
        asset: None,
        reason: Some(Arc::<str>::from("failed")),
        instant: Instant::now(),
    };
    task_manager
        .add_result(task_id, result)
        .await
        .expect("add result");
}
