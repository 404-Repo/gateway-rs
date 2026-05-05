use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use base64_simd::STANDARD;
use foldhash::HashSet;
use schnorrkel::{ExpansionMode, MiniSecretKey, context::signing_context};
use serde_json::Value;
use uuid::Uuid;

use gateway::crypto::crypto_provider::ApiKeyHasher;
use gateway::crypto::hotkey::Hotkey;
use gateway::task::TaskManager;
use gateway::test_support::{
    MultipartFilePart, RateLimitContext, build_multipart_form, current_timestamp_secs,
    ensure_test_crypto_provider,
};

use crate::common::real_harness::{
    CompanyKeySeed, GatewayRuntimeAppSettingsUpdate, GatewayRuntimeStart, PersonalKeySeed,
    insert_company_api_key, insert_personal_api_key, update_app_settings_gateway_runtime,
};
use crate::common::{GatewayHarnessOptions, GatewayRuntimeHarness};
pub(crate) use crate::common::{TestClient, read_response, tiny_png_bytes};

const SIGNING_CTX: &[u8] = b"substrate";
const GATEWAY_PREFIX: &str = "404_GATEWAY_";

#[derive(Debug, Clone)]
pub(crate) struct ActivityEventRecord {
    pub(crate) account_id: Option<i64>,
    pub(crate) user_id: Option<i64>,
    pub(crate) company_id: Option<Uuid>,
    pub(crate) company_name: Option<String>,
    pub(crate) action: String,
    pub(crate) event_family: String,
    pub(crate) client_origin: String,
    pub(crate) task_kind: Option<String>,
    pub(crate) model: Option<String>,
    pub(crate) task_id: Option<Uuid>,
}

#[derive(Debug, Clone)]
pub(crate) struct WorkerEventRecord {
    pub(crate) task_id: Option<Uuid>,
    pub(crate) worker_id: Option<String>,
    pub(crate) action: String,
    pub(crate) task_kind: String,
    pub(crate) model: Option<String>,
    pub(crate) reason: Option<String>,
    pub(crate) metadata_json: Value,
}

fn ensure_crypto_provider() {
    ensure_test_crypto_provider();
}

fn api_key_hash_bytes(core: &GatewayRuntimeHarness, api_key: &str) -> [u8; 32] {
    let hasher = ApiKeyHasher::new(&core.config.http.api_key_secret).expect("api key hasher");
    hasher.compute_hash_array(api_key)
}

async fn top_up_personal_api_key_balance(
    core: &GatewayRuntimeHarness,
    api_key: &str,
    amount_cents: i64,
) {
    let key_hash = api_key_hash_bytes(core, api_key);
    let account_row = core
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
    core.db_client
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
    core.gateway
        .sync_db_caches_for_test()
        .await
        .expect("sync db caches");
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

pub(crate) struct AddResultMultipartInput<'a> {
    pub(crate) task_id: Uuid,
    pub(crate) worker_hotkey: &'a Hotkey,
    pub(crate) worker_id: &'a str,
    pub(crate) assignment_token: Uuid,
    pub(crate) timestamp: &'a str,
    pub(crate) signature: &'a str,
    pub(crate) status: &'a str,
    pub(crate) asset: Option<&'a [u8]>,
    pub(crate) reason: Option<&'a str>,
}

pub(crate) fn multipart_add_result(input: AddResultMultipartInput<'_>) -> (String, Vec<u8>) {
    let task_id_str = input.task_id.to_string();
    let assignment_token_str = input.assignment_token.to_string();
    let mut fields = vec![
        ("id", task_id_str.as_str()),
        ("signature", input.signature),
        ("timestamp", input.timestamp),
        ("worker_hotkey", input.worker_hotkey.as_ref()),
        ("worker_id", input.worker_id),
        ("assignment_token", assignment_token_str.as_str()),
        ("status", input.status),
    ];
    if let Some(reason) = input.reason {
        fields.push(("reason", reason));
    }

    let mut files = Vec::new();
    if let Some(asset) = input.asset {
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
    pub(crate) service: crate::common::TestService,
    pub(crate) task_manager: TaskManager,
    pub(crate) api_key: String,
    core: GatewayRuntimeHarness,
}

pub(crate) async fn build_harness(
    rate_ctx: RateLimitContext,
    worker_whitelist: Option<HashSet<Hotkey>>,
) -> EventHarness {
    build_harness_with_options(rate_ctx, worker_whitelist, GatewayHarnessOptions::default()).await
}

pub(crate) async fn build_harness_with_options(
    rate_ctx: RateLimitContext,
    worker_whitelist: Option<HashSet<Hotkey>>,
    mut options: GatewayHarnessOptions,
) -> EventHarness {
    ensure_crypto_provider();

    options.include_gateway_info = true;
    options.worker_whitelist = worker_whitelist;

    let GatewayRuntimeStart {
        harness: core,
        runtime_config: _runtime_config,
        config_path: _config_path,
        generic_key,
        admin_key: _admin_key,
        default_user_api_key,
        ..
    } = GatewayRuntimeHarness::start(options).await;

    let api_key = if rate_ctx.is_company_key {
        let company = rate_ctx.company.as_ref().expect("company limits");
        let api_key = Uuid::new_v4().to_string();
        let hasher = ApiKeyHasher::new(&core.config.http.api_key_secret).expect("api key hasher");
        insert_company_api_key(
            &core.db_client,
            &hasher,
            CompanyKeySeed {
                api_key: &api_key,
                company_id: company.id,
                company_name: company.name.as_ref(),
                concurrent_limit: company.concurrent_limit.min(i32::MAX as u64) as i32,
                daily_limit: company.daily_limit.min(i32::MAX as u64) as i32,
                balance_cents: 100_000,
            },
        )
        .await
        .expect("insert company key");
        core.gateway
            .sync_db_caches_for_test()
            .await
            .expect("sync db caches");
        api_key
    } else if rate_ctx.is_generic_key {
        generic_key.to_string()
    } else if rate_ctx.key_is_uuid {
        if rate_ctx.user_email.is_none() && rate_ctx.user_limits.is_none() {
            top_up_personal_api_key_balance(&core, &default_user_api_key, 100_000).await;
            default_user_api_key
        } else {
            let api_key = Uuid::new_v4().to_string();
            let (concurrent_limit, daily_limit): (u64, u64) =
                rate_ctx.user_limits.unwrap_or((1, 10));
            let hasher =
                ApiKeyHasher::new(&core.config.http.api_key_secret).expect("api key hasher");
            insert_personal_api_key(
                &core.db_client,
                &hasher,
                PersonalKeySeed {
                    api_key: &api_key,
                    user_email: rate_ctx.user_email.as_deref(),
                    concurrent_limit: concurrent_limit.min(i32::MAX as u64) as i32,
                    daily_limit: daily_limit.min(i32::MAX as u64) as i32,
                    balance_cents: 100_000,
                },
            )
            .await
            .expect("insert personal key");
            core.gateway
                .sync_db_caches_for_test()
                .await
                .expect("sync db caches");
            api_key
        }
    } else {
        generic_key.to_string()
    };

    EventHarness {
        service: core.service.clone(),
        task_manager: core.gateway.task_manager(),
        api_key,
        core,
    }
}

pub(crate) async fn set_request_file_size_limit(
    harness: &EventHarness,
    request_file_size_limit: i64,
) {
    update_app_settings_gateway_runtime(
        &harness.core.db_client,
        GatewayRuntimeAppSettingsUpdate {
            generic_key: harness.core.config.http.generic_key.expect("generic key"),
            global_limit: 0,
            per_ip_limit: 0,
            unauthorized_per_ip_limit: harness
                .core
                .config
                .http
                .add_task_unauthorized_per_ip_daily_rate_limit
                as i32,
            rate_limit_whitelist: harness
                .core
                .config
                .http
                .rate_limit_whitelist
                .iter()
                .cloned()
                .collect(),
            max_task_queue_len: harness.core.config.http.max_task_queue_len as i32,
            request_file_size_limit,
            guest_generation_limit: 1,
            guest_window_ms: 86_400_000,
            registered_generation_limit: 0,
            registered_window_ms: 86_400_000,
        },
    )
    .await
    .expect("update gateway runtime settings");
    harness
        .core
        .gateway
        .sync_db_caches_for_test()
        .await
        .expect("sync db caches");
}

pub(crate) fn default_rate_ctx() -> RateLimitContext {
    RateLimitContext {
        user_id: Some(42),
        has_valid_api_key: true,
        key_is_uuid: true,
        ..RateLimitContext::default()
    }
}

pub(crate) async fn wait_for_activity(
    harness: &EventHarness,
    expected: usize,
) -> Vec<ActivityEventRecord> {
    for _ in 0..50 {
        harness.core.gateway.flush_events_for_test().await;
        let rows = fetch_activity_events(&harness.core)
            .await
            .expect("fetch activity events");
        if rows.len() >= expected {
            return rows;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    fetch_activity_events(&harness.core)
        .await
        .expect("fetch activity events")
}

pub(crate) async fn wait_for_worker(
    harness: &EventHarness,
    expected: usize,
) -> Vec<WorkerEventRecord> {
    for _ in 0..50 {
        harness.core.gateway.flush_events_for_test().await;
        let rows = fetch_worker_events(&harness.core)
            .await
            .expect("fetch worker events");
        if rows.len() >= expected {
            return rows;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    fetch_worker_events(&harness.core)
        .await
        .expect("fetch worker events")
}

pub(crate) async fn analytics_foreign_key_columns(harness: &EventHarness) -> Vec<(String, String)> {
    let rows = harness
        .core
        .db_client
        .query(
            "SELECT tc.table_name, kcu.column_name
             FROM information_schema.table_constraints AS tc
             JOIN information_schema.key_column_usage AS kcu
               ON tc.constraint_name = kcu.constraint_name
              AND tc.table_schema = kcu.table_schema
              AND tc.table_name = kcu.table_name
             WHERE tc.constraint_type = 'FOREIGN KEY'
               AND tc.table_schema = 'public'
               AND tc.table_name IN ('activity_events', 'worker_events')
             ORDER BY tc.table_name ASC, kcu.ordinal_position ASC, kcu.column_name ASC",
            &[],
        )
        .await
        .expect("query analytics foreign keys");
    rows.into_iter()
        .map(|row| (row.get("table_name"), row.get("column_name")))
        .collect()
}

pub(crate) async fn purge_terminal_generation_task_in_db(harness: &EventHarness, task_id: Uuid) {
    let completed_before_ms = current_timestamp() as i64 * 1000;
    let rows = harness
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

pub(crate) async fn timeout_generation_task_in_db(harness: &EventHarness, task_id: Uuid) {
    let now = current_timestamp() as i64 * 1000;
    let updated = harness
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

    let rows = harness
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

pub(crate) async fn record_assignment_in_memory_and_db(
    harness: &EventHarness,
    task_id: Uuid,
    worker: &Hotkey,
    worker_id: &str,
) -> Uuid {
    let task_ids = vec![task_id];
    let assigned = harness
        .core
        .db_client
        .query(
            "SELECT task_id, assignment_token
             FROM generation_record_task_assignments($1, $2, $3, $4)",
            &[
                &task_ids,
                &worker.as_ref(),
                &worker_id,
                &(current_timestamp() as i64 * 1000),
            ],
        )
        .await
        .expect("record generation task assignment");
    let assigned_task_ids: Vec<Uuid> = assigned.iter().map(|row| row.get("task_id")).collect();
    assert_eq!(
        assigned_task_ids,
        vec![task_id],
        "assignment should be recorded in SQL"
    );
    let assignment_token: Uuid = assigned[0].get("assignment_token");
    harness
        .task_manager
        .record_assignment_with_token(
            task_id,
            worker.clone(),
            Arc::<str>::from(worker_id.to_string()),
            assignment_token,
        )
        .await;
    assignment_token
}

pub(crate) async fn finalize_assignment_in_db_for_replay(
    harness: &EventHarness,
    task_id: Uuid,
    worker: &Hotkey,
    worker_id: &str,
    assignment_token: Uuid,
    success: bool,
) {
    let result_metadata_json = serde_json::json!({
        "worker_hotkey": worker.as_ref(),
        "worker_id": worker_id,
        "assignment_token": assignment_token,
        "success": success,
        "gateway_name": harness.core.config.network.name.clone(),
    })
    .to_string();
    let row = harness
        .core
        .db_client
        .query_one(
            "SELECT assignment_outcome
             FROM generation_finalize_task_assignment($1, $2, $3, $4, $5, $6, $7::TEXT::jsonb, $8)",
            &[
                &task_id,
                &worker.as_ref(),
                &worker_id,
                &assignment_token,
                &if success { "succeeded" } else { "failed" },
                &if success {
                    None::<String>
                } else {
                    Some("worker failed".to_string())
                },
                &result_metadata_json,
                &(current_timestamp() as i64 * 1000),
            ],
        )
        .await
        .expect("finalize generation task assignment for replay setup");
    let outcome: String = row.get("assignment_outcome");
    assert!(
        outcome == "applied",
        "expected replay setup finalize to apply once, got {:?}",
        outcome
    );
}

async fn fetch_activity_events(
    harness: &GatewayRuntimeHarness,
) -> Result<Vec<ActivityEventRecord>> {
    let rows = harness
        .db_client
        .query(
            "SELECT task_id, account_id, user_id, company_id, action, event_family, client_origin, task_kind, model, metadata_json::TEXT AS metadata_json FROM activity_events ORDER BY created_at ASC, id ASC",
            &[],
        )
        .await
        .context("query activity events")?;
    rows.into_iter()
        .map(|row| {
            let metadata_text: String = row.get("metadata_json");
            let metadata: Value = serde_json::from_str(&metadata_text).unwrap_or(Value::Null);
            Ok(ActivityEventRecord {
                task_id: row.get("task_id"),
                account_id: row.get("account_id"),
                user_id: row.get("user_id"),
                company_id: row.get("company_id"),
                company_name: metadata
                    .get("companyName")
                    .and_then(|value| value.as_str())
                    .map(str::to_string),
                action: row.get("action"),
                event_family: row.get("event_family"),
                client_origin: row.get("client_origin"),
                task_kind: row.get("task_kind"),
                model: row.get("model"),
            })
        })
        .collect()
}

async fn fetch_worker_events(harness: &GatewayRuntimeHarness) -> Result<Vec<WorkerEventRecord>> {
    let rows = harness
        .db_client
        .query(
            "SELECT task_id, worker_id, action, task_kind, model, reason, metadata_json::TEXT AS metadata_json FROM worker_events ORDER BY created_at ASC, id ASC",
            &[],
        )
        .await
        .context("query worker events")?;
    Ok(rows
        .into_iter()
        .map(|row| {
            let metadata_text: String = row.get("metadata_json");
            let metadata_json: Value = serde_json::from_str(&metadata_text).unwrap_or(Value::Null);
            WorkerEventRecord {
                task_id: row.get("task_id"),
                worker_id: row.get("worker_id"),
                action: row.get("action"),
                task_kind: row.get("task_kind"),
                model: row.get("model"),
                reason: row.get("reason"),
                metadata_json,
            }
        })
        .collect())
}
