use std::net::{IpAddr, Ipv4Addr};
use std::time::Duration;

use http::StatusCode;
use serde_json::Value;
use tokio::time::{Instant, sleep};
use uuid::Uuid;

use gateway::crypto::crypto_provider::GuestIpHasher;

use crate::common::{
    GatewayHarnessOptions, GatewayRuntimeHarness, TestClient, TestService, read_response,
};

fn loopback_ip(last_octet: u8) -> IpAddr {
    IpAddr::V4(Ipv4Addr::new(127, 0, 0, last_octet))
}

fn service_for_ip(harness: &GatewayRuntimeHarness, last_octet: u8) -> TestService {
    harness.service.with_local_address(loopback_ip(last_octet))
}

async fn post_add_task(service: &TestService, api_key: &str, prompt: &str) -> (StatusCode, Value) {
    let response = TestClient::post("http://localhost/add_task")
        .add_header("x-api-key", api_key, true)
        .json(&serde_json::json!({ "prompt": prompt }))
        .send(service)
        .await;
    let (status, _headers, body) = read_response(response).await;
    let payload: Value = serde_json::from_slice(&body).unwrap_or_else(|err| {
        panic!(
            "json response parse failed with status {}: {} ({err})",
            status,
            String::from_utf8_lossy(&body),
        )
    });
    (status, payload)
}

async fn create_generic_task(service: &TestService, api_key: &str, prompt: &str) -> Uuid {
    let (status, payload) = post_add_task(service, api_key, prompt).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "generic add_task failed: {payload:?}"
    );
    let task_id = payload.get("id").and_then(Value::as_str).expect("task id");
    Uuid::parse_str(task_id).expect("uuid task id")
}

async fn wait_for_generic_success(
    service: &TestService,
    api_key: &str,
    prompt: &str,
    timeout: Duration,
) -> Uuid {
    let deadline = Instant::now() + timeout;
    loop {
        let (status, payload) = post_add_task(service, api_key, prompt).await;
        if status == StatusCode::OK {
            let task_id = payload.get("id").and_then(Value::as_str).expect("task id");
            return Uuid::parse_str(task_id).expect("uuid task id");
        }
        assert_eq!(
            status,
            StatusCode::TOO_MANY_REQUESTS,
            "unexpected retry status while waiting for generic slot release: {payload:?}"
        );
        if Instant::now() >= deadline {
            panic!("generic request did not unblock before timeout: {payload:?}");
        }
        sleep(Duration::from_millis(500)).await;
    }
}

async fn set_generic_rate_limit_settings(
    harness: &GatewayRuntimeHarness,
    generic_key: Uuid,
    global_limit: i32,
    per_ip_limit: i32,
    window_ms: i64,
) {
    let updated = harness
        .db_client
        .execute(
            "UPDATE app_settings
             SET gateway_generic_key = $1,
                 gateway_generic_global_daily_limit = $2,
                 gateway_generic_per_ip_daily_limit = $3,
                 gateway_generic_window_ms = $4,
                 updated_at = FLOOR(EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT
             WHERE id = 1",
            &[&generic_key, &global_limit, &per_ip_limit, &window_ms],
        )
        .await
        .expect("update generic rate limit settings");
    assert_eq!(updated, 1, "expected exactly one app_settings row");
    harness
        .gateway
        .sync_db_caches_for_test()
        .await
        .expect("sync db caches");
}

#[derive(Debug)]
struct GenericSubmitResult {
    ok: bool,
    error_code: Option<String>,
    error_message: Option<String>,
}

fn current_time_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_millis() as i64
}

fn generic_key_hash(secret: &str, subject: &str) -> Vec<u8> {
    GuestIpHasher::new(secret)
        .expect("guest ip hasher")
        .compute_hash_128(subject)
        .to_vec()
}

async fn submit_generic_task_with_now(
    harness: &GatewayRuntimeHarness,
    now_ms: i64,
    global_limit: i32,
    per_ip_limit: i32,
    window_ms: i64,
    key_hash: &[u8],
) -> GenericSubmitResult {
    let deadline_at_ms = now_ms + 60_000;
    let row = harness
        .db_client
        .query_one(
            "SELECT ok, error_code, error_message
             FROM generation_submit_task(
               $1,
               NULL,
               NULL,
               NULL,
               NULL,
               'text_to_3d',
               '404-3dgs',
               1,
               $2,
               'test-node',
               'guest_generic',
               '{}'::jsonb,
               NULL,
               NULL,
               $3,
               NULL,
               NULL,
               NULL,
               'generic_key',
               $4,
               $5,
               $6,
               $7
             )",
            &[
                &Uuid::new_v4(),
                &deadline_at_ms,
                &now_ms,
                &global_limit,
                &per_ip_limit,
                &window_ms,
                &&key_hash[..],
            ],
        )
        .await
        .expect("submit generic guest task");
    GenericSubmitResult {
        ok: row.get("ok"),
        error_code: row.get("error_code"),
        error_message: row.get("error_message"),
    }
}

#[tokio::test]
async fn generic_key_concurrent_limit_returns_concurrent_limit() {
    let start = GatewayRuntimeHarness::start(GatewayHarnessOptions {
        generic_key_concurrent_limit: Some(1),
        ..GatewayHarnessOptions::default()
    })
    .await;
    set_generic_rate_limit_settings(&start.harness, start.generic_key, 10, 10, 86_400_000).await;

    let api_key = start.generic_key.to_string();
    let _task_id = create_generic_task(&start.harness.service, api_key.as_str(), "robot").await;

    let (status, payload) =
        post_add_task(&start.harness.service, api_key.as_str(), "robot again").await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(payload["error"], "concurrent_limit");
    assert_eq!(
        payload["message"],
        "Generic key concurrent task limit exceeded."
    );
}

#[tokio::test]
async fn generic_key_default_concurrent_limit_is_two() {
    let start = GatewayRuntimeHarness::start(GatewayHarnessOptions::default()).await;
    set_generic_rate_limit_settings(&start.harness, start.generic_key, 10, 10, 86_400_000).await;

    let api_key = start.generic_key.to_string();
    let _task_a = create_generic_task(&start.harness.service, api_key.as_str(), "robot-a").await;
    let _task_b = create_generic_task(&start.harness.service, api_key.as_str(), "robot-b").await;

    let (status, payload) =
        post_add_task(&start.harness.service, api_key.as_str(), "robot-c").await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(payload["error"], "concurrent_limit");
    assert_eq!(
        payload["message"],
        "Generic key concurrent task limit exceeded."
    );
}

#[tokio::test]
async fn generic_key_concurrent_limit_is_shared_across_source_ips_end_to_end() {
    let start = GatewayRuntimeHarness::start(GatewayHarnessOptions {
        generic_key_concurrent_limit: Some(1),
        ..GatewayHarnessOptions::default()
    })
    .await;
    set_generic_rate_limit_settings(&start.harness, start.generic_key, 10, 10, 86_400_000).await;

    let api_key = start.generic_key.to_string();
    let service_a = service_for_ip(&start.harness, 6);
    let service_b = service_for_ip(&start.harness, 7);

    let _task_a = create_generic_task(&service_a, api_key.as_str(), "robot-a").await;

    let (status_b, payload_b) = post_add_task(&service_b, api_key.as_str(), "robot-b").await;
    assert_eq!(status_b, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(payload_b["error"], "concurrent_limit");
    assert_eq!(
        payload_b["message"],
        "Generic key concurrent task limit exceeded."
    );
}

#[tokio::test]
async fn generic_key_concurrent_limit_recovers_after_restart_timeout_end_to_end() {
    let start = GatewayRuntimeHarness::start(GatewayHarnessOptions {
        taskmanager_cleanup_interval_secs: Some(1),
        taskmanager_result_lifetime_secs: Some(2),
        generic_key_concurrent_limit: Some(1),
        ..GatewayHarnessOptions::default()
    })
    .await;
    set_generic_rate_limit_settings(&start.harness, start.generic_key, 10, 10, 86_400_000).await;

    let api_key = start.generic_key.to_string();
    let _task_id = create_generic_task(&start.harness.service, api_key.as_str(), "robot").await;

    let (blocked_status, blocked_payload) =
        post_add_task(&start.harness.service, api_key.as_str(), "robot again").await;
    assert_eq!(blocked_status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(blocked_payload["error"], "concurrent_limit");
    assert_eq!(
        blocked_payload["message"],
        "Generic key concurrent task limit exceeded."
    );

    let restarted = start.restart().await;

    let (post_restart_status, post_restart_payload) = post_add_task(
        &restarted.harness.service,
        api_key.as_str(),
        "robot after restart",
    )
    .await;
    assert_eq!(post_restart_status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(post_restart_payload["error"], "concurrent_limit");
    assert_eq!(
        post_restart_payload["message"],
        "Generic key concurrent task limit exceeded."
    );

    let _task_after_timeout = wait_for_generic_success(
        &restarted.harness.service,
        api_key.as_str(),
        "robot after timeout",
        Duration::from_secs(12),
    )
    .await;
}

#[tokio::test]
async fn generic_key_global_rolling_window_unblocks_after_expiration() {
    let start = GatewayRuntimeHarness::start(GatewayHarnessOptions::default()).await;
    let now_ms = current_time_ms();
    let key_hash = generic_key_hash(
        start.harness.config.http.api_key_secret.as_str(),
        "global-ip",
    );

    let first =
        submit_generic_task_with_now(&start.harness, now_ms, 1, 10, 60_000, &key_hash).await;
    assert!(
        first.ok,
        "first generic rolling-window task should succeed: {first:?}"
    );

    let blocked =
        submit_generic_task_with_now(&start.harness, now_ms + 1, 1, 10, 60_000, &key_hash).await;
    assert!(
        !blocked.ok,
        "second global generic task should be blocked: {blocked:?}"
    );
    assert_eq!(blocked.error_code.as_deref(), Some("daily_limit"));
    assert_eq!(
        blocked.error_message.as_deref(),
        Some("Generic key global rolling limit exceeded.")
    );

    let after_expiry =
        submit_generic_task_with_now(&start.harness, now_ms + 60_001, 1, 10, 60_000, &key_hash)
            .await;
    assert!(
        after_expiry.ok,
        "generic global rolling window should unblock after expiration: {after_expiry:?}"
    );
}

#[tokio::test]
async fn generic_key_per_ip_rolling_window_unblocks_after_expiration() {
    let start = GatewayRuntimeHarness::start(GatewayHarnessOptions::default()).await;
    let now_ms = current_time_ms();
    let key_hash = generic_key_hash(start.harness.config.http.api_key_secret.as_str(), "per-ip");

    let first =
        submit_generic_task_with_now(&start.harness, now_ms, 10, 1, 60_000, &key_hash).await;
    assert!(
        first.ok,
        "first generic per-ip rolling task should succeed: {first:?}"
    );

    let blocked =
        submit_generic_task_with_now(&start.harness, now_ms + 1, 10, 1, 60_000, &key_hash).await;
    assert!(
        !blocked.ok,
        "second per-ip generic task should be blocked: {blocked:?}"
    );
    assert_eq!(blocked.error_code.as_deref(), Some("daily_limit"));
    assert_eq!(
        blocked.error_message.as_deref(),
        Some("Generic key per-IP rolling limit exceeded.")
    );

    let after_expiry =
        submit_generic_task_with_now(&start.harness, now_ms + 60_001, 10, 1, 60_000, &key_hash)
            .await;
    assert!(
        after_expiry.ok,
        "generic per-ip rolling window should unblock after expiration: {after_expiry:?}"
    );
}
