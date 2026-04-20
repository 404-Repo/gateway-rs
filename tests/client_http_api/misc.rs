use http::StatusCode;

use crate::support::{
    TestClient, build_harness, build_harness_with_gateway_info, read_response,
    set_personal_api_key_create_limit, try_create_personal_api_key_without_timestamps,
    try_delete_api_key,
};

#[tokio::test]
async fn get_load_success() {
    let h = build_harness().await;
    let res = TestClient::get("http://localhost/get_load")
        .send(&h.service)
        .await;
    let (status, _headers, body) = read_response(res).await;
    assert_eq!(status, StatusCode::OK);
    let payload: serde_json::Value = serde_json::from_slice(&body).expect("json response");
    let gateways = payload.get("gateways").and_then(|v| v.as_array()).unwrap();
    assert_eq!(gateways.len(), 1);
    let domain = gateways[0].get("domain").and_then(|v| v.as_str()).unwrap();
    assert_eq!(domain, h.config.network.domain);
}

#[tokio::test]
async fn get_load_empty() {
    let h = build_harness_with_gateway_info(false).await;
    let res = TestClient::get("http://localhost/get_load")
        .send(&h.service)
        .await;
    let (status, _headers, body) = read_response(res).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "body: {body:?}");
}

#[tokio::test]
async fn get_version_success() {
    let h = build_harness().await;
    let res = TestClient::get("http://localhost/get_version")
        .add_header("x-admin-key", h.admin_key.to_string(), true)
        .send(&h.service)
        .await;
    let (status, _headers, body) = read_response(res).await;
    assert_eq!(status, StatusCode::OK);
    assert!(!body.is_empty());
}

#[tokio::test]
async fn metrics_success_without_admin_key() {
    let h = build_harness().await;
    let res = TestClient::get("http://localhost/metrics")
        .send(&h.service)
        .await;
    let (status, headers, body) = read_response(res).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        headers
            .get("content-type")
            .and_then(|value| value.to_str().ok()),
        Some(prometheus::TEXT_FORMAT)
    );
    assert!(!body.is_empty());
}

#[tokio::test]
async fn id_success() {
    let h = build_harness().await;
    let res = TestClient::get("http://localhost/id")
        .send(&h.service)
        .await;
    let (status, _headers, body) = read_response(res).await;
    assert_eq!(status, StatusCode::OK);
    let body_str = String::from_utf8_lossy(&body);
    assert_eq!(body_str.trim(), h.config.network.node_id.to_string());
}

#[tokio::test]
async fn add_task_size_limit_updates_after_runtime_reload() {
    let h = build_harness().await;
    let payload = serde_json::json!({
        "prompt": "x".repeat(1024),
    });

    let res = TestClient::post("http://localhost/add_task")
        .add_header("x-api-key", h.api_key.to_string(), true)
        .json(&payload)
        .send(&h.service)
        .await;
    let (status_before, _headers, body_before) = read_response(res).await;
    assert_eq!(
        status_before,
        StatusCode::OK,
        "body: {}",
        String::from_utf8_lossy(&body_before)
    );

    let mut updated = h.config.as_ref().clone();
    updated.http.add_task_size_limit = 128;
    let updated_toml = toml::to_string(&updated).expect("serialize updated config");
    std::fs::write(&h.config_path, updated_toml).expect("write updated config");
    assert!(h.runtime_config.reload_from_disk().await);

    let res = TestClient::post("http://localhost/add_task")
        .add_header("x-api-key", h.api_key.to_string(), true)
        .json(&payload)
        .send(&h.service)
        .await;
    let (status_after, _headers, _body_after) = read_response(res).await;
    assert!(matches!(
        status_after,
        StatusCode::BAD_REQUEST | StatusCode::PAYLOAD_TOO_LARGE
    ));
}

#[tokio::test]
async fn deleting_last_non_primary_personal_api_key_is_allowed_when_replacement_can_be_created() {
    let h = build_harness().await;

    let deleted = try_delete_api_key(&h, &h.user_api_key)
        .await
        .expect("deleting the last non-primary personal key should succeed when quota remains");
    assert_eq!(deleted, 1, "expected exactly one api key row to delete");
}

#[tokio::test]
async fn deleting_last_non_primary_personal_api_key_is_rejected_when_creation_quota_is_exhausted() {
    let h = build_harness().await;
    set_personal_api_key_create_limit(&h, 1).await;

    let err = try_delete_api_key(&h, &h.user_api_key).await.expect_err(
        "deleting the last non-primary personal key should fail when quota is exhausted",
    );
    let err_message = format!("{err:#}");
    assert!(err_message.contains("must keep at least one non-primary active personal API key"));
}

#[tokio::test]
async fn personal_api_key_creation_limit_applies_without_explicit_timestamps() {
    let h = build_harness().await;
    set_personal_api_key_create_limit(&h, 1).await;

    let err = try_create_personal_api_key_without_timestamps(&h, &h.user_api_key)
        .await
        .expect_err("creation limit should apply before timestamp trigger fills created_at");

    let err_message = format!("{err:#}");
    assert!(err_message.contains("cannot create more than 1 personal API keys"));
}
