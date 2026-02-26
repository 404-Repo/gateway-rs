use http::StatusCode;
use salvo::test::TestClient;

use crate::support::{build_harness, build_harness_with_gateway_info, read_response};

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
        .send(&h.service)
        .await;
    let (status, _headers, body) = read_response(res).await;
    assert_eq!(status, StatusCode::OK);
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
