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
