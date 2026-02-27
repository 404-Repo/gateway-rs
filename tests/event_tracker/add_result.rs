use http::StatusCode;
use salvo::test::TestClient;
use uuid::Uuid;

use crate::support::{
    build_harness, current_timestamp, default_rate_ctx, multipart_add_result, multipart_custom,
    read_response, sign_worker, sign_worker_with_timestamp, tiny_png_bytes,
};

#[tokio::test]
async fn add_result_missing_worker_id_returns_bad_request() {
    let h = build_harness(default_rate_ctx(), None).await;
    let task_id = Uuid::new_v4();
    let (worker_hotkey, timestamp, signature) = sign_worker([1u8; 32]);

    let fields = vec![
        ("id".to_string(), task_id.to_string()),
        ("signature".to_string(), signature),
        ("timestamp".to_string(), timestamp),
        ("worker_hotkey".to_string(), worker_hotkey.to_string()),
        ("status".to_string(), "success".to_string()),
    ];
    let (boundary, body) = multipart_custom(fields, Some(("asset", b"spz")));
    let res = TestClient::post("http://localhost/add_result")
        .add_header(
            "content-type",
            format!("multipart/form-data; boundary={}", boundary),
            true,
        )
        .body(body)
        .send(&h.service)
        .await;
    let (status, _headers, _body) = read_response(res).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn add_result_missing_asset_returns_bad_request() {
    let h = build_harness(default_rate_ctx(), None).await;
    let task_id = Uuid::new_v4();
    let (worker_hotkey, timestamp, signature) = sign_worker([1u8; 32]);

    let fields = vec![
        ("id".to_string(), task_id.to_string()),
        ("signature".to_string(), signature),
        ("timestamp".to_string(), timestamp),
        ("worker_hotkey".to_string(), worker_hotkey.to_string()),
        ("worker_id".to_string(), "worker-1".to_string()),
        ("status".to_string(), "success".to_string()),
    ];
    let (boundary, body) = multipart_custom(fields, None);
    let res = TestClient::post("http://localhost/add_result")
        .add_header(
            "content-type",
            format!("multipart/form-data; boundary={}", boundary),
            true,
        )
        .body(body)
        .send(&h.service)
        .await;
    let (status, _headers, _body) = read_response(res).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn add_result_missing_reason_returns_bad_request() {
    let h = build_harness(default_rate_ctx(), None).await;
    let task_id = Uuid::new_v4();
    let (worker_hotkey, timestamp, signature) = sign_worker([1u8; 32]);

    let fields = vec![
        ("id".to_string(), task_id.to_string()),
        ("signature".to_string(), signature),
        ("timestamp".to_string(), timestamp),
        ("worker_hotkey".to_string(), worker_hotkey.to_string()),
        ("worker_id".to_string(), "worker-1".to_string()),
        ("status".to_string(), "failure".to_string()),
    ];
    let (boundary, body) = multipart_custom(fields, None);
    let res = TestClient::post("http://localhost/add_result")
        .add_header(
            "content-type",
            format!("multipart/form-data; boundary={}", boundary),
            true,
        )
        .body(body)
        .send(&h.service)
        .await;
    let (status, _headers, _body) = read_response(res).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn add_result_invalid_status_returns_bad_request() {
    let h = build_harness(default_rate_ctx(), None).await;
    let task_id = Uuid::new_v4();
    let (worker_hotkey, timestamp, signature) = sign_worker([1u8; 32]);

    let fields = vec![
        ("id".to_string(), task_id.to_string()),
        ("signature".to_string(), signature),
        ("timestamp".to_string(), timestamp),
        ("worker_hotkey".to_string(), worker_hotkey.to_string()),
        ("worker_id".to_string(), "worker-1".to_string()),
        ("status".to_string(), "maybe".to_string()),
    ];
    let (boundary, body) = multipart_custom(fields, None);
    let res = TestClient::post("http://localhost/add_result")
        .add_header(
            "content-type",
            format!("multipart/form-data; boundary={}", boundary),
            true,
        )
        .body(body)
        .send(&h.service)
        .await;
    let (status, _headers, _body) = read_response(res).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn add_result_invalid_timestamp_returns_internal() {
    let h = build_harness(default_rate_ctx(), None).await;
    let task_id = Uuid::new_v4();
    let timestamp = "not-a-timestamp";
    let (worker_hotkey, signature) = sign_worker_with_timestamp([1u8; 32], timestamp);

    let (boundary, body) = multipart_add_result(
        task_id,
        &worker_hotkey,
        "worker-1",
        timestamp,
        &signature,
        "success",
        Some(tiny_png_bytes().as_slice()),
        None,
    );
    let res = TestClient::post("http://localhost/add_result")
        .add_header(
            "content-type",
            format!("multipart/form-data; boundary={}", boundary),
            true,
        )
        .body(body)
        .send(&h.service)
        .await;
    let (status, _headers, _body) = read_response(res).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test]
async fn add_result_stale_timestamp_returns_internal() {
    let h = build_harness(default_rate_ctx(), None).await;
    let task_id = Uuid::new_v4();
    let old_timestamp = current_timestamp().saturating_sub(3600);
    let timestamp = format!("404_GATEWAY_{}", old_timestamp);
    let (worker_hotkey, signature) = sign_worker_with_timestamp([1u8; 32], &timestamp);

    let (boundary, body) = multipart_add_result(
        task_id,
        &worker_hotkey,
        "worker-1",
        &timestamp,
        &signature,
        "success",
        Some(tiny_png_bytes().as_slice()),
        None,
    );
    let res = TestClient::post("http://localhost/add_result")
        .add_header(
            "content-type",
            format!("multipart/form-data; boundary={}", boundary),
            true,
        )
        .body(body)
        .send(&h.service)
        .await;
    let (status, _headers, _body) = read_response(res).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test]
async fn add_result_invalid_worker_hotkey_returns_bad_request() {
    let h = build_harness(default_rate_ctx(), None).await;
    let task_id = Uuid::new_v4();
    let (_worker_hotkey, timestamp, signature) = sign_worker([1u8; 32]);

    let fields = vec![
        ("id".to_string(), task_id.to_string()),
        ("signature".to_string(), signature),
        ("timestamp".to_string(), timestamp),
        ("worker_hotkey".to_string(), "not-a-hotkey".to_string()),
        ("worker_id".to_string(), "worker-1".to_string()),
        ("status".to_string(), "success".to_string()),
    ];
    let (boundary, body) = multipart_custom(fields, Some(("asset", b"spz")));
    let res = TestClient::post("http://localhost/add_result")
        .add_header(
            "content-type",
            format!("multipart/form-data; boundary={}", boundary),
            true,
        )
        .body(body)
        .send(&h.service)
        .await;
    let (status, _headers, _body) = read_response(res).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn add_result_not_assigned_returns_unauthorized() {
    let h = build_harness(default_rate_ctx(), None).await;
    let task_id = Uuid::new_v4();
    h.task_manager
        .add_task(gateway::api::Task {
            id: task_id,
            prompt: Some(std::sync::Arc::new("robot".to_string())),
            image: None,
            model: None,
            seed: 0,
            model_params: Some(r#"{"preset":"default"}"#.to_string()),
        })
        .await;

    let (worker_hotkey, timestamp, signature) = sign_worker([1u8; 32]);
    let fields = vec![
        ("id".to_string(), task_id.to_string()),
        ("signature".to_string(), signature),
        ("timestamp".to_string(), timestamp),
        ("worker_hotkey".to_string(), worker_hotkey.to_string()),
        ("worker_id".to_string(), "worker-1".to_string()),
        ("status".to_string(), "success".to_string()),
    ];
    let (boundary, body) = multipart_custom(fields, Some(("asset", b"spz")));
    let res = TestClient::post("http://localhost/add_result")
        .add_header(
            "content-type",
            format!("multipart/form-data; boundary={}", boundary),
            true,
        )
        .body(body)
        .send(&h.service)
        .await;
    let (status, _headers, _body) = read_response(res).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}
