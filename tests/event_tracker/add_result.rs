use http::StatusCode;
use uuid::Uuid;

use crate::support::{
    AddResultMultipartInput, TestClient, build_harness, current_timestamp, default_rate_ctx,
    finalize_assignment_in_db_for_replay, multipart_add_result, multipart_custom, read_response,
    record_assignment_in_memory_and_db, set_request_file_size_limit, sign_worker,
    sign_worker_with_timestamp, tiny_png_bytes, wait_for_worker,
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
async fn add_result_file_size_limit_updates_after_gateway_sync() {
    let h = build_harness(default_rate_ctx(), None).await;
    set_request_file_size_limit(&h, 8).await;
    let task_id = Uuid::new_v4();
    let (worker_hotkey, timestamp, signature) = sign_worker([7u8; 32]);

    let large_asset = [1u8; 64];
    let (boundary, body) = multipart_add_result(AddResultMultipartInput {
        task_id,
        worker_hotkey: &worker_hotkey,
        worker_id: "worker-1",
        assignment_token: Uuid::nil(),
        timestamp: &timestamp,
        signature: &signature,
        status: "success",
        asset: Some(large_asset.as_slice()),
        reason: None,
    });
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
    assert!(matches!(
        status,
        StatusCode::BAD_REQUEST | StatusCode::PAYLOAD_TOO_LARGE
    ));
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
async fn add_result_invalid_timestamp_returns_bad_request() {
    let h = build_harness(default_rate_ctx(), None).await;
    let task_id = Uuid::new_v4();
    let timestamp = "not-a-timestamp";
    let (worker_hotkey, signature) = sign_worker_with_timestamp([1u8; 32], timestamp);

    let (boundary, body) = multipart_add_result(AddResultMultipartInput {
        task_id,
        worker_hotkey: &worker_hotkey,
        worker_id: "worker-1",
        assignment_token: Uuid::nil(),
        timestamp,
        signature: &signature,
        status: "success",
        asset: Some(tiny_png_bytes().as_slice()),
        reason: None,
    });
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
async fn add_result_stale_timestamp_returns_unauthorized() {
    let h = build_harness(default_rate_ctx(), None).await;
    let task_id = Uuid::new_v4();
    let old_timestamp = current_timestamp().saturating_sub(3600);
    let timestamp = format!("404_GATEWAY_{}", old_timestamp);
    let (worker_hotkey, signature) = sign_worker_with_timestamp([1u8; 32], &timestamp);

    let (boundary, body) = multipart_add_result(AddResultMultipartInput {
        task_id,
        worker_hotkey: &worker_hotkey,
        worker_id: "worker-1",
        assignment_token: Uuid::nil(),
        timestamp: &timestamp,
        signature: &signature,
        status: "success",
        asset: Some(tiny_png_bytes().as_slice()),
        reason: None,
    });
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
    let create = TestClient::post("http://localhost/add_task")
        .add_header("x-api-key", h.api_key.to_string(), true)
        .json(&serde_json::json!({"prompt": "robot"}))
        .send(&h.service)
        .await;
    let (create_status, _headers, create_body) = read_response(create).await;
    assert_eq!(
        create_status,
        StatusCode::OK,
        "add_task body: {}",
        String::from_utf8_lossy(&create_body)
    );
    let payload: serde_json::Value =
        serde_json::from_slice(&create_body).expect("task creation response");
    let task_id = payload
        .get("id")
        .and_then(|value| value.as_str())
        .and_then(|value| Uuid::parse_str(value).ok())
        .expect("task id");

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

#[tokio::test]
async fn add_result_missing_task_context_returns_gone() {
    let h = build_harness(default_rate_ctx(), None).await;
    let create = TestClient::post("http://localhost/add_task")
        .add_header("x-api-key", h.api_key.to_string(), true)
        .json(&serde_json::json!({"prompt": "robot"}))
        .send(&h.service)
        .await;
    let (create_status, _headers, create_body) = read_response(create).await;
    assert_eq!(
        create_status,
        StatusCode::OK,
        "add_task body: {}",
        String::from_utf8_lossy(&create_body)
    );
    let payload: serde_json::Value =
        serde_json::from_slice(&create_body).expect("task creation response");
    let task_id = payload
        .get("id")
        .and_then(|value| value.as_str())
        .and_then(|value| Uuid::parse_str(value).ok())
        .expect("task id");

    h.task_manager.finalize_task(task_id).await;

    let (worker_hotkey, timestamp, signature) = sign_worker([9u8; 32]);
    let (boundary, body) = multipart_add_result(AddResultMultipartInput {
        task_id,
        worker_hotkey: &worker_hotkey,
        worker_id: "worker-9",
        assignment_token: Uuid::nil(),
        timestamp: &timestamp,
        signature: &signature,
        status: "success",
        asset: Some(tiny_png_bytes().as_slice()),
        reason: None,
    });
    let res = TestClient::post("http://localhost/add_result")
        .add_header(
            "content-type",
            format!("multipart/form-data; boundary={}", boundary),
            true,
        )
        .body(body)
        .send(&h.service)
        .await;
    let (status, headers, body) = read_response(res).await;
    assert_eq!(status, StatusCode::GONE);
    assert_eq!(
        headers
            .get(http::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok()),
        Some("application/json; charset=utf-8")
    );

    let payload: serde_json::Value = serde_json::from_slice(&body).expect("gone json body");
    assert_eq!(payload["error"], "task_context_unavailable");
    assert_eq!(
        payload["message"],
        "Task context is unavailable on this gateway after restart; the task will be refunded on timeout."
    );
}

#[tokio::test]
async fn add_result_duplicate_replay_restores_local_result_and_records_worker_event() {
    let h = build_harness(default_rate_ctx(), None).await;
    let create = TestClient::post("http://localhost/add_task")
        .add_header("x-api-key", h.api_key.to_string(), true)
        .json(&serde_json::json!({"prompt": "robot"}))
        .send(&h.service)
        .await;
    let (create_status, _headers, create_body) = read_response(create).await;
    assert_eq!(
        create_status,
        StatusCode::OK,
        "add_task body: {}",
        String::from_utf8_lossy(&create_body)
    );
    let payload: serde_json::Value =
        serde_json::from_slice(&create_body).expect("task creation response");
    let task_id = payload
        .get("id")
        .and_then(|value| value.as_str())
        .and_then(|value| Uuid::parse_str(value).ok())
        .expect("task id");

    let (worker_hotkey, timestamp, signature) = sign_worker([10u8; 32]);
    let worker_id = "worker-replay";
    let assignment_token =
        record_assignment_in_memory_and_db(&h, task_id, &worker_hotkey, worker_id).await;
    finalize_assignment_in_db_for_replay(
        &h,
        task_id,
        &worker_hotkey,
        worker_id,
        assignment_token,
        true,
    )
    .await;

    assert!(
        h.task_manager.get_result(task_id).await.is_none(),
        "precondition: local result should still be missing before replay"
    );
    assert!(
        h.task_manager.get_prompt(task_id).await.is_some(),
        "precondition: task should still look active locally before replay restore"
    );

    let (boundary, body) = multipart_add_result(AddResultMultipartInput {
        task_id,
        worker_hotkey: &worker_hotkey,
        worker_id,
        assignment_token,
        timestamp: &timestamp,
        signature: &signature,
        status: "success",
        asset: Some(tiny_png_bytes().as_slice()),
        reason: None,
    });
    let res = TestClient::post("http://localhost/add_result")
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
        "duplicate replay add_result body: {}",
        String::from_utf8_lossy(&body)
    );

    let result_bundle = h
        .task_manager
        .get_result(task_id)
        .await
        .expect("duplicate replay should restore the result locally");
    assert_eq!(result_bundle.results.len(), 1);

    let rows = wait_for_worker(&h, 1).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].task_id, Some(task_id));
    assert_eq!(rows[0].worker_id.as_deref(), Some(worker_id));
    assert_eq!(rows[0].action, "result_success");
}
