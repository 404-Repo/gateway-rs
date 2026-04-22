use std::time::Duration;

use http::StatusCode;
use uuid::Uuid;

use gateway::test_support::RateLimitContext;

use crate::common::GatewayHarnessOptions;
use crate::support::{
    AddResultMultipartInput, TestClient, build_harness, build_harness_with_options,
    default_rate_ctx, multipart_add_result, purge_terminal_generation_task_in_db, read_response,
    sign_worker, timeout_generation_task_in_db, tiny_png_bytes, wait_for_worker,
};

#[tokio::test]
async fn records_worker_events() {
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
    let payload: serde_json::Value = serde_json::from_slice(&create_body).expect("json response");
    let task_id = payload
        .get("id")
        .and_then(|value| value.as_str())
        .and_then(|value| Uuid::parse_str(value).ok())
        .expect("task id");

    let (worker_hotkey, timestamp, signature) = sign_worker([1u8; 32]);
    let worker_hotkey_str = worker_hotkey.to_string();
    let res = TestClient::post("http://localhost/get_tasks")
        .json(&serde_json::json!({
            "worker_hotkey": worker_hotkey.to_string(),
            "worker_id": "worker-1",
            "signature": signature,
            "timestamp": timestamp,
            "requested_task_count": 1,
            "model": "404-3dgs"
        }))
        .send(&h.service)
        .await;
    let (status, _headers, body) = read_response(res).await;
    assert_eq!(status, StatusCode::OK);
    let get_tasks_payload: serde_json::Value =
        serde_json::from_slice(&body).expect("get_tasks response");
    let assignment_token = get_tasks_payload
        .get("tasks")
        .and_then(|value| value.as_array())
        .and_then(|tasks| tasks.first())
        .and_then(|task| task.get("assignment_token"))
        .and_then(|value| value.as_str())
        .and_then(|value| Uuid::parse_str(value).ok())
        .expect("assignment token");

    let (worker_hotkey, timestamp, signature) = sign_worker([1u8; 32]);
    let (boundary, body) = multipart_add_result(AddResultMultipartInput {
        task_id,
        worker_hotkey: &worker_hotkey,
        worker_id: "worker-1",
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
    let (status, _headers, _body) = read_response(res).await;
    assert_eq!(status, StatusCode::OK);

    let rows = wait_for_worker(&h, 2).await;
    let actions: Vec<_> = rows.iter().map(|r| r.action.as_str()).collect();
    assert!(actions.contains(&"task_assigned"));
    assert!(actions.contains(&"result_success"));

    let assigned = rows
        .iter()
        .find(|r| r.action == "task_assigned")
        .expect("assigned");
    assert_eq!(assigned.task_id, Some(task_id));
    assert_eq!(assigned.task_kind, "txt3d");
    assert_eq!(assigned.model.as_deref(), Some("404-3dgs"));
    assert_eq!(assigned.worker_id.as_deref(), Some("worker-1"));
    assert_eq!(
        assigned.metadata_json["worker_hotkey"].as_str(),
        Some(worker_hotkey_str.as_str())
    );
    assert_eq!(
        assigned.metadata_json["assignment_token"],
        serde_json::json!(assignment_token)
    );

    let success = rows
        .iter()
        .find(|r| r.action == "result_success")
        .expect("success");
    assert_eq!(success.task_id, Some(task_id));
    assert_eq!(success.task_kind, "txt3d");
    assert_eq!(success.model.as_deref(), Some("404-3dgs"));
    assert_eq!(success.worker_id.as_deref(), Some("worker-1"));
    assert_eq!(
        success.metadata_json["worker_hotkey"].as_str(),
        Some(worker_hotkey_str.as_str())
    );
    assert_eq!(
        success.metadata_json["assignment_token"],
        serde_json::json!(assignment_token)
    );
    assert_eq!(
        success.metadata_json["replayed_durable_result"],
        serde_json::json!(false)
    );
}

#[tokio::test]
async fn records_worker_failure_event() {
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
    let payload: serde_json::Value = serde_json::from_slice(&create_body).expect("json response");
    let task_id = payload
        .get("id")
        .and_then(|value| value.as_str())
        .and_then(|value| Uuid::parse_str(value).ok())
        .expect("task id");

    let (worker_hotkey, timestamp, signature) = sign_worker([1u8; 32]);
    let worker_hotkey_str = worker_hotkey.to_string();
    let res = TestClient::post("http://localhost/get_tasks")
        .json(&serde_json::json!({
            "worker_hotkey": worker_hotkey.to_string(),
            "worker_id": "worker-1",
            "signature": signature,
            "timestamp": timestamp,
            "requested_task_count": 1,
            "model": "404-3dgs"
        }))
        .send(&h.service)
        .await;
    let (status, _headers, body) = read_response(res).await;
    assert_eq!(status, StatusCode::OK);
    let get_tasks_payload: serde_json::Value =
        serde_json::from_slice(&body).expect("get_tasks response");
    let assignment_token = get_tasks_payload
        .get("tasks")
        .and_then(|value| value.as_array())
        .and_then(|tasks| tasks.first())
        .and_then(|task| task.get("assignment_token"))
        .and_then(|value| value.as_str())
        .and_then(|value| Uuid::parse_str(value).ok())
        .expect("assignment token");

    let (worker_hotkey, timestamp, signature) = sign_worker([1u8; 32]);
    let (boundary, body) = multipart_add_result(AddResultMultipartInput {
        task_id,
        worker_hotkey: &worker_hotkey,
        worker_id: "worker-1",
        assignment_token,
        timestamp: &timestamp,
        signature: &signature,
        status: "failure",
        asset: None,
        reason: Some("bad model output"),
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
    assert_eq!(status, StatusCode::OK);

    let rows = wait_for_worker(&h, 2).await;
    let failure = rows
        .iter()
        .find(|r| r.action == "result_failure")
        .expect("result_failure");
    assert_eq!(failure.task_id, Some(task_id));
    assert_eq!(failure.task_kind, "txt3d");
    assert_eq!(failure.model.as_deref(), Some("404-3dgs"));
    assert_eq!(failure.reason.as_deref(), Some("bad model output"));
    assert_eq!(failure.worker_id.as_deref(), Some("worker-1"));
    assert_eq!(
        failure.metadata_json["worker_hotkey"].as_str(),
        Some(worker_hotkey_str.as_str())
    );
    assert_eq!(
        failure.metadata_json["assignment_token"],
        serde_json::json!(assignment_token)
    );
    assert_eq!(
        failure.metadata_json["replayed_durable_result"],
        serde_json::json!(false)
    );
}

#[tokio::test]
async fn records_worker_timeout_event() {
    let h = build_harness_with_options(
        RateLimitContext {
            user_id: Some(42),
            has_valid_api_key: true,
            key_is_uuid: true,
            ..default_rate_ctx()
        },
        None,
        GatewayHarnessOptions {
            taskmanager_cleanup_interval_secs: Some(1),
            taskmanager_result_lifetime_secs: Some(1),
            ..GatewayHarnessOptions::default()
        },
    )
    .await;

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
    let payload: serde_json::Value = serde_json::from_slice(&create_body).expect("json response");
    let task_id = payload
        .get("id")
        .and_then(|value| value.as_str())
        .and_then(|value| Uuid::parse_str(value).ok())
        .expect("task id");

    let (worker_hotkey, timestamp, signature) = sign_worker([3u8; 32]);
    let worker_hotkey_str = worker_hotkey.to_string();
    let res = TestClient::post("http://localhost/get_tasks")
        .json(&serde_json::json!({
            "worker_hotkey": worker_hotkey.to_string(),
            "worker_id": "worker-3",
            "signature": signature,
            "timestamp": timestamp,
            "requested_task_count": 1,
            "model": "404-3dgs"
        }))
        .send(&h.service)
        .await;
    let (status, _headers, body) = read_response(res).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "get_tasks body: {}",
        String::from_utf8_lossy(&body)
    );

    tokio::time::sleep(Duration::from_secs(2)).await;

    let rows = wait_for_worker(&h, 2).await;
    let timeout = rows
        .iter()
        .find(|r| r.action == "timeout")
        .expect("timeout");
    assert_eq!(timeout.task_id, Some(task_id));
    assert_eq!(timeout.task_kind, "txt3d");
    assert_eq!(timeout.model.as_deref(), Some("404-3dgs"));
    assert_eq!(timeout.worker_id.as_deref(), Some("worker-3"));
    assert_eq!(
        timeout.metadata_json["worker_hotkey"].as_str(),
        Some(worker_hotkey_str.as_str())
    );
}

#[tokio::test]
async fn worker_event_task_id_survives_generation_task_purge() {
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
    let payload: serde_json::Value = serde_json::from_slice(&create_body).expect("json response");
    let task_id = payload
        .get("id")
        .and_then(|value| value.as_str())
        .and_then(|value| Uuid::parse_str(value).ok())
        .expect("task id");

    let (worker_hotkey, timestamp, signature) = sign_worker([4u8; 32]);
    let res = TestClient::post("http://localhost/get_tasks")
        .json(&serde_json::json!({
            "worker_hotkey": worker_hotkey.to_string(),
            "worker_id": "worker-4",
            "signature": signature,
            "timestamp": timestamp,
            "requested_task_count": 1,
            "model": "404-3dgs"
        }))
        .send(&h.service)
        .await;
    let (status, _headers, body) = read_response(res).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "get_tasks body: {}",
        String::from_utf8_lossy(&body)
    );

    let rows_before_purge = wait_for_worker(&h, 1).await;
    let assigned_before_purge = rows_before_purge
        .iter()
        .find(|row| row.action == "task_assigned")
        .expect("task_assigned before purge");
    assert_eq!(assigned_before_purge.task_id, Some(task_id));

    timeout_generation_task_in_db(&h, task_id).await;

    purge_terminal_generation_task_in_db(&h, task_id).await;

    let rows_after_purge = wait_for_worker(&h, 1).await;
    let assigned = rows_after_purge
        .iter()
        .find(|row| row.action == "task_assigned")
        .expect("task_assigned");
    assert_eq!(assigned.task_id, Some(task_id));
}
