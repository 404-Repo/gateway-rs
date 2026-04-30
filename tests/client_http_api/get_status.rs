use http::StatusCode;

use crate::support::{
    HOTKEY_SEED_BASE, TestClient, add_failure_result, add_success_result,
    add_success_result_with_worker_id, add_task_prompt, add_task_prompt_with_api_key,
    build_harness, create_personal_api_key, hotkey_from_seed, read_response, sign_worker,
    top_up_personal_api_key_balance,
};

#[tokio::test]
async fn get_status_missing_id() {
    let h = build_harness().await;
    let res = TestClient::get("http://localhost/get_status")
        .add_header("x-api-key", h.api_key.to_string(), true)
        .send(&h.service)
        .await;
    let (status, _headers, _body) = read_response(res).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn get_status_invalid_id() {
    let h = build_harness().await;
    let res = TestClient::get("http://localhost/get_status?id=not-a-uuid")
        .add_header("x-api-key", h.api_key.to_string(), true)
        .send(&h.service)
        .await;
    let (status, _headers, _body) = read_response(res).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn get_status_allows_missing_api_key() {
    let h = build_harness().await;
    let task_id = add_task_prompt(&h, "robot", None).await;
    let res = TestClient::get(format!("http://localhost/get_status?id={task_id}"))
        .send(&h.service)
        .await;
    let (status, _headers, body) = read_response(res).await;
    assert_eq!(status, StatusCode::OK);
    let payload: serde_json::Value = serde_json::from_slice(&body).expect("status json");
    assert_eq!(
        payload.get("status").and_then(|v| v.as_str()),
        Some("NoResult")
    );
}

#[tokio::test]
async fn get_status_ignores_invalid_api_key_format() {
    let h = build_harness().await;
    let task_id = add_task_prompt(&h, "robot", None).await;
    let res = TestClient::get(format!("http://localhost/get_status?id={task_id}"))
        .add_header("x-api-key", "not-a-uuid", true)
        .send(&h.service)
        .await;
    let (status, _headers, body) = read_response(res).await;
    assert_eq!(status, StatusCode::OK);
    let payload: serde_json::Value = serde_json::from_slice(&body).expect("status json");
    assert_eq!(
        payload.get("status").and_then(|v| v.as_str()),
        Some("NoResult")
    );
}

#[tokio::test]
async fn get_status_no_result() {
    let h = build_harness().await;
    let task_id = add_task_prompt(&h, "robot", None).await;

    let res = TestClient::get(format!("http://localhost/get_status?id={task_id}"))
        .add_header("x-api-key", h.api_key.to_string(), true)
        .send(&h.service)
        .await;
    let (status, _headers, body) = read_response(res).await;
    assert_eq!(status, StatusCode::OK);
    let payload: serde_json::Value = serde_json::from_slice(&body).expect("status json");
    assert_eq!(
        payload.get("status").and_then(|v| v.as_str()),
        Some("NoResult")
    );
}

#[tokio::test]
async fn get_status_in_progress_after_worker_takes_task() {
    let h = build_harness().await;
    let task_id = add_task_prompt(&h, "robot", None).await;
    let (worker_hotkey, timestamp, signature) = sign_worker([8u8; 32]);

    let get_tasks = TestClient::post("http://localhost/get_tasks")
        .json(&serde_json::json!({
            "worker_hotkey": worker_hotkey.to_string(),
            "worker_id": "worker-in-progress",
            "signature": signature,
            "timestamp": timestamp,
            "requested_task_count": 1,
            "model": "404-3dgs"
        }))
        .send(&h.service)
        .await;
    let (get_tasks_status, _headers, get_tasks_body) = read_response(get_tasks).await;
    assert_eq!(
        get_tasks_status,
        StatusCode::OK,
        "get_tasks body: {}",
        String::from_utf8_lossy(&get_tasks_body)
    );
    let get_tasks_payload: serde_json::Value =
        serde_json::from_slice(&get_tasks_body).expect("get_tasks json");
    assert_eq!(
        get_tasks_payload
            .get("tasks")
            .and_then(|v| v.as_array())
            .map(Vec::len),
        Some(1)
    );

    let res = TestClient::get(format!("http://localhost/get_status?id={task_id}"))
        .add_header("x-api-key", h.api_key.to_string(), true)
        .send(&h.service)
        .await;
    let (status, _headers, body) = read_response(res).await;
    assert_eq!(status, StatusCode::OK);
    let payload: serde_json::Value = serde_json::from_slice(&body).expect("status json");
    assert_eq!(
        payload.get("status").and_then(|v| v.as_str()),
        Some("InProgress")
    );
}

#[tokio::test]
async fn get_status_partial_result() {
    let h = build_harness().await;
    let task_id = add_task_prompt(&h, "robot", None).await;
    let worker = hotkey_from_seed(HOTKEY_SEED_BASE);
    add_success_result(&h.task_manager, task_id, worker, b"spz".to_vec()).await;

    let res = TestClient::get(format!("http://localhost/get_status?id={task_id}"))
        .add_header("x-api-key", h.api_key.to_string(), true)
        .send(&h.service)
        .await;
    let (status, _headers, body) = read_response(res).await;
    assert_eq!(status, StatusCode::OK);
    let payload: serde_json::Value = serde_json::from_slice(&body).expect("status json");
    assert_eq!(
        payload.get("status").and_then(|v| v.as_str()),
        Some("PartialResult(1)")
    );
}

#[tokio::test]
async fn get_status_first_failure_stays_partial() {
    let h = build_harness().await;
    let task_id = add_task_prompt(&h, "robot", None).await;

    let worker = hotkey_from_seed(HOTKEY_SEED_BASE + 20);
    add_failure_result(&h.task_manager, task_id, worker).await;

    let res = TestClient::get(format!("http://localhost/get_status?id={task_id}"))
        .add_header("x-api-key", h.api_key.to_string(), true)
        .send(&h.service)
        .await;
    let (status, _headers, body) = read_response(res).await;
    assert_eq!(status, StatusCode::OK);
    let payload: serde_json::Value = serde_json::from_slice(&body).expect("status json");
    assert_eq!(
        payload.get("status").and_then(|v| v.as_str()),
        Some("PartialResult(0)")
    );
}

#[tokio::test]
async fn get_status_success() {
    let h = build_harness().await;
    let task_id = add_task_prompt(&h, "robot", None).await;

    let expected = h.config.basic.unique_workers_per_task;
    for idx in 0..expected {
        let worker = hotkey_from_seed(HOTKEY_SEED_BASE + idx as u8);
        add_success_result(
            &h.task_manager,
            task_id,
            worker,
            format!("asset-{idx}").into_bytes(),
        )
        .await;
    }

    let res = TestClient::get(format!("http://localhost/get_status?id={task_id}"))
        .add_header("x-api-key", h.api_key.to_string(), true)
        .send(&h.service)
        .await;
    let (status, _headers, body) = read_response(res).await;
    assert_eq!(status, StatusCode::OK);
    let payload: serde_json::Value = serde_json::from_slice(&body).expect("status json");
    assert_eq!(
        payload.get("status").and_then(|v| v.as_str()),
        Some("Success")
    );
    assert!(payload.get("worker_id").is_some());
}

#[tokio::test]
async fn get_status_worker_id_propagates() {
    let h = build_harness().await;
    let task_id = add_task_prompt(&h, "robot", None).await;

    let expected = h.config.basic.unique_workers_per_task;
    let winner_worker_id = "worker-winner".to_string();
    for idx in 0..expected {
        let worker = hotkey_from_seed(HOTKEY_SEED_BASE + idx as u8);
        let worker_id = if idx == 0 {
            winner_worker_id.clone()
        } else {
            format!("worker-{idx}")
        };
        add_success_result_with_worker_id(
            &h.task_manager,
            task_id,
            worker,
            worker_id,
            format!("asset-{idx}").into_bytes(),
        )
        .await;
    }

    let res = TestClient::get(format!("http://localhost/get_status?id={task_id}"))
        .add_header("x-api-key", h.api_key.to_string(), true)
        .send(&h.service)
        .await;
    let (status, _headers, body) = read_response(res).await;
    assert_eq!(status, StatusCode::OK);
    let payload: serde_json::Value = serde_json::from_slice(&body).expect("status json");
    assert_eq!(
        payload.get("status").and_then(|v| v.as_str()),
        Some("Success")
    );
    assert_eq!(
        payload.get("worker_id").and_then(|v| v.as_str()),
        Some(winner_worker_id.as_str())
    );
}

#[tokio::test]
async fn get_status_failure() {
    let h = build_harness().await;
    let task_id = add_task_prompt(&h, "robot", None).await;

    let expected = h.config.basic.unique_workers_per_task;
    for idx in 0..expected {
        let worker = hotkey_from_seed(HOTKEY_SEED_BASE + 3 + idx as u8);
        add_failure_result(&h.task_manager, task_id, worker).await;
    }

    let res = TestClient::get(format!("http://localhost/get_status?id={task_id}"))
        .add_header("x-api-key", h.api_key.to_string(), true)
        .send(&h.service)
        .await;
    let (status, _headers, body) = read_response(res).await;
    assert_eq!(status, StatusCode::OK);
    let payload: serde_json::Value = serde_json::from_slice(&body).expect("status json");
    assert_eq!(
        payload.get("status").and_then(|v| v.as_str()),
        Some("Failure")
    );
    assert_eq!(
        payload.get("reason").and_then(|v| v.as_str()),
        Some("failed")
    );
}

#[tokio::test]
async fn get_status_ignores_owner_api_key() {
    let h = build_harness().await;
    top_up_personal_api_key_balance(&h, &h.user_api_key, 100_000).await;
    let task_id = add_task_prompt_with_api_key(&h, &h.user_api_key, "robot", None).await;

    let res = TestClient::get(format!("http://localhost/get_status?id={task_id}"))
        .add_header("x-api-key", &h.user_api_key, true)
        .send(&h.service)
        .await;
    let (status, _headers, body) = read_response(res).await;
    assert_eq!(status, StatusCode::OK);
    let payload: serde_json::Value = serde_json::from_slice(&body).expect("status json");
    assert_eq!(
        payload.get("status").and_then(|v| v.as_str()),
        Some("NoResult")
    );
}

#[tokio::test]
async fn get_status_does_not_check_task_owner() {
    let h = build_harness().await;
    top_up_personal_api_key_balance(&h, &h.user_api_key, 100_000).await;
    let task_id = add_task_prompt_with_api_key(&h, &h.user_api_key, "robot", None).await;
    let second_api_key = create_personal_api_key(&h).await;

    let res = TestClient::get(format!("http://localhost/get_status?id={task_id}"))
        .add_header("x-api-key", second_api_key, true)
        .send(&h.service)
        .await;
    let (status, _headers, body) = read_response(res).await;
    assert_eq!(status, StatusCode::OK);
    let payload: serde_json::Value = serde_json::from_slice(&body).expect("status json");
    assert_eq!(
        payload.get("status").and_then(|v| v.as_str()),
        Some("NoResult")
    );
}
