use foldhash::{HashSet, HashSetExt};
use gateway::crypto::hotkey::Hotkey;
use http::StatusCode;

use crate::support::{
    TestClient, add_task_prompt, add_task_prompt_with_api_key, build_harness,
    build_harness_with_worker_whitelist, create_personal_api_key, multipart_body,
    purge_terminal_generation_task_in_db, read_response, sign_worker,
    timeout_generation_task_in_db, tiny_png_bytes, top_up_personal_api_key_balance,
    update_personal_api_key_limits_without_timestamp,
};

#[tokio::test]
async fn get_tasks_invalid_signature_returns_unauthorized() {
    let h = build_harness().await;
    let (worker_hotkey, timestamp, _signature) = sign_worker([1u8; 32]);
    let res = TestClient::post("http://localhost/get_tasks")
        .json(&serde_json::json!({
            "worker_hotkey": worker_hotkey.to_string(),
            "worker_id": "worker-1",
            "signature": "invalid-signature",
            "timestamp": timestamp,
            "requested_task_count": 1,
            "model": "404-3dgs"
        }))
        .send(&h.service)
        .await;
    let (status, _headers, _body) = read_response(res).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn get_tasks_invalid_model_returns_bad_request() {
    let h = build_harness().await;
    let (worker_hotkey, timestamp, signature) = sign_worker([1u8; 32]);
    let res = TestClient::post("http://localhost/get_tasks")
        .json(&serde_json::json!({
            "worker_hotkey": worker_hotkey.to_string(),
            "worker_id": "worker-1",
            "signature": signature,
            "timestamp": timestamp,
            "requested_task_count": 1,
            "model": "missing-model"
        }))
        .send(&h.service)
        .await;
    let (status, _headers, _body) = read_response(res).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn get_tasks_whitelist_rejects_worker() {
    let mut whitelist = HashSet::new();
    whitelist.insert(Hotkey::from_bytes(&[9u8; 32]));
    let h = build_harness_with_worker_whitelist(whitelist).await;
    let (worker_hotkey, timestamp, signature) = sign_worker([1u8; 32]);
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
    let (status, _headers, _body) = read_response(res).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn get_tasks_requested_count_zero_returns_empty() {
    let h = build_harness().await;
    top_up_personal_api_key_balance(&h, &h.user_api_key, 100_000).await;
    let _task_id = add_task_prompt_with_api_key(&h, &h.user_api_key, "robot", None).await;

    let (worker_hotkey, timestamp, signature) = sign_worker([1u8; 32]);
    let res = TestClient::post("http://localhost/get_tasks")
        .json(&serde_json::json!({
            "worker_hotkey": worker_hotkey.to_string(),
            "worker_id": "worker-1",
            "signature": signature,
            "timestamp": timestamp,
            "requested_task_count": 0,
            "model": "404-3dgs"
        }))
        .send(&h.service)
        .await;
    let (status, _headers, body) = read_response(res).await;
    assert_eq!(status, StatusCode::OK);
    let payload: serde_json::Value = serde_json::from_slice(&body).expect("json response");
    let tasks = payload
        .get("tasks")
        .and_then(|v| v.as_array())
        .expect("tasks");
    assert_eq!(tasks.len(), 0);
}

#[tokio::test]
async fn get_tasks_assigns_generic_guest_task() {
    let h = build_harness().await;
    let task_id = add_task_prompt(&h, "robot", None).await;

    let (worker_hotkey, timestamp, signature) = sign_worker([8u8; 32]);
    let res = TestClient::post("http://localhost/get_tasks")
        .json(&serde_json::json!({
            "worker_hotkey": worker_hotkey.to_string(),
            "worker_id": "worker-guest",
            "signature": signature,
            "timestamp": timestamp,
            "requested_task_count": 1,
            "model": "404-3dgs"
        }))
        .send(&h.service)
        .await;
    let (status, _headers, body) = read_response(res).await;
    assert_eq!(status, StatusCode::OK);
    let payload: serde_json::Value = serde_json::from_slice(&body).expect("json response");
    let tasks = payload
        .get("tasks")
        .and_then(|v| v.as_array())
        .expect("tasks");
    assert_eq!(tasks.len(), 1);
    let returned_task_id = tasks[0]
        .get("id")
        .and_then(|v| v.as_str())
        .and_then(|v| uuid::Uuid::parse_str(v).ok())
        .expect("task id");
    assert_eq!(returned_task_id, task_id);
}

#[tokio::test]
async fn get_tasks_requested_count_large_returns_available() {
    let h = build_harness().await;
    top_up_personal_api_key_balance(&h, &h.user_api_key, 100_000).await;
    let create = TestClient::post("http://localhost/add_task")
        .add_header("x-api-key", &h.user_api_key, true)
        .json(&serde_json::json!({
            "prompt": "robot",
            "model_params": {"quality": "high"}
        }))
        .send(&h.service)
        .await;
    let (create_status, _headers, create_body) = read_response(create).await;
    assert_eq!(
        create_status,
        StatusCode::OK,
        "add_task body: {}",
        String::from_utf8_lossy(&create_body)
    );

    let (worker_hotkey, timestamp, signature) = sign_worker([1u8; 32]);
    let res = TestClient::post("http://localhost/get_tasks")
        .json(&serde_json::json!({
            "worker_hotkey": worker_hotkey.to_string(),
            "worker_id": "worker-1",
            "signature": signature,
            "timestamp": timestamp,
            "requested_task_count": 100,
            "model": "404-3dgs"
        }))
        .send(&h.service)
        .await;
    let (status, _headers, body) = read_response(res).await;
    assert_eq!(status, StatusCode::OK);
    let payload: serde_json::Value = serde_json::from_slice(&body).expect("json response");
    let tasks = payload
        .get("tasks")
        .and_then(|v| v.as_array())
        .expect("tasks");
    assert_eq!(tasks.len(), 1);
    assert!(
        tasks[0]
            .get("model_params")
            .and_then(|value| value.as_object())
            .is_some()
    );
}

#[tokio::test]
async fn get_tasks_single_model_filters_results() {
    let h = build_harness().await;
    let second_api_key = create_personal_api_key(&h).await;
    top_up_personal_api_key_balance(&h, &h.user_api_key, 100_000).await;
    top_up_personal_api_key_balance(&h, &second_api_key, 100_000).await;
    let create_a = TestClient::post("http://localhost/add_task")
        .add_header("x-api-key", &h.user_api_key, true)
        .json(&serde_json::json!({
            "prompt": "robot",
            "model": "404-3dgs",
            "model_params": {"preset": "fast"}
        }))
        .send(&h.service)
        .await;
    let (status_a, _headers, body_a) = read_response(create_a).await;
    assert_eq!(
        status_a,
        StatusCode::OK,
        "task_a body: {}",
        String::from_utf8_lossy(&body_a)
    );
    let image = tiny_png_bytes();
    let (boundary_b, body_b) = multipart_body(
        None,
        Some(&image),
        Some("404-mesh"),
        None,
        Some(r#"{"preset":"high_quality"}"#),
    );
    let create_b = TestClient::post("http://localhost/add_task")
        .add_header("x-api-key", &second_api_key, true)
        .add_header(
            "content-type",
            format!("multipart/form-data; boundary={boundary_b}"),
            true,
        )
        .body(body_b)
        .send(&h.service)
        .await;
    let (status_b, _headers, body_b) = read_response(create_b).await;
    assert_eq!(
        status_b,
        StatusCode::OK,
        "task_b body: {}",
        String::from_utf8_lossy(&body_b)
    );

    let (worker_hotkey, timestamp, signature) = sign_worker([1u8; 32]);
    let res = TestClient::post("http://localhost/get_tasks")
        .json(&serde_json::json!({
            "worker_hotkey": worker_hotkey.to_string(),
            "worker_id": "worker-1",
            "signature": signature,
            "timestamp": timestamp,
            "requested_task_count": 10,
            "model": "404-3dgs"
        }))
        .send(&h.service)
        .await;
    let (status, _headers, body) = read_response(res).await;
    assert_eq!(status, StatusCode::OK);
    let payload: serde_json::Value = serde_json::from_slice(&body).expect("json response");
    let tasks = payload
        .get("tasks")
        .and_then(|v| v.as_array())
        .expect("tasks");
    assert_eq!(tasks.len(), 1);
    let model = tasks[0]
        .get("model")
        .and_then(|v| v.as_str())
        .expect("model");
    assert_eq!(model, "404-3dgs");
}

#[tokio::test]
async fn get_tasks_multiple_models_returns_all_matches() {
    let h = build_harness().await;
    let second_api_key = create_personal_api_key(&h).await;
    top_up_personal_api_key_balance(&h, &h.user_api_key, 100_000).await;
    top_up_personal_api_key_balance(&h, &second_api_key, 100_000).await;
    let create_a = TestClient::post("http://localhost/add_task")
        .add_header("x-api-key", &h.user_api_key, true)
        .json(&serde_json::json!({
            "prompt": "robot",
            "model": "404-3dgs",
            "model_params": {"preset": "fast"}
        }))
        .send(&h.service)
        .await;
    let (status_a, _headers, body_a) = read_response(create_a).await;
    assert_eq!(
        status_a,
        StatusCode::OK,
        "task_a body: {}",
        String::from_utf8_lossy(&body_a)
    );
    let image = tiny_png_bytes();
    let (boundary_b, body_b) = multipart_body(
        None,
        Some(&image),
        Some("404-mesh"),
        None,
        Some(r#"{"preset":"high_quality"}"#),
    );
    let create_b = TestClient::post("http://localhost/add_task")
        .add_header("x-api-key", &second_api_key, true)
        .add_header(
            "content-type",
            format!("multipart/form-data; boundary={boundary_b}"),
            true,
        )
        .body(body_b)
        .send(&h.service)
        .await;
    let (status_b, _headers, body_b) = read_response(create_b).await;
    assert_eq!(
        status_b,
        StatusCode::OK,
        "task_b body: {}",
        String::from_utf8_lossy(&body_b)
    );

    let (worker_hotkey, timestamp, signature) = sign_worker([1u8; 32]);
    let res = TestClient::post("http://localhost/get_tasks")
        .json(&serde_json::json!({
            "worker_hotkey": worker_hotkey.to_string(),
            "worker_id": "worker-1",
            "signature": signature,
            "timestamp": timestamp,
            "requested_task_count": 10,
            "model": ["404-3dgs", "404-mesh"]
        }))
        .send(&h.service)
        .await;
    let (status, _headers, body) = read_response(res).await;
    assert_eq!(status, StatusCode::OK);
    let payload: serde_json::Value = serde_json::from_slice(&body).expect("json response");
    let tasks = payload
        .get("tasks")
        .and_then(|v| v.as_array())
        .expect("tasks");
    assert_eq!(tasks.len(), 2);
    let models: HashSet<&str> = tasks
        .iter()
        .filter_map(|task| task.get("model").and_then(|v| v.as_str()))
        .collect();
    assert!(models.contains("404-3dgs"));
    assert!(models.contains("404-mesh"));
}

#[tokio::test]
async fn get_tasks_retires_db_timed_out_task_without_reoffering_it() {
    let h = build_harness().await;
    top_up_personal_api_key_balance(&h, &h.user_api_key, 100_000).await;
    let task_id = add_task_prompt_with_api_key(&h, &h.user_api_key, "robot", None).await;
    timeout_generation_task_in_db(&h, task_id).await;

    let (worker_hotkey, timestamp, signature) = sign_worker([1u8; 32]);
    let first = TestClient::post("http://localhost/get_tasks")
        .json(&serde_json::json!({
            "worker_hotkey": worker_hotkey.to_string(),
            "worker_id": "worker-1",
            "signature": signature,
            "timestamp": timestamp,
            "requested_task_count": 10,
            "model": "404-3dgs"
        }))
        .send(&h.service)
        .await;
    let (first_status, _headers, first_body) = read_response(first).await;
    assert_eq!(first_status, StatusCode::OK);
    let first_payload: serde_json::Value =
        serde_json::from_slice(&first_body).expect("json response");
    let first_tasks = first_payload
        .get("tasks")
        .and_then(|v| v.as_array())
        .expect("tasks");
    assert_eq!(first_tasks.len(), 0);

    let (worker_hotkey, timestamp, signature) = sign_worker([2u8; 32]);
    let second = TestClient::post("http://localhost/get_tasks")
        .json(&serde_json::json!({
            "worker_hotkey": worker_hotkey.to_string(),
            "worker_id": "worker-2",
            "signature": signature,
            "timestamp": timestamp,
            "requested_task_count": 10,
            "model": "404-3dgs"
        }))
        .send(&h.service)
        .await;
    let (second_status, _headers, second_body) = read_response(second).await;
    assert_eq!(second_status, StatusCode::OK);
    let second_payload: serde_json::Value =
        serde_json::from_slice(&second_body).expect("json response");
    let second_tasks = second_payload
        .get("tasks")
        .and_then(|v| v.as_array())
        .expect("tasks");
    assert_eq!(second_tasks.len(), 0);
}

#[tokio::test]
async fn get_tasks_returns_only_live_tasks_when_batch_contains_purged_task() {
    let h = build_harness().await;
    top_up_personal_api_key_balance(&h, &h.user_api_key, 100_000).await;
    update_personal_api_key_limits_without_timestamp(&h, &h.user_api_key, 10, 10).await;
    let stale_task_id =
        add_task_prompt_with_api_key(&h, &h.user_api_key, "stale robot", None).await;
    let live_task_id = add_task_prompt_with_api_key(&h, &h.user_api_key, "live robot", None).await;
    timeout_generation_task_in_db(&h, stale_task_id).await;
    purge_terminal_generation_task_in_db(&h, stale_task_id).await;

    let (worker_hotkey, timestamp, signature) = sign_worker([3u8; 32]);
    let first = TestClient::post("http://localhost/get_tasks")
        .json(&serde_json::json!({
            "worker_hotkey": worker_hotkey.to_string(),
            "worker_id": "worker-3",
            "signature": signature,
            "timestamp": timestamp,
            "requested_task_count": 10,
            "model": "404-3dgs"
        }))
        .send(&h.service)
        .await;
    let (first_status, _headers, first_body) = read_response(first).await;
    assert_eq!(first_status, StatusCode::OK);
    let first_payload: serde_json::Value =
        serde_json::from_slice(&first_body).expect("json response");
    let first_tasks = first_payload
        .get("tasks")
        .and_then(|v| v.as_array())
        .expect("tasks");
    assert_eq!(first_tasks.len(), 1);
    let returned_task_id = first_tasks[0]
        .get("id")
        .and_then(|v| v.as_str())
        .and_then(|v| uuid::Uuid::parse_str(v).ok())
        .expect("task id");
    assert_eq!(returned_task_id, live_task_id);
    assert_ne!(returned_task_id, stale_task_id);

    let (worker_hotkey, timestamp, signature) = sign_worker([4u8; 32]);
    let second = TestClient::post("http://localhost/get_tasks")
        .json(&serde_json::json!({
            "worker_hotkey": worker_hotkey.to_string(),
            "worker_id": "worker-4",
            "signature": signature,
            "timestamp": timestamp,
            "requested_task_count": 10,
            "model": "404-3dgs"
        }))
        .send(&h.service)
        .await;
    let (second_status, _headers, second_body) = read_response(second).await;
    assert_eq!(second_status, StatusCode::OK);
    let second_payload: serde_json::Value =
        serde_json::from_slice(&second_body).expect("json response");
    let second_tasks = second_payload
        .get("tasks")
        .and_then(|v| v.as_array())
        .expect("tasks");
    let second_task_ids: HashSet<uuid::Uuid> = second_tasks
        .iter()
        .filter_map(|task| task.get("id").and_then(|v| v.as_str()))
        .filter_map(|value| uuid::Uuid::parse_str(value).ok())
        .collect();
    assert!(
        !second_task_ids.contains(&stale_task_id),
        "purged task should never be re-offered to later workers"
    );
}
