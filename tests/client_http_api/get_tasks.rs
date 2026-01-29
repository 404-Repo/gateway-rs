use std::sync::Arc;

use foldhash::{HashSet, HashSetExt};
use http::StatusCode;
use salvo::test::TestClient;
use uuid::Uuid;

use gateway::api::Task;
use gateway::crypto::hotkey::Hotkey;

use crate::support::{
    build_harness, build_harness_with_worker_whitelist, read_response, sign_worker,
};

#[tokio::test]
async fn get_tasks_invalid_signature_returns_internal() {
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
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
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
    let task_id = Uuid::new_v4();
    let task = Task {
        id: task_id,
        prompt: Some(Arc::new("robot".to_string())),
        image: None,
        model: None,
    };
    h.task_manager.add_task(task.clone()).await;
    h.task_queue.push(task);

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
async fn get_tasks_requested_count_large_returns_available() {
    let h = build_harness().await;
    let task_id = Uuid::new_v4();
    let task = Task {
        id: task_id,
        prompt: Some(Arc::new("robot".to_string())),
        image: None,
        model: None,
    };
    h.task_manager.add_task(task.clone()).await;
    h.task_queue.push(task);

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
}

#[tokio::test]
async fn get_tasks_single_model_filters_results() {
    let h = build_harness().await;
    let task_a = Task {
        id: Uuid::new_v4(),
        prompt: Some(Arc::new("robot".to_string())),
        image: None,
        model: Some("404-3dgs".to_string()),
    };
    let task_b = Task {
        id: Uuid::new_v4(),
        prompt: Some(Arc::new("car".to_string())),
        image: None,
        model: Some("404-mesh".to_string()),
    };
    h.task_manager.add_task(task_a.clone()).await;
    h.task_manager.add_task(task_b.clone()).await;
    h.task_queue.push(task_a);
    h.task_queue.push(task_b);

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
    let task_a = Task {
        id: Uuid::new_v4(),
        prompt: Some(Arc::new("robot".to_string())),
        image: None,
        model: Some("404-3dgs".to_string()),
    };
    let task_b = Task {
        id: Uuid::new_v4(),
        prompt: Some(Arc::new("car".to_string())),
        image: None,
        model: Some("404-mesh".to_string()),
    };
    h.task_manager.add_task(task_a.clone()).await;
    h.task_manager.add_task(task_b.clone()).await;
    h.task_queue.push(task_a);
    h.task_queue.push(task_b);

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
