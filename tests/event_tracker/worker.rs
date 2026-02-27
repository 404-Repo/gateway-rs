use std::sync::Arc;
use std::time::Duration;

use http::StatusCode;
use salvo::test::TestClient;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use gateway::api::Task;
use gateway::crypto::hotkey::Hotkey;
use gateway::db::{EventRecorder, EventSinkHandle, InMemoryEventSink};
use gateway::metrics::Metrics;
use gateway::task::TaskManager;

use crate::support::{
    build_harness, default_rate_ctx, multipart_add_result, read_response, sign_worker,
    tiny_png_bytes, wait_for_worker,
};

#[tokio::test]
async fn records_worker_events() {
    let h = build_harness(default_rate_ctx(), None).await;

    let task_id = Uuid::new_v4();
    let task = Task {
        id: task_id,
        prompt: Some(Arc::new("robot".to_string())),
        image: None,
        model: None,
        seed: 0,
        model_params: Some(
            serde_json::from_str(r#"{"preset":"default"}"#).expect("model params object"),
        ),
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
            "requested_task_count": 1,
            "model": "404-3dgs"
        }))
        .send(&h.service)
        .await;
    let (status, _headers, _body) = read_response(res).await;
    assert_eq!(status, StatusCode::OK);

    let (worker_hotkey, timestamp, signature) = sign_worker([1u8; 32]);
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
    assert_eq!(status, StatusCode::OK);

    let rows = wait_for_worker(&h.event_recorder, &h.event_sink, 2).await;
    let actions: Vec<_> = rows.iter().map(|r| r.action.as_str()).collect();
    assert!(actions.contains(&"task_assigned"));
    assert!(actions.contains(&"result_success"));

    let assigned = rows
        .iter()
        .find(|r| r.action == "task_assigned")
        .expect("assigned");
    assert_eq!(assigned.task_id, Some(task_id));
    assert_eq!(assigned.task_kind, "txt3d");
    assert_eq!(assigned.worker_id.as_deref(), Some("worker-1"));

    let success = rows
        .iter()
        .find(|r| r.action == "result_success")
        .expect("success");
    assert_eq!(success.task_id, Some(task_id));
    assert_eq!(success.task_kind, "txt3d");
    assert_eq!(success.worker_id.as_deref(), Some("worker-1"));
}

#[tokio::test]
async fn records_worker_failure_event() {
    let h = build_harness(default_rate_ctx(), None).await;

    let task_id = Uuid::new_v4();
    let task = Task {
        id: task_id,
        prompt: Some(Arc::new("robot".to_string())),
        image: None,
        model: None,
        seed: 0,
        model_params: Some(
            serde_json::from_str(r#"{"preset":"default"}"#).expect("model params object"),
        ),
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
            "requested_task_count": 1,
            "model": "404-3dgs"
        }))
        .send(&h.service)
        .await;
    let (status, _headers, _body) = read_response(res).await;
    assert_eq!(status, StatusCode::OK);

    let (worker_hotkey, timestamp, signature) = sign_worker([1u8; 32]);
    let (boundary, body) = multipart_add_result(
        task_id,
        &worker_hotkey,
        "worker-1",
        &timestamp,
        &signature,
        "failure",
        None,
        Some("bad model output"),
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
    assert_eq!(status, StatusCode::OK);

    let rows = wait_for_worker(&h.event_recorder, &h.event_sink, 2).await;
    let failure = rows
        .iter()
        .find(|r| r.action == "result_failure")
        .expect("result_failure");
    assert_eq!(failure.task_id, Some(task_id));
    assert_eq!(failure.task_kind, "txt3d");
    assert_eq!(failure.reason.as_deref(), Some("bad model output"));
    assert_eq!(failure.worker_id.as_deref(), Some("worker-1"));
}

#[tokio::test]
async fn records_worker_timeout_event() {
    let shutdown = CancellationToken::new();
    let event_sink = InMemoryEventSink::default();
    let event_recorder = EventRecorder::new(
        std::sync::Arc::new(EventSinkHandle::InMemory(event_sink.clone())),
        std::sync::Arc::from("test-gateway"),
        Duration::from_millis(5),
        1_000,
        shutdown.clone(),
    );
    let metrics = Metrics::new(0.05).expect("metrics");
    let task_manager = TaskManager::new(
        1,
        1,
        Duration::from_millis(5),
        Duration::from_millis(10),
        metrics,
        Some(event_recorder.clone()),
    )
    .await;

    let task_id = Uuid::new_v4();
    let task = Task {
        id: task_id,
        prompt: Some(Arc::new("robot".to_string())),
        image: None,
        model: None,
        seed: 0,
        model_params: Some(
            serde_json::from_str(r#"{"preset":"default"}"#).expect("model params object"),
        ),
    };
    task_manager.add_task(task).await;
    let worker = Hotkey::from_bytes(&[3u8; 32]);
    task_manager
        .record_assignment(task_id, worker.clone(), worker.to_string().into())
        .await;

    tokio::time::sleep(Duration::from_millis(50)).await;

    event_recorder.flush_once_for_test().await;
    let rows = wait_for_worker(&event_recorder, &event_sink, 1).await;
    let timeout = rows
        .iter()
        .find(|r| r.action == "timeout")
        .expect("timeout");
    assert_eq!(timeout.task_id, Some(task_id));
    assert_eq!(timeout.task_kind, "txt3d");
    assert_eq!(timeout.worker_id.as_deref(), Some(worker.as_ref()));
}
