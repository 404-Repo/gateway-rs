use std::sync::Arc;

use http::StatusCode;
use uuid::Uuid;

use gateway::test_support::{CompanyRateLimit, RateLimitContext};

use crate::support::{
    AddResultMultipartInput, TestClient, analytics_foreign_key_columns, build_harness,
    default_rate_ctx, multipart_add_result, multipart_add_task,
    purge_terminal_generation_task_in_db, read_response, record_assignment_in_memory_and_db,
    sign_worker, timeout_generation_task_in_db, tiny_png_bytes, wait_for_activity,
};

#[tokio::test]
async fn records_client_activity_events() {
    let h = build_harness(default_rate_ctx(), None).await;

    let res = TestClient::post("http://localhost/add_task")
        .add_header("x-api-key", h.api_key.to_string(), true)
        .add_header("x-client-origin", "api", true)
        .json(&serde_json::json!({"prompt": "robot"}))
        .send(&h.service)
        .await;
    let (status, _headers, body) = read_response(res).await;
    assert_eq!(status, StatusCode::OK);
    let payload: serde_json::Value = serde_json::from_slice(&body).expect("json response");
    let task_id = payload.get("id").and_then(|v| v.as_str()).expect("id");
    let task_id = Uuid::parse_str(task_id).expect("uuid");

    let (worker_hotkey, timestamp, signature) = sign_worker([42u8; 32]);
    let assignment_token =
        record_assignment_in_memory_and_db(&h, task_id, &worker_hotkey, "worker-1").await;
    let (boundary, body) = multipart_add_result(AddResultMultipartInput {
        task_id,
        worker_hotkey: &worker_hotkey,
        worker_id: "worker-1",
        assignment_token,
        timestamp: &timestamp,
        signature: &signature,
        status: "success",
        asset: Some(b"spz"),
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

    let res = TestClient::get(format!("http://localhost/get_result?id={task_id}"))
        .add_header("x-api-key", h.api_key.to_string(), true)
        .add_header("x-client-origin", "api", true)
        .send(&h.service)
        .await;
    let (status, _headers, _body) = read_response(res).await;
    assert_eq!(status, StatusCode::OK);

    let rows = wait_for_activity(&h, 2).await;
    let actions: Vec<_> = rows.iter().map(|r| r.action.as_str()).collect();
    assert!(actions.contains(&"add_task"));
    assert!(actions.contains(&"get_result"));

    let add_task = rows
        .iter()
        .find(|r| r.action == "add_task")
        .expect("add_task");
    assert_eq!(add_task.event_family, "other");
    assert_eq!(add_task.task_kind.as_deref(), Some("txt3d"));
    assert_eq!(add_task.task_id, Some(task_id));
    assert_eq!(add_task.client_origin, "api");
    assert_eq!(add_task.model.as_deref(), Some("404-3dgs"));
    assert!(add_task.account_id.is_some());

    let get_result = rows
        .iter()
        .find(|r| r.action == "get_result")
        .expect("get_result");
    assert_eq!(get_result.event_family, "other");
    assert_eq!(get_result.task_kind.as_deref(), Some("txt3d"));
    assert_eq!(get_result.task_id, Some(task_id));
    assert_eq!(get_result.client_origin, "api");
    assert_eq!(get_result.model.as_deref(), Some("404-3dgs"));
    assert!(get_result.account_id.is_some());
}

#[tokio::test]
async fn records_company_activity_with_image_task() {
    let company_id = Uuid::new_v4();
    let rate_ctx = RateLimitContext {
        is_company_key: true,
        key_is_uuid: true,
        company: Some(CompanyRateLimit {
            id: company_id,
            name: Arc::from("Acme"),
            concurrent_limit: 100,
            daily_limit: 1000,
        }),
        ..RateLimitContext::default()
    };
    let h = build_harness(rate_ctx, None).await;

    let (boundary, body) = multipart_add_task(None, Some(tiny_png_bytes().as_slice()));
    let res = TestClient::post("http://localhost/add_task")
        .add_header("x-api-key", h.api_key.to_string(), true)
        .add_header("x-client-origin", "blender", true)
        .add_header(
            "content-type",
            format!("multipart/form-data; boundary={}", boundary),
            true,
        )
        .body(body)
        .send(&h.service)
        .await;
    let (status, _headers, body) = read_response(res).await;
    assert_eq!(status, StatusCode::OK);
    let payload: serde_json::Value = serde_json::from_slice(&body).expect("json response");
    let task_id = payload.get("id").and_then(|v| v.as_str()).expect("id");
    let task_id = Uuid::parse_str(task_id).expect("uuid");

    let rows = wait_for_activity(&h, 1).await;
    let add_task = rows
        .iter()
        .find(|r| r.action == "add_task")
        .expect("add_task");
    assert_eq!(add_task.event_family, "other");
    assert_eq!(add_task.task_kind.as_deref(), Some("img3d"));
    assert_eq!(add_task.task_id, Some(task_id));
    assert_eq!(add_task.client_origin, "blender");
    assert!(add_task.account_id.is_some());
    assert_eq!(add_task.company_id, Some(company_id));
    assert_eq!(add_task.company_name.as_deref(), Some("Acme"));
    assert_eq!(add_task.model.as_deref(), Some("404-3dgs"));
}

#[tokio::test]
async fn records_generic_key_activity_event() {
    let rate_ctx = RateLimitContext {
        is_generic_key: true,
        key_is_uuid: true,
        ..RateLimitContext::default()
    };
    let h = build_harness(rate_ctx, None).await;

    let res = TestClient::post("http://localhost/add_task")
        .add_header("x-api-key", h.api_key.to_string(), true)
        .add_header("x-client-origin", "blender", true)
        .json(&serde_json::json!({"prompt": "robot"}))
        .send(&h.service)
        .await;
    let (status, _headers, _body) = read_response(res).await;
    assert_eq!(status, StatusCode::OK);

    let rows = wait_for_activity(&h, 1).await;
    let add_task = rows
        .iter()
        .find(|r| r.action == "add_task")
        .expect("add_task");
    assert_eq!(add_task.event_family, "other");
    assert_eq!(add_task.client_origin, "blender");
    assert!(add_task.account_id.is_none());
    assert!(add_task.user_id.is_none());
    assert!(add_task.company_id.is_none());
}

#[tokio::test]
async fn records_generic_key_activity_event_for_unity_origin() {
    let rate_ctx = RateLimitContext {
        is_generic_key: true,
        key_is_uuid: true,
        ..RateLimitContext::default()
    };
    let h = build_harness(rate_ctx, None).await;

    let res = TestClient::post("http://localhost/add_task")
        .add_header("x-api-key", h.api_key.to_string(), true)
        .add_header("x-client-origin", "unity", true)
        .json(&serde_json::json!({"prompt": "robot"}))
        .send(&h.service)
        .await;
    let (status, _headers, _body) = read_response(res).await;
    assert_eq!(status, StatusCode::OK);

    let rows = wait_for_activity(&h, 1).await;
    let add_task = rows
        .iter()
        .find(|r| r.action == "add_task")
        .expect("add_task");
    assert_eq!(add_task.event_family, "other");
    assert_eq!(add_task.client_origin, "unity");
    assert!(add_task.account_id.is_none());
    assert!(add_task.user_id.is_none());
    assert!(add_task.company_id.is_none());
}

#[tokio::test]
async fn activity_event_task_id_survives_generation_task_purge() {
    let h = build_harness(default_rate_ctx(), None).await;

    let res = TestClient::post("http://localhost/add_task")
        .add_header("x-api-key", h.api_key.to_string(), true)
        .add_header("x-client-origin", "api", true)
        .json(&serde_json::json!({"prompt": "robot"}))
        .send(&h.service)
        .await;
    let (status, _headers, body) = read_response(res).await;
    assert_eq!(status, StatusCode::OK);
    let payload: serde_json::Value = serde_json::from_slice(&body).expect("json response");
    let task_id = payload.get("id").and_then(|v| v.as_str()).expect("id");
    let task_id = Uuid::parse_str(task_id).expect("uuid");

    let rows_before_purge = wait_for_activity(&h, 1).await;
    let add_task = rows_before_purge
        .iter()
        .find(|row| row.action == "add_task")
        .expect("add_task before purge");
    assert_eq!(add_task.task_id, Some(task_id));

    timeout_generation_task_in_db(&h, task_id).await;
    purge_terminal_generation_task_in_db(&h, task_id).await;

    let rows_after_purge = wait_for_activity(&h, 1).await;
    let add_task = rows_after_purge
        .iter()
        .find(|row| row.action == "add_task")
        .expect("add_task after purge");
    assert_eq!(add_task.task_id, Some(task_id));
}

#[tokio::test]
async fn analytics_event_tables_have_no_mutating_foreign_keys() {
    let h = build_harness(default_rate_ctx(), None).await;

    let foreign_keys = analytics_foreign_key_columns(&h).await;
    assert_eq!(
        foreign_keys,
        Vec::<(String, String)>::new(),
        "analytics tables must keep historical ids immutable"
    );
}
