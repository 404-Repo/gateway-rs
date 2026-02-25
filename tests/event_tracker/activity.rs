use std::sync::Arc;

use http::StatusCode;
use salvo::test::TestClient;
use uuid::Uuid;

use gateway::crypto::hotkey::Hotkey;
use gateway::test_support::{CompanyRateLimit, RateLimitContext};

use crate::support::{
    add_success_result, build_harness, default_rate_ctx, multipart_add_task, read_response,
    tiny_png_bytes, wait_for_activity,
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

    add_success_result(
        &h.task_manager,
        task_id,
        Hotkey::from_bytes(&[42u8; 32]),
        b"spz".to_vec(),
    )
    .await;

    let res = TestClient::get(format!("http://localhost/get_result?id={task_id}"))
        .add_header("x-api-key", h.api_key.to_string(), true)
        .add_header("x-client-origin", "api", true)
        .send(&h.service)
        .await;
    let (status, _headers, _body) = read_response(res).await;
    assert_eq!(status, StatusCode::OK);

    let rows = wait_for_activity(&h.event_recorder, &h.event_sink, 2).await;
    let actions: Vec<_> = rows.iter().map(|r| r.action.as_str()).collect();
    assert!(actions.contains(&"add_task"));
    assert!(actions.contains(&"get_result"));

    let add_task = rows
        .iter()
        .find(|r| r.action == "add_task")
        .expect("add_task");
    assert_eq!(add_task.task_kind, "txt3d");
    assert_eq!(add_task.task_id, Some(task_id));
    assert_eq!(add_task.tool, "api");
    assert_eq!(add_task.model.as_deref(), Some("404-3dgs"));

    let get_result = rows
        .iter()
        .find(|r| r.action == "get_result")
        .expect("get_result");
    assert_eq!(get_result.task_kind, "unknown");
    assert_eq!(get_result.task_id, Some(task_id));
    assert_eq!(get_result.tool, "api");
    assert!(get_result.model.is_none());
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
            hourly_limit: 100,
            daily_limit: 1000,
        }),
        ..RateLimitContext::default()
    };
    let h = build_harness(rate_ctx, None).await;

    let company_key = Uuid::new_v4().to_string();
    h.key_validator
        .seed_company_key(&company_key, company_id, "Acme", 100, 1000)
        .await;

    let (boundary, body) = multipart_add_task(None, Some(tiny_png_bytes().as_slice()));
    let res = TestClient::post("http://localhost/add_task")
        .add_header("x-api-key", company_key, true)
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

    let rows = wait_for_activity(&h.event_recorder, &h.event_sink, 1).await;
    let add_task = rows
        .iter()
        .find(|r| r.action == "add_task")
        .expect("add_task");
    assert_eq!(add_task.task_kind, "img3d");
    assert_eq!(add_task.task_id, Some(task_id));
    assert_eq!(add_task.tool, "blender");
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

    let rows = wait_for_activity(&h.event_recorder, &h.event_sink, 1).await;
    let add_task = rows
        .iter()
        .find(|r| r.action == "add_task")
        .expect("add_task");
    assert_eq!(add_task.tool, "blender");
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

    let rows = wait_for_activity(&h.event_recorder, &h.event_sink, 1).await;
    let add_task = rows
        .iter()
        .find(|r| r.action == "add_task")
        .expect("add_task");
    assert_eq!(add_task.tool, "unity");
    assert!(add_task.user_id.is_none());
    assert!(add_task.company_id.is_none());
}
