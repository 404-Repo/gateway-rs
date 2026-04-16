use std::time::Duration;

use std::sync::Arc;

use http::StatusCode;
use uuid::Uuid;

use gateway::api::Task;
use gateway::crypto::hotkey::Hotkey;
use gateway::raft::rate_limit::RateLimitPolicies;

use crate::common::GatewayHarnessOptions;
use crate::support::{
    TestClient, add_task_prompt, add_task_prompt_with_api_key, build_harness,
    build_harness_with_options, create_personal_api_key, fetch_generation_task_lifecycle,
    lookup_api_key, multipart_body, read_response, revoke_api_key_without_timestamp,
    set_gateway_runtime_settings, set_generic_rate_limit_policies, set_guest_free_settings,
    set_registered_free_settings, tiny_png_bytes, top_up_personal_api_key_balance,
    update_company_api_key_limits_without_timestamp,
    update_personal_api_key_limits_without_timestamp,
    update_personal_api_key_limits_without_timestamp_no_sync,
};

fn model_params_json_object_text_with_total_len(total_len: usize) -> String {
    // {"p":"<payload>"} -> fixed overhead is 8 bytes/chars.
    assert!(total_len >= 8, "total_len must be at least 8");
    format!(r#"{{"p":"{}"}}"#, "a".repeat(total_len - 8))
}

fn model_params_json_object_with_total_len(total_len: usize) -> serde_json::Value {
    serde_json::from_str(&model_params_json_object_text_with_total_len(total_len))
        .expect("model params object")
}

fn pop_seed_for_task(h: &crate::support::TestHarness, task_id: Uuid) -> i32 {
    let worker = Hotkey::from_bytes(&[200u8; 32]);
    let tasks = h.task_queue.pop(1, &worker);
    assert_eq!(tasks.len(), 1, "expected one queued task");
    let task = &tasks[0].0;
    assert_eq!(task.id, task_id, "seed check popped unexpected task");
    task.seed
}

#[tokio::test]
async fn add_task_json_success() {
    let h = build_harness().await;
    let _task_id = add_task_prompt(&h, "robot", None).await;
}

#[tokio::test]
async fn add_task_rejects_non_object_model_params_json() {
    let h = build_harness().await;
    let res = TestClient::post("http://localhost/add_task")
        .add_header("x-api-key", h.api_key.to_string(), true)
        .json(&serde_json::json!({
            "prompt": "robot",
            "model_params": "not-an-object"
        }))
        .send(&h.service)
        .await;
    let (status, _headers, body) = read_response(res).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let body_text = String::from_utf8_lossy(&body);
    assert!(body_text.contains("Model params must be a JSON object"));
}

#[tokio::test]
async fn add_task_rejects_null_model_params_json() {
    let h = build_harness().await;
    let res = TestClient::post("http://localhost/add_task")
        .add_header("x-api-key", h.api_key.to_string(), true)
        .json(&serde_json::json!({
            "prompt": "robot",
            "model_params": serde_json::Value::Null
        }))
        .send(&h.service)
        .await;
    let (status, _headers, body) = read_response(res).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let body_text = String::from_utf8_lossy(&body);
    assert!(body_text.contains("Model params must be a JSON object"));
}

#[tokio::test]
async fn add_task_rejects_array_model_params_json() {
    let h = build_harness().await;
    let res = TestClient::post("http://localhost/add_task")
        .add_header("x-api-key", h.api_key.to_string(), true)
        .json(&serde_json::json!({
            "prompt": "robot",
            "model_params": [1, 2, 3]
        }))
        .send(&h.service)
        .await;
    let (status, _headers, body) = read_response(res).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let body_text = String::from_utf8_lossy(&body);
    assert!(body_text.contains("Model params must be a JSON object"));
}

#[tokio::test]
async fn add_task_multipart_rejects_invalid_model_params() {
    let h = build_harness().await;
    let (boundary, body) = multipart_body(Some("robot"), None, None, None, Some("not-json"));
    let res = TestClient::post("http://localhost/add_task")
        .add_header("x-api-key", h.api_key.to_string(), true)
        .add_header(
            "content-type",
            format!("multipart/form-data; boundary={}", boundary),
            true,
        )
        .body(body)
        .send(&h.service)
        .await;
    let (status, _headers, body) = read_response(res).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let body_text = String::from_utf8_lossy(&body);
    assert!(body_text.contains("Model params must be valid JSON"));
}

#[tokio::test]
async fn add_task_multipart_rejects_non_object_model_params() {
    let h = build_harness().await;
    let (boundary, body) = multipart_body(Some("robot"), None, None, None, Some("[1,2,3]"));
    let res = TestClient::post("http://localhost/add_task")
        .add_header("x-api-key", h.api_key.to_string(), true)
        .add_header(
            "content-type",
            format!("multipart/form-data; boundary={}", boundary),
            true,
        )
        .body(body)
        .send(&h.service)
        .await;
    let (status, _headers, body) = read_response(res).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let body_text = String::from_utf8_lossy(&body);
    assert!(body_text.contains("Model params must be a JSON object"));
}

#[tokio::test]
async fn add_task_rejects_too_long_model_params_json() {
    let h = build_harness().await;
    let max_len = h.config.model_params.max_len;
    let too_long = model_params_json_object_with_total_len(max_len + 1);

    let res = TestClient::post("http://localhost/add_task")
        .add_header("x-api-key", h.api_key.to_string(), true)
        .json(&serde_json::json!({
            "prompt": "robot",
            "model_params": too_long
        }))
        .send(&h.service)
        .await;
    let (status, _headers, body) = read_response(res).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let body_text = String::from_utf8_lossy(&body);
    assert!(body_text.contains("Model params is too long"));
}

#[tokio::test]
async fn add_task_multipart_rejects_too_long_model_params() {
    let h = build_harness().await;
    let max_len = h.config.model_params.max_len;
    let too_long = model_params_json_object_text_with_total_len(max_len + 1);
    let (boundary, body) = multipart_body(Some("robot"), None, None, None, Some(&too_long));
    let res = TestClient::post("http://localhost/add_task")
        .add_header("x-api-key", h.api_key.to_string(), true)
        .add_header(
            "content-type",
            format!("multipart/form-data; boundary={}", boundary),
            true,
        )
        .body(body)
        .send(&h.service)
        .await;
    let (status, _headers, body) = read_response(res).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let body_text = String::from_utf8_lossy(&body);
    let body_lower = body_text.to_ascii_lowercase();
    assert!(
        body_text.contains("Model params is too long")
            || (body_lower.contains("model_params")
                && (body_lower.contains("exceed") || body_lower.contains("size"))),
        "unexpected multipart over-limit error: {body_text}"
    );
}

#[tokio::test]
async fn add_task_accepts_model_params_json_at_max_len() {
    let h = build_harness().await;
    let params = model_params_json_object_with_total_len(h.config.model_params.max_len);

    let res = TestClient::post("http://localhost/add_task")
        .add_header("x-api-key", h.api_key.to_string(), true)
        .json(&serde_json::json!({
            "prompt": "robot",
            "model_params": params
        }))
        .send(&h.service)
        .await;
    let (status, _headers, _body) = read_response(res).await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn add_task_accepts_model_params_multipart_at_max_len() {
    let h = build_harness().await;
    let params = model_params_json_object_text_with_total_len(h.config.model_params.max_len);
    let (boundary, body) = multipart_body(Some("robot"), None, None, None, Some(&params));

    let res = TestClient::post("http://localhost/add_task")
        .add_header("x-api-key", h.api_key.to_string(), true)
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
}

#[tokio::test]
async fn add_task_accepts_valid_model_params_json() {
    let h = build_harness().await;
    let params = serde_json::json!({"temperature": 0.5});
    let res = TestClient::post("http://localhost/add_task")
        .add_header("x-api-key", h.api_key.to_string(), true)
        .json(&serde_json::json!({
            "prompt": "robot",
            "model_params": params
        }))
        .send(&h.service)
        .await;
    let (status, _headers, _body) = read_response(res).await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn add_task_origin_header_success() {
    let h = build_harness().await;
    set_guest_free_settings(&h, 2, 86_400_000).await;
    let res = TestClient::post("http://localhost/add_task")
        .add_header("x-api-key", h.api_key.to_string(), true)
        .add_header("x-client-origin", "discord", true)
        .json(&serde_json::json!({"prompt": "robot"}))
        .send(&h.service)
        .await;
    let (status, _headers, body) = read_response(res).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "add_task body: {}",
        String::from_utf8_lossy(&body)
    );

    let res = TestClient::post("http://localhost/add_task")
        .add_header("x-api-key", h.api_key.to_string(), true)
        .add_header("x-client-origin", "unknown-tool", true)
        .json(&serde_json::json!({"prompt": "robot"}))
        .send(&h.service)
        .await;
    let (status, _headers, body) = read_response(res).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "add_task body: {}",
        String::from_utf8_lossy(&body)
    );
}

#[tokio::test]
async fn add_task_image_multipart_success() {
    let h = build_harness().await;
    let image = tiny_png_bytes();
    let (boundary, body) = multipart_body(None, Some(&image), None, Some("1"), None);
    let res = TestClient::post("http://localhost/add_task")
        .add_header("x-api-key", h.api_key.to_string(), true)
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
        "add_task body: {}",
        String::from_utf8_lossy(&body)
    );
}

#[tokio::test]
async fn add_task_without_seed_is_ok() {
    let h = build_harness().await;
    let image = tiny_png_bytes();
    let (boundary, body) = multipart_body(None, Some(&image), None, None, None);
    let res = TestClient::post("http://localhost/add_task")
        .add_header("x-api-key", h.api_key.to_string(), true)
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
        "add_task body: {}",
        String::from_utf8_lossy(&body)
    );
}

#[tokio::test]
async fn add_task_negative_seed_is_ok() {
    let h = build_harness().await;
    let image = tiny_png_bytes();
    let (boundary, body) = multipart_body(None, Some(&image), None, Some("-1"), None);
    let res = TestClient::post("http://localhost/add_task")
        .add_header("x-api-key", h.api_key.to_string(), true)
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
        "add_task body: {}",
        String::from_utf8_lossy(&body)
    );
    let payload: serde_json::Value = serde_json::from_slice(&body).expect("json response");
    let task_id = payload.get("id").and_then(|v| v.as_str()).expect("id");
    let task_id = Uuid::parse_str(task_id).expect("uuid");
    assert_ne!(pop_seed_for_task(&h, task_id), -1);
}

#[tokio::test]
async fn add_task_min_i32_seed_is_ok() {
    let h = build_harness().await;
    let image = tiny_png_bytes();
    let (boundary, body) = multipart_body(None, Some(&image), None, Some("-2147483648"), None);
    let res = TestClient::post("http://localhost/add_task")
        .add_header("x-api-key", h.api_key.to_string(), true)
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
        "add_task body: {}",
        String::from_utf8_lossy(&body)
    );
}

#[tokio::test]
async fn add_task_high_u32_seed_converts_to_signed_for_multipart() {
    let h = build_harness().await;
    let image = tiny_png_bytes();
    let (boundary, body) = multipart_body(None, Some(&image), None, Some("2147483648"), None);
    let res = TestClient::post("http://localhost/add_task")
        .add_header("x-api-key", h.api_key.to_string(), true)
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
        "add_task body: {}",
        String::from_utf8_lossy(&body)
    );
    let payload: serde_json::Value = serde_json::from_slice(&body).expect("json response");
    let task_id = payload.get("id").and_then(|v| v.as_str()).expect("id");
    let task_id = Uuid::parse_str(task_id).expect("uuid");
    assert_eq!(pop_seed_for_task(&h, task_id), i32::MIN);
}

#[tokio::test]
async fn add_task_high_u32_seed_converts_to_signed_for_json() {
    let h = build_harness().await;
    let res = TestClient::post("http://localhost/add_task")
        .add_header("x-api-key", h.api_key.to_string(), true)
        .json(&serde_json::json!({"prompt": "robot", "seed": 2147483648u64}))
        .send(&h.service)
        .await;
    let (status, _headers, body) = read_response(res).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "add_task body: {}",
        String::from_utf8_lossy(&body)
    );
    let payload: serde_json::Value = serde_json::from_slice(&body).expect("json response");
    let task_id = payload.get("id").and_then(|v| v.as_str()).expect("id");
    let task_id = Uuid::parse_str(task_id).expect("uuid");
    assert_eq!(pop_seed_for_task(&h, task_id), i32::MIN);
}

#[tokio::test]
async fn add_task_max_u32_seed_randomizes_for_multipart() {
    let h = build_harness().await;
    let image = tiny_png_bytes();
    let (boundary, body) = multipart_body(None, Some(&image), None, Some("4294967295"), None);
    let res = TestClient::post("http://localhost/add_task")
        .add_header("x-api-key", h.api_key.to_string(), true)
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
        "add_task body: {}",
        String::from_utf8_lossy(&body)
    );
    let payload: serde_json::Value = serde_json::from_slice(&body).expect("json response");
    let task_id = payload.get("id").and_then(|v| v.as_str()).expect("id");
    let task_id = Uuid::parse_str(task_id).expect("uuid");
    assert_ne!(pop_seed_for_task(&h, task_id), -1);
}

#[tokio::test]
async fn add_task_max_u32_seed_randomizes_for_json() {
    let h = build_harness().await;
    let res = TestClient::post("http://localhost/add_task")
        .add_header("x-api-key", h.api_key.to_string(), true)
        .json(&serde_json::json!({"prompt": "robot", "seed": 4294967295u64}))
        .send(&h.service)
        .await;
    let (status, _headers, body) = read_response(res).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "add_task body: {}",
        String::from_utf8_lossy(&body)
    );
    let payload: serde_json::Value = serde_json::from_slice(&body).expect("json response");
    let task_id = payload.get("id").and_then(|v| v.as_str()).expect("id");
    let task_id = Uuid::parse_str(task_id).expect("uuid");
    assert_ne!(pop_seed_for_task(&h, task_id), -1);
}

#[tokio::test]
async fn add_task_json_negative_seed_randomizes() {
    let h = build_harness().await;
    let res = TestClient::post("http://localhost/add_task")
        .add_header("x-api-key", h.api_key.to_string(), true)
        .json(&serde_json::json!({"prompt": "robot", "seed": -1}))
        .send(&h.service)
        .await;
    let (status, _headers, body) = read_response(res).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "add_task body: {}",
        String::from_utf8_lossy(&body)
    );
    let payload: serde_json::Value = serde_json::from_slice(&body).expect("json response");
    let task_id = payload.get("id").and_then(|v| v.as_str()).expect("id");
    let task_id = Uuid::parse_str(task_id).expect("uuid");
    assert_ne!(pop_seed_for_task(&h, task_id), -1);
}

#[tokio::test]
async fn add_task_invalid_seed_returns_bad_request() {
    let h = build_harness().await;
    let image = tiny_png_bytes();
    let (boundary, body) = multipart_body(None, Some(&image), None, Some("not-a-number"), None);
    let res = TestClient::post("http://localhost/add_task")
        .add_header("x-api-key", h.api_key.to_string(), true)
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
async fn add_task_out_of_range_seed_returns_bad_request() {
    let h = build_harness().await;
    let image = tiny_png_bytes();
    let (boundary, body) = multipart_body(None, Some(&image), None, Some("4294967296"), None);
    let res = TestClient::post("http://localhost/add_task")
        .add_header("x-api-key", h.api_key.to_string(), true)
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
async fn add_task_rejects_invalid_image_data() {
    let h = build_harness().await;
    let (boundary, body) = multipart_body(None, Some(b"not-an-image"), None, None, None);
    let res = TestClient::post("http://localhost/add_task")
        .add_header("x-api-key", h.api_key.to_string(), true)
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
async fn add_task_missing_content_type() {
    let h = build_harness().await;
    let res = TestClient::post("http://localhost/add_task")
        .add_header("x-api-key", h.api_key.to_string(), true)
        .body(br#"{\"prompt\":\"robot\"}"#.to_vec())
        .send(&h.service)
        .await;
    let (status, _headers, _body) = read_response(res).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn add_task_missing_multipart_boundary() {
    let h = build_harness().await;
    let res = TestClient::post("http://localhost/add_task")
        .add_header("x-api-key", h.api_key.to_string(), true)
        .add_header("content-type", "multipart/form-data", true)
        .body(Vec::new())
        .send(&h.service)
        .await;
    let (status, _headers, _body) = read_response(res).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn add_task_wrong_method_honors_accept_json() {
    let h = build_harness().await;
    let res = TestClient::get("http://localhost/add_task")
        .add_header("accept", "application/json", true)
        .send(&h.service)
        .await;
    let (status, headers, body) = read_response(res).await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);

    let content_type = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(
        content_type.starts_with("application/json"),
        "content-type: {content_type}, body: {}",
        String::from_utf8_lossy(&body)
    );

    let payload: serde_json::Value = serde_json::from_slice(&body).expect("json response");
    assert_eq!(
        payload
            .get("error")
            .and_then(|v| v.get("code"))
            .and_then(|v| v.as_u64()),
        Some(StatusCode::METHOD_NOT_ALLOWED.as_u16() as u64)
    );
}

#[tokio::test]
async fn add_task_rejects_missing_prompt_and_image() {
    let h = build_harness().await;
    let res = TestClient::post("http://localhost/add_task")
        .add_header("x-api-key", h.api_key.to_string(), true)
        .json(&serde_json::json!({}))
        .send(&h.service)
        .await;
    let (status, _headers, _body) = read_response(res).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn add_task_rejects_prompt_and_image() {
    let h = build_harness().await;
    let image = tiny_png_bytes();
    let (boundary, body) = multipart_body(Some("robot"), Some(&image), None, None, None);
    let res = TestClient::post("http://localhost/add_task")
        .add_header("x-api-key", h.api_key.to_string(), true)
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
async fn add_task_rejects_short_prompt() {
    let h = build_harness().await;
    let short_len = h.config.prompt.min_len.saturating_sub(1).max(1);
    let prompt = "a".repeat(short_len);
    let res = TestClient::post("http://localhost/add_task")
        .add_header("x-api-key", h.api_key.to_string(), true)
        .json(&serde_json::json!({"prompt": prompt}))
        .send(&h.service)
        .await;
    let (status, _headers, _body) = read_response(res).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn add_task_rejects_invalid_prompt_chars() {
    let h = build_harness().await;
    let res = TestClient::post("http://localhost/add_task")
        .add_header("x-api-key", h.api_key.to_string(), true)
        .json(&serde_json::json!({"prompt": "bad@prompt"}))
        .send(&h.service)
        .await;
    let (status, _headers, _body) = read_response(res).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn add_task_rejects_unknown_model() {
    let h = build_harness().await;
    let res = TestClient::post("http://localhost/add_task")
        .add_header("x-api-key", h.api_key.to_string(), true)
        .json(&serde_json::json!({"prompt": "robot", "model": "invalid"}))
        .send(&h.service)
        .await;
    let (status, _headers, body) = read_response(res).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let payload: serde_json::Value = serde_json::from_slice(&body).expect("json response");
    assert_eq!(
        payload.get("error").and_then(|v| v.as_str()),
        Some("invalid_field")
    );
}

#[tokio::test]
async fn add_task_missing_api_key() {
    let h = build_harness().await;
    let res = TestClient::post("http://localhost/add_task")
        .json(&serde_json::json!({"prompt": "robot"}))
        .send(&h.service)
        .await;
    let (status, _headers, _body) = read_response(res).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn add_task_invalid_api_key_format() {
    let h = build_harness().await;
    let res = TestClient::post("http://localhost/add_task")
        .add_header("x-api-key", "not-a-uuid", true)
        .json(&serde_json::json!({"prompt": "robot"}))
        .send(&h.service)
        .await;
    let (status, _headers, _body) = read_response(res).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn add_task_unknown_api_key() {
    let h = build_harness().await;
    let mut bad_key = Uuid::new_v4();
    while bad_key == h.api_key {
        bad_key = Uuid::new_v4();
    }
    let res = TestClient::post("http://localhost/add_task")
        .add_header("x-api-key", bad_key.to_string(), true)
        .json(&serde_json::json!({"prompt": "robot"}))
        .send(&h.service)
        .await;
    let (status, _headers, _body) = read_response(res).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn add_task_personal_key_limit_updates_take_effect_without_manual_updated_at() {
    let h = build_harness().await;
    update_personal_api_key_limits_without_timestamp(&h, &h.user_api_key, 10, 1).await;

    let lookup = lookup_api_key(&h, &h.user_api_key).await;
    assert!(
        lookup.user_id.is_some(),
        "personal key should resolve to a user"
    );
    assert_eq!(lookup.user_limits, Some((10, 1)));
}

#[tokio::test]
async fn add_task_personal_key_without_balance_and_with_zero_registered_free_limit_is_rejected() {
    let h = build_harness().await;
    update_personal_api_key_limits_without_timestamp(&h, &h.user_api_key, 1, 10).await;
    set_registered_free_settings(&h, 0, 86_400_000).await;

    let res = TestClient::post("http://localhost/add_task")
        .add_header("x-api-key", h.user_api_key.clone(), true)
        .json(&serde_json::json!({"prompt": "robot"}))
        .send(&h.service)
        .await;
    let (status, _headers, body) = read_response(res).await;

    assert_eq!(
        status,
        StatusCode::PAYMENT_REQUIRED,
        "personal zero-balance key without free allowance should be rejected, got {}",
        String::from_utf8_lossy(&body)
    );

    let payload: serde_json::Value =
        serde_json::from_slice(&body).expect("payment-required body should be valid json");
    assert_eq!(payload["error"], "insufficient_balance");
    assert_eq!(payload["message"], "Insufficient user balance.");
    assert_eq!(
        h.task_queue.len(),
        0,
        "rejected task should not enter the queue"
    );
}

#[tokio::test]
async fn add_task_personal_key_uses_registered_free_limit_before_paid_billing() {
    let h = build_harness().await;
    update_personal_api_key_limits_without_timestamp(&h, &h.user_api_key, 10, 10).await;
    set_registered_free_settings(&h, 1, 86_400_000).await;

    let first = TestClient::post("http://localhost/add_task")
        .add_header("x-api-key", h.user_api_key.clone(), true)
        .json(&serde_json::json!({"prompt": "robot"}))
        .send(&h.service)
        .await;
    let (first_status, _headers, first_body) = read_response(first).await;
    assert_eq!(
        first_status,
        StatusCode::OK,
        "first registered free task should succeed, got {}",
        String::from_utf8_lossy(&first_body)
    );

    let second = TestClient::post("http://localhost/add_task")
        .add_header("x-api-key", h.user_api_key.clone(), true)
        .json(&serde_json::json!({"prompt": "robot again"}))
        .send(&h.service)
        .await;
    let (second_status, _headers, second_body) = read_response(second).await;
    assert_eq!(
        second_status,
        StatusCode::PAYMENT_REQUIRED,
        "second task should fall through to paid billing and fail without balance, got {}",
        String::from_utf8_lossy(&second_body)
    );
}

#[tokio::test]
async fn add_task_registered_free_window_is_rolling() {
    let h = build_harness().await;
    update_personal_api_key_limits_without_timestamp(&h, &h.user_api_key, 10, 10).await;
    set_registered_free_settings(&h, 1, 1).await;

    let first = TestClient::post("http://localhost/add_task")
        .add_header("x-api-key", h.user_api_key.clone(), true)
        .json(&serde_json::json!({"prompt": "robot"}))
        .send(&h.service)
        .await;
    let (first_status, _headers, first_body) = read_response(first).await;
    assert_eq!(
        first_status,
        StatusCode::OK,
        "first rolling-window task should succeed, got {}",
        String::from_utf8_lossy(&first_body)
    );

    tokio::time::sleep(Duration::from_millis(10)).await;

    let second = TestClient::post("http://localhost/add_task")
        .add_header("x-api-key", h.user_api_key.clone(), true)
        .json(&serde_json::json!({"prompt": "robot again"}))
        .send(&h.service)
        .await;
    let (second_status, _headers, second_body) = read_response(second).await;
    assert_eq!(
        second_status,
        StatusCode::OK,
        "second task should succeed after the rolling window expires, got {}",
        String::from_utf8_lossy(&second_body)
    );
}

#[tokio::test]
async fn add_task_company_key_limit_updates_take_effect_without_manual_updated_at() {
    let h = build_harness().await;
    update_company_api_key_limits_without_timestamp(&h, &h.company_api_key, 100, 1).await;

    let lookup = lookup_api_key(&h, &h.company_api_key).await;
    assert_eq!(lookup.company_id, Some(h.company_id));
    let company = lookup.company_info.expect("company lookup info");
    assert_eq!(company.1, 100);
    assert_eq!(company.2, 1);
}

#[tokio::test]
async fn add_task_revoked_personal_key_is_rejected_after_sync_without_manual_updated_at() {
    let h = build_harness().await;
    let api_key = create_personal_api_key(&h).await;
    top_up_personal_api_key_balance(&h, &api_key, 100_000).await;

    let _task_id = add_task_prompt_with_api_key(&h, &api_key, "robot", None).await;

    revoke_api_key_without_timestamp(&h, &api_key).await;

    let lookup = lookup_api_key(&h, &api_key).await;
    assert!(
        lookup.billing_owner.is_none(),
        "revoked key should no longer resolve to a billing owner"
    );
    assert!(
        lookup.user_id.is_none(),
        "revoked key should not resolve to a user"
    );

    let res = TestClient::post("http://localhost/add_task")
        .add_header("x-api-key", api_key.as_str(), true)
        .json(&serde_json::json!({"prompt": "robot again"}))
        .send(&h.service)
        .await;
    let (status, _headers, _body) = read_response(res).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn add_task_personal_key_limit_updates_take_effect_via_background_polling() {
    let h = build_harness_with_options(GatewayHarnessOptions {
        api_keys_update_interval_secs: Some(1),
        ..GatewayHarnessOptions::default()
    })
    .await;

    update_personal_api_key_limits_without_timestamp_no_sync(&h, &h.user_api_key, 7, 2).await;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let lookup = lookup_api_key(&h, &h.user_api_key).await;
        if lookup.user_limits == Some((7, 2)) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "background polling did not refresh user limits in time; last lookup: {:?}",
            lookup.user_limits
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn add_task_unauthorized_limit_hot_updates_without_resetting_counters() {
    let h = build_harness().await;
    let bad_key = Uuid::new_v4().to_string();
    set_gateway_runtime_settings(
        &h,
        RateLimitPolicies::from_config(&h.config.http),
        1,
        &[],
        h.config.http.max_task_queue_len as i32,
        h.config.http.request_file_size_limit as i64,
    )
    .await;

    let first = TestClient::post("http://localhost/add_task")
        .add_header("x-api-key", bad_key.as_str(), true)
        .json(&serde_json::json!({"prompt": "robot"}))
        .send(&h.service)
        .await;
    let (first_status, _headers, _body) = read_response(first).await;
    assert_eq!(first_status, StatusCode::UNAUTHORIZED);

    let second = TestClient::post("http://localhost/add_task")
        .add_header("x-api-key", bad_key.as_str(), true)
        .json(&serde_json::json!({"prompt": "robot"}))
        .send(&h.service)
        .await;
    let (second_status, _headers, _body) = read_response(second).await;
    assert_eq!(second_status, StatusCode::TOO_MANY_REQUESTS);

    set_gateway_runtime_settings(
        &h,
        RateLimitPolicies::from_config(&h.config.http),
        2,
        &[],
        h.config.http.max_task_queue_len as i32,
        h.config.http.request_file_size_limit as i64,
    )
    .await;

    let third = TestClient::post("http://localhost/add_task")
        .add_header("x-api-key", bad_key.as_str(), true)
        .json(&serde_json::json!({"prompt": "robot"}))
        .send(&h.service)
        .await;
    let (third_status, _headers, _body) = read_response(third).await;
    assert_eq!(third_status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn add_task_generic_key_ok() {
    let h = build_harness().await;
    let task_id = add_task_prompt(&h, "robot", None).await;
    let lifecycle = fetch_generation_task_lifecycle(&h, task_id)
        .await
        .expect("guest lifecycle row");
    assert_eq!(lifecycle.billing_mode, "guest_free");
    assert_eq!(lifecycle.reservation_status, "reserved");
    assert_eq!(lifecycle.task_status, "queued");
    assert_eq!(lifecycle.account_id, None);
    assert_eq!(lifecycle.user_id, None);
    assert_eq!(lifecycle.company_id, None);
    assert_eq!(lifecycle.api_key_id, None);
}

#[tokio::test]
async fn add_task_generic_key_ignores_site_guest_limits() {
    let h = build_harness().await;
    set_guest_free_settings(&h, 0, 86_400_000).await;

    let res = TestClient::post("http://localhost/add_task")
        .add_header("x-api-key", h.api_key.to_string(), true)
        .json(&serde_json::json!({"prompt": "robot"}))
        .send(&h.service)
        .await;
    let (status, _headers, body) = read_response(res).await;

    assert_eq!(
        status,
        StatusCode::OK,
        "body: {}",
        String::from_utf8_lossy(&body)
    );
}

#[tokio::test]
async fn add_task_generic_key_respects_sql_generic_policies() {
    let h = build_harness().await;
    set_generic_rate_limit_policies(
        &h,
        RateLimitPolicies {
            generic_global_daily_limit: 1,
            generic_per_ip_daily_limit: 1,
        },
    )
    .await;

    let first = TestClient::post("http://localhost/add_task")
        .add_header("x-api-key", h.api_key.to_string(), true)
        .json(&serde_json::json!({"prompt": "robot"}))
        .send(&h.service)
        .await;
    let (first_status, _headers, _body) = read_response(first).await;
    assert_eq!(first_status, StatusCode::OK);

    let second = TestClient::post("http://localhost/add_task")
        .add_header("x-api-key", h.api_key.to_string(), true)
        .json(&serde_json::json!({"prompt": "robot again"}))
        .send(&h.service)
        .await;
    let (second_status, _headers, second_body) = read_response(second).await;
    assert_eq!(second_status, StatusCode::TOO_MANY_REQUESTS);
    let payload: serde_json::Value =
        serde_json::from_slice(&second_body).expect("rate limit body should be valid json");
    assert_eq!(payload["error"], "daily_limit");
    assert_eq!(
        payload["message"],
        "Generic key global rolling limit exceeded."
    );
}

#[tokio::test]
async fn add_task_rejects_when_queue_full() {
    let h = build_harness().await;
    let max_len = h.config.http.max_task_queue_len;
    for _ in 0..max_len {
        h.task_queue.push(Task {
            id: Uuid::new_v4(),
            prompt: Some(Arc::new("robot".to_string())),
            image: None,
            model: None,
            seed: 0,
            model_params: None,
        });
    }
    let res = TestClient::post("http://localhost/add_task")
        .add_header("x-api-key", h.api_key.to_string(), true)
        .json(&serde_json::json!({"prompt": "robot"}))
        .send(&h.service)
        .await;
    let (status, _headers, _body) = read_response(res).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn add_task_queue_full_before_reservation_does_not_consume_distributed_quota() {
    let h = build_harness().await;
    set_gateway_runtime_settings(
        &h,
        RateLimitPolicies::from_config(&h.config.http),
        h.config.http.add_task_unauthorized_per_ip_daily_rate_limit as i32,
        &[],
        0,
        h.config.http.request_file_size_limit as i64,
    )
    .await;

    for attempt in 1..=2 {
        let res = TestClient::post("http://localhost/add_task")
            .add_header("x-api-key", h.api_key.to_string(), true)
            .json(&serde_json::json!({"prompt": "robot"}))
            .send(&h.service)
            .await;
        let (status, _headers, body) = read_response(res).await;
        assert_eq!(
            status,
            StatusCode::SERVICE_UNAVAILABLE,
            "attempt {attempt} should fail from queue-full (not rate-limited), got {}",
            String::from_utf8_lossy(&body)
        );
    }
}

#[tokio::test]
async fn lowered_queue_cap_only_blocks_new_admissions() {
    let h = build_harness().await;
    let _existing_task = add_task_prompt(&h, "robot", None).await;
    assert_eq!(h.task_queue.len(), 1);

    set_gateway_runtime_settings(
        &h,
        RateLimitPolicies::from_config(&h.config.http),
        h.config.http.add_task_unauthorized_per_ip_daily_rate_limit as i32,
        &[],
        0,
        h.config.http.request_file_size_limit as i64,
    )
    .await;

    let res = TestClient::post("http://localhost/add_task")
        .add_header("x-api-key", h.api_key.to_string(), true)
        .json(&serde_json::json!({"prompt": "second robot"}))
        .send(&h.service)
        .await;
    let (status, _headers, _body) = read_response(res).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        h.task_queue.len(),
        1,
        "existing queued work should remain intact"
    );
}

#[tokio::test]
async fn add_task_bad_request_does_not_consume_guest_quota() {
    let h = build_harness().await;
    set_generic_rate_limit_policies(
        &h,
        RateLimitPolicies {
            generic_global_daily_limit: 1,
            generic_per_ip_daily_limit: 1,
        },
    )
    .await;

    // Invalid payload should be rejected by validation.
    let bad = TestClient::post("http://localhost/add_task")
        .add_header("x-api-key", h.api_key.to_string(), true)
        .json(&serde_json::json!({}))
        .send(&h.service)
        .await;
    let (bad_status, _headers, _body) = read_response(bad).await;
    assert_eq!(bad_status, StatusCode::BAD_REQUEST);

    // First valid request should still pass (bad payload did not consume the SQL generic quota).
    let first_valid = TestClient::post("http://localhost/add_task")
        .add_header("x-api-key", h.api_key.to_string(), true)
        .json(&serde_json::json!({"prompt": "robot"}))
        .send(&h.service)
        .await;
    let (first_status, _headers, first_body) = read_response(first_valid).await;
    assert_eq!(
        first_status,
        StatusCode::OK,
        "first valid request failed: {}",
        String::from_utf8_lossy(&first_body)
    );

    // Second valid request should now be rejected by the SQL generic rolling window.
    let second_valid = TestClient::post("http://localhost/add_task")
        .add_header("x-api-key", h.api_key.to_string(), true)
        .json(&serde_json::json!({"prompt": "robot"}))
        .send(&h.service)
        .await;
    let (next_status, _headers, next_body) = read_response(second_valid).await;
    assert_eq!(next_status, StatusCode::TOO_MANY_REQUESTS);
    let payload: serde_json::Value =
        serde_json::from_slice(&next_body).expect("guest limit body should be valid json");
    assert_eq!(payload["error"], "daily_limit");
    assert_eq!(
        payload["message"],
        "Generic key global rolling limit exceeded."
    );
}

#[tokio::test]
async fn add_task_attempt_limiter_applies_before_validation() {
    let h = build_harness().await;

    let mut updated = (*h.config).clone();
    updated.http.basic_rate_limit = 1;

    let updated_toml = toml::to_string(&updated).expect("serialize config");
    std::fs::write(&h.config_path, updated_toml).expect("write config");
    assert!(h.runtime_config.reload_from_disk().await);

    let bad = TestClient::post("http://localhost/add_task")
        .add_header("x-api-key", h.api_key.to_string(), true)
        .json(&serde_json::json!({}))
        .send(&h.service)
        .await;
    let (bad_status, _headers, _body) = read_response(bad).await;
    assert_eq!(bad_status, StatusCode::BAD_REQUEST);

    let next = TestClient::post("http://localhost/add_task")
        .add_header("x-api-key", h.api_key.to_string(), true)
        .json(&serde_json::json!({"prompt": "robot"}))
        .send(&h.service)
        .await;
    let (next_status, _headers, _body) = read_response(next).await;
    assert_eq!(next_status, StatusCode::TOO_MANY_REQUESTS);
}
