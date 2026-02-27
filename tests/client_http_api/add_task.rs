use std::sync::Arc;

use http::StatusCode;
use salvo::test::TestClient;
use uuid::Uuid;

use gateway::api::Task;

use crate::support::{
    add_task_prompt, build_harness, multipart_body, read_response, tiny_png_bytes,
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
    assert_ne!(h.task_manager.get_seed(task_id).await, Some(-1));
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
    assert_eq!(h.task_manager.get_seed(task_id).await, Some(i32::MIN));
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
    assert_eq!(h.task_manager.get_seed(task_id).await, Some(i32::MIN));
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
    assert_ne!(h.task_manager.get_seed(task_id).await, Some(-1));
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
    assert_ne!(h.task_manager.get_seed(task_id).await, Some(-1));
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
    assert_ne!(h.task_manager.get_seed(task_id).await, Some(-1));
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
async fn add_task_generic_key_ok() {
    let h = build_harness().await;
    let _task_id = add_task_prompt(&h, "robot", None).await;
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
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
}
