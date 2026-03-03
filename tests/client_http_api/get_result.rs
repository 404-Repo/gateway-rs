use std::sync::Arc;

use futures_lite::io::AsyncReadExt;
use http::StatusCode;
use salvo::test::TestClient;
use uuid::Uuid;

use gateway::api::Task;

use crate::support::{
    HOTKEY_SEED_BASE, add_failure_result, add_success_result, add_task_image, add_task_prompt,
    build_harness, hotkey_from_seed, read_response, tiny_png_bytes, tiny_spz_bytes,
};

#[tokio::test]
async fn get_result_not_found_for_pending_task() {
    let h = build_harness().await;
    let task_id = add_task_prompt(&h, "robot", None).await;

    let res = TestClient::get(format!("http://localhost/get_result?id={task_id}"))
        .add_header("x-api-key", h.api_key.to_string(), true)
        .send(&h.service)
        .await;
    let (status, _headers, _body) = read_response(res).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn get_result_missing_id() {
    let h = build_harness().await;
    let res = TestClient::get("http://localhost/get_result")
        .add_header("x-api-key", h.api_key.to_string(), true)
        .send(&h.service)
        .await;
    let (status, _headers, _body) = read_response(res).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn get_result_invalid_id() {
    let h = build_harness().await;
    let res = TestClient::get("http://localhost/get_result?id=not-a-uuid")
        .add_header("x-api-key", h.api_key.to_string(), true)
        .send(&h.service)
        .await;
    let (status, _headers, _body) = read_response(res).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn get_result_missing_api_key() {
    let h = build_harness().await;
    let task_id = add_task_prompt(&h, "robot", None).await;
    let res = TestClient::get(format!("http://localhost/get_result?id={task_id}"))
        .send(&h.service)
        .await;
    let (status, _headers, _body) = read_response(res).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn get_result_invalid_api_key_format() {
    let h = build_harness().await;
    let task_id = add_task_prompt(&h, "robot", None).await;
    let res = TestClient::get(format!("http://localhost/get_result?id={task_id}"))
        .add_header("x-api-key", "not-a-uuid", true)
        .send(&h.service)
        .await;
    let (status, _headers, _body) = read_response(res).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn get_result_all_failed() {
    let h = build_harness().await;
    let task_id = add_task_prompt(&h, "robot", None).await;

    let worker = hotkey_from_seed(HOTKEY_SEED_BASE + 11);
    add_failure_result(&h.task_manager, task_id, worker).await;

    let res = TestClient::get(format!("http://localhost/get_result?id={task_id}"))
        .add_header("x-api-key", h.api_key.to_string(), true)
        .send(&h.service)
        .await;
    let (status, _headers, _body) = read_response(res).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn get_result_first_success() {
    let h = build_harness().await;
    let task_id = add_task_prompt(&h, "robot", None).await;

    let worker1 = hotkey_from_seed(HOTKEY_SEED_BASE + 4);
    let worker2 = hotkey_from_seed(HOTKEY_SEED_BASE + 5);
    let chosen_bytes = b"chosen-asset".to_vec();
    add_success_result(&h.task_manager, task_id, worker1, chosen_bytes.clone()).await;
    add_success_result(&h.task_manager, task_id, worker2, b"ignored".to_vec()).await;

    let res = TestClient::get(format!(
        "http://localhost/get_result?id={task_id}&format=spz"
    ))
    .add_header("x-api-key", h.api_key.to_string(), true)
    .send(&h.service)
    .await;
    let (status, headers, body) = read_response(res).await;
    assert_eq!(status, StatusCode::OK);
    let disposition = headers
        .get("content-disposition")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default();
    assert!(disposition.contains("result.spz"));
    assert_eq!(body, chosen_bytes);
}

#[tokio::test]
async fn get_result_all_zip() {
    let h = build_harness().await;
    let task_id = add_task_prompt(&h, "robot", None).await;

    let worker1 = hotkey_from_seed(HOTKEY_SEED_BASE + 6);
    let worker2 = hotkey_from_seed(HOTKEY_SEED_BASE + 7);
    add_success_result(&h.task_manager, task_id, worker1, b"asset1".to_vec()).await;
    add_success_result(&h.task_manager, task_id, worker2, b"asset2".to_vec()).await;

    let res = TestClient::get(format!(
        "http://localhost/get_result?id={task_id}&all=true&format=spz"
    ))
    .add_header("x-api-key", h.api_key.to_string(), true)
    .send(&h.service)
    .await;
    let (status, headers, body) = read_response(res).await;
    assert_eq!(status, StatusCode::OK);
    let disposition = headers
        .get("content-disposition")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default();
    assert!(disposition.contains("results.zip"));
    assert!(!body.is_empty());
}

#[tokio::test]
async fn get_result_all_zip_multiple_results() {
    let h = build_harness().await;
    let task_id = add_task_prompt(&h, "robot", None).await;

    let worker1 = hotkey_from_seed(HOTKEY_SEED_BASE + 13);
    let worker2 = hotkey_from_seed(HOTKEY_SEED_BASE + 14);
    add_success_result(&h.task_manager, task_id, worker1, b"asset-a".to_vec()).await;
    add_success_result(&h.task_manager, task_id, worker2, b"asset-b".to_vec()).await;

    let res = TestClient::get(format!(
        "http://localhost/get_result?id={task_id}&all=true&format=spz"
    ))
    .add_header("x-api-key", h.api_key.to_string(), true)
    .send(&h.service)
    .await;
    let (status, _headers, body) = read_response(res).await;
    assert_eq!(status, StatusCode::OK);

    let zip = async_zip::base::read::mem::ZipFileReader::new(body)
        .await
        .expect("zip reader");
    let entries = zip.file().entries().to_vec();
    assert_eq!(entries.len(), 2);

    let mut contents = Vec::new();
    for idx in 0..entries.len() {
        let mut reader = zip.reader_without_entry(idx).await.expect("zip entry");
        let mut data = Vec::new();
        reader.read_to_end(&mut data).await.expect("read entry");
        contents.push(data);
    }
    contents.sort();
    assert_eq!(contents, vec![b"asset-a".to_vec(), b"asset-b".to_vec()]);
}

#[tokio::test]
async fn get_result_format_spz_success() {
    let h = build_harness().await;
    let task_id = add_task_prompt(&h, "robot", None).await;

    let worker = hotkey_from_seed(HOTKEY_SEED_BASE + 8);
    let payload = b"spz-bytes".to_vec();
    add_success_result(&h.task_manager, task_id, worker, payload.clone()).await;

    let res = TestClient::get(format!(
        "http://localhost/get_result?id={task_id}&format=spz"
    ))
    .add_header("x-api-key", h.api_key.to_string(), true)
    .send(&h.service)
    .await;
    let (status, headers, body) = read_response(res).await;
    assert_eq!(status, StatusCode::OK);
    let disposition = headers
        .get("content-disposition")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default();
    assert!(disposition.contains("result.spz"));
    assert_eq!(body, payload);
}

#[tokio::test]
async fn get_result_format_ply_success() {
    let h = build_harness().await;
    let task_id = add_task_prompt(&h, "robot", None).await;

    let worker = hotkey_from_seed(HOTKEY_SEED_BASE + 15);
    let payload = tiny_spz_bytes();
    add_success_result(&h.task_manager, task_id, worker, payload.clone()).await;

    let res = TestClient::get(format!(
        "http://localhost/get_result?id={task_id}&format=ply"
    ))
    .add_header("x-api-key", h.api_key.to_string(), true)
    .send(&h.service)
    .await;
    let (status, headers, body) = read_response(res).await;
    assert_eq!(status, StatusCode::OK);
    let disposition = headers
        .get("content-disposition")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default();
    assert!(disposition.contains("result.ply"));
    assert!(body.starts_with(b"ply\nformat binary_little_endian 1.0\n"));
}

#[tokio::test]
async fn get_result_compress_false_returns_ply() {
    let h = build_harness().await;
    let task_id = add_task_prompt(&h, "robot", None).await;

    let worker = hotkey_from_seed(HOTKEY_SEED_BASE + 16);
    let payload = tiny_spz_bytes();
    add_success_result(&h.task_manager, task_id, worker, payload.clone()).await;

    let res = TestClient::get(format!(
        "http://localhost/get_result?id={task_id}&compress=false"
    ))
    .add_header("x-api-key", h.api_key.to_string(), true)
    .send(&h.service)
    .await;
    let (status, headers, body) = read_response(res).await;
    assert_eq!(status, StatusCode::OK);
    let disposition = headers
        .get("content-disposition")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default();
    assert!(disposition.contains("result.ply"));
    assert!(body.starts_with(b"ply\nformat binary_little_endian 1.0\n"));
}

#[tokio::test]
async fn get_result_invalid_model_config() {
    let h = build_harness().await;
    let task_id = Uuid::new_v4();
    h.task_manager
        .add_task_with_rate_limit_reservation(
            Task {
                id: task_id,
                prompt: Some(Arc::new("robot".to_string())),
                image: None,
                model: Some("missing-model".to_string()),
                seed: 0,
                model_params: Some(
                    serde_json::from_str(r#"{"preset":"default"}"#).expect("model params object"),
                ),
            },
            None,
        )
        .await;

    let worker = hotkey_from_seed(HOTKEY_SEED_BASE + 17);
    add_success_result(&h.task_manager, task_id, worker, b"spz".to_vec()).await;

    let res = TestClient::get(format!("http://localhost/get_result?id={task_id}"))
        .add_header("x-api-key", h.api_key.to_string(), true)
        .send(&h.service)
        .await;
    let (status, _headers, _body) = read_response(res).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test]
async fn get_result_allows_none_model_params() {
    let h = build_harness().await;
    let task_id = Uuid::new_v4();
    h.task_manager
        .add_task_with_rate_limit_reservation(
            Task {
                id: task_id,
                prompt: Some(Arc::new("robot".to_string())),
                image: None,
                model: Some("404-3dgs".to_string()),
                seed: 0,
                model_params: None,
            },
            None,
        )
        .await;

    let worker = hotkey_from_seed(HOTKEY_SEED_BASE + 18);
    let payload = b"spz".to_vec();
    add_success_result(&h.task_manager, task_id, worker, payload.clone()).await;

    let res = TestClient::get(format!("http://localhost/get_result?id={task_id}"))
        .add_header("x-api-key", h.api_key.to_string(), true)
        .send(&h.service)
        .await;
    let (status, _headers, body) = read_response(res).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, payload);
}

#[tokio::test]
async fn get_result_model_glb_success() {
    let h = build_harness().await;
    let image = tiny_png_bytes();
    let task_id = add_task_image(&h, &image, Some("404-mesh")).await;

    let worker = hotkey_from_seed(HOTKEY_SEED_BASE + 12);
    let payload = b"glb-bytes".to_vec();
    add_success_result(&h.task_manager, task_id, worker, payload.clone()).await;

    let res = TestClient::get(format!("http://localhost/get_result?id={task_id}"))
        .add_header("x-api-key", h.api_key.to_string(), true)
        .send(&h.service)
        .await;
    let (status, headers, body) = read_response(res).await;
    assert_eq!(status, StatusCode::OK);
    let disposition = headers
        .get("content-disposition")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default();
    assert!(disposition.contains("result.glb"));
    let content_type = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default();
    assert_eq!(content_type, "model/gltf-binary");
    assert_eq!(body, payload);
}

#[tokio::test]
async fn get_result_compress_flag_success() {
    let h = build_harness().await;
    let task_id = add_task_prompt(&h, "robot", None).await;

    let worker = hotkey_from_seed(HOTKEY_SEED_BASE + 9);
    let payload = b"spz-bytes".to_vec();
    add_success_result(&h.task_manager, task_id, worker, payload.clone()).await;

    let res = TestClient::get(format!(
        "http://localhost/get_result?id={task_id}&compress=1"
    ))
    .add_header("x-api-key", h.api_key.to_string(), true)
    .send(&h.service)
    .await;
    let (status, headers, body) = read_response(res).await;
    assert_eq!(status, StatusCode::OK);
    let disposition = headers
        .get("content-disposition")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default();
    assert!(disposition.contains("result.spz"));
    assert_eq!(body, payload);
}

#[tokio::test]
async fn get_result_invalid_compress() {
    let h = build_harness().await;
    let task_id = add_task_prompt(&h, "robot", None).await;

    let worker = hotkey_from_seed(HOTKEY_SEED_BASE + 10);
    add_success_result(&h.task_manager, task_id, worker, b"spz".to_vec()).await;

    let res = TestClient::get(format!(
        "http://localhost/get_result?id={task_id}&compress=maybe"
    ))
    .add_header("x-api-key", h.api_key.to_string(), true)
    .send(&h.service)
    .await;
    let (status, _headers, _body) = read_response(res).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}
