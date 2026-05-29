use foldhash::{HashSet, HashSetExt};
use gateway::crypto::hotkey::Hotkey;
use http::StatusCode;

use crate::support::{
    TestClient, TestHarness, add_task_prompt, add_task_prompt_with_api_key, build_harness,
    build_harness_with_worker_whitelist, create_personal_api_key, multipart_body,
    purge_terminal_generation_task_in_db, read_response, sign_worker,
    timeout_generation_task_in_db, tiny_png_bytes, top_up_personal_api_key_balance,
    update_company_worker_tags, update_personal_api_key_limits_without_timestamp,
};

async fn signed_get_tasks_json(
    h: &TestHarness,
    seed: u8,
    worker_id: &str,
    requested_task_count: usize,
    worker_tags: Option<serde_json::Value>,
) -> (StatusCode, serde_json::Value) {
    let (worker_hotkey, timestamp, signature) = sign_worker([seed; 32]);
    let mut payload = serde_json::json!({
        "worker_hotkey": worker_hotkey.to_string(),
        "worker_id": worker_id,
        "signature": signature,
        "timestamp": timestamp,
        "requested_task_count": requested_task_count,
        "model": "404-3dgs"
    });
    if let Some(worker_tags) = worker_tags {
        payload["worker_tags"] = worker_tags;
    }

    let res = TestClient::post("http://localhost/get_tasks")
        .json(&payload)
        .send(&h.service)
        .await;
    let (status, _headers, body) = read_response(res).await;
    let payload: serde_json::Value = serde_json::from_slice(&body).expect("json response");
    (status, payload)
}

fn task_ids_from_get_tasks_payload(payload: &serde_json::Value) -> Vec<uuid::Uuid> {
    payload
        .get("tasks")
        .and_then(|value| value.as_array())
        .expect("tasks")
        .iter()
        .map(|task| {
            task.get("id")
                .and_then(|value| value.as_str())
                .and_then(|value| uuid::Uuid::parse_str(value).ok())
                .expect("task id")
        })
        .collect()
}

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
async fn get_tasks_rejects_invalid_worker_tags() {
    let h = build_harness().await;
    let invalid_worker_tags = [
        serde_json::json!([""]),
        serde_json::json!(["bad tag"]),
        serde_json::json!([format!("{}x", "a".repeat(32))]),
    ];

    for (idx, worker_tags) in invalid_worker_tags.into_iter().enumerate() {
        let (worker_hotkey, timestamp, signature) = sign_worker([40u8 + idx as u8; 32]);
        let res = TestClient::post("http://localhost/get_tasks")
            .json(&serde_json::json!({
                "worker_hotkey": worker_hotkey.to_string(),
                "worker_id": format!("worker-invalid-tags-{idx}"),
                "signature": signature,
                "timestamp": timestamp,
                "requested_task_count": 1,
                "model": "404-3dgs",
                "worker_tags": worker_tags
            }))
            .send(&h.service)
            .await;

        let (status, _headers, body) = read_response(res).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let payload: serde_json::Value = serde_json::from_slice(&body).expect("json response");
        assert_eq!(
            payload.get("error").and_then(|value| value.as_str()),
            Some("invalid_field")
        );
        assert_eq!(
            payload.get("field").and_then(|value| value.as_str()),
            Some("worker_tags")
        );
    }
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
async fn get_tasks_requires_matching_worker_tag_for_tagged_company_task() {
    let h = build_harness().await;
    update_company_worker_tags(&h, &["acme", "premium"]).await;
    let task_id = add_task_prompt_with_api_key(&h, &h.company_api_key, "company robot", None).await;

    let (worker_hotkey, timestamp, signature) = sign_worker([5u8; 32]);
    let no_match = TestClient::post("http://localhost/get_tasks")
        .json(&serde_json::json!({
            "worker_hotkey": worker_hotkey.to_string(),
            "worker_id": "worker-no-match",
            "signature": signature,
            "timestamp": timestamp,
            "requested_task_count": 1,
            "model": "404-3dgs",
            "worker_tags": ["other"]
        }))
        .send(&h.service)
        .await;
    let (status, _headers, body) = read_response(no_match).await;
    assert_eq!(status, StatusCode::OK);
    let payload: serde_json::Value = serde_json::from_slice(&body).expect("json response");
    assert_eq!(
        payload
            .get("tasks")
            .and_then(|value| value.as_array())
            .expect("tasks")
            .len(),
        0
    );

    let (worker_hotkey, timestamp, signature) = sign_worker([6u8; 32]);
    let matching = TestClient::post("http://localhost/get_tasks")
        .json(&serde_json::json!({
            "worker_hotkey": worker_hotkey.to_string(),
            "worker_id": "worker-match",
            "signature": signature,
            "timestamp": timestamp,
            "requested_task_count": 1,
            "model": "404-3dgs",
            "worker_tags": ["premium"]
        }))
        .send(&h.service)
        .await;
    let (status, _headers, body) = read_response(matching).await;
    assert_eq!(status, StatusCode::OK);
    let payload: serde_json::Value = serde_json::from_slice(&body).expect("json response");
    let tasks = payload
        .get("tasks")
        .and_then(|value| value.as_array())
        .expect("tasks");
    assert_eq!(tasks.len(), 1);
    let returned_task_id = tasks[0]
        .get("id")
        .and_then(|value| value.as_str())
        .and_then(|value| uuid::Uuid::parse_str(value).ok())
        .expect("task id");
    assert_eq!(returned_task_id, task_id);
}

#[tokio::test]
async fn get_tasks_queued_company_task_keeps_original_worker_tags_after_company_update() {
    let h = build_harness().await;
    update_company_worker_tags(&h, &["acme"]).await;
    let task_id =
        add_task_prompt_with_api_key(&h, &h.company_api_key, "company snapshot robot", None).await;

    update_company_worker_tags(&h, &["other"]).await;

    let (status, payload) = signed_get_tasks_json(
        &h,
        52,
        "worker-new-company-tag",
        1,
        Some(serde_json::json!(["other"])),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        task_ids_from_get_tasks_payload(&payload).is_empty(),
        "task queued with acme must not dynamically switch to the updated company tag"
    );

    let (status, payload) = signed_get_tasks_json(
        &h,
        53,
        "worker-original-company-tag",
        1,
        Some(serde_json::json!(["acme"])),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(task_ids_from_get_tasks_payload(&payload), vec![task_id]);
}

#[tokio::test]
async fn get_tasks_normalizes_worker_tags_before_matching_company_task() {
    let h = build_harness().await;
    update_company_worker_tags(&h, &["premium"]).await;
    let task_id =
        add_task_prompt_with_api_key(&h, &h.company_api_key, "normalized tag robot", None).await;

    let (status, payload) = signed_get_tasks_json(
        &h,
        54,
        "worker-normalized-tags",
        1,
        Some(serde_json::json!([" Premium ", "premium"])),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(task_ids_from_get_tasks_payload(&payload), vec![task_id]);
}

#[tokio::test]
async fn get_tasks_normalizes_company_worker_tags_before_matching_worker() {
    let h = build_harness().await;
    update_company_worker_tags(&h, &[" PREMIUM "]).await;
    let task_id =
        add_task_prompt_with_api_key(&h, &h.company_api_key, "normalized company tag robot", None)
            .await;

    let (status, payload) = signed_get_tasks_json(
        &h,
        58,
        "worker-matches-normalized-company-tag",
        1,
        Some(serde_json::json!(["premium"])),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(task_ids_from_get_tasks_payload(&payload), vec![task_id]);
}

#[tokio::test]
async fn get_tasks_batch_filters_public_and_restricted_company_tasks_by_worker_tags() {
    let h = build_harness().await;
    update_company_worker_tags(&h, &["acme"]).await;
    let restricted_task_id =
        add_task_prompt_with_api_key(&h, &h.company_api_key, "restricted company robot", None)
            .await;
    let public_task_id = add_task_prompt(&h, "public robot", None).await;

    let (status, payload) = signed_get_tasks_json(&h, 55, "worker-without-tags", 10, None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        task_ids_from_get_tasks_payload(&payload),
        vec![public_task_id],
        "worker without tags should only receive public tasks from a mixed batch"
    );

    let (status, payload) = signed_get_tasks_json(
        &h,
        55,
        "worker-with-restricted-tag",
        10,
        Some(serde_json::json!(["acme"])),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        task_ids_from_get_tasks_payload(&payload),
        vec![restricted_task_id]
    );
}

#[tokio::test]
async fn get_tasks_worker_with_multiple_tags_matches_company_task_by_any_tag() {
    let h = build_harness().await;
    update_company_worker_tags(&h, &["acme"]).await;
    let task_id =
        add_task_prompt_with_api_key(&h, &h.company_api_key, "multi-tag worker robot", None).await;

    let (status, payload) = signed_get_tasks_json(
        &h,
        57,
        "worker-multiple-tags",
        1,
        Some(serde_json::json!(["other", "acme"])),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(task_ids_from_get_tasks_payload(&payload), vec![task_id]);
}

#[tokio::test]
async fn get_tasks_tagged_worker_does_not_receive_untagged_company_task() {
    let h = build_harness().await;
    let task_id =
        add_task_prompt_with_api_key(&h, &h.company_api_key, "untagged company robot", None).await;

    let (status, payload) = signed_get_tasks_json(
        &h,
        59,
        "tagged-worker-public-company-task",
        1,
        Some(serde_json::json!(["acme"])),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert!(
        task_ids_from_get_tasks_payload(&payload).is_empty(),
        "worker requesting tagged work must not receive an untagged company task"
    );

    let (status, payload) =
        signed_get_tasks_json(&h, 60, "untagged-worker-company-task", 1, None).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(task_ids_from_get_tasks_payload(&payload), vec![task_id]);
}

#[tokio::test]
async fn get_tasks_allows_untagged_company_task_without_worker_tags() {
    let h = build_harness().await;
    let task_id = add_task_prompt_with_api_key(&h, &h.company_api_key, "company robot", None).await;

    let (worker_hotkey, timestamp, signature) = sign_worker([7u8; 32]);
    let res = TestClient::post("http://localhost/get_tasks")
        .json(&serde_json::json!({
            "worker_hotkey": worker_hotkey.to_string(),
            "worker_id": "worker-public",
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
        .and_then(|value| value.as_array())
        .expect("tasks");
    assert_eq!(tasks.len(), 1);
    let returned_task_id = tasks[0]
        .get("id")
        .and_then(|value| value.as_str())
        .and_then(|value| uuid::Uuid::parse_str(value).ok())
        .expect("task id");
    assert_eq!(returned_task_id, task_id);
}

#[tokio::test]
async fn get_tasks_allows_untagged_company_task_with_empty_worker_tags() {
    let h = build_harness().await;
    let task_id =
        add_task_prompt_with_api_key(&h, &h.company_api_key, "empty tags company robot", None)
            .await;

    let (status, payload) =
        signed_get_tasks_json(&h, 61, "worker-empty-tags", 1, Some(serde_json::json!([]))).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(task_ids_from_get_tasks_payload(&payload), vec![task_id]);
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
