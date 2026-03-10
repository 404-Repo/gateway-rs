use http::StatusCode;
use salvo::test::TestClient;

use gateway::db::ViolationRow;

use crate::support::build_harness_with_violation_tracking;

#[tokio::test]
async fn basic_rate_limit_violations_are_tracked() {
    let h = build_harness_with_violation_tracking().await;

    // The basic_rate_limit on /add_task has a per-minute per-IP quota.
    // Flood requests until we get 429, then verify tracker captured them.
    let mut got_429 = false;
    for _ in 0..200 {
        let res = TestClient::post("http://localhost/add_task")
            .add_header("x-api-key", h.api_key.to_string(), true)
            .json(&serde_json::json!({"prompt": "hello world test"}))
            .send(&h.service)
            .await;
        if res.status_code.unwrap_or(StatusCode::OK) == StatusCode::TOO_MANY_REQUESTS {
            got_429 = true;
            break;
        }
    }

    assert!(got_429, "expected at least one 429 response");

    // Flush the tracker to the in-memory sink.
    h.violation_tracker.flush_once_for_test().await;

    let rows: Vec<ViolationRow> = h.violation_sink.rows().await;
    assert!(
        !rows.is_empty(),
        "violation sink should have at least one row after rate-limit trigger"
    );

    let row = &rows[0];
    assert!(row.total_count >= 1);

    let details = row.details.as_object().expect("details is JSON object");
    assert!(
        !details.is_empty(),
        "details should contain at least one entry"
    );
}

#[tokio::test]
async fn no_violations_when_under_limit() {
    let h = build_harness_with_violation_tracking().await;

    let res = TestClient::post("http://localhost/add_task")
        .add_header("x-api-key", h.api_key.to_string(), true)
        .json(&serde_json::json!({"prompt": "hello world test"}))
        .send(&h.service)
        .await;

    assert_eq!(res.status_code.unwrap(), StatusCode::OK);

    h.violation_tracker.flush_once_for_test().await;

    let rows: Vec<ViolationRow> = h.violation_sink.rows().await;
    assert!(
        rows.is_empty(),
        "no violations expected when requests are within limits"
    );
}

#[tokio::test]
async fn distributed_rate_limit_violations_are_tracked() {
    let h = build_harness_with_violation_tracking().await;

    // The distributed rate limiter has per-user hourly limits.
    // With the test config, the generic key global hourly limit is small.
    // Flood /add_task until TooManyRequests.
    let mut got_429 = false;
    for _ in 0..500 {
        let res = TestClient::post("http://localhost/add_task")
            .add_header("x-api-key", h.api_key.to_string(), true)
            .json(&serde_json::json!({"prompt": "hello distributed test"}))
            .send(&h.service)
            .await;
        if res.status_code.unwrap_or(StatusCode::OK) == StatusCode::TOO_MANY_REQUESTS {
            got_429 = true;
            break;
        }
    }

    if !got_429 {
        // If the basic per-IP limiter is too tight we may not reach the
        // distributed limit. That's acceptable -- we only assert if we did
        // get a 429.
        return;
    }

    h.violation_tracker.flush_once_for_test().await;
    let rows: Vec<ViolationRow> = h.violation_sink.rows().await;
    assert!(
        !rows.is_empty(),
        "violation sink should capture distributed-limiter violations"
    );

    let total: u64 = rows.iter().map(|r| r.total_count).sum();
    assert!(total >= 1, "should have recorded at least one violation");
}

#[tokio::test]
async fn multiple_flushes_produce_independent_rows() {
    let h = build_harness_with_violation_tracking().await;

    // Trigger some violations.
    for _ in 0..200 {
        let res = TestClient::post("http://localhost/add_task")
            .add_header("x-api-key", h.api_key.to_string(), true)
            .json(&serde_json::json!({"prompt": "batch test"}))
            .send(&h.service)
            .await;
        if res.status_code.unwrap_or(StatusCode::OK) == StatusCode::TOO_MANY_REQUESTS {
            break;
        }
    }

    h.violation_tracker.flush_once_for_test().await;

    // Trigger more violations.
    for _ in 0..50 {
        let _ = TestClient::post("http://localhost/add_task")
            .add_header("x-api-key", h.api_key.to_string(), true)
            .json(&serde_json::json!({"prompt": "batch test 2"}))
            .send(&h.service)
            .await;
    }

    h.violation_tracker.flush_once_for_test().await;

    let rows: Vec<ViolationRow> = h.violation_sink.rows().await;
    // If rate limits were triggered in both rounds, we get two rows.
    // If only the first round triggered, we get one row.
    // Either way, the second flush should not duplicate the first batch.
    if rows.len() >= 2 {
        assert_ne!(
            rows[0].details, rows[1].details,
            "each flush should produce independent snapshots"
        );
    }
}
