use std::sync::Arc;
use std::time::Duration;

use http::StatusCode;
use salvo::prelude::*;
use salvo::test::TestClient;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use gateway::db::{EventRecorder, EventSinkHandle};
use gateway::test_support::{
    RateLimitContext, build_shared_harness_core, enforce_rate_limit, ensure_test_crypto_provider,
    load_test_single_node_config,
};

use crate::support::read_response;

#[handler]
async fn ok_handler() -> &'static str {
    "ok"
}

#[tokio::test]
async fn distributed_user_rate_limit_returns_429() {
    ensure_test_crypto_provider();
    let (mut config, _path) = load_test_single_node_config();
    config
        .http
        .add_task_authenticated_per_user_hourly_rate_limit = 1;
    let config = Arc::new(config);

    let config_file = tempfile::Builder::new()
        .suffix(".toml")
        .tempfile()
        .expect("temp config file");
    let config_toml = toml::to_string(config.as_ref()).expect("serialize test config");
    std::fs::write(config_file.path(), config_toml).expect("write temp config");
    let config_path = config_file.path().to_path_buf();

    let shutdown = CancellationToken::new();
    let event_recorder = EventRecorder::new(
        Arc::new(EventSinkHandle::Noop),
        Arc::from(config.network.name.as_str()),
        Duration::from_secs(30),
        config.db.events_queue_capacity.max(1),
        shutdown.clone(),
    );
    let core = build_shared_harness_core(config.clone(), config_path, event_recorder, true).await;
    let state = core.state;

    let router = Router::new().hoop(affix_state::inject(state)).push(
        Router::with_path("/rl-probe")
            .hoop(affix_state::inject(RateLimitContext {
                user_id: Some(Uuid::new_v4()),
                has_valid_api_key: true,
                key_is_uuid: true,
                ..RateLimitContext::default()
            }))
            .hoop(enforce_rate_limit)
            .get(ok_handler),
    );
    let service = Service::new(router);

    let first = TestClient::get("http://localhost/rl-probe")
        .send(&service)
        .await;
    let (first_status, _headers, _body) = read_response(first).await;
    assert_eq!(first_status, StatusCode::OK);

    let second = TestClient::get("http://localhost/rl-probe")
        .send(&service)
        .await;
    let (second_status, _headers, body) = read_response(second).await;
    assert_eq!(
        second_status,
        StatusCode::TOO_MANY_REQUESTS,
        "second probe body: {}",
        String::from_utf8_lossy(&body)
    );
}
