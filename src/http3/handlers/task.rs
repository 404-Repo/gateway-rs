use anyhow::Result;
use salvo::prelude::*;
use tracing::info;
use uuid::Uuid;

use crate::api::request::{AddTaskRequest, GetTasksRequest};
use crate::api::response::{GetTasksResponse, LoadResponse};
use crate::api::Task;
use crate::bittensor::crypto::verify_hotkey;
use crate::bittensor::subnet_state::SubnetState;
use crate::common::queue::DupQueue;
use crate::config::{HTTPConfig, PromptConfig};
use crate::http3::error::ServerError;
use crate::http3::handlers::common::DepotExt;
use crate::metrics::Metrics;
use crate::raft::gateway_state::GatewayState;
use regex::Regex;

// curl --http3 -X POST "https://gateway-eu.404.xyz:4443/add_task" -H "content-type: application/json" -H "x-api-key: 123e4567-e89b-12d3-a456-426614174001" -d '{"prompt": "mechanic robot"}'
#[handler]
pub async fn add_task_handler(
    depot: &mut Depot,
    req: &mut Request,
    res: &mut Response,
) -> Result<(), ServerError> {
    let add_task = req
        .parse_json::<AddTaskRequest>()
        .await
        .map_err(|e| ServerError::BadRequest(e.to_string()))?;

    let prompt_cfg = depot.require::<PromptConfig>()?;
    let len = add_task.prompt.chars().count();
    if len < prompt_cfg.min_len {
        return Err(ServerError::BadRequest(format!(
            "Prompt is too short: minimum length is {} characters (got {})",
            prompt_cfg.min_len, len
        )));
    }
    if len > prompt_cfg.max_len {
        return Err(ServerError::BadRequest(format!(
            "Prompt is too long: maximum length is {} characters (got {})",
            prompt_cfg.max_len, len
        )));
    }

    let prompt_regex = depot.require::<Regex>()?;
    if !prompt_regex.is_match(&add_task.prompt) {
        return Err(ServerError::BadRequest(format!(
            "Prompt contains invalid characters; allowed pattern: {}",
            prompt_cfg.allowed_pattern
        )));
    }

    let task = Task {
        id: Uuid::new_v4(),
        prompt: add_task.prompt,
    };

    let queue = depot.require::<DupQueue<Task>>()?;
    let http_cfg = depot.require::<HTTPConfig>()?;

    if queue.len() >= http_cfg.max_task_queue_len {
        return Err(ServerError::Internal("Task queue is full".to_string()));
    }

    let gateway_state = depot.require::<GatewayState>()?;

    queue.push(task.clone());
    info!(
        "A new task has been pushed with ID: {}, prompt: {}",
        task.id, task.prompt
    );
    gateway_state
        .task_manager()
        .add_task(task.id, task.prompt.clone());

    res.render(Json(task));
    Ok(())
}

// curl --http3 -X POST https://gateway-eu.404.xyz:4443/get_tasks \
//   -H "content-type: application/json" \
//   -d '{"validator_hotkey": "abc123", "signature": "signatureinbase64", "timestamp": "404_GATEWAY_1743657200", "requested_task_count": 10}'
#[handler]
pub async fn get_tasks_handler(
    depot: &mut Depot,
    req: &mut Request,
    res: &mut Response,
) -> Result<(), ServerError> {
    let get_tasks = req
        .parse_json::<GetTasksRequest>()
        .await
        .map_err(|e| ServerError::BadRequest(e.to_string()))?;

    let http_cfg = depot.require::<HTTPConfig>()?;
    let gateway_state = depot.require::<GatewayState>()?;
    let queue = depot.require::<DupQueue<Task>>()?;
    let metrics = depot.require::<Metrics>()?;

    let gateways = gateway_state
        .gateways()
        .await
        .map_err(|e| ServerError::Internal(format!("Failed to obtain gateways: {:?}", e)))?;

    let subnet_state = depot.require::<SubnetState>()?;

    subnet_state
        .validate_hotkey(&get_tasks.validator_hotkey)
        .map_err(|e| {
            ServerError::Internal(format!("Hotkey is not registered in the subnet: {:?}", e))
        })?;

    verify_hotkey(
        &get_tasks.timestamp,
        &get_tasks.validator_hotkey,
        &get_tasks.signature,
        http_cfg.signature_freshness_threshold,
    )
    .map_err(|e| ServerError::Internal(format!("Failed to verify GetTasksRequest: {:?}", e)))?;

    info!(
        "Validator {} has requested {} tasks.",
        get_tasks.validator_hotkey, get_tasks.requested_task_count
    );

    let mut tasks = Vec::new();
    for (t, d) in queue.pop(get_tasks.requested_task_count, &get_tasks.validator_hotkey) {
        tasks.push(t);
        if let Some(dur) = d {
            metrics.record_queue_time(dur.as_secs_f64());
        }
    }

    gateway_state.update_task_acquisition().map_err(|e| {
        ServerError::Internal(format!(
            "Failed to execute update_task_acquisition: {:?}",
            e
        ))
    })?;

    metrics.inc_tasks_received(&get_tasks.validator_hotkey, tasks.len());

    if !tasks.is_empty() {
        info!(
            "Validator {} received {} tasks",
            get_tasks.validator_hotkey,
            tasks.len()
        );
    }

    let response = GetTasksResponse { tasks, gateways };
    res.render(Json(response));
    Ok(())
}

// curl --http3 -X GET -k https://gateway-eu.404.xyz:4443/get_load
#[handler]
pub async fn get_load_handler(
    depot: &mut Depot,
    _req: &mut Request,
    res: &mut Response,
) -> Result<(), ServerError> {
    let gateway_state = depot.require::<GatewayState>()?;

    let gateways = gateway_state.gateways().await.map_err(|e| {
        ServerError::Internal(format!(
            "Failed to obtain the gateways for get_load: {:?}",
            e
        ))
    })?;

    let load_response = LoadResponse { gateways };
    res.render(Json(load_response));
    Ok(())
}
