use std::time::{Duration, Instant};

use anyhow::anyhow;
use anyhow::Result;
use async_zip::base::write::ZipFileWriter;
use async_zip::ZipEntryBuilder;
use futures::io::Cursor;
use futures::TryStreamExt;
use http::HeaderValue;
use multer::{Constraints, Multipart};
use prometheus::{Encoder as _, TextEncoder, TEXT_FORMAT};
use rustls::SupportedProtocolVersion;
use salvo::catcher::Catcher;
use salvo::conn::quinn::QuinnListener;
use salvo::conn::rustls::RustlsConfig;
use salvo::http::request::SecureMaxSize;
use salvo::prelude::*;

use tokio_util::codec::{BytesCodec, FramedRead};
use tokio_util::io::StreamReader;
use tracing::{info, warn};
use uuid::Uuid;

use crate::api::request::{
    AddTaskRequest, AddTaskResultRequest, GatewayInfoExt, GetTaskResultRequest, GetTaskStatus,
    GetTasksRequest, TaskResultType, UpdateGenericKeyRequest,
};
use crate::api::response::{
    GenericKeyResponse, GetTaskStatusResponse, GetTasksResponse, LeaderResponse, LoadResponse,
};
use crate::api::Task;
use crate::bittensor::crypto::verify_hotkey;
use crate::bittensor::subnet_state::SubnetState;
use crate::common::log::get_build_information;
use crate::common::queue::DupQueue;
use crate::config::{HTTPConfig, NodeConfig, PromptConfig};
use crate::http3::response::custom_response;
use crate::metrics::Metrics;
use crate::raft::gateway_state::GatewayState;
use multer::SizeLimit;
use regex::Regex;
use std::net::SocketAddr;
use std::sync::Arc;

use super::error::ServerError;
use super::user_rate_limits::RateLimits;
use crate::http3::company_rate_limits::{company_rate_limit, CompanyRateLimiterStore};

const MAX_REASON_LENGTH: u64 = u8::MAX as u64;

pub(crate) trait DepotExt {
    fn require<T: Send + Sync + 'static>(&self) -> Result<&T, ServerError>;
}

impl DepotExt for salvo::Depot {
    fn require<T: Send + Sync + 'static>(&self) -> Result<&T, ServerError> {
        self.obtain::<T>().map_err(|e| {
            ServerError::Internal(format!(
                "Failed to obtain {}: {:?}",
                std::any::type_name::<T>(),
                e
            ))
        })
    }
}

#[inline(always)]
async fn read_text_field(
    field: multer::Field<'_>,
    name: &'static str,
) -> Result<String, ServerError> {
    field
        .text()
        .await
        .map_err(|e| ServerError::BadRequest(format!("Failed to read {}: {}", name, e)))
}

// curl --http3 -X POST "https://gateway-eu.404.xyz:4443/add_task" -H "content-type: application/json" -H "x-api-key: 123e4567-e89b-12d3-a456-426614174001" -d '{"prompt": "mechanic robot"}'
#[handler]
async fn add_task_handler(
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

// curl --http3 -X POST "https://gateway-eu.404.xyz:4443/update_key" -H "x-admin-key: b6c8597a-00e9-493a-b6cd-5dfc7244d46b" -H "content-type: application/json" -d '{"generic_key": "6f3a2de1-f25d-4413-b0ad-4631eabbbb79"}'
#[handler]
async fn generic_key_update_handler(
    depot: &mut Depot,
    req: &mut Request,
    res: &mut Response,
) -> Result<(), ServerError> {
    let ugk = req
        .parse_json::<UpdateGenericKeyRequest>()
        .await
        .map_err(|e| ServerError::BadRequest(e.to_string()))?;

    let gateway_state = depot.require::<GatewayState>()?;
    let current_node_id = *depot.require::<usize>()? as u64;
    let admin_key = req
        .headers()
        .get("x-admin-key")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    gateway_state
        .update_gateway_generic_key(current_node_id, Some(ugk.generic_key), admin_key)
        .await
        .map_err(|e| {
            ServerError::Internal(format!("Failed to update gateway generic key: {:?}", e))
        })?;

    res.status_code(StatusCode::OK);
    res.render(Text::Plain("Ok"));
    Ok(())
}

// curl --http3 -X GET "https://gateway-eu.404.xyz:4443/get_key" -H "x-admin-key: b6c8597a-00e9-493a-b6cd-5dfc7244d46b"
#[handler]
async fn generic_key_read_handler(
    depot: &mut Depot,
    _req: &mut Request,
    res: &mut Response,
) -> Result<(), ServerError> {
    let gateway_state = depot.require::<GatewayState>()?;

    if let Some(uuid) = gateway_state.generic_key().await {
        let response = GenericKeyResponse { generic_key: uuid };
        res.render(Json(response));
        Ok(())
    } else {
        Err(ServerError::Internal("Generic key not found".to_string()))
    }
}

// curl --http3 -X POST https://gateway-eu.404.xyz:4443/add_result \
//   -F id=123e4567-e89b-12d3-a456-426614174001 \
//   -F signature=signature \
//   -F timestamp=404_GATEWAY_1713096000 \
//   -F validator_hotkey=validator_key_123 \
//   -F miner_hotkey=miner_key_456 \
//   -F status=success \
//   -F asset=@/path/to/asset.spz \
//   -F score=0.95

// curl -X POST https://gateway-eu.404.xyz:4443/add_result \
//   -F id=123e4567-e89b-12d3-a456-426614174001 \
//   -F signature=signature \
//   -F timestamp=404_GATEWAY_1713096000 \
//   -F validator_hotkey=validator_key_123 \
//   -F status=failure \
//   -F reason="Task timed out"
#[handler]
async fn add_result_handler(
    depot: &mut Depot,
    req: &mut Request,
    res: &mut Response,
) -> Result<(), ServerError> {
    let content_type = req
        .headers()
        .get("content-type")
        .and_then(|ct| ct.to_str().ok())
        .map(|s| s.to_string())
        .ok_or(ServerError::BadRequest("Missing content-type".into()))?;

    if !content_type
        .to_lowercase()
        .starts_with("multipart/form-data")
    {
        return Err(ServerError::BadRequest(
            "Invalid content-type, expected multipart/form-data".into(),
        ));
    }

    let boundary = content_type
        .split(';')
        .map(|s| s.trim())
        .find(|part| part.to_lowercase().starts_with("boundary="))
        .and_then(|part| part.split('=').nth(1))
        .ok_or(ServerError::BadRequest(
            "Missing boundary in content-type".into(),
        ))?;

    let raw_stream = req
        .take_body()
        .into_stream()
        .map_err(|err| std::io::Error::other(format!("Stream error: {}", err)))
        .and_then(|frame| async move {
            frame
                .into_data()
                .map_err(|_| std::io::Error::other("Frame data error".to_string()))
        });

    let stream_reader = StreamReader::new(raw_stream);
    let byte_stream = FramedRead::new(stream_reader, BytesCodec::new()).map_ok(|b| b.freeze());

    let http_cfg = depot.require::<HTTPConfig>()?;

    let constraints = Constraints::new()
        .allowed_fields(vec![
            "id",
            "signature",
            "timestamp",
            "validator_hotkey",
            "miner_hotkey",
            "status",
            "asset",
            "score",
            "reason",
        ])
        .size_limit(
            SizeLimit::new()
                .whole_stream(http_cfg.request_file_size_limit)
                .for_field("reason", MAX_REASON_LENGTH),
        );

    let mut multipart = Multipart::with_constraints(byte_stream, boundary, constraints);

    let mut id = None;
    let mut signature = None;
    let mut timestamp = None;
    let mut validator_hotkey = None;
    let mut miner_hotkey = None;
    let mut status = None;
    let mut asset = None;
    let mut score = None;
    let mut reason = None;

    while let Some(mut field) = multipart
        .next_field()
        .await
        .map_err(|e| ServerError::BadRequest(format!("Field error: {}", e)))?
    {
        let name = field
            .name()
            .ok_or_else(|| ServerError::BadRequest("Unnamed field".into()))?;

        match name {
            "id" => id = Some(read_text_field(field, "id").await?),
            "signature" => signature = Some(read_text_field(field, "signature").await?),
            "timestamp" => timestamp = Some(read_text_field(field, "timestamp").await?),
            "validator_hotkey" => {
                validator_hotkey = Some(read_text_field(field, "validator_hotkey").await?)
            }
            "miner_hotkey" => miner_hotkey = Some(read_text_field(field, "miner_hotkey").await?),
            "status" => status = Some(read_text_field(field, "status").await?),
            "asset" => {
                let mut content = Vec::new();
                while let Some(chunk) = field.chunk().await.map_err(|e| {
                    ServerError::BadRequest(format!("Failed to read asset chunk: {}", e))
                })? {
                    content.extend_from_slice(&chunk);
                }
                asset = Some(content);
            }
            "score" => {
                score = Some(
                    read_text_field(field, "score")
                        .await?
                        .parse::<f32>()
                        .map_err(|e| {
                            ServerError::BadRequest(format!("Invalid score format: {}", e))
                        })?,
                )
            }
            "reason" => reason = Some(read_text_field(field, "reason").await?),
            _ => continue,
        }
    }

    let id = id.ok_or(ServerError::BadRequest("Missing id field".into()))?;
    let signature = signature.ok_or(ServerError::BadRequest("Missing signature field".into()))?;
    let timestamp = timestamp.ok_or(ServerError::BadRequest("Missing timestamp field".into()))?;
    let validator_hotkey = validator_hotkey.ok_or(ServerError::BadRequest(
        "Missing validator_hotkey field".into(),
    ))?;
    let status = status.ok_or(ServerError::BadRequest("Missing status field".into()))?;

    let task_id = Uuid::parse_str(&id)
        .map_err(|e| ServerError::BadRequest(format!("Invalid id format: {}", e)))?;

    let task_result = match status.as_str() {
        "success" => {
            let asset = asset.ok_or(ServerError::BadRequest("Missing asset for success".into()))?;
            let score = score.ok_or(ServerError::BadRequest("Missing score for success".into()))?;
            let miner_hotkey =
                miner_hotkey.ok_or(ServerError::BadRequest("Missing miner_hotkey field".into()))?;
            AddTaskResultRequest {
                validator_hotkey,
                miner_hotkey: Some(miner_hotkey),
                result: TaskResultType::Success { asset, score },
                instant: Instant::now(),
            }
        }
        "failure" => {
            let reason =
                reason.ok_or(ServerError::BadRequest("Missing reason for failure".into()))?;
            AddTaskResultRequest {
                validator_hotkey,
                miner_hotkey: None,
                result: TaskResultType::Failure { reason },
                instant: Instant::now(),
            }
        }
        _ => return Err(ServerError::BadRequest("Invalid status".into())),
    };

    let subnet_state = depot.require::<SubnetState>()?;

    subnet_state
        .validate_hotkey(&task_result.validator_hotkey)
        .map_err(|e| {
            ServerError::Internal(format!("Hotkey is not registered in the subnet: {:?}", e))
        })?;

    verify_hotkey(
        &timestamp,
        &task_result.validator_hotkey,
        &signature,
        http_cfg.signature_freshness_threshold,
    )
    .map_err(|e| ServerError::Internal(format!("Failed to verify AddTaskRequest: {:?}", e)))?;

    let gateway_state = depot.require::<GatewayState>()?;
    let manager = gateway_state.task_manager();
    let metrics = depot.require::<Metrics>()?;

    match &task_result.result {
        TaskResultType::Success { score, .. } => {
            info!(
                "Task {} , prompt: '{}' , succeeded from validator {}, miner {}, score {}",
                task_id,
                manager
                    .get_prompt(task_id)
                    .as_deref()
                    .unwrap_or("<unknown>"),
                &task_result.validator_hotkey,
                &task_result.miner_hotkey.as_ref().map_or("Unknown", |v| v),
                score
            );
            metrics.inc_task_completed(&task_result.validator_hotkey);
        }
        TaskResultType::Failure { reason } => {
            warn!(
                "Task {} , prompt: '{}' , failed from validator {}, reason: {}",
                task_id,
                manager
                    .get_prompt(task_id)
                    .as_deref()
                    .unwrap_or("<unknown>"),
                &task_result.validator_hotkey,
                reason
            );
            metrics.inc_task_failed(&task_result.validator_hotkey);
        }
    }

    metrics.record_completion_time(&task_result.validator_hotkey, manager.get_time(task_id));
    manager.add_result(task_id, task_result).await;
    manager.remove_time(task_id);

    res.status_code(StatusCode::OK);
    res.render(Text::Plain("Ok"));
    Ok(())
}

// curl --http3 "https://gateway-eu.404.xyz:4443/get_result?id=123e4567-e89b-12d3-a456-426614174000" \
//   -H "x-api-key: 123e4567-e89b-12d3-a456-426614174001" \
//   -o result.spz

// curl --http3 "https://gateway-eu.404.xyz:4443/get_result?id=123e4567-e89b-12d3-a456-426614174000&all=true" \
//   -H "x-api-key: 123e4567-e89b-12d3-a456-426614174001" \
//   -o results.zip
#[inline(always)]
async fn process_asset(asset: Vec<u8>, is_ply: bool) -> Result<Vec<u8>, ServerError> {
    if is_ply {
        let mut data = Vec::new();
        spz_lib::decompress_async(&asset, false, &mut data)
            .await
            .map_err(|e| ServerError::Internal(format!("Failed to decompress asset: {:?}", e)))?;
        Ok(data)
    } else {
        Ok(asset)
    }
}

#[handler]
pub async fn get_result_handler(
    depot: &mut Depot,
    req: &mut Request,
    res: &mut Response,
) -> Result<(), ServerError> {
    let get_task = req
        .parse_queries::<GetTaskResultRequest>()
        .map_err(|e| ServerError::BadRequest(e.to_string()))?;

    let is_ply = get_task.format.as_deref() == Some("ply");

    let task_manager = depot.require::<GatewayState>()?.task_manager();

    let mut results_vec = task_manager
        .get_result(get_task.id)
        .await
        .ok_or_else(|| ServerError::NotFound(format!("Task ID {} not found", get_task.id)))?;

    if results_vec.is_empty() {
        return Err(ServerError::NotFound(format!(
            "No results found for task ID {}",
            get_task.id
        )));
    }

    let any_success = results_vec.iter().any(|r| r.is_success());
    if !any_success {
        let reason = results_vec
            .iter()
            .rev()
            .find_map(|r| match &r.result {
                TaskResultType::Failure { reason } => Some(reason.clone()),
                _ => None,
            })
            .unwrap_or_else(|| "Unknown failure".to_string());
        return Err(ServerError::NotFound(format!(
            "No successful results found for task ID {}. Failure reason: {}",
            get_task.id, reason
        )));
    }

    let mut successful_results: Vec<AddTaskResultRequest> =
        results_vec.drain(..).filter(|r| r.is_success()).collect();

    let metrics = depot.require::<Metrics>()?;

    let (content_type, content_disposition, body, best_validator) = if get_task.all {
        successful_results.sort_by(|a, b| {
            b.get_score()
                .unwrap_or(0.0)
                .partial_cmp(&a.get_score().unwrap_or(0.0))
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        let best_validator = std::mem::take(
            &mut successful_results
                .first_mut()
                .ok_or_else(|| ServerError::Internal("No TaskResult after filtering".into()))?
                .validator_hotkey,
        );

        let mut buffer = Vec::new();
        let mut zip_writer = ZipFileWriter::new(Cursor::new(&mut buffer));

        for (i, result) in successful_results.iter_mut().enumerate() {
            let asset = result.get_asset().ok_or_else(|| {
                ServerError::Internal("Missing asset on TaskResult during ZIP build".into())
            })?;

            let data = process_asset(asset, is_ply).await?;

            let extension = if is_ply { "ply" } else { "spz" };
            let entry = ZipEntryBuilder::new(
                format!("{}.{}", i + 1, extension).into(),
                async_zip::Compression::Stored,
            )
            .build();

            zip_writer
                .write_entry_whole(entry, &data)
                .await
                .map_err(|e| {
                    ServerError::Internal(format!(
                        "Failed to write ZIP entry {} for task {}: {:?}",
                        i + 1,
                        get_task.id,
                        e
                    ))
                })?;
        }

        zip_writer.close().await.map_err(|e| {
            ServerError::Internal(format!(
                "Failed to close ZIP archive for task {}: {:?}",
                get_task.id, e
            ))
        })?;

        (
            "application/zip",
            "attachment; filename=\"results.zip\"".to_string(),
            buffer,
            best_validator,
        )
    } else {
        let mut best = successful_results
            .into_iter()
            .max_by(|a, b| {
                b.get_score()
                    .unwrap_or(0.0)
                    .partial_cmp(&a.get_score().unwrap_or(0.0))
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .ok_or_else(|| ServerError::Internal("Failed to select best TaskResult".into()))?;

        let best_validator = std::mem::take(&mut best.validator_hotkey);
        let asset = best
            .into_asset()
            .ok_or_else(|| ServerError::Internal("Missing asset on best TaskResult".into()))?;

        let data = process_asset(asset, is_ply).await?;

        let extension = if is_ply { "ply" } else { "spz" };
        (
            "application/octet-stream",
            format!("attachment; filename=\"result.{}\"", extension),
            data,
            best_validator,
        )
    };

    metrics.inc_best_task(&best_validator);

    res.headers_mut()
        .insert("content-type", HeaderValue::from_static(content_type));
    res.headers_mut().insert(
        "Content-Disposition",
        HeaderValue::from_str(&content_disposition)
            .map_err(|e| ServerError::Internal(format!("Invalid header value: {:?}", e)))?,
    );
    res.headers_mut().insert(
        "content-length",
        HeaderValue::from_str(&body.len().to_string())
            .map_err(|e| ServerError::Internal(format!("Invalid header value: {:?}", e)))?,
    );
    res.body(body);

    Ok(())
}

// curl --http3 "https://gateway-eu.404.xyz:4443/get_status?id=123e4567-e89b-12d3-a456-426614174000" -H "x-api-key: 123e4567-e89b-12d3-a456-426614174001"
#[handler]
async fn get_status_handler(
    depot: &mut Depot,
    req: &mut Request,
    res: &mut Response,
) -> Result<(), ServerError> {
    let get_status = req
        .parse_queries::<GetTaskStatus>()
        .map_err(|e| ServerError::BadRequest(e.to_string()))?;

    let gateway_state = depot.require::<GatewayState>()?;

    let status = gateway_state.task_manager().get_status(get_status.id).await;

    res.render(Json(GetTaskStatusResponse::from(status)));

    Ok(())
}

// curl --http3 -X POST https://gateway-eu.404.xyz:4443/get_tasks \
//   -H "content-type: application/json" \
//   -d '{"validator_hotkey": "abc123", "signature": "signatureinbase64", "timestamp": "404_GATEWAY_1743657200", "requested_task_count": 10}'
#[handler]
async fn get_tasks_handler(
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
async fn get_load_handler(
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

#[handler]
async fn write_handler(
    depot: &mut Depot,
    req: &mut Request,
    res: &mut Response,
) -> Result<(), ServerError> {
    let gi = req
        .parse_json::<GatewayInfoExt>()
        .await
        .map_err(|e| ServerError::BadRequest(e.to_string()))?;

    let gateway_state = depot.require::<GatewayState>()?;

    if gi.cluster_name != gateway_state.cluster_name() {
        return Err(ServerError::Unauthorized("Unauthorized access".to_string()));
    }

    gateway_state
        .set_gateway_info(gi.into())
        .await
        .map_err(|e| ServerError::Internal(format!("Failed to set gateway info: {:?}", e)))?;
    res.status_code(StatusCode::OK);
    res.render(Text::Plain("Ok"));
    Ok(())
}

#[handler]
async fn get_leader_handler(
    depot: &mut Depot,
    _req: &mut Request,
    res: &mut Response,
) -> Result<(), ServerError> {
    let gateway_state = depot.require::<GatewayState>()?;
    let leader_id = match gateway_state.leader().await {
        Some(id) => id,
        None => {
            return Err(ServerError::Internal("The leader is not elected".into()));
        }
    };
    let gateway_info = gateway_state
        .gateway(leader_id)
        .await
        .map_err(|e| ServerError::Internal(format!("Failed to obtain GatewayState: {:?}", e)))?;

    let response = LeaderResponse {
        leader_id,
        domain: gateway_info.domain,
        ip: gateway_info.ip,
        http_port: gateway_info.http_port,
    };
    res.render(Json(response));
    Ok(())
}

#[handler]
async fn version_handler(_depot: &mut Depot, _req: &mut Request, res: &mut Response) {
    let build_info = get_build_information(true);
    res.render(Text::Html(build_info));
}

#[handler]
async fn id_handler(
    depot: &mut Depot,
    _req: &mut Request,
    res: &mut Response,
) -> Result<(), ServerError> {
    let node_id = *depot.require::<usize>()?;

    res.render(node_id.to_string());
    Ok(())
}

#[handler]
async fn api_or_generic_key_check(depot: &mut Depot, req: &mut Request) -> Result<(), ServerError> {
    let gateway_state = depot.require::<GatewayState>()?;

    let key_str = req
        .headers()
        .get("x-api-key")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| ServerError::Unauthorized("Missing API key".to_string()))?;

    if key_str.len() != uuid::fmt::Hyphenated::LENGTH {
        return Err(ServerError::BadRequest(
            "Invalid API key length".to_string(),
        ));
    }

    let uuid = Uuid::parse_str(key_str)
        .map_err(|_| ServerError::BadRequest("API key must be a valid UUID format".to_string()))?;

    if gateway_state.is_valid_api_key(key_str)
        || gateway_state.is_generic_key(&uuid).await
        || gateway_state.is_company_key(key_str)
    {
        Ok(())
    } else {
        Err(ServerError::Unauthorized("Invalid API key".to_string()))
    }
}

#[handler]
async fn metrics_handler(depot: &mut Depot, res: &mut Response) -> Result<(), ServerError> {
    let metrics = depot.require::<Metrics>()?;

    let metric_families = metrics.registry().gather();
    let encoder = TextEncoder::new();
    let mut buffer = Vec::new();

    encoder
        .encode(&metric_families, &mut buffer)
        .map_err(|e| ServerError::Internal(format!("Failed to encode metrics: {}", e)))?;

    res.headers_mut()
        .insert("content-type", HeaderValue::from_static(TEXT_FORMAT));
    res.status_code(StatusCode::OK);
    res.body(buffer);

    Ok(())
}

#[handler]
async fn admin_key_check(depot: &mut Depot, req: &mut Request) -> Result<(), ServerError> {
    let http_cfg = depot.require::<HTTPConfig>()?;

    let is_admin = req
        .headers()
        .get("x-admin-key")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| Uuid::parse_str(s).ok())
        == Some(http_cfg.admin_key);

    if is_admin {
        Ok(())
    } else {
        Err(ServerError::Unauthorized(
            "Invalid or missing admin key".to_string(),
        ))
    }
}

pub struct Http3Server {
    join_handle: tokio::task::JoinHandle<()>,
    subnet_state: SubnetState,
}

impl Http3Server {
    pub async fn run(
        config: Arc<NodeConfig>,
        tls_config: RustlsConfig,
        gateway_state: GatewayState,
        task_queue: DupQueue<Task>,
    ) -> Result<Self> {
        let addr_str = format!("{}:{}", config.network.bind_ip, config.http.port);
        let addr: SocketAddr = addr_str
            .parse()
            .map_err(|e| anyhow!("Invalid listen address {}: {}", addr_str, e))?;

        let subnet_state = SubnetState::new(
            config.http.wss_bittensor.clone(),
            config.http.subnet_number,
            None,
            Duration::from_secs(config.http.subnet_poll_interval_sec),
            config.http.wss_max_message_size,
        );

        let router = Self::setup_router(config, &subnet_state, &gateway_state, &task_queue)?;

        let service = Service::new(router).catcher(Catcher::default().hoop(custom_response));

        const TLS13_ONLY: &[&SupportedProtocolVersion] = &[&rustls::version::TLS13];
        let tls_config = tls_config.tls_versions(TLS13_ONLY);

        let join_handle = tokio::spawn(async move {
            let tcp_listener = TcpListener::new(addr).rustls(tls_config.clone());
            let acceptor = QuinnListener::new(tls_config, addr)
                .join(tcp_listener)
                .bind()
                .await;
            Server::new(acceptor).serve(service).await;
        });

        Ok(Self {
            join_handle,
            subnet_state,
        })
    }

    fn setup_router(
        config: Arc<NodeConfig>,
        subnet_state: &SubnetState,
        gateway_state: &GatewayState,
        task_queue: &DupQueue<Task>,
    ) -> Result<Router> {
        let http_config = &config.http;
        let prompt_config = &config.prompt;

        let prompt_regex = Regex::new(&prompt_config.allowed_pattern)
            .map_err(|e| anyhow!("Invalid prompt regex: {}", e))?;

        let rate_limits = RateLimits::new(http_config);

        let request_size_limit = http_config.request_size_limit as usize;
        let size_limit_handler = || SecureMaxSize(request_size_limit);
        let request_file_size_limit = http_config.request_file_size_limit as usize;
        let file_size_limit_handler = || SecureMaxSize(request_file_size_limit);
        let raft_write_size_limit = http_config.raft_write_size_limit as usize;
        let raft_write_size_limit_handler = || SecureMaxSize(raft_write_size_limit);

        let metrics = Metrics::new(0.05).map_err(|e| anyhow!(e))?;
        let company_store = CompanyRateLimiterStore::new();

        let router = if http_config.compression {
            Router::new().hoop(
                Compression::new()
                    .force_priority(true)
                    .enable_zstd(CompressionLevel::Precise(http_config.compression_lvl)),
            )
        } else {
            Router::new()
        };

        Ok(router
            .hoop(affix_state::inject(task_queue.clone()))
            .hoop(affix_state::inject(gateway_state.clone()))
            .hoop(affix_state::inject(subnet_state.clone()))
            .hoop(affix_state::inject(http_config.clone()))
            .hoop(affix_state::inject(config.network.node_id as usize))
            .hoop(affix_state::inject(metrics))
            .hoop(affix_state::inject(company_store))
            .hoop(affix_state::inject(prompt_config.clone()))
            .hoop(affix_state::inject(prompt_regex))
            .push(
                Router::with_path("/add_task")
                    .hoop(size_limit_handler())
                    .hoop(rate_limits.unauthorized_only_limiter)
                    .hoop(api_or_generic_key_check)
                    .hoop(rate_limits.generic_global_limiter)
                    .hoop(rate_limits.generic_per_ip_limiter)
                    .hoop(rate_limits.user_id_global_limiter)
                    .hoop(rate_limits.user_id_per_user_limiter)
                    .hoop(company_rate_limit)
                    .post(add_task_handler),
            )
            .push(
                Router::with_path("/add_result")
                    .hoop(file_size_limit_handler())
                    .hoop(rate_limits.result_limiter)
                    .post(add_result_handler),
            )
            .push(
                Router::with_path("/get_tasks")
                    .hoop(size_limit_handler())
                    .post(get_tasks_handler),
            )
            .push(
                Router::with_path("/get_load")
                    .hoop(size_limit_handler())
                    .hoop(rate_limits.load_limiter)
                    .get(get_load_handler),
            )
            .push(
                Router::with_path("/get_leader")
                    .hoop(size_limit_handler())
                    .hoop(rate_limits.leader_limiter)
                    .get(get_leader_handler),
            )
            .push(
                Router::with_path("/write")
                    .hoop(raft_write_size_limit_handler())
                    .hoop(rate_limits.write_limiter)
                    .post(write_handler),
            )
            .push(
                Router::with_path("/get_version")
                    .hoop(size_limit_handler())
                    .hoop(rate_limits.basic_limiter)
                    .get(version_handler),
            )
            .push(
                Router::with_path("/id")
                    .hoop(size_limit_handler())
                    .get(id_handler),
            )
            .push(
                Router::with_path("/get_result")
                    .hoop(size_limit_handler())
                    .hoop(api_or_generic_key_check)
                    .get(get_result_handler),
            )
            .push(
                Router::with_path("/get_status")
                    .hoop(size_limit_handler())
                    .hoop(rate_limits.status_limiter)
                    .hoop(api_or_generic_key_check)
                    .get(get_status_handler),
            )
            .push(
                Router::with_path("/update_key")
                    .hoop(size_limit_handler())
                    .hoop(rate_limits.update_limiter)
                    .hoop(admin_key_check)
                    .post(generic_key_update_handler),
            )
            .push(
                Router::with_path("/metrics")
                    .hoop(size_limit_handler())
                    .hoop(rate_limits.metric_limiter)
                    .get(metrics_handler),
            )
            .push(
                Router::with_path("/get_key")
                    .hoop(size_limit_handler())
                    .hoop(rate_limits.read_limiter)
                    .hoop(admin_key_check)
                    .get(generic_key_read_handler),
            ))
    }

    pub fn abort(&self) {
        self.join_handle.abort();
        self.subnet_state.abort();
    }
}
