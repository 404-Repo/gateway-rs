use anyhow::Result;
use bytes::{Bytes, BytesMut};
use futures::{Stream, TryStreamExt};
use multer::{Constraints, Multipart, SizeLimit};
use salvo::prelude::*;
use std::sync::Arc;
use tokio_util::codec::{BytesCodec, FramedRead};
use tokio_util::io::StreamReader;
use tracing::info;
use uuid::Uuid;

use crate::api::Task;
use crate::api::request::{AddTaskRequest, GetTasksRequest};
use crate::api::response::{GetTasksResponse, LoadResponse};
use crate::common::image::validate_image;
use crate::common::queue::DupQueue;
use crate::config::{HTTPConfig, ImageConfig, ModelConfigStore, ModelResolveError, PromptConfig};
use crate::crypto::verify_hotkey;
use crate::http3::error::ServerError;
use crate::http3::handlers::common::{BOUNDARY_PREFIX, DepotExt, MULTIPART_PREFIX};
use crate::http3::rate_limits::RateLimitContext;
use crate::metrics::{Metrics, TaskKind};
use crate::raft::gateway_state::GatewayState;
use regex::Regex;
use serde_json::json;

fn extract_origin(req: &Request) -> &str {
    req.headers()
        .get("x-client-origin")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("unknown")
}

fn format_known_models(known: &[String]) -> String {
    if known.is_empty() {
        "none configured".to_string()
    } else {
        known.join(", ")
    }
}

fn model_error_to_server_error(err: ModelResolveError) -> ServerError {
    match err {
        ModelResolveError::UnknownModel { model, known } => {
            let message = format!(
                "Invalid value '{}'. Expected one of: {}",
                model,
                format_known_models(&known)
            );
            ServerError::BadRequestJson(json!({
                "error": "invalid_field",
                "field": "model",
                "message": message,
            }))
        }
        ModelResolveError::EmptyConfig => {
            ServerError::Internal("Model configuration is empty".to_string())
        }
        ModelResolveError::MissingDefault {
            default_model,
            known,
        } => ServerError::Internal(format!(
            "Default model '{}' not configured (known models: {})",
            default_model,
            format_known_models(&known)
        )),
        ModelResolveError::InvalidOutput { model, output } => ServerError::Internal(format!(
            "Invalid output '{}' configured for model '{}'",
            output, model
        )),
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

struct AddTaskMultipartData {
    prompt: Option<String>,
    image: Option<Bytes>,
    model: Option<String>,
}

async fn parse_add_task_multipart(
    depot: &mut Depot,
    req: &mut Request,
    boundary: &str,
) -> Result<AddTaskMultipartData, ServerError> {
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

    let image_cfg = depot.require::<ImageConfig>()?;
    let prompt_cfg = depot.require::<PromptConfig>()?;

    let constraints = Constraints::new()
        .allowed_fields(vec!["prompt", "image", "model"])
        .size_limit(
            SizeLimit::new()
                .for_field("image", image_cfg.max_size_bytes as u64)
                .for_field("prompt", prompt_cfg.max_len as u64)
                .for_field("model", 64),
        );

    let mut multipart = Multipart::with_constraints(byte_stream, boundary, constraints);
    let mut prompt = None;
    let mut image = None;
    let mut model = None;

    while let Some(mut field) = multipart
        .next_field()
        .await
        .map_err(|e| ServerError::BadRequest(format!("Field error: {}", e)))?
    {
        let name = field
            .name()
            .ok_or_else(|| ServerError::BadRequest("Unnamed field".into()))?;

        match name {
            "prompt" => {
                prompt = Some(read_text_field(field, "prompt").await?);
            }
            "image" => {
                let (lower, upper) = field.size_hint();
                let hinted = upper.or(Some(lower)).filter(|&bytes| bytes > 0);
                let capacity = hinted.unwrap_or(64 * 1024).min(image_cfg.max_size_bytes);

                let mut content = BytesMut::with_capacity(capacity);
                let mut total = 0usize;
                while let Some(chunk) = field.chunk().await.map_err(|e| {
                    if let multer::Error::FieldSizeExceeded { limit, .. } = e {
                        ServerError::BadRequest(format!(
                            "Image exceeds maximum allowed size ({} bytes)",
                            limit
                        ))
                    } else {
                        ServerError::BadRequest(format!("Failed to read image chunk: {}", e))
                    }
                })? {
                    total = total.checked_add(chunk.len()).ok_or_else(|| {
                        ServerError::BadRequest("Image size overflowed usize".to_string())
                    })?;

                    if total > image_cfg.max_size_bytes {
                        return Err(ServerError::BadRequest(format!(
                            "Image exceeds maximum allowed size ({} bytes)",
                            image_cfg.max_size_bytes
                        )));
                    }

                    content.extend_from_slice(&chunk);
                }
                image = Some(content.freeze());
            }
            "model" => {
                model = Some(read_text_field(field, "model").await?);
            }
            _ => continue,
        }
    }

    Ok(AddTaskMultipartData {
        prompt,
        image,
        model,
    })
}

// curl --http3 -X POST "https://gateway-eu.404.xyz:4443/add_task" -H "content-type: application/json" -H "x-api-key: 123e4567-e89b-12d3-a456-426614174001" -d '{"prompt": "mechanic robot"}'
// curl --http3 -X POST "https://gateway-eu.404.xyz:4443/add_task" -F "prompt=a robot" -H "x-api-key: 123e4567-e89b-12d3-a456-426614174001"
// curl --http3 -X POST "https://gateway-eu.404.xyz:4443/add_task" -F "image=@image.jpg" -H "x-api-key: 123e4567-e89b-12d3-a456-426614174001"
#[handler]
pub async fn add_task_handler(
    depot: &mut Depot,
    req: &mut Request,
    res: &mut Response,
) -> Result<(), ServerError> {
    let content_type = req
        .headers()
        .get("content-type")
        .and_then(|ct| ct.to_str().ok())
        .ok_or(ServerError::BadRequest("Missing content-type".into()))?
        .to_owned();

    let add_task = if content_type
        .get(..MULTIPART_PREFIX.len())
        .is_some_and(|s| s.eq_ignore_ascii_case(MULTIPART_PREFIX))
    {
        let boundary = content_type
            .split(';')
            .map(|s| s.trim())
            .find(|part| {
                part.get(..BOUNDARY_PREFIX.len())
                    .is_some_and(|p| p.eq_ignore_ascii_case(BOUNDARY_PREFIX))
            })
            .and_then(|part| part.split('=').nth(1))
            .ok_or(ServerError::BadRequest(
                "Missing boundary in content-type".into(),
            ))?;
        parse_add_task_multipart(depot, req, boundary).await?
    } else {
        let add_task = req
            .parse_json::<AddTaskRequest>()
            .await
            .map_err(|e| ServerError::BadRequest(e.to_string()))?;
        AddTaskMultipartData {
            prompt: add_task.prompt,
            image: None,
            model: add_task.model,
        }
    };

    // Validate that either prompt or image is provided, but not both
    let has_prompt = add_task.prompt.as_ref().is_some_and(|p| !p.is_empty());
    let has_image = add_task.image.as_ref().is_some_and(|b| !b.is_empty());
    if has_prompt == has_image {
        return Err(ServerError::BadRequest(if has_prompt {
            "Cannot provide both prompt and image. Choose one.".into()
        } else {
            "Must provide either prompt or image".into()
        }));
    }

    let task_kind = if has_image {
        TaskKind::ImageTo3D
    } else {
        TaskKind::TextTo3D
    };

    if let Some(prompt) = &add_task.prompt {
        let prompt_cfg = depot.require::<PromptConfig>()?;
        let len = prompt.chars().count();
        if len < prompt_cfg.min_len {
            return Err(ServerError::BadRequest(format!(
                "Prompt is too short: minimum length is {} characters (got {})",
                prompt_cfg.min_len, len
            )));
        }

        let prompt_regex = depot.require::<Regex>()?;
        if !prompt_regex.is_match(prompt) {
            return Err(ServerError::BadRequest(format!(
                "Prompt contains invalid characters; allowed pattern: {}",
                prompt_cfg.allowed_pattern
            )));
        }
    }

    let queue = depot.require::<DupQueue<Task>>()?;
    let metrics = depot.require::<Metrics>()?;
    let http_cfg = depot.require::<HTTPConfig>()?;
    let model_store = depot.require::<Arc<ModelConfigStore>>()?.clone();
    let model_cfg = model_store.get().await;
    let is_company_request = depot.require::<RateLimitContext>()?.is_company_key;

    // Determine and validate origin
    let origin = extract_origin(req);
    let record_origin = if http_cfg.allowed_origins.contains(origin) {
        origin
    } else {
        "unknown"
    };

    metrics.inc_request_origin(record_origin);

    if queue.len() >= http_cfg.max_task_queue_len {
        return Err(ServerError::Internal("Task queue is full".to_string()));
    }

    let resolved_model = model_cfg
        .resolve_model(add_task.model.as_deref())
        .map_err(model_error_to_server_error)?;

    // Validate image if provided
    let validated_image = if let Some(image_data) = add_task.image {
        let image_cfg = depot.require::<ImageConfig>()?;
        Some(validate_image(image_data, image_cfg)?)
    } else {
        None
    };

    let task_id = Uuid::new_v4();
    let task_description = if let Some(prompt) = &add_task.prompt {
        format!("prompt: {}", prompt)
    } else if let Some(img) = &validated_image {
        format!(
            "image - format: {:?}, dimensions: {}x{}",
            img.format, img.width, img.height
        )
    } else {
        return Err(ServerError::Internal(
            "Neither prompt nor image present after validation".to_string(),
        ));
    };

    let gateway_state = depot.require::<GatewayState>()?;

    let task = Task {
        id: task_id,
        prompt: add_task.prompt.map(Arc::new),
        image: validated_image.as_ref().map(|img| img.data.clone()),
        model: Some(resolved_model.model),
    };

    queue.push(task.clone());
    metrics.set_queue_len(queue.len());

    info!(
        "A new task has been pushed with ID: {}, {}",
        task_id, task_description
    );

    gateway_state.task_manager().add_task(task).await;

    if is_company_request
        && let Some(api_key) = req
            .headers()
            .get("x-api-key")
            .and_then(|value| value.to_str().ok())
        && let Some((company_id, _)) = gateway_state.get_company_info_from_key(api_key).await
    {
        gateway_state
            .record_company_usage(company_id, task_kind)
            .await;
    }

    res.render(Json(serde_json::json!({
        "id": task_id
    })));
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
    let model_store = depot.require::<Arc<ModelConfigStore>>()?.clone();
    let model_cfg = model_store.get().await;
    let gateway_state = depot.require::<GatewayState>()?;
    let queue = depot.require::<DupQueue<Task>>()?;
    let metrics = depot.require::<Metrics>()?;

    let gateways = gateway_state
        .gateways()
        .await
        .map_err(|e| ServerError::Internal(format!("Failed to obtain gateways: {:?}", e)))?;

    verify_hotkey(
        &get_tasks.timestamp,
        &get_tasks.worker_hotkey,
        &get_tasks.signature,
        http_cfg.signature_freshness_threshold,
    )
    .map_err(|e| ServerError::Internal(format!("Failed to verify GetTasksRequest: {:?}", e)))?;
    if !http_cfg.worker_whitelist.is_empty()
        && !http_cfg.worker_whitelist.contains(&get_tasks.worker_hotkey)
    {
        return Err(ServerError::Unauthorized(
            "Worker hotkey is not whitelisted".to_string(),
        ));
    }

    info!(
        "Worker {} has requested {} tasks.",
        get_tasks.worker_hotkey, get_tasks.requested_task_count
    );

    let model_filter = if let Some(model) = get_tasks.model.as_deref() {
        model_cfg
            .output_for(model)
            .map_err(model_error_to_server_error)?;
        Some(model.to_string())
    } else {
        None
    };

    let mut tasks = Vec::new();
    let task_manager = gateway_state.task_manager();
    if let Some(model) = model_filter {
        let default_model = model_cfg.default_model.clone();
        for (t, d) in queue.pop_with_filter(
            get_tasks.requested_task_count,
            &get_tasks.worker_hotkey,
            |task| task.model.as_deref().unwrap_or(default_model.as_str()) == model.as_str(),
        ) {
            task_manager
                .record_assignment(t.id, get_tasks.worker_hotkey.clone())
                .await;
            tasks.push(t);
            if let Some(dur) = d {
                metrics.record_queue_time(dur.as_secs_f64());
            }
        }
    } else {
        for (t, d) in queue.pop(get_tasks.requested_task_count, &get_tasks.worker_hotkey) {
            task_manager
                .record_assignment(t.id, get_tasks.worker_hotkey.clone())
                .await;
            tasks.push(t);
            if let Some(dur) = d {
                metrics.record_queue_time(dur.as_secs_f64());
            }
        }
    }

    metrics.set_queue_len(queue.len());

    gateway_state.update_task_acquisition().map_err(|e| {
        ServerError::Internal(format!(
            "Failed to execute update_task_acquisition: {:?}",
            e
        ))
    })?;

    metrics
        .inc_tasks_received(&get_tasks.worker_hotkey, tasks.len())
        .await;

    if !tasks.is_empty() {
        info!(
            "Worker {} received {} tasks",
            get_tasks.worker_hotkey,
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
