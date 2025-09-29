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

use crate::api::request::{AddTaskRequest, GetTasksRequest};
use crate::api::response::{GetTasksResponse, LoadResponse};
use crate::api::Task;
use crate::bittensor::crypto::verify_hotkey;
use crate::bittensor::subnet_state::SubnetState;
use crate::common::image::validate_image;
use crate::common::queue::DupQueue;
use crate::config::{HTTPConfig, ImageConfig, PromptConfig};
use crate::http3::error::ServerError;
use crate::http3::handlers::common::{DepotExt, BOUNDARY_PREFIX, MULTIPART_PREFIX};
use crate::metrics::Metrics;
use crate::raft::gateway_state::GatewayState;
use regex::Regex;

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
        .allowed_fields(vec!["prompt", "image"])
        .size_limit(
            SizeLimit::new()
                .for_field("image", image_cfg.max_size_bytes as u64)
                .for_field("prompt", prompt_cfg.max_len as u64),
        );

    let mut multipart = Multipart::with_constraints(byte_stream, boundary, constraints);
    let mut prompt = None;
    let mut image = None;

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
            _ => continue,
        }
    }

    Ok(AddTaskMultipartData { prompt, image })
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
    let http_cfg = depot.require::<HTTPConfig>()?;
    if queue.len() >= http_cfg.max_task_queue_len {
        return Err(ServerError::Internal("Task queue is full".to_string()));
    }

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
    };

    queue.push(task.clone());

    info!(
        "A new task has been pushed with ID: {}, {}",
        task_id, task_description
    );

    gateway_state.task_manager().add_task(task).await;

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
        .await
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
    let task_manager = gateway_state.task_manager();
    for (t, d) in queue.pop(get_tasks.requested_task_count, &get_tasks.validator_hotkey) {
        task_manager
            .record_assignment(t.id, get_tasks.validator_hotkey.clone())
            .await;
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

    metrics
        .inc_tasks_received(&get_tasks.validator_hotkey, tasks.len())
        .await;

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
