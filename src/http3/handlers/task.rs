use anyhow::Result;
use bytes::{Bytes, BytesMut};
use futures::{Stream, StreamExt, TryStreamExt, stream};
use multer::{Constraints, Multipart, SizeLimit};
use salvo::prelude::*;
use std::{collections::HashSet, sync::Arc};
use tokio::sync::OwnedSemaphorePermit;
use tokio_util::codec::{BytesCodec, FramedRead};
use tokio_util::io::StreamReader;
use tracing::info;
use uuid::Uuid;

use crate::api::Task;
use crate::api::request::{AddTaskRequest, GetTasksRequest};
use crate::api::response::{GetTasksResponse, LoadResponse};
use crate::common::image::validate_image;
use crate::config::ModelResolveError;
use crate::crypto::verify_hotkey;
use crate::http3::depot_ext::DepotExt;
use crate::http3::error::ServerError;
use crate::http3::handlers::common::{BOUNDARY_PREFIX, MULTIPART_PREFIX};
use crate::http3::rate_limits::RateLimitContext;
use crate::http3::state::HttpState;
use crate::metrics::TaskKind;
use serde_json::json;

const ASSIGNMENT_RECORD_CONCURRENCY: usize = 16;

fn extract_origin(req: &Request) -> &str {
    req.headers()
        .get("x-client-origin")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("api")
}

fn format_known_models(known: &[String]) -> String {
    if known.is_empty() {
        "none configured".to_string()
    } else {
        known.join(", ")
    }
}

fn task_kind_label(task: &Task) -> &'static str {
    if task.image.is_some() {
        "img3d"
    } else {
        "txt3d"
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
        ModelResolveError::UnsupportedInput { model, input } => {
            let message = format!("Model '{}' does not support {} input", model, input);
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
        ModelResolveError::InvalidInputSupport { model } => ServerError::Internal(format!(
            "Model '{}' must support at least one input mode",
            model
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

struct ValidatedAddTask {
    prompt: Option<String>,
    image: Option<crate::common::image::ImageValidationResult>,
    model: Option<String>,
    task_kind: TaskKind,
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

    let state = depot.require::<HttpState>()?.clone();
    let image_cfg = state.image_config();
    let prompt_cfg = state.prompt_config();
    let upload_limiter = state.image_upload_limiter().clone();
    let mut image_permit: Option<OwnedSemaphorePermit> = None;

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
                if image_permit.is_none() {
                    image_permit = Some(upload_limiter.acquire().await?);
                }
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

async fn parse_add_task_request(
    depot: &mut Depot,
    req: &mut Request,
) -> Result<AddTaskMultipartData, ServerError> {
    let content_type = req
        .headers()
        .get("content-type")
        .and_then(|ct| ct.to_str().ok())
        .ok_or(ServerError::BadRequest("Missing content-type".into()))?
        .to_owned();

    if content_type
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
        parse_add_task_multipart(depot, req, boundary).await
    } else {
        let add_task = req
            .parse_json::<AddTaskRequest>()
            .await
            .map_err(|e| ServerError::BadRequest(e.to_string()))?;
        Ok(AddTaskMultipartData {
            prompt: add_task.prompt,
            image: None,
            model: add_task.model,
        })
    }
}

fn validate_add_task_input(
    depot: &mut Depot,
    add_task: AddTaskMultipartData,
) -> Result<ValidatedAddTask, ServerError> {
    let state = depot.require::<HttpState>()?.clone();
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
        let prompt_cfg = state.prompt_config();
        let len = prompt.chars().count();
        if len < prompt_cfg.min_len {
            return Err(ServerError::BadRequest(format!(
                "Prompt is too short: minimum length is {} characters (got {})",
                prompt_cfg.min_len, len
            )));
        }

        let prompt_regex = state.prompt_regex();
        if !prompt_regex.is_match(prompt) {
            return Err(ServerError::BadRequest(format!(
                "Prompt contains invalid characters; allowed pattern: {}",
                prompt_cfg.allowed_pattern
            )));
        }
    }

    let validated_image = if let Some(image_data) = add_task.image {
        let image_cfg = state.image_config();
        Some(validate_image(image_data, image_cfg)?)
    } else {
        None
    };

    Ok(ValidatedAddTask {
        prompt: add_task.prompt,
        image: validated_image,
        model: add_task.model,
        task_kind,
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
    let add_task = parse_add_task_request(depot, req).await?;
    let validated = validate_add_task_input(depot, add_task)?;
    let has_prompt = validated.prompt.is_some();
    let has_image = validated.image.is_some();
    let task_kind = validated.task_kind;

    let state = depot.require::<HttpState>()?.clone();
    let queue = state.task_queue().clone();
    let metrics = state.metrics().clone();
    let http_cfg = state.http_config();
    let rate_ctx = depot.require::<RateLimitContext>()?;
    let is_company_request = rate_ctx.is_company_key;
    let user_id = rate_ctx.user_id;
    let user_email = rate_ctx.user_email.as_deref();

    // Determine and validate origin
    let origin = extract_origin(req);
    let record_origin = if http_cfg.allowed_origins.contains(origin) {
        origin
    } else {
        "api"
    };

    metrics.inc_request_origin(record_origin);

    if queue.len() >= http_cfg.max_task_queue_len {
        return Err(ServerError::Internal("Task queue is full".to_string()));
    }

    let model_store = Arc::clone(state.model_store());
    let model_cfg = model_store.get().await;
    let resolved_model = model_cfg
        .resolve_model(validated.model.as_deref())
        .map_err(model_error_to_server_error)?;
    model_cfg
        .validate_input_support(&resolved_model.model, has_prompt, has_image)
        .map_err(model_error_to_server_error)?;

    let task_id = Uuid::new_v4();
    let task_description = if let Some(prompt) = &validated.prompt {
        format!("prompt: {}", prompt)
    } else if let Some(img) = &validated.image {
        format!(
            "image - format: {:?}, dimensions: {}x{}",
            img.format, img.width, img.height
        )
    } else {
        return Err(ServerError::Internal(
            "Neither prompt nor image present after validation".to_string(),
        ));
    };

    let gateway_state = state.gateway_state().clone();

    let task = Task {
        id: task_id,
        prompt: validated.prompt.map(Arc::new),
        image: validated.image.as_ref().map(|img| img.data.clone()),
        model: Some(resolved_model.model),
    };

    queue.push(task.clone());
    metrics.set_queue_len(queue.len());

    let model_name = task.model.as_deref().unwrap_or("unknown");
    info!(
        "A new task has been pushed with ID: {}, model: {}, origin: {}, {}",
        task_id, model_name, record_origin, task_description
    );

    gateway_state.task_manager().add_task(task).await;

    let mut company_id = None;
    let mut company_name: Option<Arc<str>> = None;
    if is_company_request && let Some(company) = rate_ctx.company.as_ref() {
        company_id = Some(company.id);
        company_name = Some(company.name.clone());
    }

    if user_id.is_some() || company_id.is_some() {
        gateway_state.record_activity(
            user_id,
            user_email,
            company_id,
            company_name.as_deref(),
            "add_task",
            record_origin,
            task_kind.label(),
            Some(task_id),
        );
    }

    res.render(Json(serde_json::json!({
        "id": task_id
    })));
    Ok(())
}

// curl --http3 -X POST https://gateway-eu.404.xyz:4443/get_tasks \
//   -H "content-type: application/json" \
//   -d '{"validator_hotkey": "abc123", "worker_id": "worker-123", "signature": "signatureinbase64", "timestamp": "404_GATEWAY_1743657200", "requested_task_count": 10}'
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

    let state = depot.require::<HttpState>()?.clone();
    let http_cfg = state.http_config();
    let model_store = Arc::clone(state.model_store());
    let model_cfg = model_store.get().await;
    let gateway_state = state.gateway_state().clone();
    let queue = state.task_queue().clone();
    let metrics = state.metrics().clone();

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

    let model_filter = {
        let mut models = get_tasks.model.to_vec();
        if models.is_empty() {
            return Err(ServerError::BadRequestJson(json!({
                "error": "invalid_field",
                "field": "model",
                "message": "Expected at least one model",
            })));
        }
        let mut seen = HashSet::with_capacity(models.len());
        models.retain(|model| seen.insert(model.clone()));

        for model in &models {
            model_cfg
                .output_for(model)
                .map_err(model_error_to_server_error)?;
        }

        models
    };

    info!(
        "Worker {} has requested {} tasks, model: {}.",
        get_tasks.worker_hotkey,
        get_tasks.requested_task_count,
        model_filter.join(", ").as_str()
    );

    let gateways = gateway_state
        .gateways()
        .await
        .map_err(|e| ServerError::Internal(format!("Failed to obtain gateways: {:?}", e)))?;

    let requested_task_count = get_tasks
        .requested_task_count
        .min(http_cfg.max_task_queue_len.max(1));
    let mut tasks = Vec::with_capacity(requested_task_count);
    let mut task_ids = Vec::with_capacity(requested_task_count);
    let task_manager = gateway_state.task_manager();
    let model_set: HashSet<String> = model_filter.into_iter().collect();
    let default_model = model_cfg.default_model.as_str();
    for (task, dur) in
        queue.pop_with_filter(requested_task_count, &get_tasks.worker_hotkey, |task| {
            let model = task.model.as_deref().unwrap_or(default_model);
            model_set.contains(model)
        })
    {
        if let Some(dur) = dur {
            metrics.record_queue_time(dur.as_secs_f64());
        }
        task_ids.push(task.id);
        tasks.push(task);
    }

    if !task_ids.is_empty() {
        let worker_hotkey = get_tasks.worker_hotkey.clone();
        let worker_id = get_tasks.worker_id.clone();
        let concurrency = ASSIGNMENT_RECORD_CONCURRENCY.min(task_ids.len());
        stream::iter(task_ids)
            .for_each_concurrent(Some(concurrency.max(1)), |task_id| {
                let task_manager = task_manager.clone();
                let worker_hotkey = worker_hotkey.clone();
                let worker_id = worker_id.clone();
                async move {
                    task_manager
                        .record_assignment(task_id, worker_hotkey, worker_id)
                        .await;
                }
            })
            .await;
    }

    for task in &tasks {
        gateway_state.record_worker_event(
            Some(task.id),
            Some(get_tasks.worker_id.as_ref()),
            "task_assigned",
            task_kind_label(task),
            None,
        );
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
    let state = depot.require::<HttpState>()?.clone();
    let gateway_state = state.gateway_state().clone();

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
