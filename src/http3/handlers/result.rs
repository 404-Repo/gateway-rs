use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use futures::{Stream, TryStreamExt};
use multer::{Constraints, Multipart};
use salvo::prelude::*;
use tokio_util::codec::{BytesCodec, FramedRead};
use tokio_util::io::StreamReader;
use tracing::{info, warn};
use uuid::Uuid;

use crate::api::request::{AddTaskResultRequest, GetTaskResultRequest, GetTaskStatus};
use crate::api::response::GetTaskStatusResponse;
use crate::config::{HTTPConfig, ModelOutput, ModelResolveError};
use crate::crypto::hotkey::Hotkey;
use crate::crypto::verify_hotkey;
use crate::http3::depot_ext::DepotExt;
use crate::http3::error::ServerError;
use crate::http3::handlers::common::{BOUNDARY_PREFIX, MULTIPART_PREFIX};
use crate::http3::rate_limits::RateLimitContext;
use crate::http3::state::HttpState;
use crate::raft::gateway_state::GatewayState;
use crate::task::AddResultError;
use async_zip::ZipEntryBuilder;
use async_zip::base::write::ZipFileWriter;
use futures::io::Cursor;
use http::HeaderValue;
use itoa::Buffer;
use multer::SizeLimit;

const MAX_REASON_LENGTH: u64 = u8::MAX as u64;

#[inline(always)]
async fn read_text_field(field: multer::Field<'_>, name: &str) -> Result<String, ServerError> {
    field
        .text()
        .await
        .map_err(|e| ServerError::BadRequest(format!("Failed to read {}: {}", name, e)))
}

#[inline(always)]
async fn process_asset(asset: Vec<u8>, decompress_spz: bool) -> Result<Vec<u8>, ServerError> {
    if decompress_spz {
        let mut data = Vec::new();
        spz_lib::decompress_async(&asset, false, &mut data)
            .await
            .map_err(|e| ServerError::Internal(format!("Failed to decompress asset: {:?}", e)))?;
        Ok(data)
    } else {
        Ok(asset)
    }
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
        ModelResolveError::UnknownModel { model, known } => ServerError::Internal(format!(
            "Model '{}' not configured (known models: {})",
            model,
            format_known_models(&known)
        )),
        ModelResolveError::InvalidOutput { model, output } => ServerError::Internal(format!(
            "Invalid output '{}' configured for model '{}'",
            output, model
        )),
        ModelResolveError::UnsupportedInput { model, input } => ServerError::Internal(format!(
            "Model '{}' does not support {} input",
            model, input
        )),
        ModelResolveError::InvalidInputSupport { model } => ServerError::Internal(format!(
            "Model '{}' must support at least one input mode",
            model
        )),
    }
}

fn parse_compress_flag(value: &str) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

fn record_origin<'a>(req: &'a Request, http_cfg: &HTTPConfig) -> &'a str {
    let origin = req
        .headers()
        .get("x-client-origin")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("api");
    if http_cfg.allowed_origins.contains(origin) {
        origin
    } else {
        "api"
    }
}

async fn record_task_activity(
    gateway_state: &GatewayState,
    rate_ctx: &RateLimitContext,
    _req: &Request,
    action: &str,
    origin: &str,
    task_kind: &str,
    task_id: Option<Uuid>,
) {
    let mut company_id = None;
    let mut company_name: Option<Arc<str>> = None;
    if rate_ctx.is_company_key
        && let Some(company) = rate_ctx.company.as_ref()
    {
        company_id = Some(company.id);
        company_name = Some(company.name.clone());
    }
    if rate_ctx.user_id.is_some() || company_id.is_some() {
        gateway_state.record_activity(
            rate_ctx.user_id,
            rate_ctx.user_email.as_deref(),
            company_id,
            company_name.as_deref(),
            action,
            origin,
            task_kind,
            task_id,
        );
    }
}

// curl --http3 "https://gateway-eu.404.xyz:4443/get_result?id=123e4567-e89b-12d3-a456-426614174000" \
//   -H "x-api-key: 123e4567-e89b-12d3-a456-426614174001" \
//   -o result.ply

// curl --http3 "https://gateway-eu.404.xyz:4443/get_result?id=123e4567-e89b-12d3-a456-426614174000&all=true" \
//   -H "x-api-key: 123e4567-e89b-12d3-a456-426614174001" \
//   -o results.zip
#[handler]
pub async fn get_result_handler(
    depot: &mut Depot,
    req: &mut Request,
    res: &mut Response,
) -> Result<(), ServerError> {
    let get_task = req
        .parse_queries::<GetTaskResultRequest>()
        .map_err(|e| ServerError::BadRequest(e.to_string()))?;

    let state = depot.require::<HttpState>()?.clone();
    let http_cfg = state.http_config();
    let record_origin = record_origin(req, http_cfg);
    let gateway_state = state.gateway_state().clone();
    let task_manager = gateway_state.task_manager();
    let rate_ctx = depot.require::<RateLimitContext>()?;
    let task_kind = "unknown";
    record_task_activity(
        &gateway_state,
        rate_ctx,
        req,
        "get_result",
        record_origin,
        task_kind,
        Some(get_task.id),
    )
    .await;
    let model_store = Arc::clone(state.model_store());
    let model_cfg = model_store.get().await;

    let results_bundle = task_manager
        .get_result(get_task.id)
        .await
        .ok_or_else(|| ServerError::NotFound(format!("Task ID {} not found", get_task.id)))?;

    let requested_format = get_task
        .format
        .as_deref()
        .map(|format| format.to_ascii_lowercase());
    let requested_compress = match get_task.compress.as_deref() {
        Some(value) => Some(parse_compress_flag(value).ok_or_else(|| {
            ServerError::BadRequest("Invalid compress value. Use 1/0/true/false.".to_string())
        })?),
        None => None,
    };
    let output = if let Some(model) = results_bundle.model.as_deref() {
        model_cfg
            .output_for(model)
            .map_err(model_error_to_server_error)?
    } else if matches!(requested_format.as_deref(), Some("ply" | "spz"))
        || requested_compress.is_some()
    {
        ModelOutput::Ply
    } else {
        model_cfg
            .output_for(model_cfg.default_model.as_str())
            .map_err(model_error_to_server_error)?
    };

    let wants_ply = if matches!(requested_format.as_deref(), Some("ply")) {
        true
    } else if matches!(requested_format.as_deref(), Some("spz")) {
        false
    } else if let Some(compress) = requested_compress {
        !compress
    } else {
        false
    };

    let (extension, content_type, decompress_spz) = match output {
        ModelOutput::Ply => {
            if wants_ply {
                ("ply", output.content_type(), true)
            } else {
                ("spz", "application/octet-stream", false)
            }
        }
        ModelOutput::Glb => ("glb", output.content_type(), false),
    };

    let mut results_vec = results_bundle.results;

    if results_vec.is_empty() {
        return Err(ServerError::NotFound(format!(
            "No results found for task ID {}",
            get_task.id
        )));
    }

    let any_success = results_vec.iter().any(|r| r.is_success());
    if !any_success {
        let reason: Arc<str> = results_vec
            .iter()
            .rev()
            .find_map(|r| r.reason.clone())
            .unwrap_or_else(|| Arc::<str>::from("Unknown failure"));
        return Err(ServerError::NotFound(format!(
            "No successful results found for task ID {}. Failure reason: {}",
            get_task.id, reason
        )));
    }

    results_vec.retain(|r| r.is_success());
    let mut successful_results = results_vec;

    let state = depot.require::<HttpState>()?.clone();
    let metrics = state.metrics().clone();

    let (content_disposition, body, best_worker) = if get_task.all {
        let best_worker = successful_results
            .first()
            .ok_or_else(|| ServerError::Internal("No TaskResult after filtering".into()))?
            .worker_hotkey
            .clone();

        let estimated = successful_results
            .iter()
            .filter_map(|r| r.asset.as_ref().map(|v| v.len()))
            .sum::<usize>();
        let mut buffer =
            Vec::with_capacity(estimated.saturating_add(estimated / 10).saturating_add(512));
        let mut zip_writer = ZipFileWriter::new(Cursor::new(&mut buffer));

        for (i, result) in successful_results.iter_mut().enumerate() {
            let asset = result.get_asset().ok_or_else(|| {
                ServerError::Internal("Missing asset on TaskResult during ZIP build".into())
            })?;

            let data = process_asset(asset, decompress_spz).await?;

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
            "attachment; filename=\"results.zip\"".to_string(),
            buffer,
            best_worker,
        )
    } else {
        let best = successful_results
            .into_iter()
            .next()
            .ok_or_else(|| ServerError::Internal("Failed to select best TaskResult".into()))?;

        let AddTaskResultRequest {
            worker_hotkey: best_worker,
            asset,
            ..
        } = best;
        let asset = asset
            .ok_or_else(|| ServerError::Internal("Missing asset on best TaskResult".into()))?;

        let data = process_asset(asset, decompress_spz).await?;

        (
            format!("attachment; filename=\"result.{}\"", extension),
            data,
            best_worker,
        )
    };

    metrics.inc_best_task(&best_worker).await;

    res.headers_mut()
        .insert("content-type", HeaderValue::from_static(content_type));
    res.headers_mut().insert(
        "Content-Disposition",
        HeaderValue::from_str(&content_disposition)
            .map_err(|e| ServerError::Internal(format!("Invalid header value: {:?}", e)))?,
    );
    let mut len_buf = Buffer::new();
    let len_str = len_buf.format(body.len());
    res.headers_mut().insert(
        "content-length",
        HeaderValue::from_str(len_str)
            .map_err(|e| ServerError::Internal(format!("Invalid header value: {:?}", e)))?,
    );
    res.body(body);

    Ok(())
}

// curl --http3 "https://gateway-eu.404.xyz:4443/get_status?id=123e4567-e89b-12d3-a456-426614174000" -H "x-api-key: 123e4567-e89b-12d3-a456-426614174001"
#[handler]
pub async fn get_status_handler(
    depot: &mut Depot,
    req: &mut Request,
    res: &mut Response,
) -> Result<(), ServerError> {
    let get_status = req
        .parse_queries::<GetTaskStatus>()
        .map_err(|e| ServerError::BadRequest(e.to_string()))?;

    let state = depot.require::<HttpState>()?.clone();
    let http_cfg = state.http_config();
    let record_origin = record_origin(req, http_cfg);
    let gateway_state = state.gateway_state().clone();
    let rate_ctx = depot.require::<RateLimitContext>()?;

    let status = gateway_state.task_manager().get_status(get_status.id).await;

    record_task_activity(
        &gateway_state,
        rate_ctx,
        req,
        "get_status",
        record_origin,
        "unknown",
        Some(get_status.id),
    )
    .await;

    res.render(Json(GetTaskStatusResponse::from(status)));

    Ok(())
}

struct AddResultMultipartData {
    id: Option<String>,
    signature: Option<String>,
    timestamp: Option<String>,
    worker_hotkey: Option<Hotkey>,
    worker_id: Option<String>,
    status: Option<String>,
    asset: Option<Vec<u8>>,
    reason: Option<String>,
}

async fn parse_add_result_multipart(
    mut multipart: Multipart<'_>,
    max_asset_bytes: u64,
) -> Result<AddResultMultipartData, ServerError> {
    let mut id = None;
    let mut signature = None;
    let mut timestamp = None;
    let mut worker_hotkey = None;
    let mut worker_id = None;
    let mut status = None;
    let mut asset = None;
    let mut reason = None;

    while let Some(mut field) = multipart
        .next_field()
        .await
        .map_err(|e| ServerError::BadRequest(format!("Field error: {}", e)))?
    {
        let name = field
            .name()
            .ok_or_else(|| ServerError::BadRequest("Unnamed field".into()))?
            .to_string();

        match name.as_str() {
            "id" => id = Some(read_text_field(field, "id").await?),
            "signature" => signature = Some(read_text_field(field, "signature").await?),
            "timestamp" => timestamp = Some(read_text_field(field, "timestamp").await?),
            "worker_hotkey" | "validator_hotkey" => {
                let hotkey_str = read_text_field(field, name.as_str()).await?;
                let hotkey = hotkey_str
                    .parse::<Hotkey>()
                    .map_err(|e| ServerError::BadRequest(format!("Invalid {}: {}", name, e)))?;
                worker_hotkey = Some(hotkey);
            }
            "worker_id" => worker_id = Some(read_text_field(field, "worker_id").await?),
            "status" => status = Some(read_text_field(field, "status").await?),
            "asset" => {
                let (lower, upper) = field.size_hint();
                let hinted = upper.or(Some(lower)).filter(|&bytes| bytes > 0);
                let capacity = hinted.unwrap_or(64 * 1024).min(max_asset_bytes as usize);

                let mut content = Vec::with_capacity(capacity);
                let mut total = 0usize;
                while let Some(chunk) = field.chunk().await.map_err(|e| {
                    if let multer::Error::FieldSizeExceeded { limit, .. } = e {
                        ServerError::BadRequest(format!(
                            "Asset exceeds maximum allowed size ({} bytes)",
                            limit
                        ))
                    } else {
                        ServerError::BadRequest(format!("Failed to read asset chunk: {}", e))
                    }
                })? {
                    total = total.checked_add(chunk.len()).ok_or_else(|| {
                        ServerError::BadRequest("Asset size overflowed usize".to_string())
                    })?;

                    if total as u64 > max_asset_bytes {
                        return Err(ServerError::BadRequest(format!(
                            "Asset exceeds maximum allowed size ({} bytes)",
                            max_asset_bytes
                        )));
                    }

                    content.extend_from_slice(&chunk);
                }
                asset = Some(content);
            }
            "reason" => reason = Some(read_text_field(field, "reason").await?),
            _ => continue,
        }
    }

    Ok(AddResultMultipartData {
        id,
        signature,
        timestamp,
        worker_hotkey,
        worker_id,
        status,
        asset,
        reason,
    })
}

async fn parse_add_result_request(
    depot: &mut Depot,
    req: &mut Request,
) -> Result<(Uuid, AddTaskResultRequest, String, String), ServerError> {
    let content_type = req
        .headers()
        .get("content-type")
        .and_then(|ct| ct.to_str().ok())
        .ok_or(ServerError::BadRequest("Missing content-type".into()))?
        .to_owned();

    if !content_type
        .get(..MULTIPART_PREFIX.len())
        .is_some_and(|s| s.eq_ignore_ascii_case(MULTIPART_PREFIX))
    {
        return Err(ServerError::BadRequest(
            "Invalid content-type, expected multipart/form-data".into(),
        ));
    }

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
    let http_cfg = state.http_config();

    let constraints = Constraints::new()
        .allowed_fields(vec![
            "id",
            "signature",
            "timestamp",
            "worker_hotkey",
            "validator_hotkey",
            "worker_id",
            "status",
            "asset",
            "reason",
        ])
        .size_limit(
            SizeLimit::new()
                .whole_stream(http_cfg.request_file_size_limit)
                .for_field("asset", http_cfg.request_file_size_limit)
                .for_field("reason", MAX_REASON_LENGTH),
        );

    let multipart = Multipart::with_constraints(byte_stream, boundary, constraints);
    let form_data = parse_add_result_multipart(multipart, http_cfg.request_file_size_limit).await?;

    let id = form_data
        .id
        .ok_or(ServerError::BadRequest("Missing id field".into()))?;
    let signature = form_data
        .signature
        .ok_or(ServerError::BadRequest("Missing signature field".into()))?;
    let timestamp = form_data
        .timestamp
        .ok_or(ServerError::BadRequest("Missing timestamp field".into()))?;
    let worker_hotkey = form_data.worker_hotkey.ok_or(ServerError::BadRequest(
        "Missing validator_hotkey or worker_hotkey field".into(),
    ))?;
    let worker_id = form_data
        .worker_id
        .ok_or(ServerError::BadRequest("Missing worker_id field".into()))?;
    let status = form_data
        .status
        .ok_or(ServerError::BadRequest("Missing status field".into()))?;

    let task_id = Uuid::parse_str(&id)
        .map_err(|e| ServerError::BadRequest(format!("Invalid id format: {}", e)))?;

    let task_result = match status.as_str() {
        "success" => {
            let asset = form_data
                .asset
                .ok_or(ServerError::BadRequest("Missing asset for success".into()))?;
            AddTaskResultRequest {
                worker_hotkey,
                worker_id: Arc::<str>::from(worker_id),
                asset: Some(asset),
                reason: None,
                instant: Instant::now(),
            }
        }
        "failure" => {
            let reason = form_data
                .reason
                .ok_or(ServerError::BadRequest("Missing reason for failure".into()))?;
            AddTaskResultRequest {
                worker_hotkey,
                worker_id: Arc::<str>::from(worker_id),
                asset: None,
                reason: Some(Arc::<str>::from(reason)),
                instant: Instant::now(),
            }
        }
        _ => return Err(ServerError::BadRequest("Invalid status".into())),
    };

    Ok((task_id, task_result, timestamp, signature))
}

// Add successful result
// curl --http3 -X POST https://gateway-eu.404.xyz:4443/add_result \
//   -F id=123e4567-e89b-12d3-a456-426614174001 \
//   -F signature=signature \
//   -F timestamp=404_GATEWAY_1713096000 \
//   -F validator_hotkey=worker_key_123 \
//   -F worker_id=worker-123 \
//   -F status=success \
//   -F asset=@/path/to/result.spz

// Add failed result
// curl --http3 -X POST https://gateway-eu.404.xyz:4443/add_result \
//   -F id=987e6543-e21b-45d6-c789-123456789abc \
//   -F signature=signature \
//   -F timestamp=404_GATEWAY_1713096000 \
//   -F validator_hotkey=worker_key_123 \
//   -F worker_id=worker-123 \
//   -F status=failure \
//   -F reason="Task timed out"

#[handler]
pub async fn add_result_handler(
    depot: &mut Depot,
    req: &mut Request,
    res: &mut Response,
) -> Result<(), ServerError> {
    let (task_id, task_result, timestamp, signature) = parse_add_result_request(depot, req).await?;
    let state = depot.require::<HttpState>()?.clone();
    let http_cfg = state.http_config();
    if !http_cfg.worker_whitelist.is_empty()
        && !http_cfg
            .worker_whitelist
            .contains(&task_result.worker_hotkey)
    {
        return Err(ServerError::Unauthorized(
            "Worker hotkey is not whitelisted".to_string(),
        ));
    }
    verify_hotkey(
        &timestamp,
        &task_result.worker_hotkey,
        &signature,
        http_cfg.signature_freshness_threshold,
    )
    .map_err(|e| ServerError::Internal(format!("Failed to verify AddTaskRequest: {:?}", e)))?;

    let gateway_state = state.gateway_state().clone();
    let manager = gateway_state.task_manager();
    let metrics = state.metrics().clone();

    let (task_description, task_kind) = if let Some(prompt) = manager.get_prompt(task_id).await {
        (
            format!("prompt: '{}'", prompt),
            crate::metrics::TaskKind::TextTo3D,
        )
    } else if let Some(_image_data) = manager.get_image(task_id).await {
        (
            "image task".to_string(),
            crate::metrics::TaskKind::ImageTo3D,
        )
    } else {
        return Err(ServerError::Internal(
            "Logic error: task has neither prompt nor image data".to_string(),
        ));
    };

    let outcome = match manager.add_result(task_id, task_result).await {
        Ok(outcome) => outcome,
        Err(AddResultError::NotAssigned) => {
            return Err(ServerError::Unauthorized(
                "Worker hotkey is not assigned to task".to_string(),
            ));
        }
    };
    let reason = outcome.reason.as_deref();

    gateway_state.record_worker_event(
        Some(task_id),
        Some(outcome.worker_id.as_ref()),
        if outcome.is_success {
            "result_success"
        } else {
            "result_failure"
        },
        task_kind.label(),
        reason,
    );

    if outcome.is_success {
        info!(
            "Task {}, {}, succeeded from worker {}, worker_id {}",
            task_id,
            task_description,
            &outcome.worker_hotkey,
            outcome.worker_id.as_ref(),
        );
        metrics
            .inc_task_completed(outcome.worker_hotkey.as_ref())
            .await;
        metrics.inc_task_completed_kind(task_kind);
    } else {
        let reason = reason.unwrap_or("Unknown failure");
        warn!(
            "Task {}, {}, failed from worker {}, reason: {}",
            task_id, task_description, &outcome.worker_hotkey, reason
        );
        metrics
            .inc_task_failed(outcome.worker_hotkey.as_ref())
            .await;
    }

    if let Some(elapsed) = manager.get_time(task_id).await {
        metrics
            .record_completion_time(outcome.worker_hotkey.as_ref(), elapsed)
            .await;
    }

    if outcome.completed {
        manager.finalize_task(task_id).await;
    }

    res.status_code(StatusCode::OK);
    res.render(Text::Plain("Ok"));
    Ok(())
}
