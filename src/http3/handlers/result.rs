use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use futures::TryStreamExt;
use multer::{Constraints, Multipart};
use salvo::prelude::*;
use tokio_util::codec::{BytesCodec, FramedRead};
use tokio_util::io::StreamReader;
use tracing::{info, warn};
use uuid::Uuid;

use crate::api::request::{AddTaskResultRequest, GetTaskResultRequest, GetTaskStatus};
use crate::api::response::GetTaskStatusResponse;
use crate::config::{HTTPConfig, ModelConfigStore, ModelOutput, ModelResolveError};
use crate::crypto::hotkey::Hotkey;
use crate::crypto::verify_hotkey;
use crate::http3::error::ServerError;
use crate::http3::handlers::common::{BOUNDARY_PREFIX, DepotExt, MULTIPART_PREFIX};
use crate::metrics::Metrics;
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
    }
}

fn parse_compress_flag(value: &str) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
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

    let task_manager = depot.require::<GatewayState>()?.task_manager();
    let model_store = depot.require::<Arc<ModelConfigStore>>()?.clone();
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
        let reason = results_vec
            .iter()
            .rev()
            .find_map(|r| r.reason.clone())
            .unwrap_or_else(|| "Unknown failure".to_string());
        return Err(ServerError::NotFound(format!(
            "No successful results found for task ID {}. Failure reason: {}",
            get_task.id, reason
        )));
    }

    results_vec.retain(|r| r.is_success());
    let mut successful_results = results_vec;

    let metrics = depot.require::<Metrics>()?;

    let (content_disposition, body, best_worker) = if get_task.all {
        successful_results.sort_by(|a, b| {
            b.get_score()
                .unwrap_or(0.0)
                .partial_cmp(&a.get_score().unwrap_or(0.0))
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        let best_worker = successful_results
            .first()
            .ok_or_else(|| ServerError::Internal("No TaskResult after filtering".into()))?
            .worker_hotkey
            .clone();

        let mut buffer = Vec::new();
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
            .max_by(|a, b| {
                b.get_score()
                    .unwrap_or(0.0)
                    .partial_cmp(&a.get_score().unwrap_or(0.0))
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .ok_or_else(|| ServerError::Internal("Failed to select best TaskResult".into()))?;

        let best_worker = best.worker_hotkey.clone();
        let asset = best
            .into_asset()
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

    let gateway_state = depot.require::<GatewayState>()?;

    let status = gateway_state.task_manager().get_status(get_status.id).await;

    res.render(Json(GetTaskStatusResponse::from(status)));

    Ok(())
}

struct AddResultMultipartData {
    id: Option<String>,
    signature: Option<String>,
    timestamp: Option<String>,
    worker_hotkey: Option<Hotkey>,
    miner_hotkey: Option<Hotkey>,
    status: Option<String>,
    asset: Option<Vec<u8>>,
    score: Option<f32>,
    reason: Option<String>,
    miner_uid: Option<u32>,
    miner_rating: Option<f32>,
}

async fn parse_add_result_multipart(
    mut multipart: Multipart<'_>,
) -> Result<AddResultMultipartData, ServerError> {
    let mut id = None;
    let mut signature = None;
    let mut timestamp = None;
    let mut worker_hotkey = None;
    let mut miner_hotkey = None;
    let mut status = None;
    let mut asset = None;
    let mut score = None;
    let mut reason = None;
    let mut miner_uid = None;
    let mut miner_rating = None;

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
            "miner_hotkey" => {
                let hotkey_str = read_text_field(field, "miner_hotkey").await?;
                let hotkey = hotkey_str
                    .parse::<Hotkey>()
                    .map_err(|e| ServerError::BadRequest(format!("Invalid miner_hotkey: {}", e)))?;
                miner_hotkey = Some(hotkey);
            }
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
            "miner_uid" => {
                miner_uid = Some(
                    read_text_field(field, "miner_uid")
                        .await?
                        .parse::<u32>()
                        .map_err(|e| {
                            ServerError::BadRequest(format!("Invalid miner_uid format: {}", e))
                        })?,
                )
            }
            "miner_rating" => {
                miner_rating = Some(
                    read_text_field(field, "miner_rating")
                        .await?
                        .parse::<f32>()
                        .map_err(|e| {
                            ServerError::BadRequest(format!("Invalid miner_rating format: {}", e))
                        })?,
                )
            }
            _ => continue,
        }
    }

    Ok(AddResultMultipartData {
        id,
        signature,
        timestamp,
        worker_hotkey,
        miner_hotkey,
        status,
        asset,
        score,
        reason,
        miner_uid,
        miner_rating,
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

    let http_cfg = depot.require::<HTTPConfig>()?;

    let constraints = Constraints::new()
        .allowed_fields(vec![
            "id",
            "signature",
            "timestamp",
            "worker_hotkey",
            "validator_hotkey",
            "miner_hotkey",
            "status",
            "asset",
            "score",
            "reason",
            "miner_uid",
            "miner_rating",
        ])
        .size_limit(
            SizeLimit::new()
                .whole_stream(http_cfg.request_file_size_limit)
                .for_field("reason", MAX_REASON_LENGTH),
        );

    let multipart = Multipart::with_constraints(byte_stream, boundary, constraints);
    let form_data = parse_add_result_multipart(multipart).await?;

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
            let score = form_data
                .score
                .ok_or(ServerError::BadRequest("Missing score for success".into()))?;
            let miner_hotkey = form_data
                .miner_hotkey
                .ok_or(ServerError::BadRequest("Missing miner_hotkey field".into()))?;
            AddTaskResultRequest {
                worker_hotkey,
                miner_hotkey: Some(miner_hotkey),
                miner_uid: form_data.miner_uid,
                miner_rating: form_data.miner_rating,
                asset: Some(asset),
                score: Some(score),
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
                miner_hotkey: None,
                miner_uid: None,
                miner_rating: None,
                asset: None,
                score: None,
                reason: Some(reason),
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
//   -F miner_hotkey=miner_key_456 \
//   -F status=success \
//   -F asset=@/path/to/result.spz \
//   -F score=0.95 \
//   -F miner_uid=42 \
//   -F miner_rating=4.5

// Add failed result
// curl --http3 -X POST https://gateway-eu.404.xyz:4443/add_result \
//   -F id=987e6543-e21b-45d6-c789-123456789abc \
//   -F signature=signature \
//   -F timestamp=404_GATEWAY_1713096000 \
//   -F validator_hotkey=worker_key_123 \
//   -F status=failure \
//   -F reason="Task timed out"

#[handler]
pub async fn add_result_handler(
    depot: &mut Depot,
    req: &mut Request,
    res: &mut Response,
) -> Result<(), ServerError> {
    let (task_id, task_result, timestamp, signature) = parse_add_result_request(depot, req).await?;
    let http_cfg = depot.require::<HTTPConfig>()?;
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

    let gateway_state = depot.require::<GatewayState>()?;
    let manager = gateway_state.task_manager();
    let metrics = depot.require::<Metrics>()?;

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

    let worker_hotkey = task_result.worker_hotkey.clone();
    let miner_hotkey = task_result.miner_hotkey.clone();
    let miner_uid = task_result.miner_uid;
    let miner_rating = task_result.miner_rating;
    let score = task_result.score;
    let reason = task_result.reason.clone();
    let is_success = task_result.is_success();

    let all_assignments_completed = match manager.add_result(task_id, task_result).await {
        Ok(completed) => completed,
        Err(AddResultError::NotAssigned) => {
            return Err(ServerError::Unauthorized(
                "Worker hotkey is not assigned to task".to_string(),
            ));
        }
    };

    if is_success {
        info!(
            "Task {}, {}, succeeded from worker {}, miner {}, miner_uid {:?}, miner_rating {:?}, score {}",
            task_id,
            task_description,
            &worker_hotkey,
            &miner_hotkey.as_ref().map_or("Unknown", |v| v.as_ref()),
            miner_uid,
            miner_rating,
            score.unwrap_or(0.0),
        );
        metrics.inc_task_completed(&worker_hotkey).await;
        metrics.inc_task_completed_kind(task_kind);
    } else {
        let reason = reason.as_deref().unwrap_or("Unknown failure");
        warn!(
            "Task {}, {}, failed from worker {}, reason: {}",
            task_id, task_description, &worker_hotkey, reason
        );
        metrics.inc_task_failed(&worker_hotkey).await;
    }

    if let Some(elapsed) = manager.get_time(task_id).await {
        metrics
            .record_completion_time(&worker_hotkey, elapsed)
            .await;
    }

    if all_assignments_completed {
        manager.finalize_task(task_id).await;
    }

    res.status_code(StatusCode::OK);
    res.render(Text::Plain("Ok"));
    Ok(())
}
