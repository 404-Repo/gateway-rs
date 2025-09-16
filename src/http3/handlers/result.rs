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
use crate::bittensor::crypto::verify_hotkey;
use crate::bittensor::hotkey::Hotkey;
use crate::bittensor::subnet_state::SubnetState;
use crate::config::HTTPConfig;
use crate::http3::error::ServerError;
use crate::http3::handlers::common::{DepotExt, BOUNDARY_PREFIX, MULTIPART_PREFIX};
use crate::metrics::Metrics;
use crate::raft::gateway_state::GatewayState;
use async_zip::base::write::ZipFileWriter;
use async_zip::ZipEntryBuilder;
use futures::io::Cursor;
use http::HeaderValue;
use multer::SizeLimit;

const MAX_REASON_LENGTH: u64 = u8::MAX as u64;

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

// curl --http3 "https://gateway-eu.404.xyz:4443/get_result?id=123e4567-e89b-12d3-a456-426614174000" \
//   -H "x-api-key: 123e4567-e89b-12d3-a456-426614174001" \
//   -o result.spz

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
            .find_map(|r| r.reason.clone())
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

        let best_validator = successful_results
            .first()
            .ok_or_else(|| ServerError::Internal("No TaskResult after filtering".into()))?
            .validator_hotkey
            .clone();

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
        let best = successful_results
            .into_iter()
            .max_by(|a, b| {
                b.get_score()
                    .unwrap_or(0.0)
                    .partial_cmp(&a.get_score().unwrap_or(0.0))
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .ok_or_else(|| ServerError::Internal("Failed to select best TaskResult".into()))?;

        let best_validator = best.validator_hotkey.clone();
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

    metrics.inc_best_task(&best_validator).await;

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
    validator_hotkey: Option<Hotkey>,
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
    let mut validator_hotkey = None;
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
            .ok_or_else(|| ServerError::BadRequest("Unnamed field".into()))?;

        match name {
            "id" => id = Some(read_text_field(field, "id").await?),
            "signature" => signature = Some(read_text_field(field, "signature").await?),
            "timestamp" => timestamp = Some(read_text_field(field, "timestamp").await?),
            "validator_hotkey" => {
                let hotkey_str = read_text_field(field, "validator_hotkey").await?;
                let hotkey = hotkey_str.parse::<Hotkey>().map_err(|e| {
                    ServerError::BadRequest(format!("Invalid validator_hotkey: {}", e))
                })?;
                validator_hotkey = Some(hotkey);
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
        validator_hotkey,
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
    let validator_hotkey = form_data.validator_hotkey.ok_or(ServerError::BadRequest(
        "Missing validator_hotkey field".into(),
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
                validator_hotkey,
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
                validator_hotkey,
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
pub async fn add_result_handler(
    depot: &mut Depot,
    req: &mut Request,
    res: &mut Response,
) -> Result<(), ServerError> {
    let (task_id, task_result, timestamp, signature) = parse_add_result_request(depot, req).await?;
    let http_cfg = depot.require::<HTTPConfig>()?;
    let subnet_state = depot.require::<SubnetState>()?;

    subnet_state
        .validate_hotkey(&task_result.validator_hotkey)
        .await
        .map_err(|e| ServerError::BadRequest(format!("Invalid hotkey: {}", e)))?;

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

    let task_description = if let Some(prompt) = manager.get_prompt(task_id).await {
        format!("prompt: '{}'", prompt)
    } else if let Some(_image_data) = manager.get_image(task_id).await {
        "image task".to_string()
    } else {
        return Err(ServerError::Internal(
            "Logic error: task has neither prompt nor image data".to_string(),
        ));
    };

    if task_result.is_success() {
        info!(
            "Task {}, {}, succeeded from validator {}, miner {}, miner_uid {:?}, miner_rating {:?}, score {}",
            task_id,
            task_description,
            &task_result.validator_hotkey,
            &task_result.miner_hotkey.as_ref().map_or("Unknown", |v| v),
            task_result.miner_uid,
            task_result.miner_rating,
            task_result.score.unwrap_or(0.0),
        );
        metrics
            .inc_task_completed(&task_result.validator_hotkey)
            .await;
    } else {
        let reason = task_result.reason.as_deref().unwrap_or("Unknown failure");
        warn!(
            "Task {}, {}, failed from validator {}, reason: {}",
            task_id, task_description, &task_result.validator_hotkey, reason
        );
        metrics.inc_task_failed(&task_result.validator_hotkey).await;
    }

    if let Some(elapsed) = manager.get_time(task_id).await {
        metrics
            .record_completion_time(&task_result.validator_hotkey, elapsed)
            .await;
    }
    manager.add_result(task_id, task_result).await;
    manager.remove_time(task_id).await;

    res.status_code(StatusCode::OK);
    res.render(Text::Plain("Ok"));
    Ok(())
}
