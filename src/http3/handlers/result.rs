use std::convert::Infallible;
use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use bytes::Bytes;
use futures::stream;
use multer::{Constraints, Multipart};
use salvo::prelude::*;
use tokio::io::duplex;
use tokio_util::io::ReaderStream;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::api::request::{AddTaskResultRequest, GetTaskResultRequest, GetTaskStatus};
use crate::api::response::GetTaskStatusResponse;
use crate::config::ModelOutput;
use crate::crypto::hotkey::Hotkey;
use crate::db::{
    FinalizeGenerationTaskAssignmentInput, FinalizeGenerationTaskAssignmentOutcome,
    GenerationTaskStatusSnapshot,
};
use crate::http3::depot_ext::DepotExt;
use crate::http3::error::ServerError;
use crate::http3::handlers::common::activity::{TaskActivityContext, record_task_activity};
use crate::http3::handlers::common::model_errors::{
    ModelErrorContext, model_error_to_server_error,
};
use crate::http3::handlers::common::multipart::{
    is_multipart_form, multipart_stream, parse_boundary, read_binary_field, read_text_field,
};
use crate::http3::handlers::common::origin::normalize_origin;
use crate::http3::handlers::common::worker_auth::{WorkerAuthContext, validate_worker_request};
use crate::http3::rate_limits::RateLimitContext;
use crate::http3::state::HttpState;
use crate::raft::gateway_state::{GatewayState, WorkerEventRef};
use crate::task::{AddResultError, TaskStatus};
use async_zip::ZipEntryBuilder;
use async_zip::base::write::ZipFileWriter;
use http::HeaderValue;
use itoa::Buffer;
use multer::SizeLimit;

const MAX_REASON_LENGTH: u64 = u8::MAX as u64;
const ZIP_STREAM_BUFFER_BYTES: usize = 64 * 1024;

#[inline(always)]
async fn process_asset(asset: Bytes, decompress_spz: bool) -> Result<Bytes, ServerError> {
    if decompress_spz {
        let mut data = Vec::new();
        spz_lib::decompress_async(asset, false, &mut data)
            .await
            .map_err(|e| ServerError::Internal(format!("Failed to decompress asset: {:?}", e)))?;
        Ok(Bytes::from(data))
    } else {
        Ok(asset)
    }
}

fn set_download_headers(
    res: &mut Response,
    content_type: &'static str,
    content_disposition: &str,
) -> Result<(), ServerError> {
    res.headers_mut()
        .insert("content-type", HeaderValue::from_static(content_type));
    res.headers_mut().insert(
        "Content-Disposition",
        HeaderValue::from_str(content_disposition)
            .map_err(|e| ServerError::Internal(format!("Invalid header value: {:?}", e)))?,
    );
    Ok(())
}

fn set_content_length_header(res: &mut Response, len: usize) -> Result<(), ServerError> {
    let mut len_buf = Buffer::new();
    let len_str = len_buf.format(len);
    res.headers_mut().insert(
        "content-length",
        HeaderValue::from_str(len_str)
            .map_err(|e| ServerError::Internal(format!("Invalid header value: {:?}", e)))?,
    );
    Ok(())
}

fn parse_compress_flag(value: &str) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

fn task_status_from_snapshot(task: &GenerationTaskStatusSnapshot) -> TaskStatus {
    match task.status.as_str() {
        "succeeded" => TaskStatus::Success {
            worker_id: Arc::<str>::from(
                task.result_worker_id.as_deref().unwrap_or("unknown-worker"),
            ),
        },
        "failed" | "timed_out" | "cancelled" => TaskStatus::Failure {
            reason: Arc::<str>::from(
                task.error_message
                    .as_deref()
                    .unwrap_or("Task failed without a stored error message."),
            ),
        },
        "running" if task.finished_results_count > 0 => {
            TaskStatus::PartialResult(task.success_count.max(0) as usize)
        }
        _ => TaskStatus::NoResult,
    }
}

fn generation_task_status_snapshot_error(err: &anyhow::Error) -> ServerError {
    error!(
        "Failed to read generation task status from the database: {:?}",
        err
    );
    ServerError::Internal("Failed to read generation task status from the database".to_string())
}

fn generation_task_account_lookup_error(err: &anyhow::Error) -> ServerError {
    error!(
        "Failed to read generation task owner from the database: {:?}",
        err
    );
    ServerError::Internal("Failed to read generation task owner from the database".to_string())
}

fn task_not_found_error(task_id: Uuid) -> ServerError {
    ServerError::NotFound(format!("Task ID {} not found", task_id))
}

fn task_context_unavailable_error() -> ServerError {
    ServerError::Json(
        StatusCode::GONE,
        serde_json::json!({
            "error": "task_context_unavailable",
            "message": "Task context is unavailable on this gateway after restart; the task will be refunded on timeout.",
        }),
    )
}

async fn authorize_task_read_access(
    gateway_state: &GatewayState,
    rate_ctx: &RateLimitContext,
    task_id: Uuid,
) -> Result<(), ServerError> {
    if !rate_ctx.key_is_uuid {
        return Ok(());
    }

    if rate_ctx.is_generic_key {
        // Shared generic-key tasks persist as ownerless rows. Allow reads only when the task is
        // likewise ownerless so the shared key cannot bypass personal/company ownership checks.
        let task_owner = gateway_state
            .get_generation_task_access_owner(task_id)
            .await
            .map_err(|err| generation_task_account_lookup_error(&err))?;
        return if task_owner.is_none() {
            Ok(())
        } else {
            Err(task_not_found_error(task_id))
        };
    }

    if !(rate_ctx.user_id.is_some() || rate_ctx.is_company_key) {
        return Ok(());
    }

    let owner = rate_ctx.billing_owner.as_ref().ok_or_else(|| {
        ServerError::Unauthorized("API key is no longer active for billing.".to_string())
    })?;
    let task_owner = gateway_state
        .get_generation_task_access_owner(task_id)
        .await
        .map_err(|err| generation_task_account_lookup_error(&err))?;

    let is_owned = task_owner.is_some_and(|task_owner| {
        if let Some(company_id) = owner.company_id {
            task_owner.company_id == Some(company_id)
        } else if let Some(user_id) = owner.user_id {
            task_owner.user_id == Some(user_id)
        } else {
            task_owner.account_id == Some(owner.account_id)
        }
    });

    if is_owned {
        Ok(())
    } else {
        Err(task_not_found_error(task_id))
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
    let cfg = state.config();
    let http_cfg = cfg.http();
    let metrics = state.metrics().clone();
    let record_origin = normalize_origin(req, http_cfg);
    let gateway_state = state.gateway_state().clone();
    let task_manager = gateway_state.task_manager();
    let rate_ctx = depot.require::<RateLimitContext>()?;
    authorize_task_read_access(&gateway_state, rate_ctx, get_task.id).await?;
    let task_kind = "unknown";
    record_task_activity(
        TaskActivityContext {
            gateway_state: &gateway_state,
            rate_ctx,
            origin: record_origin,
            task_kind,
            model: None,
            task_id: Some(get_task.id),
        },
        "get_result",
    );
    let model_cfg = &cfg.node().model_config;

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
            .map_err(|err| model_error_to_server_error(err, ModelErrorContext::Internal))?
    } else if matches!(requested_format.as_deref(), Some("ply" | "spz"))
        || requested_compress.is_some()
    {
        ModelOutput::Ply
    } else {
        model_cfg
            .output_for(model_cfg.default_model.as_str())
            .map_err(|err| model_error_to_server_error(err, ModelErrorContext::Internal))?
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
    results_vec.sort_by(|a, b| a.instant.cmp(&b.instant));
    let successful_results = results_vec;

    if get_task.all {
        let best_worker = successful_results
            .first()
            .ok_or_else(|| ServerError::Internal("No TaskResult after filtering".into()))?
            .worker_hotkey
            .clone();
        metrics.inc_best_task(&best_worker).await;

        let content_disposition = "attachment; filename=\"results.zip\"";
        set_download_headers(res, "application/zip", content_disposition)?;

        let task_id = get_task.id;
        let (writer, reader) = duplex(ZIP_STREAM_BUFFER_BYTES);
        tokio::spawn(async move {
            let mut zip_writer = ZipFileWriter::with_tokio(writer);

            for (i, result) in successful_results.into_iter().enumerate() {
                let Some(asset) = result.asset else {
                    error!(
                        task_id = %task_id,
                        entry_index = i + 1,
                        "Missing asset on TaskResult during streamed ZIP build"
                    );
                    return;
                };

                let data = match process_asset(asset, decompress_spz).await {
                    Ok(data) => data,
                    Err(err) => {
                        error!(
                            task_id = %task_id,
                            entry_index = i + 1,
                            error = ?err,
                            "Failed to prepare streamed ZIP entry"
                        );
                        return;
                    }
                };

                let entry = ZipEntryBuilder::new(
                    format!("{}.{}", i + 1, extension).into(),
                    async_zip::Compression::Stored,
                )
                .build();

                if let Err(err) = zip_writer.write_entry_whole(entry, data.as_ref()).await {
                    error!(
                        task_id = %task_id,
                        entry_index = i + 1,
                        error = ?err,
                        "Failed to write streamed ZIP entry"
                    );
                    return;
                }
            }

            if let Err(err) = zip_writer.close().await {
                error!(
                    task_id = %task_id,
                    error = ?err,
                    "Failed to finalize streamed ZIP archive"
                );
            }
        });

        res.stream(ReaderStream::new(reader));
        return Ok(());
    }

    let best = successful_results
        .into_iter()
        .next()
        .ok_or_else(|| ServerError::Internal("Failed to select best TaskResult".into()))?;

    let AddTaskResultRequest {
        worker_hotkey: best_worker,
        asset,
        ..
    } = best;
    let asset =
        asset.ok_or_else(|| ServerError::Internal("Missing asset on best TaskResult".into()))?;
    let data = process_asset(asset, decompress_spz).await?;

    metrics.inc_best_task(&best_worker).await;

    let content_disposition = format!("attachment; filename=\"result.{}\"", extension);
    set_download_headers(res, content_type, &content_disposition)?;
    set_content_length_header(res, data.len())?;
    res.stream(stream::once(async move { Ok::<_, Infallible>(data) }));

    Ok(())
}

// curl --http3 "https://gateway-eu.404.xyz:4443/get_status?id=123e4567-e89b-12d3-a456-426614174000"
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
    let gateway_state = state.gateway_state().clone();

    let mut status = gateway_state.task_manager().get_status(get_status.id).await;
    if matches!(status, TaskStatus::NoResult)
        && let Some(snapshot) = gateway_state
            .get_generation_task_status_snapshot(get_status.id)
            .await
            .map_err(|err| generation_task_status_snapshot_error(&err))?
    {
        status = task_status_from_snapshot(&snapshot);
    }

    res.render(Json(GetTaskStatusResponse::from(status)));

    Ok(())
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum AddResultStatus {
    Success,
    Failure,
}

impl AddResultStatus {
    fn parse(value: &str) -> Result<Self, ServerError> {
        match value {
            "success" => Ok(Self::Success),
            "failure" => Ok(Self::Failure),
            _ => Err(ServerError::BadRequest("Invalid status".into())),
        }
    }
}

#[derive(Default)]
struct PendingAddResultMetadata {
    id: Option<String>,
    signature: Option<String>,
    timestamp: Option<String>,
    worker_hotkey: Option<Hotkey>,
    worker_id: Option<String>,
    assignment_token: Option<Uuid>,
    status: Option<AddResultStatus>,
}

struct AuthorizedAddResultMetadata {
    task_id: Uuid,
    worker_hotkey: Hotkey,
    worker_id: Arc<str>,
    assignment_token: Uuid,
    status: AddResultStatus,
    task_description: String,
    task_kind: crate::metrics::TaskKind,
}

struct ParsedAddResultRequest {
    task_id: Uuid,
    task_result: AddTaskResultRequest,
    task_description: String,
    task_kind: crate::metrics::TaskKind,
}

fn add_result_metadata_ready(metadata: &PendingAddResultMetadata) -> bool {
    metadata.id.is_some()
        && metadata.signature.is_some()
        && metadata.timestamp.is_some()
        && metadata.worker_hotkey.is_some()
        && metadata.worker_id.is_some()
        && metadata.status.is_some()
}

fn ensure_field_not_seen<T>(field: &Option<T>, field_name: &str) -> Result<(), ServerError> {
    if field.is_some() {
        Err(ServerError::BadRequest(format!(
            "Duplicate {field_name} field"
        )))
    } else {
        Ok(())
    }
}

fn ensure_asset_field_allowed(
    metadata_authorized: bool,
    status: Option<AddResultStatus>,
) -> Result<(), ServerError> {
    if !metadata_authorized {
        return Err(ServerError::BadRequest(
            "Asset field must come after id, signature, timestamp, worker hotkey, worker_id, status, and optional assignment_token fields".into(),
        ));
    }

    match status {
        Some(AddResultStatus::Success) => Ok(()),
        Some(AddResultStatus::Failure) => Err(ServerError::BadRequest(
            "Asset is only allowed when status=success".into(),
        )),
        None => Err(ServerError::BadRequest(
            "Asset field must come after id, signature, timestamp, worker hotkey, worker_id, status, and optional assignment_token fields".into(),
        )),
    }
}

async fn authorize_add_result_metadata(
    state: &HttpState,
    metadata: &PendingAddResultMetadata,
) -> Result<AuthorizedAddResultMetadata, ServerError> {
    let id = metadata
        .id
        .as_deref()
        .ok_or(ServerError::BadRequest("Missing id field".into()))?;
    let signature = metadata
        .signature
        .as_deref()
        .ok_or(ServerError::BadRequest("Missing signature field".into()))?;
    let timestamp = metadata
        .timestamp
        .as_deref()
        .ok_or(ServerError::BadRequest("Missing timestamp field".into()))?;
    let worker_hotkey = metadata
        .worker_hotkey
        .clone()
        .ok_or(ServerError::BadRequest(
            "Missing validator_hotkey or worker_hotkey field".into(),
        ))?;
    let worker_id = Arc::<str>::from(
        metadata
            .worker_id
            .clone()
            .ok_or(ServerError::BadRequest("Missing worker_id field".into()))?,
    );
    let assignment_token = metadata.assignment_token;
    let status = metadata
        .status
        .ok_or(ServerError::BadRequest("Missing status field".into()))?;

    let task_id = Uuid::parse_str(id)
        .map_err(|e| ServerError::BadRequest(format!("Invalid id format: {}", e)))?;

    let cfg = state.config();
    validate_worker_request(
        cfg.http(),
        &worker_hotkey,
        timestamp,
        signature,
        WorkerAuthContext::AddResult,
    )?;

    let gateway_state = state.gateway_state().clone();
    let manager = gateway_state.task_manager();
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
        return Err(task_context_unavailable_error());
    };

    let assigned = match assignment_token {
        Some(token) => {
            manager
                .is_assigned_with_token(task_id, &worker_hotkey, token)
                .await
        }
        None => {
            manager
                .is_assigned_to_worker_id(task_id, &worker_hotkey, &worker_id)
                .await
        }
    };
    if !assigned {
        return Err(ServerError::Unauthorized(
            "Worker hotkey is not assigned to task".to_string(),
        ));
    }

    Ok(AuthorizedAddResultMetadata {
        task_id,
        worker_hotkey,
        worker_id,
        assignment_token: assignment_token.unwrap_or_else(Uuid::nil),
        status,
        task_description,
        task_kind,
    })
}

async fn parse_add_result_request(
    req: &mut Request,
    state: &HttpState,
) -> Result<ParsedAddResultRequest, ServerError> {
    let content_type = req
        .headers()
        .get("content-type")
        .and_then(|ct| ct.to_str().ok())
        .ok_or(ServerError::BadRequest("Missing content-type".into()))?
        .to_owned();

    if !is_multipart_form(&content_type) {
        return Err(ServerError::BadRequest(
            "Invalid content-type, expected multipart/form-data".into(),
        ));
    }

    let boundary = parse_boundary(&content_type)?;
    let byte_stream = multipart_stream(req);

    let request_file_size_limit = state.gateway_state().request_file_size_limit();

    let constraints = Constraints::new()
        .allowed_fields(vec![
            "id",
            "signature",
            "timestamp",
            "worker_hotkey",
            "validator_hotkey",
            "worker_id",
            "assignment_token",
            "status",
            "asset",
            "reason",
        ])
        .size_limit(
            SizeLimit::new()
                .whole_stream(request_file_size_limit)
                .for_field("asset", request_file_size_limit)
                .for_field("reason", MAX_REASON_LENGTH),
        );

    let mut multipart = Multipart::with_constraints(byte_stream, boundary, constraints);
    let mut metadata = PendingAddResultMetadata::default();
    let mut authorized = None;
    let mut asset = None;
    let mut reason = None;

    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|e| ServerError::BadRequest(format!("Field error: {}", e)))?
    {
        let name = field
            .name()
            .ok_or_else(|| ServerError::BadRequest("Unnamed field".into()))?
            .to_string();

        match name.as_str() {
            "id" => {
                ensure_field_not_seen(&metadata.id, "id")?;
                metadata.id = Some(read_text_field(field, "id").await?);
            }
            "signature" => {
                ensure_field_not_seen(&metadata.signature, "signature")?;
                metadata.signature = Some(read_text_field(field, "signature").await?);
            }
            "timestamp" => {
                ensure_field_not_seen(&metadata.timestamp, "timestamp")?;
                metadata.timestamp = Some(read_text_field(field, "timestamp").await?);
            }
            "worker_hotkey" | "validator_hotkey" => {
                ensure_field_not_seen(&metadata.worker_hotkey, "worker_hotkey")?;
                let hotkey_str = read_text_field(field, name.as_str()).await?;
                let hotkey = hotkey_str
                    .parse::<Hotkey>()
                    .map_err(|e| ServerError::BadRequest(format!("Invalid {}: {}", name, e)))?;
                metadata.worker_hotkey = Some(hotkey);
            }
            "worker_id" => {
                ensure_field_not_seen(&metadata.worker_id, "worker_id")?;
                metadata.worker_id = Some(read_text_field(field, "worker_id").await?);
            }
            "assignment_token" => {
                ensure_field_not_seen(&metadata.assignment_token, "assignment_token")?;
                let raw_token = read_text_field(field, "assignment_token").await?;
                metadata.assignment_token =
                    Some(Uuid::parse_str(raw_token.as_str()).map_err(|err| {
                        ServerError::BadRequest(format!("Invalid assignment_token format: {}", err))
                    })?);
            }
            "status" => {
                ensure_field_not_seen(&metadata.status, "status")?;
                let raw_status = read_text_field(field, "status").await?;
                metadata.status = Some(AddResultStatus::parse(&raw_status)?);
            }
            "asset" => {
                ensure_field_not_seen(&asset, "asset")?;
                ensure_asset_field_allowed(add_result_metadata_ready(&metadata), metadata.status)?;
                if authorized.is_none() {
                    authorized = Some(authorize_add_result_metadata(state, &metadata).await?);
                }
                asset = Some(read_binary_field(field, request_file_size_limit, "asset").await?);
            }
            "reason" => {
                ensure_field_not_seen(&reason, "reason")?;
                reason = Some(read_text_field(field, "reason").await?);
            }
            _ => continue,
        }
    }

    let authorized = match authorized {
        Some(authorized) => authorized,
        None => {
            if !add_result_metadata_ready(&metadata) {
                authorize_add_result_metadata(state, &metadata).await?
            } else if matches!(metadata.status, Some(AddResultStatus::Success)) && asset.is_none() {
                return Err(ServerError::BadRequest("Missing asset for success".into()));
            } else if matches!(metadata.status, Some(AddResultStatus::Failure)) && reason.is_none()
            {
                return Err(ServerError::BadRequest("Missing reason for failure".into()));
            } else {
                authorize_add_result_metadata(state, &metadata).await?
            }
        }
    };

    let AuthorizedAddResultMetadata {
        task_id,
        worker_hotkey,
        worker_id,
        assignment_token,
        status,
        task_description,
        task_kind,
    } = authorized;

    let task_result = match status {
        AddResultStatus::Success => {
            let asset = asset.ok_or(ServerError::BadRequest("Missing asset for success".into()))?;
            AddTaskResultRequest {
                worker_hotkey,
                worker_id,
                assignment_token,
                asset: Some(Bytes::from(asset)),
                reason: None,
                instant: Instant::now(),
            }
        }
        AddResultStatus::Failure => {
            if asset.is_some() {
                return Err(ServerError::BadRequest(
                    "Asset is only allowed when status=success".into(),
                ));
            }
            let reason =
                reason.ok_or(ServerError::BadRequest("Missing reason for failure".into()))?;
            AddTaskResultRequest {
                worker_hotkey,
                worker_id,
                assignment_token,
                asset: None,
                reason: Some(Arc::<str>::from(reason)),
                instant: Instant::now(),
            }
        }
    };

    Ok(ParsedAddResultRequest {
        task_id,
        task_result,
        task_description,
        task_kind,
    })
}

// Add successful result
// curl --http3 -X POST https://gateway-eu.404.xyz:4443/add_result \
//   -F id=123e4567-e89b-12d3-a456-426614174001 \
//   -F signature=signature \
//   -F timestamp=404_GATEWAY_1713096000 \
//   -F validator_hotkey=worker_key_123 \
//   -F worker_id=worker-123 \
//   -F assignment_token=123e4567-e89b-12d3-a456-426614174002 \
//   -F status=success \
//   -F asset=@/path/to/result.spz

// Add failed result
// curl --http3 -X POST https://gateway-eu.404.xyz:4443/add_result \
//   -F id=987e6543-e21b-45d6-c789-123456789abc \
//   -F signature=signature \
//   -F timestamp=404_GATEWAY_1713096000 \
//   -F validator_hotkey=worker_key_123 \
//   -F worker_id=worker-123 \
//   -F assignment_token=987e6543-e21b-45d6-c789-123456789abd \
//   -F status=failure \
//   -F reason="Task timed out"

#[handler]
pub async fn add_result_handler(
    depot: &mut Depot,
    req: &mut Request,
    res: &mut Response,
) -> Result<(), ServerError> {
    let state = depot.require::<HttpState>()?.clone();
    let ParsedAddResultRequest {
        task_id,
        task_result,
        task_description,
        task_kind,
    } = parse_add_result_request(req, &state).await?;
    let cfg = state.config();
    let gateway_state = state.gateway_state().clone();
    let manager = gateway_state.task_manager();
    let metrics = state.metrics().clone();

    let is_success = task_result.reason.is_none();
    let worker_hotkey_ref = task_result.worker_hotkey.clone();
    let worker_id_ref = task_result.worker_id.clone();
    let assignment_token = task_result.assignment_token;
    let reason_ref = task_result.reason.clone();
    let reason_str = reason_ref.as_deref();
    let completed_at_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0);
    let result_metadata_json = serde_json::json!({
        "worker_hotkey": worker_hotkey_ref.as_ref(),
        "worker_id": worker_id_ref.as_ref(),
        "assignment_token": assignment_token,
        "success": is_success,
        "gateway_name": cfg.node().network.name.clone(),
    })
    .to_string();

    match manager.stage_result(task_id, task_result).await {
        Ok(()) => {}
        Err(AddResultError::NotAssigned) => {
            return Err(ServerError::Unauthorized(
                "Worker hotkey is not assigned to task".to_string(),
            ));
        }
        Err(AddResultError::AlreadyStaged) => {
            return Err(ServerError::ServiceUnavailable(
                "Result is already being processed; please retry".to_string(),
            ));
        }
        Err(AddResultError::PendingResultMissing) => {
            return Err(ServerError::Internal(
                "Failed to stage result before persistence".to_string(),
            ));
        }
    }

    let finalize_outcome = gateway_state
        .finalize_generation_task_assignment(FinalizeGenerationTaskAssignmentInput {
            task_id,
            worker_hotkey: worker_hotkey_ref.as_ref().to_owned(),
            worker_id: worker_id_ref.as_ref().to_owned(),
            assignment_token: (!assignment_token.is_nil()).then_some(assignment_token),
            assignment_status: if is_success { "succeeded" } else { "failed" }.to_owned(),
            failure_reason: reason_str.map(str::to_owned),
            result_metadata_json,
            completed_at_ms,
        })
        .await;

    let replayed_durable_result = match &finalize_outcome {
        Ok(FinalizeGenerationTaskAssignmentOutcome::DuplicateReplay) => {
            info!(
                task_id = %task_id,
                worker_hotkey = %worker_hotkey_ref.as_ref(),
                "Duplicate result replay detected; restoring matching durable result into local state"
            );
            true
        }
        Ok(FinalizeGenerationTaskAssignmentOutcome::DuplicateConflict(conflict)) => {
            manager
                .rollback_staged_result(task_id, &worker_hotkey_ref)
                .await;
            warn!(
                task_id = %task_id,
                worker_hotkey = %worker_hotkey_ref.as_ref(),
                worker_id = %worker_id_ref.as_ref(),
                submitted_status = %if is_success { "succeeded" } else { "failed" },
                submitted_reason = reason_str,
                stored_assignment_status = %conflict.stored_assignment_status,
                stored_failure_reason = conflict.stored_failure_reason.as_deref(),
                stored_result_metadata_json = %conflict.stored_result_metadata_json,
                "Duplicate assignment finalization payload differed from the stored outcome; keeping first write"
            );
            res.status_code(StatusCode::OK);
            res.render(Text::Plain("Ok"));
            return Ok(());
        }
        Ok(FinalizeGenerationTaskAssignmentOutcome::Applied) => false,
        Err(err) => {
            manager
                .rollback_staged_result(task_id, &worker_hotkey_ref)
                .await;
            error!(
                "Failed to finalize generation task {} in billing database for worker {}: {:?}; rejecting so worker can retry",
                task_id,
                worker_hotkey_ref.as_ref(),
                err
            );
            return Err(ServerError::Internal(
                "Failed to persist result; please retry".to_string(),
            ));
        }
    };

    let outcome = match manager
        .commit_staged_result(task_id, &worker_hotkey_ref)
        .await
    {
        Ok(outcome) => outcome,
        Err(AddResultError::NotAssigned) => {
            return Err(ServerError::Unauthorized(
                "Worker hotkey is not assigned to task".to_string(),
            ));
        }
        Err(AddResultError::AlreadyStaged) => {
            return Err(ServerError::ServiceUnavailable(
                "Result is already being processed; please retry".to_string(),
            ));
        }
        Err(AddResultError::PendingResultMissing) => {
            return Err(ServerError::Internal(
                "Failed to commit persisted result locally".to_string(),
            ));
        }
    };
    if replayed_durable_result {
        info!(
            task_id = %task_id,
            worker_hotkey = %worker_hotkey_ref.as_ref(),
            "Duplicate replay restored the result locally; continuing with normal post-commit bookkeeping"
        );
    }
    let reason = outcome.reason.as_deref();

    gateway_state.record_worker_event(WorkerEventRef {
        task_id: Some(task_id),
        worker_id: Some(outcome.worker_id.as_ref()),
        action: if outcome.is_success {
            "result_success"
        } else {
            "result_failure"
        },
        task_kind: task_kind.label(),
        reason,
    });

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

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot(
        status: &str,
        finished_results_count: i32,
        success_count: i32,
    ) -> GenerationTaskStatusSnapshot {
        GenerationTaskStatusSnapshot {
            status: status.to_string(),
            finished_results_count,
            success_count,
            error_message: None,
            result_worker_id: None,
        }
    }

    #[test]
    fn snapshot_running_status_uses_success_count_for_partial_results() {
        assert!(matches!(
            task_status_from_snapshot(&snapshot("running", 1, 0)),
            TaskStatus::PartialResult(0)
        ));
        assert!(matches!(
            task_status_from_snapshot(&snapshot("running", 2, 1)),
            TaskStatus::PartialResult(1)
        ));
    }

    #[test]
    fn snapshot_success_status_uses_result_worker_id() {
        let mut task = snapshot("succeeded", 1, 1);
        task.result_worker_id = Some("worker-7".to_string());

        match task_status_from_snapshot(&task) {
            TaskStatus::Success { worker_id } => {
                assert_eq!(worker_id.as_ref(), "worker-7");
            }
            other => panic!("expected success, got {:?}", other),
        }
    }

    #[test]
    fn status_snapshot_errors_are_sanitized() {
        let err = anyhow::anyhow!("column foo does not exist");

        match generation_task_status_snapshot_error(&err) {
            ServerError::Internal(message) => {
                assert_eq!(
                    message,
                    "Failed to read generation task status from the database"
                );
            }
            other => panic!("expected internal error, got {:?}", other),
        }
    }

    #[test]
    fn missing_task_context_is_reported_as_gone() {
        match task_context_unavailable_error() {
            ServerError::Json(status, payload) => {
                assert_eq!(status, StatusCode::GONE);
                assert_eq!(payload["error"], "task_context_unavailable");
                assert_eq!(
                    payload["message"],
                    "Task context is unavailable on this gateway after restart; the task will be refunded on timeout."
                );
            }
            other => panic!("expected gone json error, got {:?}", other),
        }
    }

    #[test]
    fn add_result_status_parses_known_values() {
        assert!(matches!(
            AddResultStatus::parse("success"),
            Ok(AddResultStatus::Success)
        ));
        assert!(matches!(
            AddResultStatus::parse("failure"),
            Ok(AddResultStatus::Failure)
        ));
        assert!(matches!(
            AddResultStatus::parse("other"),
            Err(ServerError::BadRequest(_))
        ));
    }

    #[test]
    fn asset_field_requires_authorized_metadata() {
        match ensure_asset_field_allowed(false, Some(AddResultStatus::Success)) {
            Err(ServerError::BadRequest(message)) => {
                assert!(message.contains("Asset field must come after"));
            }
            other => panic!("expected bad request, got {:?}", other),
        }
    }

    #[test]
    fn asset_field_rejects_failure_status() {
        match ensure_asset_field_allowed(true, Some(AddResultStatus::Failure)) {
            Err(ServerError::BadRequest(message)) => {
                assert_eq!(message, "Asset is only allowed when status=success");
            }
            other => panic!("expected bad request, got {:?}", other),
        }
    }

    #[test]
    fn duplicate_field_is_rejected() {
        let field = Some("value");
        match ensure_field_not_seen(&field, "status") {
            Err(ServerError::BadRequest(message)) => {
                assert_eq!(message, "Duplicate status field");
            }
            other => panic!("expected bad request, got {:?}", other),
        }
    }
}
