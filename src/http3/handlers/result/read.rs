use std::convert::Infallible;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use futures::stream;
use salvo::prelude::*;
use tokio::io::duplex;
use tokio_util::io::ReaderStream;
use tracing::error;
use uuid::Uuid;

use crate::api::request::{GetTaskResultRequest, GetTaskStatus};
use crate::api::response::GetTaskStatusResponse;
use crate::config::ModelOutput;
use crate::db::GenerationTaskStatusSnapshot;
use crate::http3::depot_ext::DepotExt;
use crate::http3::error::ServerError;
use crate::http3::handlers::common::activity::{TaskActivityContext, record_task_activity};
use crate::http3::handlers::common::model_errors::{
    ModelErrorContext, model_error_to_server_error,
};
use crate::http3::handlers::common::origin::normalize_origin;
use crate::http3::rate_limits::RateLimitContext;
use crate::http3::state::HttpState;
use crate::raft::gateway_state::GatewayState;
use crate::task::TaskStatus;
use async_zip::ZipEntryBuilder;
use async_zip::base::write::ZipFileWriter;
use http::HeaderValue;
use itoa::Buffer;

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
        "running" => TaskStatus::InProgress,
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
    let activity_metadata = task_manager.get_activity_metadata(get_task.id).await;
    let (task_kind, task_model) = activity_metadata
        .map(|metadata| (metadata.task_kind, metadata.model))
        .unwrap_or(("unknown", None));
    record_task_activity(
        TaskActivityContext {
            gateway_state: &gateway_state,
            rate_ctx,
            origin: record_origin,
            task_kind,
            model: task_model.as_deref(),
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
    results_vec.sort_by_key(|a| a.instant);
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

    let best_worker = best.worker_hotkey.clone();
    let asset = best
        .asset
        .ok_or_else(|| ServerError::Internal("Missing asset on best TaskResult".into()))?;
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
    fn snapshot_running_without_finished_results_is_in_progress() {
        assert!(matches!(
            task_status_from_snapshot(&snapshot("running", 0, 0)),
            TaskStatus::InProgress
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
}
