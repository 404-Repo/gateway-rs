use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use multer::{Constraints, Multipart, SizeLimit};
use salvo::prelude::{Request, StatusCode};
use uuid::Uuid;

use crate::api::request::AddTaskResultRequest;
use crate::crypto::hotkey::Hotkey;
use crate::http3::error::ServerError;
use crate::http3::handlers::common::multipart::{
    is_multipart_form, multipart_stream, parse_boundary, read_binary_field, read_text_field,
};
use crate::http3::handlers::common::worker_auth::{WorkerAuthContext, validate_worker_request};
use crate::http3::state::HttpState;
use crate::metrics::TaskKind;

const MAX_REASON_LENGTH: u64 = u8::MAX as u64;

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
    task_kind: TaskKind,
}

pub(super) struct ParsedAddResultRequest {
    pub(super) task_id: Uuid,
    pub(super) task_result: AddTaskResultRequest,
    pub(super) task_description: String,
    pub(super) task_kind: TaskKind,
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
        (format!("prompt: '{}'", prompt), TaskKind::TextTo3D)
    } else if let Some(_image_data) = manager.get_image(task_id).await {
        ("image task".to_string(), TaskKind::ImageTo3D)
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

pub(super) async fn parse_add_result_request(
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

#[cfg(test)]
mod tests {
    use super::*;

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
