use crate::config::HTTPConfig;
use crate::crypto::hotkey::Hotkey;
use crate::crypto::{GATEWAY_TIMESTAMP_PREFIX, verify_hotkey};
use crate::http3::error::ServerError;
use tracing::warn;

pub enum WorkerAuthContext {
    GetTasks,
    AddResult,
}

impl WorkerAuthContext {
    fn verify_label(&self) -> &'static str {
        match self {
            WorkerAuthContext::GetTasks => "GetTasksRequest",
            WorkerAuthContext::AddResult => "AddTaskRequest",
        }
    }
}

pub fn validate_worker_request(
    http_cfg: &HTTPConfig,
    worker_hotkey: &Hotkey,
    timestamp: &str,
    signature: &str,
    context: WorkerAuthContext,
) -> Result<(), ServerError> {
    if !http_cfg.worker_whitelist.is_empty() && !http_cfg.worker_whitelist.contains(worker_hotkey) {
        return Err(ServerError::Unauthorized(
            "Worker hotkey is not whitelisted".to_string(),
        ));
    }

    if !timestamp.starts_with(GATEWAY_TIMESTAMP_PREFIX)
        || timestamp[GATEWAY_TIMESTAMP_PREFIX.len()..]
            .parse::<u64>()
            .is_err()
    {
        return Err(ServerError::BadRequest(
            "Invalid timestamp format".to_string(),
        ));
    }

    verify_hotkey(
        timestamp,
        worker_hotkey,
        signature,
        http_cfg.signature_freshness_threshold,
    )
    .map_err(|e| {
        warn!(
            worker_hotkey = %worker_hotkey,
            request = context.verify_label(),
            error = ?e,
            "Worker signature verification failed"
        );
        ServerError::Unauthorized("Invalid worker authentication".to_string())
    })?;

    Ok(())
}
