use crate::config::HTTPConfig;
use crate::crypto::hotkey::Hotkey;
use crate::crypto::verify_hotkey;
use crate::http3::error::ServerError;

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

    verify_hotkey(
        timestamp,
        worker_hotkey,
        signature,
        http_cfg.signature_freshness_threshold,
    )
    .map_err(|e| {
        ServerError::Internal(format!(
            "Failed to verify {}: {:?}",
            context.verify_label(),
            e
        ))
    })?;

    Ok(())
}
