use std::sync::Arc;

use tracing::{error, info, warn};
use uuid::Uuid;

use crate::crypto::hotkey::Hotkey;
use crate::http3::error::ServerError;
use crate::metrics::TaskKind;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(super) enum ResultSubmissionOutcome {
    RejectedNotAssigned,
    AlreadyStaged,
    PendingResultMissing,
    DuplicateConflict,
    PersistFailed,
    CommitRejectedNotAssigned,
    CommitAlreadyStaged,
    CommitPendingResultMissing,
    AcceptedApplied,
    AcceptedDuplicateReplay,
}

impl ResultSubmissionOutcome {
    fn as_str(self) -> &'static str {
        match self {
            Self::RejectedNotAssigned => "rejected_not_assigned",
            Self::AlreadyStaged => "already_staged",
            Self::PendingResultMissing => "pending_result_missing",
            Self::DuplicateConflict => "duplicate_conflict",
            Self::PersistFailed => "persist_failed",
            Self::CommitRejectedNotAssigned => "commit_rejected_not_assigned",
            Self::CommitAlreadyStaged => "commit_already_staged",
            Self::CommitPendingResultMissing => "commit_pending_result_missing",
            Self::AcceptedApplied => "accepted_applied",
            Self::AcceptedDuplicateReplay => "accepted_duplicate_replay",
        }
    }
}

pub(super) struct ResultSubmissionLogParams {
    pub(super) task_id: Uuid,
    pub(super) task_kind: TaskKind,
    pub(super) task_description: String,
    pub(super) worker_hotkey: Hotkey,
    pub(super) worker_id: Arc<str>,
    pub(super) result_status: &'static str,
}

pub(super) struct ResultSubmissionLogContext {
    worker_ip: String,
    task_id: Uuid,
    task_kind: TaskKind,
    task_description: String,
    worker_hotkey: Hotkey,
    worker_id: Arc<str>,
    result_status: &'static str,
}

impl ResultSubmissionLogContext {
    pub(super) fn parsing_started(worker_ip: &str) {
        info!(
            worker_ip = %worker_ip,
            "Started parsing worker result submission"
        );
    }

    pub(super) fn parsing_failed(worker_ip: &str, err: &ServerError) {
        warn!(
            worker_ip = %worker_ip,
            parse_error = ?err,
            "Failed to parse worker result submission"
        );
    }

    pub(super) fn new(worker_ip: &str, params: ResultSubmissionLogParams) -> Self {
        Self {
            worker_ip: worker_ip.to_owned(),
            task_id: params.task_id,
            task_kind: params.task_kind,
            task_description: params.task_description,
            worker_hotkey: params.worker_hotkey,
            worker_id: params.worker_id,
            result_status: params.result_status,
        }
    }

    pub(super) fn parsed(&self) {
        info!(
            task_id = %self.task_id,
            task_kind = %self.task_kind.label(),
            task_description = %self.task_description,
            worker_hotkey = %self.worker_hotkey.as_ref(),
            worker_id = %self.worker_id.as_ref(),
            result_status = %self.result_status,
            worker_ip = %self.worker_ip,
            "Parsed worker result submission"
        );
    }

    pub(super) fn rejected(&self, outcome: ResultSubmissionOutcome, message: &'static str) {
        warn!(
            task_id = %self.task_id,
            task_kind = %self.task_kind.label(),
            task_description = %self.task_description,
            worker_hotkey = %self.worker_hotkey.as_ref(),
            worker_id = %self.worker_id.as_ref(),
            result_status = %self.result_status,
            worker_ip = %self.worker_ip,
            outcome = %outcome.as_str(),
            "{message}"
        );
    }

    pub(super) fn failed(&self, outcome: ResultSubmissionOutcome, message: &'static str) {
        error!(
            task_id = %self.task_id,
            task_kind = %self.task_kind.label(),
            task_description = %self.task_description,
            worker_hotkey = %self.worker_hotkey.as_ref(),
            worker_id = %self.worker_id.as_ref(),
            result_status = %self.result_status,
            worker_ip = %self.worker_ip,
            outcome = %outcome.as_str(),
            "{message}"
        );
    }

    pub(super) fn duplicate_conflict(
        &self,
        submitted_reason: Option<&str>,
        stored_assignment_status: &str,
        stored_failure_reason: Option<&str>,
        stored_result_metadata_json: &str,
    ) {
        warn!(
            task_id = %self.task_id,
            task_kind = %self.task_kind.label(),
            task_description = %self.task_description,
            worker_hotkey = %self.worker_hotkey.as_ref(),
            worker_id = %self.worker_id.as_ref(),
            result_status = %self.result_status,
            worker_ip = %self.worker_ip,
            outcome = %ResultSubmissionOutcome::DuplicateConflict.as_str(),
            submitted_reason,
            stored_assignment_status = %stored_assignment_status,
            stored_failure_reason,
            stored_result_metadata_json = %stored_result_metadata_json,
            "Duplicate assignment finalization payload differed from the stored outcome; keeping first write"
        );
    }

    pub(super) fn persist_failed(&self, err: &impl std::fmt::Debug) {
        error!(
            task_id = %self.task_id,
            task_kind = %self.task_kind.label(),
            task_description = %self.task_description,
            worker_hotkey = %self.worker_hotkey.as_ref(),
            worker_id = %self.worker_id.as_ref(),
            result_status = %self.result_status,
            worker_ip = %self.worker_ip,
            outcome = %ResultSubmissionOutcome::PersistFailed.as_str(),
            persist_error = ?err,
            "Failed to finalize generation task in billing database; rejecting so worker can retry"
        );
    }

    pub(super) fn completed(
        &self,
        outcome: ResultSubmissionOutcome,
        task_completed: bool,
        elapsed_secs: Option<f64>,
        failure_reason: Option<&str>,
    ) {
        if self.result_status == "success" {
            info!(
                task_id = %self.task_id,
                task_kind = %self.task_kind.label(),
                task_description = %self.task_description,
                worker_hotkey = %self.worker_hotkey.as_ref(),
                worker_id = %self.worker_id.as_ref(),
                result_status = %self.result_status,
                worker_ip = %self.worker_ip,
                outcome = %outcome.as_str(),
                task_completed,
                elapsed_secs,
                "Worker result submission completed"
            );
        } else {
            warn!(
                task_id = %self.task_id,
                task_kind = %self.task_kind.label(),
                task_description = %self.task_description,
                worker_hotkey = %self.worker_hotkey.as_ref(),
                worker_id = %self.worker_id.as_ref(),
                result_status = %self.result_status,
                worker_ip = %self.worker_ip,
                outcome = %outcome.as_str(),
                task_completed,
                elapsed_secs,
                failure_reason,
                "Worker result submission completed"
            );
        }
    }
}
