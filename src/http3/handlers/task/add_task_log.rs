use tracing::{info, warn};
use uuid::Uuid;

use crate::http3::error::ServerError;
use crate::metrics::TaskKind;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(super) enum AddTaskOutcome {
    Queued,
    BillingRejected,
    RolledBackAfterError,
}

impl AddTaskOutcome {
    fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::BillingRejected => "billing_rejected",
            Self::RolledBackAfterError => "rolled_back_after_error",
        }
    }
}

pub(super) struct AddTaskLogParams {
    pub(super) task_id: Uuid,
    pub(super) task_kind: TaskKind,
    pub(super) model: String,
    pub(super) origin: String,
    pub(super) billing_actor: String,
    pub(super) seed_kind: &'static str,
}

pub(super) struct AddTaskLogContext {
    submitter_ip: String,
    task_id: Uuid,
    task_kind: TaskKind,
    model: String,
    origin: String,
    billing_actor: String,
    seed_kind: &'static str,
}

impl AddTaskLogContext {
    pub(super) fn new(submitter_ip: &str, params: AddTaskLogParams) -> Self {
        Self {
            submitter_ip: submitter_ip.to_owned(),
            task_id: params.task_id,
            task_kind: params.task_kind,
            model: params.model,
            origin: params.origin,
            billing_actor: params.billing_actor,
            seed_kind: params.seed_kind,
        }
    }

    pub(super) fn queued(&self) {
        info!(
            task_id = %self.task_id,
            task_kind = %self.task_kind.label(),
            model = %self.model,
            origin = %self.origin,
            billing_actor = %self.billing_actor,
            seed_kind = %self.seed_kind,
            submitter_ip = %self.submitter_ip,
            outcome = %AddTaskOutcome::Queued.as_str(),
            "Queued generation task"
        );
    }

    pub(super) fn billing_rejected(&self, rejection_code: &str) {
        warn!(
            task_id = %self.task_id,
            task_kind = %self.task_kind.label(),
            model = %self.model,
            origin = %self.origin,
            billing_actor = %self.billing_actor,
            seed_kind = %self.seed_kind,
            submitter_ip = %self.submitter_ip,
            outcome = %AddTaskOutcome::BillingRejected.as_str(),
            rejection_code = %rejection_code,
            "Generation task was rejected before queueing"
        );
    }

    pub(super) fn rollback_triggered(&self, err: &ServerError) {
        warn!(
            task_id = %self.task_id,
            task_kind = %self.task_kind.label(),
            model = %self.model,
            origin = %self.origin,
            billing_actor = %self.billing_actor,
            seed_kind = %self.seed_kind,
            submitter_ip = %self.submitter_ip,
            outcome = %AddTaskOutcome::RolledBackAfterError.as_str(),
            error = ?err,
            "Rolling back task submission state after failure"
        );
    }
}
