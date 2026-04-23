use salvo::prelude::*;

use crate::db::{FinalizeGenerationTaskAssignmentInput, FinalizeGenerationTaskAssignmentOutcome};
use crate::http3::depot_ext::DepotExt;
use crate::http3::error::ServerError;
use crate::http3::handlers::common::peer::request_ip;
use crate::http3::state::HttpState;
use crate::raft::gateway_state::WorkerEventRef;
use crate::task::AddResultError;

use super::add_result_log::{
    ResultSubmissionLogContext, ResultSubmissionLogParams, ResultSubmissionOutcome,
};
use super::add_result_parse::{ParsedAddResultRequest, parse_add_result_request};

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
    let worker_ip = request_ip(req);

    ResultSubmissionLogContext::parsing_started(&worker_ip);

    let ParsedAddResultRequest {
        task_id,
        task_result,
        task_description,
        task_kind,
    } = match parse_add_result_request(req, &state).await {
        Ok(parsed) => parsed,
        Err(err) => {
            ResultSubmissionLogContext::parsing_failed(&worker_ip, &err);
            return Err(err);
        }
    };
    let cfg = state.config();
    let gateway_state = state.gateway_state().clone();
    let manager = gateway_state.task_manager();
    let metrics = state.metrics().clone();

    let is_success = task_result.reason.is_none();
    let submission_status = if is_success { "success" } else { "failure" };
    let worker_hotkey_ref = task_result.worker_hotkey.clone();
    let worker_id_ref = task_result.worker_id.clone();
    let assignment_token = task_result.assignment_token;
    let reason_ref = task_result.reason.clone();
    let reason_str = reason_ref.as_deref();
    let submission_log = ResultSubmissionLogContext::new(
        &worker_ip,
        ResultSubmissionLogParams {
            task_id,
            task_kind,
            task_description: task_description.clone(),
            worker_hotkey: worker_hotkey_ref.clone(),
            worker_id: worker_id_ref.clone(),
            result_status: submission_status,
        },
    );
    submission_log.parsed();
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
            submission_log.rejected(
                ResultSubmissionOutcome::RejectedNotAssigned,
                "Worker result submission rejected",
            );
            return Err(ServerError::Unauthorized(
                "Worker hotkey is not assigned to task".to_string(),
            ));
        }
        Err(AddResultError::AlreadyStaged) => {
            submission_log.rejected(
                ResultSubmissionOutcome::AlreadyStaged,
                "Worker result submission rejected",
            );
            return Err(ServerError::ServiceUnavailable(
                "Result is already being processed; please retry".to_string(),
            ));
        }
        Err(AddResultError::PendingResultMissing) => {
            submission_log.failed(
                ResultSubmissionOutcome::PendingResultMissing,
                "Worker result submission failed before persistence",
            );
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
        Ok(FinalizeGenerationTaskAssignmentOutcome::DuplicateReplay) => true,
        Ok(FinalizeGenerationTaskAssignmentOutcome::DuplicateConflict(conflict)) => {
            manager
                .rollback_staged_result(task_id, &worker_hotkey_ref)
                .await;
            submission_log.duplicate_conflict(
                reason_str,
                &conflict.stored_assignment_status,
                conflict.stored_failure_reason.as_deref(),
                &conflict.stored_result_metadata_json,
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
            submission_log.persist_failed(&err);
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
            submission_log.rejected(
                ResultSubmissionOutcome::CommitRejectedNotAssigned,
                "Worker result submission rejected during local commit",
            );
            return Err(ServerError::Unauthorized(
                "Worker hotkey is not assigned to task".to_string(),
            ));
        }
        Err(AddResultError::AlreadyStaged) => {
            submission_log.rejected(
                ResultSubmissionOutcome::CommitAlreadyStaged,
                "Worker result submission could not complete local commit",
            );
            return Err(ServerError::ServiceUnavailable(
                "Result is already being processed; please retry".to_string(),
            ));
        }
        Err(AddResultError::PendingResultMissing) => {
            submission_log.failed(
                ResultSubmissionOutcome::CommitPendingResultMissing,
                "Worker result submission failed during local commit",
            );
            return Err(ServerError::Internal(
                "Failed to commit persisted result locally".to_string(),
            ));
        }
    };
    let reason = outcome.reason.as_deref();
    let task_model = manager.get_model(task_id).await;

    gateway_state.record_worker_event(WorkerEventRef {
        task_id: Some(task_id),
        worker_id: Some(outcome.worker_id.as_ref()),
        action: if outcome.is_success {
            "result_success"
        } else {
            "result_failure"
        },
        task_kind: task_kind.label(),
        model: task_model.as_deref(),
        reason,
        metadata_json: Some(serde_json::json!({
            "worker_hotkey": worker_hotkey_ref.as_ref(),
            "assignment_token": assignment_token,
            "replayed_durable_result": replayed_durable_result,
        })),
    });

    let elapsed_secs = manager.get_time(task_id).await;
    if outcome.is_success {
        metrics
            .inc_task_completed(outcome.worker_hotkey.as_ref())
            .await;
        metrics.inc_task_completed_kind(task_kind);
    } else {
        metrics
            .inc_task_failed(outcome.worker_hotkey.as_ref())
            .await;
    }

    if let Some(elapsed) = elapsed_secs {
        metrics
            .record_completion_time(outcome.worker_hotkey.as_ref(), elapsed)
            .await;
    }

    if outcome.completed {
        manager.finalize_task(task_id).await;
    }

    submission_log.completed(
        if replayed_durable_result {
            ResultSubmissionOutcome::AcceptedDuplicateReplay
        } else {
            ResultSubmissionOutcome::AcceptedApplied
        },
        outcome.completed,
        elapsed_secs,
        reason,
    );

    res.status_code(StatusCode::OK);
    res.render(Text::Plain("Ok"));
    Ok(())
}
