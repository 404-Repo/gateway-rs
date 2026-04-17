use std::collections::{HashMap, HashSet};

use salvo::prelude::*;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::api::request::GetTasksRequest;
use crate::api::response::{AssignedTask, GetTasksResponse};
use crate::db::{RecordedGenerationTaskAssignment, RecordedGenerationTaskAssignmentAction};
use crate::http3::depot_ext::DepotExt;
use crate::http3::error::ServerError;
use crate::http3::handlers::common::model_errors::{
    ModelErrorContext, model_error_to_server_error,
};
use crate::http3::handlers::common::worker_auth::{WorkerAuthContext, validate_worker_request};
use crate::http3::state::HttpState;
use crate::raft::gateway_state::WorkerEventRef;

use super::{current_time_ms, task_kind_label};

// curl --http3 -X POST https://gateway-eu.404.xyz:4443/get_tasks \
//   -H "content-type: application/json" \
//   -d '{"validator_hotkey": "abc123", "worker_id": "worker-123", "signature": "signatureinbase64", "timestamp": "404_GATEWAY_1743657200", "requested_task_count": 10}'
#[handler]
pub async fn get_tasks_handler(
    depot: &mut Depot,
    req: &mut Request,
    res: &mut Response,
) -> Result<(), ServerError> {
    let get_tasks = req
        .parse_json::<GetTasksRequest>()
        .await
        .map_err(|e| ServerError::BadRequest(e.to_string()))?;

    let state = depot.require::<HttpState>()?.clone();
    let cfg = state.config();
    let http_cfg = cfg.http();
    let model_cfg = &cfg.node().model_config;
    let gateway_state = state.gateway_state().clone();
    let queue = state.task_queue().clone();
    let metrics = state.metrics().clone();

    validate_worker_request(
        http_cfg,
        &get_tasks.worker_hotkey,
        &get_tasks.timestamp,
        &get_tasks.signature,
        WorkerAuthContext::GetTasks,
    )?;

    let model_filter = {
        let mut models = get_tasks.model.to_vec();
        if models.is_empty() {
            return Err(ServerError::BadRequestJson(serde_json::json!({
                "error": "invalid_field",
                "field": "model",
                "message": "Expected at least one model",
            })));
        }
        let mut seen = HashSet::with_capacity(models.len());
        models.retain(|model| seen.insert(model.clone()));

        for model in &models {
            model_cfg.output_for(model).map_err(|err| {
                model_error_to_server_error(err, ModelErrorContext::ClientInput { field: "model" })
            })?;
        }

        models
    };

    info!(
        worker_hotkey = %get_tasks.worker_hotkey,
        worker_id = %get_tasks.worker_id,
        requested_count = get_tasks.requested_task_count,
        model_filter_count = model_filter.len(),
        models = ?model_filter,
        "Worker requested tasks"
    );

    let gateways = gateway_state
        .gateways()
        .await
        .map_err(|e| ServerError::Internal(format!("Failed to obtain gateways: {:?}", e)))?;
    let gateway_count = gateways.len();

    let requested_task_count = get_tasks
        .requested_task_count
        .min(state.gateway_state().max_task_queue_len().max(1));
    let mut task_ids = Vec::with_capacity(requested_task_count);
    let task_manager = gateway_state.task_manager();
    let mut deliveries = queue.reserve_for_models(
        requested_task_count,
        &get_tasks.worker_hotkey,
        model_filter.as_slice(),
    );
    for delivery in &deliveries {
        if let Some(dur) = delivery.duration() {
            metrics.record_queue_time(dur.as_secs_f64());
        }
        task_ids.push(delivery.task().id);
    }
    let reserved_count = task_ids.len();
    let mut assigned_count = 0usize;
    let mut requeue_count = 0usize;
    let mut retire_count = 0usize;
    let mut tasks = Vec::with_capacity(deliveries.len());

    if !task_ids.is_empty() {
        let worker_hotkey = get_tasks.worker_hotkey.clone();
        let worker_id = get_tasks.worker_id.clone();
        let assigned_at_ms = current_time_ms();
        match gateway_state
            .record_generation_task_assignments(
                task_ids.as_slice(),
                worker_hotkey.as_ref(),
                worker_id.as_ref(),
                assigned_at_ms,
            )
            .await
        {
            Err(err) => {
                error!(
                    worker_hotkey = %worker_hotkey.as_ref(),
                    worker_id = %worker_id.as_ref(),
                    reserved_count,
                    error = ?err,
                    "Failed to persist task assignments; requeueing deliveries"
                );
                deliveries.clear();
                task_ids.clear();
            }
            Ok(assigned_assignments) => {
                let task_outcomes: HashMap<Uuid, RecordedGenerationTaskAssignment> =
                    assigned_assignments
                        .into_iter()
                        .map(|assignment| (assignment.task_id, assignment))
                        .collect();
                assigned_count = task_outcomes
                    .values()
                    .filter(|assignment| {
                        assignment.action == RecordedGenerationTaskAssignmentAction::Assigned
                    })
                    .count();
                requeue_count = task_outcomes
                    .values()
                    .filter(|assignment| {
                        assignment.action == RecordedGenerationTaskAssignmentAction::Requeue
                    })
                    .count();
                retire_count = task_outcomes
                    .values()
                    .filter(|assignment| {
                        assignment.action == RecordedGenerationTaskAssignmentAction::Retire
                    })
                    .count();
                if requeue_count > 0 || retire_count > 0 {
                    warn!(
                        worker_hotkey = %worker_hotkey.as_ref(),
                        worker_id = %worker_id.as_ref(),
                        reserved_count,
                        assigned_count,
                        requeue_count,
                        retire_count,
                        "Some queued deliveries were not assignable to this worker and were requeued or retired"
                    );
                }
                for delivery in deliveries.drain(..) {
                    let task_id = delivery.task().id;
                    let Some(assignment) = task_outcomes.get(&task_id) else {
                        delivery.retire();
                        continue;
                    };
                    match assignment.action {
                        RecordedGenerationTaskAssignmentAction::Assigned => {
                            let assignment_token = assignment
                                .assignment_token
                                .expect("assigned task is missing assignment token");
                            let task = delivery.commit().0;
                            tasks.push(AssignedTask {
                                task: task.clone(),
                                assignment_token,
                            });
                            task_manager
                                .record_assignment_with_token(
                                    task_id,
                                    worker_hotkey.clone(),
                                    worker_id.clone(),
                                    assignment_token,
                                )
                                .await;
                        }
                        RecordedGenerationTaskAssignmentAction::Requeue => {
                            delivery.requeue_for_other_hotkeys()
                        }
                        RecordedGenerationTaskAssignmentAction::Retire => delivery.retire(),
                    }
                }
            }
        }
    }

    for task in &tasks {
        gateway_state.record_worker_event(WorkerEventRef {
            task_id: Some(task.task.id),
            worker_id: Some(get_tasks.worker_id.as_ref()),
            action: "task_assigned",
            task_kind: task_kind_label(&task.task),
            reason: None,
        });
    }

    metrics.set_queue_len(queue.len());

    gateway_state.update_task_acquisition().map_err(|e| {
        ServerError::Internal(format!(
            "Failed to execute update_task_acquisition: {:?}",
            e
        ))
    })?;

    metrics
        .inc_tasks_received(&get_tasks.worker_hotkey, tasks.len())
        .await;

    if tasks.is_empty() {
        debug!(
            worker_hotkey = %get_tasks.worker_hotkey,
            worker_id = %get_tasks.worker_id,
            requested_count = requested_task_count,
            reserved_count,
            assigned_count,
            requeue_count,
            retire_count,
            gateway_count,
            "Worker task request returned no assignments"
        );
    } else {
        info!(
            worker_hotkey = %get_tasks.worker_hotkey,
            worker_id = %get_tasks.worker_id,
            requested_count = requested_task_count,
            reserved_count,
            assigned_count = tasks.len(),
            requeue_count,
            retire_count,
            gateway_count,
            "Worker task request completed"
        );
    }

    let response = GetTasksResponse { tasks, gateways };
    res.render(Json(response));
    Ok(())
}
