use std::sync::Arc;

use salvo::prelude::*;
use serde_json::json;
use tracing::warn;
use uuid::Uuid;

use crate::api::Task;
use crate::db::{CreateGenerationTaskInput, CreateGenerationTaskOutcome};
use crate::http3::depot_ext::DepotExt;
use crate::http3::error::ServerError;
use crate::http3::handlers::common::activity::{TaskActivityContext, record_task_activity};
use crate::http3::handlers::common::model_errors::{
    ModelErrorContext, model_error_to_server_error,
};
use crate::http3::handlers::common::origin::normalize_origin;
use crate::http3::handlers::common::peer::request_ip;
use crate::http3::rate_limits::{RateLimitContext, reserve_add_task_rate_limit};
use crate::http3::state::HttpState;

use super::add_task_billing::{
    billing_task_kind, generic_key_limits, guest_key_hash, registered_user_free_limits,
    task_submission_rejection_to_server_error, unexpected_task_billing_error_to_server_error,
};
use super::add_task_log::{AddTaskLogContext, AddTaskLogParams};
use super::add_task_parse::{parse_add_task_request, validate_add_task_input};
use super::add_task_rate_limit::{PendingRateLimitRollbackGuard, publish_accepted_reservation};
use super::current_time_ms;

// curl --http3 -X POST "https://gateway-eu.404.xyz:4443/add_task" -H "content-type: application/json" -H "x-api-key: 123e4567-e89b-12d3-a456-426614174001" -d '{"prompt": "mechanic robot", "seed": 12345}'
// curl --http3 -X POST "https://gateway-eu.404.xyz:4443/add_task" -F "prompt=a robot" -F "seed=12345" -H "x-api-key: 123e4567-e89b-12d3-a456-426614174001"
// curl --http3 -X POST "https://gateway-eu.404.xyz:4443/add_task" -F "image=@image.jpg" -F "seed=12345" -H "x-api-key: 123e4567-e89b-12d3-a456-426614174001"
#[handler]
pub async fn add_task_handler(
    depot: &mut Depot,
    req: &mut Request,
    res: &mut Response,
) -> Result<(), ServerError> {
    let state = depot.require::<HttpState>()?.clone();
    let gateway_state = state.gateway_state().clone();
    let submitter_ip = request_ip(req);
    let mut rollback_guard = PendingRateLimitRollbackGuard::new(None);
    let mut task_log: Option<AddTaskLogContext> = None;

    let outcome = async {
        let add_task = parse_add_task_request(depot, req).await?;
        let validated = validate_add_task_input(depot, add_task)?;
        let has_prompt = validated.prompt.is_some();
        let has_image = validated.image.is_some();
        let task_kind = validated.task_kind;
        let user_seed = validated.seed.filter(|seed| *seed != -1);
        let seed_kind = if user_seed.is_some() {
            "user"
        } else {
            "generated"
        };
        // Draw from all u32 values except u32::MAX, after cast that excludes -1 sentinel.
        let seed = user_seed.unwrap_or_else(|| rand::random_range(0..u32::MAX) as i32);
        let model_params = validated.model_params;

        let cfg = state.config();
        let queue = state.task_queue().clone();
        let metrics = state.metrics().clone();
        let http_cfg = cfg.http();
        let queue_limit = gateway_state.max_task_queue_len();
        let rate_ctx = depot.require::<RateLimitContext>()?.clone();
        let record_origin = normalize_origin(req, http_cfg);
        metrics.inc_request_origin(record_origin);

        let queue_slot = queue.try_reserve(queue_limit).ok_or_else(|| {
            warn!(
                task_kind = %task_kind.label(),
                origin = %record_origin,
                submitter_ip = %submitter_ip,
                outcome = "queue_full",
                "Task request rejected before queue reservation"
            );
            ServerError::ServiceUnavailable("Task queue is full".to_string())
        })?;

        let model_cfg = &cfg.node().model_config;
        let resolved_model = model_cfg
            .resolve_and_validate_input(validated.model.as_deref(), has_prompt, has_image)
            .map_err(|err| {
                model_error_to_server_error(err, ModelErrorContext::ClientInput { field: "model" })
            })?;
        let reservation = reserve_add_task_rate_limit(depot).await?;
        rollback_guard.arm(reservation.clone());
        if let Some(reservation) = reservation.as_ref() {
            let charge_rollback = publish_accepted_reservation(&gateway_state, reservation).await?;
            if let Some(charge_rollback) = charge_rollback {
                rollback_guard
                    .arm_published_charge_rollback(gateway_state.clone(), charge_rollback);
            }
        }
        let now_ms = current_time_ms();

        let task_id = Uuid::new_v4();
        let model_name = resolved_model.model.clone();
        let billing_owner =
            if rate_ctx.key_is_uuid && (rate_ctx.user_id.is_some() || rate_ctx.is_company_key) {
                let owner = rate_ctx.billing_owner.clone();
                if owner.is_none() {
                    return Err(ServerError::Unauthorized(
                        "API key is no longer active for billing.".to_string(),
                    ));
                }
                owner
            } else {
                None
            };
        let billing_request_json = json!({
            "seed": seed,
            "model": &model_name,
            "model_params": model_params.as_ref(),
            "prompt": validated.prompt.as_deref(),
            "image": validated.image.as_ref().map(|image| json!({
                "width": image.width,
                "height": image.height,
                "format": format!("{:?}", image.format),
            })),
        });
        let task = Task {
            id: task_id,
            prompt: validated.prompt.map(Arc::new),
            image: validated.image.as_ref().map(|img| img.data.clone()),
            model: Some(resolved_model.model),
            seed,
            model_params,
        };
        let (
            account_id,
            user_id,
            company_id,
            api_key_id,
            registered_generation_limit,
            registered_window_ms,
            guest_generation_limit,
            guest_window_ms,
            guest_key_hash,
            guest_access_mode,
            generic_global_limit,
            generic_per_ip_limit,
            generic_window_ms,
            generic_key_hash,
            billing_client_origin,
            billing_actor,
        ) = if let Some(owner) = billing_owner.as_ref() {
            let (registered_generation_limit, registered_window_ms) = if owner.user_id.is_some() {
                registered_user_free_limits(
                    rate_ctx.user_limits,
                    gateway_state.registered_generation_limit(),
                    gateway_state.registered_window_ms(),
                )?
            } else {
                (None, None)
            };
            (
                Some(owner.account_id),
                owner.user_id,
                owner.company_id,
                Some(owner.api_key_id),
                registered_generation_limit,
                registered_window_ms,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                record_origin,
                if owner.company_id.is_some() {
                    "company"
                } else {
                    "registered_personal"
                },
            )
        } else if rate_ctx.key_is_uuid && rate_ctx.is_generic_key {
            let (generic_global_limit, generic_per_ip_limit, generic_window_ms) =
                generic_key_limits(
                    gateway_state.generic_global_daily_limit(),
                    gateway_state.generic_per_ip_daily_limit(),
                    gateway_state.generic_window_ms(),
                );
            let generic_key_hash =
                Some(guest_key_hash(&rate_ctx, http_cfg.api_key_secret.as_str())?);
            (
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                Some("generic_key".to_string()),
                generic_global_limit,
                generic_per_ip_limit,
                generic_window_ms,
                generic_key_hash,
                "guest_generic",
                "guest_generic",
            )
        } else {
            return Err(ServerError::Unauthorized(
                "API key is not authorized for task billing.".to_string(),
            ));
        };
        task_log = Some(AddTaskLogContext::new(
            &submitter_ip,
            AddTaskLogParams {
                task_id,
                task_kind,
                model: model_name.clone(),
                origin: record_origin.to_string(),
                billing_actor: billing_actor.to_string(),
                seed_kind,
            },
        ));
        let input = CreateGenerationTaskInput {
            task_id,
            account_id,
            user_id,
            company_id,
            api_key_id,
            task_kind: billing_task_kind(task_kind).to_string(),
            model: model_name.clone(),
            expected_results: cfg.node().basic.unique_workers_per_task as i32,
            deadline_at_ms: now_ms
                + (cfg.node().basic.taskmanager_result_lifetime.max(1) as i64 * 1000),
            gateway_name: cfg.node().network.name.clone(),
            client_origin: billing_client_origin.to_string(),
            request_json: billing_request_json.to_string(),
            registered_generation_limit,
            registered_window_ms,
            now_ms,
            guest_generation_limit,
            guest_window_ms,
            guest_key_hash,
            guest_access_mode,
            generic_global_limit,
            generic_per_ip_limit,
            generic_window_ms,
            generic_key_hash,
        };
        match gateway_state
            .create_generation_task(&input)
            .await
            .map_err(unexpected_task_billing_error_to_server_error)?
        {
            CreateGenerationTaskOutcome::Created => {}
            CreateGenerationTaskOutcome::Rejected(rejection) => {
                if let Some(task_log) = task_log.as_ref() {
                    task_log.billing_rejected(&rejection.error_code);
                }
                return Err(task_submission_rejection_to_server_error(rejection));
            }
        }

        gateway_state
            .task_manager()
            .add_task_with_rate_limit_reservation(&task, reservation.clone())
            .await;
        queue_slot.push(task);
        metrics.set_queue_len(queue.len());

        if let Some(task_log) = task_log.as_ref() {
            task_log.queued();
        }

        record_task_activity(
            TaskActivityContext {
                gateway_state: &gateway_state,
                rate_ctx: &rate_ctx,
                origin: record_origin,
                task_kind: task_kind.label(),
                model: Some(model_name.as_str()),
                task_id: Some(task_id),
            },
            "add_task",
        );

        res.render(Json(serde_json::json!({
            "id": task_id
        })));
        Ok(())
    }
    .await;

    match outcome {
        Ok(()) => {
            rollback_guard.disarm();
            Ok(())
        }
        Err(err) => {
            if rollback_guard.has_pending_work()
                && let Some(task_log) = task_log.as_ref()
            {
                task_log.rollback_triggered(&err);
            }
            rollback_guard.rollback_now().await;
            Err(err)
        }
    }
}
