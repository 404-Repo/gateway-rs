use anyhow::Result;
use bytes::Bytes;
use http::StatusCode;
use multer::{Constraints, Multipart, SizeLimit};
use salvo::prelude::*;
use serde_json::json;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::OwnedSemaphorePermit;
use tokio::time::{Duration, sleep};
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::api::request::{AddTaskRequest, GetTasksRequest};
use crate::api::response::{AssignedTask, GetTasksResponse, LoadResponse};
use crate::api::{ModelParams, Task};
use crate::common::image::validate_image;
use crate::crypto::crypto_provider::GuestIpHasher;
use crate::db::{
    CreateGenerationTaskInput, CreateGenerationTaskOutcome, CreateGenerationTaskRejection,
    RecordedGenerationTaskAssignment, RecordedGenerationTaskAssignmentAction,
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
use crate::http3::rate_limits::{
    RateLimitContext, RateLimitReservation, reserve_add_task_rate_limit,
};
use crate::http3::state::HttpState;
use crate::metrics::TaskKind;
use crate::raft::gateway_state::{GatewayState, WorkerEventRef};
use crate::raft::store::RateLimitMutationBatch;

const RECONCILIATION_RETRY_INITIAL_DELAY: Duration = Duration::from_millis(250);
const RECONCILIATION_RETRY_MAX_DELAY: Duration = Duration::from_secs(5);
const RECONCILIATION_RETRY_MAX_ATTEMPTS: u32 = 12;

struct PendingRateLimitRollbackGuard {
    reservation: Option<RateLimitReservation>,
    published_charge_rollback: Option<(GatewayState, RateLimitMutationBatch)>,
}

impl PendingRateLimitRollbackGuard {
    fn new(reservation: Option<RateLimitReservation>) -> Self {
        Self {
            reservation,
            published_charge_rollback: None,
        }
    }

    fn arm(&mut self, reservation: Option<RateLimitReservation>) {
        self.reservation = reservation;
    }

    fn arm_published_charge_rollback(
        &mut self,
        gateway_state: GatewayState,
        batch: RateLimitMutationBatch,
    ) {
        if batch.is_empty() {
            self.published_charge_rollback = None;
        } else {
            self.published_charge_rollback = Some((gateway_state, batch));
        }
    }

    fn disarm(&mut self) {
        self.reservation = None;
        self.published_charge_rollback = None;
    }

    async fn rollback_now(&mut self) {
        if let Some(reservation) = self.reservation.as_ref() {
            reservation.rollback_pending().await;
        }
        self.reservation = None;
        if let Some((gateway_state, batch)) = self.published_charge_rollback.take() {
            rollback_published_charge(gateway_state, batch).await;
        }
    }
}

impl Drop for PendingRateLimitRollbackGuard {
    fn drop(&mut self) {
        let reservation = self.reservation.take();
        let published_charge_rollback = self.published_charge_rollback.take();

        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                if let Some(reservation) = reservation {
                    reservation.rollback_pending().await;
                }
                if let Some((gateway_state, batch)) = published_charge_rollback {
                    rollback_published_charge(gateway_state, batch).await;
                }
            });
        }
    }
}

async fn publish_accepted_reservation(
    gateway_state: &GatewayState,
    reservation: &RateLimitReservation,
) -> Result<Option<RateLimitMutationBatch>, ServerError> {
    let Some(charge_batch) = reservation.accepted_charge_batch() else {
        return Ok(None);
    };
    let rollback_batch = reservation.refund_batch();

    gateway_state
        .submit_rate_limit_mutation_batch(&charge_batch, None)
        .await
        .map_err(|err| {
            if let Some(rollback_batch) = rollback_batch.clone() {
                reconcile_failed_accepted_publish(
                    gateway_state.clone(),
                    charge_batch.clone(),
                    rollback_batch,
                );
            }
            error!(error = ?err, "Failed to publish accepted task reservation");
            ServerError::ServiceUnavailable("Task limiter is unavailable".to_string())
        })?;

    Ok(rollback_batch)
}

async fn retry_rate_limit_batch(
    gateway_state: &GatewayState,
    batch: &RateLimitMutationBatch,
    retry_context: &'static str,
) -> bool {
    let mut delay = RECONCILIATION_RETRY_INITIAL_DELAY;
    for attempt in 1..=RECONCILIATION_RETRY_MAX_ATTEMPTS {
        match gateway_state
            .submit_rate_limit_mutation_batch(batch, None)
            .await
        {
            Ok(_) => return true,
            Err(err) => {
                if attempt == RECONCILIATION_RETRY_MAX_ATTEMPTS {
                    error!(
                        error = ?err,
                        attempt,
                        max_attempts = RECONCILIATION_RETRY_MAX_ATTEMPTS,
                        request_id = batch.request_id,
                        retry_context,
                        "Rate limit reconciliation exhausted"
                    );
                    return false;
                }

                error!(
                    error = ?err,
                    attempt,
                    max_attempts = RECONCILIATION_RETRY_MAX_ATTEMPTS,
                    request_id = batch.request_id,
                    retry_context,
                    "Retrying rate limit reconciliation"
                );
                sleep(delay).await;
                delay = (delay * 2).min(RECONCILIATION_RETRY_MAX_DELAY);
            }
        }
    }
    false
}

fn reconcile_failed_accepted_publish(
    gateway_state: GatewayState,
    charge_batch: RateLimitMutationBatch,
    rollback_batch: RateLimitMutationBatch,
) {
    if charge_batch.is_empty() {
        return;
    }

    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        handle.spawn(async move {
            if retry_rate_limit_batch(
                &gateway_state,
                &charge_batch,
                "publish accepted task reservation before rollback",
            )
            .await
            {
                rollback_published_charge(gateway_state, rollback_batch).await;
            }
        });
    }
}

async fn rollback_published_charge(gateway_state: GatewayState, batch: RateLimitMutationBatch) {
    if batch.is_empty() {
        return;
    }

    if let Err(err) = gateway_state
        .submit_rate_limit_mutation_batch(&batch, None)
        .await
    {
        error!(
            error = ?err,
            request_id = batch.request_id,
            "Failed to rollback published accepted task charge"
        );
        reconcile_failed_charge_rollback(gateway_state, batch);
    }
}

fn reconcile_failed_charge_rollback(gateway_state: GatewayState, batch: RateLimitMutationBatch) {
    if batch.is_empty() {
        return;
    }

    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        handle.spawn(async move {
            let _ = retry_rate_limit_batch(&gateway_state, &batch, "rollback accepted task charge")
                .await;
        });
    }
}

fn task_kind_label(task: &Task) -> &'static str {
    if task.image.is_some() {
        "img3d"
    } else {
        "txt3d"
    }
}

fn billing_task_kind(task_kind: TaskKind) -> &'static str {
    match task_kind {
        TaskKind::TextTo3D => "text_to_3d",
        TaskKind::ImageTo3D => "image_to_3d",
    }
}

fn current_time_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

fn registered_user_free_limits(
    user_limits: Option<(u64, u64)>,
    registered_generation_limit: u64,
    registered_window_ms: u64,
) -> Result<(Option<i32>, Option<i64>), ServerError> {
    user_limits.ok_or_else(|| {
        ServerError::Internal("Authenticated user limits are missing".to_string())
    })?;
    Ok((
        Some(registered_generation_limit.min(i32::MAX as u64) as i32),
        Some(registered_window_ms.min(i64::MAX as u64) as i64),
    ))
}

fn generic_key_limits(
    generic_global_daily_limit: u64,
    generic_per_ip_daily_limit: u64,
    generic_window_ms: u64,
) -> (Option<i32>, Option<i32>, Option<i64>) {
    (
        Some(generic_global_daily_limit.min(i32::MAX as u64) as i32),
        Some(generic_per_ip_daily_limit.min(i32::MAX as u64) as i32),
        Some(generic_window_ms.min(i64::MAX as u64) as i64),
    )
}

fn guest_key_hash(ctx: &RateLimitContext, secret: &str) -> Result<Vec<u8>, ServerError> {
    let subject = ctx
        .decimal_ip
        .as_deref()
        .or(ctx.source_addr.as_deref())
        .unwrap_or("unknown");
    let hasher = GuestIpHasher::new(secret)
        .map_err(|err| ServerError::Internal(format!("Invalid API key secret: {err}")))?;
    Ok(hasher.compute_hash_128(subject).to_vec())
}

fn unexpected_task_billing_error_to_server_error(error: anyhow::Error) -> ServerError {
    error!(error = ?error, "Unexpected task billing lifecycle error");
    ServerError::Internal(String::new())
}

fn sanitize_task_rejection_message(error_code: &str, error_message: String) -> String {
    if error_code == "insufficient_balance"
        && error_message
            .to_ascii_lowercase()
            .contains("insufficient balance for account")
    {
        return "Insufficient balance.".to_string();
    }
    error_message
}

fn task_submission_rejection_to_server_error(
    rejection: CreateGenerationTaskRejection,
) -> ServerError {
    let CreateGenerationTaskRejection {
        error_code,
        error_message,
    } = rejection;
    let error_message = sanitize_task_rejection_message(&error_code, error_message);
    let payload = json!({
        "error": error_code.clone(),
        "message": error_message,
    });

    match error_code.as_str() {
        "concurrent_limit" => ServerError::Json(StatusCode::TOO_MANY_REQUESTS, payload),
        "daily_limit" => ServerError::Json(StatusCode::TOO_MANY_REQUESTS, payload),
        "insufficient_balance" => ServerError::Json(StatusCode::PAYMENT_REQUIRED, payload),
        "login_required" => ServerError::Json(StatusCode::UNAUTHORIZED, payload),
        "pricing_unavailable" => ServerError::Json(StatusCode::SERVICE_UNAVAILABLE, payload),
        _ => {
            error!(
                error_code = %error_code,
                payload = ?payload,
                "Unexpected task submission rejection code"
            );
            ServerError::Internal(String::new())
        }
    }
}

struct AddTaskMultipartData {
    seed: Option<i32>,
    prompt: Option<String>,
    image: Option<Bytes>,
    model: Option<String>,
    model_params: Option<serde_json::Value>,
}

struct ValidatedAddTask {
    seed: Option<i32>,
    prompt: Option<String>,
    image: Option<crate::common::image::ImageValidationResult>,
    model: Option<String>,
    model_params: Option<ModelParams>,
    task_kind: TaskKind,
}

async fn parse_add_task_multipart(
    depot: &mut Depot,
    req: &mut Request,
    boundary: &str,
) -> Result<AddTaskMultipartData, ServerError> {
    let byte_stream = multipart_stream(req);

    let state = depot.require::<HttpState>()?.clone();
    let cfg = state.config();
    let image_cfg = cfg.image();
    let prompt_cfg = cfg.prompt();
    let model_params_cfg = cfg.model_params();
    let upload_limiter = cfg.image_upload_limiter();
    let mut image_permit: Option<OwnedSemaphorePermit> = None;

    let constraints = Constraints::new()
        .allowed_fields(vec!["seed", "prompt", "image", "model", "model_params"])
        .size_limit(
            SizeLimit::new()
                .for_field("image", image_cfg.max_size_bytes as u64)
                .for_field("prompt", prompt_cfg.max_len as u64)
                .for_field("model", 64)
                .for_field("seed", 11)
                .for_field("model_params", model_params_cfg.max_len as u64),
        );

    let mut multipart = Multipart::with_constraints(byte_stream, boundary, constraints);
    let mut prompt = None;
    let mut seed = None;
    let mut image = None;
    let mut model = None;
    let mut model_params = None;

    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|e| ServerError::BadRequest(format!("Field error: {}", e)))?
    {
        let name = field
            .name()
            .ok_or_else(|| ServerError::BadRequest("Unnamed field".into()))?;

        match name {
            "prompt" => {
                prompt = Some(read_text_field(field, "prompt").await?);
            }
            "image" => {
                let is_whitelisted = depot
                    .obtain::<RateLimitContext>()
                    .map(|ctx| ctx.is_whitelisted_ip)
                    .unwrap_or(false);

                if image_permit.is_none() && !is_whitelisted {
                    image_permit = Some(upload_limiter.acquire().await?);
                }
                let content =
                    read_binary_field(field, image_cfg.max_size_bytes as u64, "image").await?;
                image = Some(Bytes::from(content));
            }
            "model" => {
                model = Some(read_text_field(field, "model").await?);
            }
            "seed" => {
                seed = Some(parse_seed_text(&read_text_field(field, "seed").await?)?);
            }
            "model_params" => {
                let raw_model_params = read_text_field(field, "model_params").await?;
                let parsed_model_params = serde_json::from_str::<serde_json::Value>(
                    &raw_model_params,
                )
                .map_err(|err| {
                    ServerError::BadRequest(format!("Model params must be valid JSON: {}", err))
                })?;
                model_params = Some(parsed_model_params);
            }
            _ => continue,
        }
    }

    Ok(AddTaskMultipartData {
        prompt,
        image,
        model,
        seed,
        model_params,
    })
}

async fn parse_add_task_request(
    depot: &mut Depot,
    req: &mut Request,
) -> Result<AddTaskMultipartData, ServerError> {
    let content_type = req
        .headers()
        .get("content-type")
        .and_then(|ct| ct.to_str().ok())
        .ok_or(ServerError::BadRequest("Missing content-type".into()))?
        .to_owned();

    if is_multipart_form(&content_type) {
        let boundary = parse_boundary(&content_type)?;
        parse_add_task_multipart(depot, req, boundary).await
    } else {
        let add_task_json = req
            .parse_json::<serde_json::Value>()
            .await
            .map_err(|e| ServerError::BadRequest(e.to_string()))?;
        if add_task_json
            .as_object()
            .and_then(|payload| payload.get("model_params"))
            .is_some_and(|value| value.is_null())
        {
            return Err(ServerError::BadRequest(
                "Model params must be a JSON object".into(),
            ));
        }
        let add_task: AddTaskRequest = serde_json::from_value(add_task_json)
            .map_err(|e| ServerError::BadRequest(e.to_string()))?;
        Ok(AddTaskMultipartData {
            prompt: add_task.prompt,
            image: None,
            model: add_task.model,
            seed: add_task.seed.map(|seed| seed.into_i32()),
            model_params: add_task.model_params,
        })
    }
}

fn parse_seed_text(seed: &str) -> Result<i32, ServerError> {
    let seed = seed.trim();
    if let Ok(value) = seed.parse::<i32>() {
        return Ok(value);
    }

    seed.parse::<u32>()
        .map(|value| value as i32)
        .map_err(|e| ServerError::BadRequest(format!("Invalid seed value: {}", e)))
}

fn validate_add_task_input(
    depot: &mut Depot,
    add_task: AddTaskMultipartData,
) -> Result<ValidatedAddTask, ServerError> {
    let state = depot.require::<HttpState>()?.clone();
    let cfg = state.config();
    let AddTaskMultipartData {
        seed,
        prompt,
        image,
        model,
        model_params,
    } = add_task;

    let has_prompt = prompt.as_ref().is_some_and(|p| !p.is_empty());
    let has_image = image.as_ref().is_some_and(|b| !b.is_empty());
    if has_prompt == has_image {
        return Err(ServerError::BadRequest(if has_prompt {
            "Cannot provide both prompt and image. Choose one.".into()
        } else {
            "Must provide either prompt or image".into()
        }));
    }

    let task_kind = if has_image {
        TaskKind::ImageTo3D
    } else {
        TaskKind::TextTo3D
    };

    if let Some(prompt) = &prompt {
        let prompt_cfg = cfg.prompt();
        let len = prompt.chars().count();
        if len < prompt_cfg.min_len {
            return Err(ServerError::BadRequest(format!(
                "Prompt is too short: minimum length is {} characters (got {})",
                prompt_cfg.min_len, len
            )));
        }

        let prompt_regex = cfg.prompt_regex();
        if !prompt_regex.is_match(prompt) {
            return Err(ServerError::BadRequest(format!(
                "Prompt contains invalid characters; allowed pattern: {}",
                prompt_cfg.allowed_pattern
            )));
        }
    }

    let validated_model_params = if let Some(model_params_value) = model_params {
        let model_params = model_params_value
            .as_object()
            .cloned()
            .ok_or_else(|| ServerError::BadRequest("Model params must be a JSON object".into()))?;
        let model_params_cfg = cfg.model_params();
        let serialized_len = serde_json::to_vec(&model_params)
            .map_err(|err| {
                ServerError::BadRequest(format!("Model params must be valid JSON: {}", err))
            })?
            .len();

        if serialized_len > model_params_cfg.max_len {
            return Err(ServerError::BadRequest(format!(
                "Model params is too long: maximum length is {} bytes (got {})",
                model_params_cfg.max_len, serialized_len
            )));
        }

        Some(model_params)
    } else {
        None
    };

    let validated_image = if let Some(image_data) = image {
        let image_cfg = cfg.image();
        Some(validate_image(image_data, image_cfg)?)
    } else {
        None
    };

    Ok(ValidatedAddTask {
        seed,
        prompt,
        image: validated_image,
        model,
        task_kind,
        model_params: validated_model_params,
    })
}

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
    let mut rollback_guard = PendingRateLimitRollbackGuard::new(None);

    let outcome = async {
        let add_task = parse_add_task_request(depot, req).await?;
        let validated = validate_add_task_input(depot, add_task)?;
        let has_prompt = validated.prompt.is_some();
        let has_image = validated.image.is_some();
        let task_kind = validated.task_kind;
        let user_seed = validated.seed.filter(|seed| *seed != -1);
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

        let queue_slot = queue
            .try_reserve(queue_limit)
            .ok_or_else(|| ServerError::ServiceUnavailable("Task queue is full".to_string()))?;

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
        let task_description = if let Some(prompt) = &validated.prompt {
            format!("prompt: {}", prompt)
        } else if let Some(img) = &validated.image {
            format!(
                "image - format: {:?}, dimensions: {}x{}",
                img.format, img.width, img.height
            )
        } else {
            return Err(ServerError::Internal(
                "Neither prompt nor image present after validation".to_string(),
            ));
        };

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
                return Err(task_submission_rejection_to_server_error(rejection));
            }
        }

        gateway_state
            .task_manager()
            .add_task_with_rate_limit_reservation(&task, reservation.clone())
            .await;
        queue_slot.push(task);
        metrics.set_queue_len(queue.len());

        if let Some(user_seed) = user_seed {
            info!(
                billing_actor = billing_actor,
                "A new task has been pushed with ID: {}, model: {}, origin: {}, {}, seed: {}",
                task_id,
                model_name,
                record_origin,
                task_description,
                user_seed
            );
        } else {
            info!(
                billing_actor = billing_actor,
                "A new task has been pushed with ID: {}, model: {}, origin: {}, {}",
                task_id,
                model_name,
                record_origin,
                task_description
            );
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
            rollback_guard.rollback_now().await;
            Err(err)
        }
    }
}

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
            return Err(ServerError::BadRequestJson(json!({
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
        "Worker {} has requested {} tasks, model: {}.",
        get_tasks.worker_hotkey,
        get_tasks.requested_task_count,
        model_filter.join(", ").as_str()
    );

    let gateways = gateway_state
        .gateways()
        .await
        .map_err(|e| ServerError::Internal(format!("Failed to obtain gateways: {:?}", e)))?;

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
                    "Failed to persist {} task assignments for worker {}: {:?}; requeueing tasks",
                    task_ids.len(),
                    worker_hotkey.as_ref(),
                    err
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
                let assigned_count = task_outcomes
                    .values()
                    .filter(|assignment| {
                        assignment.action == RecordedGenerationTaskAssignmentAction::Assigned
                    })
                    .count();
                let requeue_count = task_outcomes
                    .values()
                    .filter(|assignment| {
                        assignment.action == RecordedGenerationTaskAssignmentAction::Requeue
                    })
                    .count();
                let retire_count = task_outcomes
                    .values()
                    .filter(|assignment| {
                        assignment.action == RecordedGenerationTaskAssignmentAction::Retire
                    })
                    .count();
                if requeue_count > 0 || retire_count > 0 {
                    warn!(
                        requested = task_ids.len(),
                        assigned = assigned_count,
                        requeue = requeue_count,
                        retire = retire_count,
                        worker = worker_hotkey.as_ref(),
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

    if !tasks.is_empty() {
        info!(
            "Worker {} received {} tasks",
            get_tasks.worker_hotkey,
            tasks.len()
        );
    }

    let response = GetTasksResponse { tasks, gateways };
    res.render(Json(response));
    Ok(())
}

// curl --http3 -X GET -k https://gateway-eu.404.xyz:4443/get_load
#[handler]
pub async fn get_load_handler(
    depot: &mut Depot,
    _req: &mut Request,
    res: &mut Response,
) -> Result<(), ServerError> {
    let state = depot.require::<HttpState>()?.clone();
    let gateway_state = state.gateway_state().clone();

    let gateways = gateway_state.gateways().await.map_err(|e| {
        ServerError::Internal(format!(
            "Failed to obtain the gateways for get_load: {:?}",
            e
        ))
    })?;

    let load_response = LoadResponse { gateways };
    res.render(Json(load_response));
    Ok(())
}

#[cfg(test)]
mod tests {
    use anyhow::anyhow;
    use http::StatusCode;

    use super::{
        PendingRateLimitRollbackGuard, RateLimitReservation, registered_user_free_limits,
        task_submission_rejection_to_server_error, unexpected_task_billing_error_to_server_error,
    };
    use crate::db::CreateGenerationTaskRejection;
    use crate::http3::error::ServerError;
    use crate::raft::rate_limit::{CheckAndIncrParams, ClientKey, DistributedRateLimiter};
    use crate::raft::store::{RateLimitDelta, Subject};

    #[tokio::test]
    async fn rollback_guard_restores_local_pending_capacity_on_drop() {
        let limiter = DistributedRateLimiter::new(64);
        let key = ClientKey {
            subject: Subject::User,
            id: 42u128,
        };
        let day_epoch = 50u64;

        assert!(
            limiter
                .check_and_incr(CheckAndIncrParams {
                    key,
                    active_limit: 1,
                    daily_limit: 0,
                    cluster_active: 0,
                    cluster_day: 0,
                    day_epoch,
                })
                .await
                .is_ok()
        );
        assert!(
            limiter
                .check_and_incr(CheckAndIncrParams {
                    key,
                    active_limit: 1,
                    daily_limit: 0,
                    cluster_active: 0,
                    cluster_day: 0,
                    day_epoch,
                })
                .await
                .is_err()
        );

        {
            let reservation = RateLimitReservation::new(
                limiter.clone(),
                vec![RateLimitDelta {
                    subject: key.subject,
                    id: key.id,
                    day_epoch,
                    add_active: 1,
                    add_day: 0,
                }],
            );
            let mut guard = PendingRateLimitRollbackGuard::new(None);
            guard.arm(Some(reservation));
        }

        // Drop-based rollback is spawned; allow it to run.
        tokio::task::yield_now().await;

        assert!(
            limiter
                .check_and_incr(CheckAndIncrParams {
                    key,
                    active_limit: 1,
                    daily_limit: 0,
                    cluster_active: 0,
                    cluster_day: 0,
                    day_epoch,
                })
                .await
                .is_ok(),
            "rollback guard should release pending local capacity"
        );
    }

    #[tokio::test]
    async fn rollback_guard_restores_local_pending_capacity_on_explicit_rollback() {
        let limiter = DistributedRateLimiter::new(64);
        let key = ClientKey {
            subject: Subject::User,
            id: 99u128,
        };
        let day_epoch = 80u64;

        assert!(
            limiter
                .check_and_incr(CheckAndIncrParams {
                    key,
                    active_limit: 1,
                    daily_limit: 0,
                    cluster_active: 0,
                    cluster_day: 0,
                    day_epoch,
                })
                .await
                .is_ok()
        );
        assert!(
            limiter
                .check_and_incr(CheckAndIncrParams {
                    key,
                    active_limit: 1,
                    daily_limit: 0,
                    cluster_active: 0,
                    cluster_day: 0,
                    day_epoch,
                })
                .await
                .is_err()
        );

        let reservation = RateLimitReservation::new(
            limiter.clone(),
            vec![RateLimitDelta {
                subject: key.subject,
                id: key.id,
                day_epoch,
                add_active: 1,
                add_day: 0,
            }],
        );
        let mut guard = PendingRateLimitRollbackGuard::new(Some(reservation));
        guard.rollback_now().await;

        assert!(
            limiter
                .check_and_incr(CheckAndIncrParams {
                    key,
                    active_limit: 1,
                    daily_limit: 0,
                    cluster_active: 0,
                    cluster_day: 0,
                    day_epoch,
                })
                .await
                .is_ok(),
            "explicit rollback should release pending local capacity"
        );
    }

    #[test]
    fn zero_registered_free_limit_is_forwarded() {
        assert_eq!(
            registered_user_free_limits(Some((1, 0)), 0, 86_400_000)
                .expect("zero registered free limit should parse"),
            (Some(0), Some(86_400_000))
        );
    }

    #[test]
    fn positive_registered_free_settings_are_forwarded() {
        assert_eq!(
            registered_user_free_limits(Some((1, 0)), 25, 60_000)
                .expect("positive registered free settings should parse"),
            (Some(25), Some(60_000))
        );
    }

    #[test]
    fn very_large_registered_free_settings_are_clamped() {
        assert_eq!(
            registered_user_free_limits(Some((1, 0)), i32::MAX as u64 + 99, i64::MAX as u64 + 99,)
                .expect("large registered free settings should clamp"),
            (Some(i32::MAX), Some(i64::MAX))
        );
    }

    #[test]
    fn missing_limits_fail_registered_free_mapping() {
        match registered_user_free_limits(None, 5, 60_000) {
            Err(ServerError::Internal(message)) => {
                assert!(message.contains("Authenticated user limits are missing"));
            }
            other => panic!("expected internal error, got {:?}", other),
        }
    }

    #[test]
    fn unexpected_billing_errors_are_sanitized() {
        let error = anyhow!("column foo does not exist");

        match unexpected_task_billing_error_to_server_error(error) {
            ServerError::Internal(message) => {
                assert!(message.is_empty(), "internal errors should be sanitized");
            }
            other => panic!("expected internal server error, got {:?}", other),
        }
    }

    #[test]
    fn daily_limit_rejections_are_rendered_as_structured_rate_limits() {
        match task_submission_rejection_to_server_error(CreateGenerationTaskRejection {
            error_code: "daily_limit".to_string(),
            error_message: "Daily free limit exceeded on 2026-03-25.".to_string(),
        }) {
            ServerError::Json(status, payload) => {
                assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
                assert_eq!(payload["error"], "daily_limit");
                assert_eq!(
                    payload["message"],
                    "Daily free limit exceeded on 2026-03-25."
                );
            }
            other => panic!("expected rate limit response, got {:?}", other),
        }
    }

    #[test]
    fn insufficient_balance_rejections_are_rendered_as_payment_required() {
        match task_submission_rejection_to_server_error(CreateGenerationTaskRejection {
            error_code: "insufficient_balance".to_string(),
            error_message: "Insufficient balance for account 42.".to_string(),
        }) {
            ServerError::Json(status, payload) => {
                assert_eq!(status, StatusCode::PAYMENT_REQUIRED);
                assert_eq!(payload["error"], "insufficient_balance");
                assert_eq!(payload["message"], "Insufficient balance.");
            }
            other => panic!("expected payment required response, got {:?}", other),
        }
    }
}
