use bytes::Bytes;
use multer::{Constraints, Multipart, SizeLimit};
use salvo::prelude::*;
use tokio::sync::OwnedSemaphorePermit;

use crate::api::ModelParams;
use crate::api::request::AddTaskRequest;
use crate::common::image::{ImageValidationResult, validate_image};
use crate::http3::depot_ext::DepotExt;
use crate::http3::error::ServerError;
use crate::http3::handlers::common::multipart::{
    is_multipart_form, multipart_stream, parse_boundary, read_binary_field, read_text_field,
};
use crate::http3::rate_limits::RateLimitContext;
use crate::http3::state::HttpState;
use crate::metrics::TaskKind;

pub(super) struct AddTaskMultipartData {
    pub(super) seed: Option<i32>,
    pub(super) prompt: Option<String>,
    pub(super) image: Option<Bytes>,
    pub(super) model: Option<String>,
    pub(super) model_params: Option<serde_json::Value>,
}

pub(super) struct ValidatedAddTask {
    pub(super) seed: Option<i32>,
    pub(super) prompt: Option<String>,
    pub(super) image: Option<ImageValidationResult>,
    pub(super) model: Option<String>,
    pub(super) model_params: Option<ModelParams>,
    pub(super) task_kind: TaskKind,
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

pub(super) async fn parse_add_task_request(
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

pub(super) fn validate_add_task_input(
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
