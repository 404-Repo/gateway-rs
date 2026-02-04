use serde_json::json;

use crate::config::ModelResolveError;
use crate::http3::error::ServerError;

pub enum ModelErrorContext {
    ClientInput { field: &'static str },
    Internal,
}

fn format_known_models(known: &[String]) -> String {
    if known.is_empty() {
        "none configured".to_string()
    } else {
        known.join(", ")
    }
}

fn internal_model_error(err: ModelResolveError) -> ServerError {
    match err {
        ModelResolveError::EmptyConfig => {
            ServerError::Internal("Model configuration is empty".to_string())
        }
        ModelResolveError::MissingDefault {
            default_model,
            known,
        } => ServerError::Internal(format!(
            "Default model '{}' not configured (known models: {})",
            default_model,
            format_known_models(&known)
        )),
        ModelResolveError::UnknownModel { model, known } => ServerError::Internal(format!(
            "Model '{}' not configured (known models: {})",
            model,
            format_known_models(&known)
        )),
        ModelResolveError::InvalidOutput { model, output } => ServerError::Internal(format!(
            "Invalid output '{}' configured for model '{}'",
            output, model
        )),
        ModelResolveError::UnsupportedInput { model, input } => ServerError::Internal(format!(
            "Model '{}' does not support {} input",
            model, input
        )),
        ModelResolveError::InvalidInputSupport { model } => ServerError::Internal(format!(
            "Model '{}' must support at least one input mode",
            model
        )),
    }
}

pub fn model_error_to_server_error(
    err: ModelResolveError,
    context: ModelErrorContext,
) -> ServerError {
    match context {
        ModelErrorContext::ClientInput { field } => match err {
            ModelResolveError::UnknownModel { model, known } => {
                let message = format!(
                    "Invalid value '{}'. Expected one of: {}",
                    model,
                    format_known_models(&known)
                );
                ServerError::BadRequestJson(json!({
                    "error": "invalid_field",
                    "field": field,
                    "message": message,
                }))
            }
            ModelResolveError::UnsupportedInput { model, input } => {
                let message = format!("Model '{}' does not support {} input", model, input);
                ServerError::BadRequestJson(json!({
                    "error": "invalid_field",
                    "field": field,
                    "message": message,
                }))
            }
            other => internal_model_error(other),
        },
        ModelErrorContext::Internal => internal_model_error(err),
    }
}
