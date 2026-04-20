use anyhow::Result;
use http::StatusCode;
use serde_json::json;
use tracing::error;

use crate::crypto::crypto_provider::GuestIpHasher;
use crate::db::CreateGenerationTaskRejection;
use crate::http3::error::ServerError;
use crate::http3::rate_limits::RateLimitContext;
use crate::metrics::TaskKind;

pub(super) fn billing_task_kind(task_kind: TaskKind) -> &'static str {
    match task_kind {
        TaskKind::TextTo3D => "text_to_3d",
        TaskKind::ImageTo3D => "image_to_3d",
    }
}

pub(super) fn registered_user_free_limits(
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

pub(super) fn generic_key_limits(
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

pub(super) fn guest_key_hash(ctx: &RateLimitContext, secret: &str) -> Result<Vec<u8>, ServerError> {
    let subject = ctx
        .decimal_ip
        .as_deref()
        .or(ctx.source_addr.as_deref())
        .unwrap_or("unknown");
    let hasher = GuestIpHasher::new(secret)
        .map_err(|err| ServerError::Internal(format!("Invalid API key secret: {err}")))?;
    Ok(hasher.compute_hash_128(subject).to_vec())
}

pub(super) fn unexpected_task_billing_error_to_server_error(error: anyhow::Error) -> ServerError {
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

pub(super) fn task_submission_rejection_to_server_error(
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

#[cfg(test)]
mod tests {
    use anyhow::anyhow;
    use http::StatusCode;

    use super::{
        registered_user_free_limits, task_submission_rejection_to_server_error,
        unexpected_task_billing_error_to_server_error,
    };
    use crate::db::CreateGenerationTaskRejection;
    use crate::http3::error::ServerError;

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
            registered_user_free_limits(Some((1, 0)), i32::MAX as u64 + 99, i64::MAX as u64 + 99)
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
