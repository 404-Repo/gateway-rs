use bytes::Bytes;
use futures::stream;
use http::{HeaderValue, StatusCode, header::CONTENT_TYPE};
use salvo::{Depot, Request, Response, Writer, async_trait};
use serde_json::Value;
use std::convert::Infallible;
use tracing::error;

#[derive(Debug)]
pub enum ServerError {
    BadRequest(String),
    BadRequestJson(Value),
    Json(StatusCode, Value),
    Internal(String),
    Unauthorized(String),
    NotFound(String),
    ServiceUnavailable(String),
}

impl ServerError {
    pub fn status_code(&self) -> StatusCode {
        match self {
            ServerError::BadRequest(_) => StatusCode::BAD_REQUEST,
            ServerError::BadRequestJson(_) => StatusCode::BAD_REQUEST,
            ServerError::Json(status, _) => *status,
            ServerError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
            ServerError::Unauthorized(_) => StatusCode::UNAUTHORIZED,
            ServerError::NotFound(_) => StatusCode::NOT_FOUND,
            ServerError::ServiceUnavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
        }
    }
}

#[async_trait]
impl Writer for ServerError {
    async fn write(self, _req: &mut Request, _depot: &mut Depot, res: &mut Response) {
        res.status_code(self.status_code());
        let (content_type, msg) = match self {
            ServerError::BadRequest(msg) => {
                ("text/plain; charset=utf-8", format!("Bad request: {}", msg))
            }
            ServerError::BadRequestJson(payload) => (
                "application/json; charset=utf-8",
                serde_json::to_string(&payload).unwrap_or_else(|_| "{}".to_string()),
            ),
            ServerError::Json(_, payload) => (
                "application/json; charset=utf-8",
                serde_json::to_string(&payload).unwrap_or_else(|_| "{}".to_string()),
            ),
            ServerError::Internal(details) => {
                if !details.is_empty() {
                    error!(details = %details, "Internal server error");
                }
                (
                    "text/plain; charset=utf-8",
                    "Internal Server Error".to_string(),
                )
            }
            ServerError::Unauthorized(msg) => (
                "text/plain; charset=utf-8",
                format!("Unauthorized request: {}", msg),
            ),
            ServerError::NotFound(msg) => ("text/plain; charset=utf-8", msg),
            ServerError::ServiceUnavailable(msg) => (
                "text/plain; charset=utf-8",
                format!("Service unavailable: {}", msg),
            ),
        };

        res.headers_mut()
            .insert(CONTENT_TYPE, HeaderValue::from_static(content_type));
        let bytes = Bytes::from(msg);
        let single = stream::once(async move { Ok::<_, Infallible>(bytes) });
        res.stream(single);
    }
}
