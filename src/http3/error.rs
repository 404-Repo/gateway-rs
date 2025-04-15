use bytes::Bytes;
use futures::stream;
use http::{header::CONTENT_TYPE, HeaderValue, StatusCode};
use salvo::{async_trait, Depot, Request, Response, Writer};
use std::convert::Infallible;

#[derive(Debug)]
pub enum ServerError {
    BadRequest(String),
    Internal(String),
    Unauthorized(String),
    NotFound(String),
}

impl ServerError {
    pub fn status_code(&self) -> StatusCode {
        match self {
            ServerError::BadRequest(_) => StatusCode::BAD_REQUEST,
            ServerError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
            ServerError::Unauthorized(_) => StatusCode::UNAUTHORIZED,
            ServerError::NotFound(_) => StatusCode::NOT_FOUND,
        }
    }
}

#[async_trait]
impl Writer for ServerError {
    async fn write(self, _req: &mut Request, _depot: &mut Depot, res: &mut Response) {
        res.status_code(self.status_code());
        res.headers_mut().insert(
            CONTENT_TYPE,
            HeaderValue::from_static("text/plain; charset=utf-8"),
        );
        let msg = match self {
            ServerError::BadRequest(msg) => format!("Bad request: {}", msg),
            ServerError::Internal(details) => {
                let mut m = "Internal Server Error".to_string();
                if !details.is_empty() {
                    m.push_str(": ");
                    m.push_str(&details);
                }
                m
            }
            ServerError::Unauthorized(msg) => format!("Unauthorized request: {}", msg),
            ServerError::NotFound(msg) => msg,
        };
        let bytes = Bytes::from(msg);
        let single = stream::once(async move { Ok::<_, Infallible>(bytes) });
        res.stream(single);
    }
}
