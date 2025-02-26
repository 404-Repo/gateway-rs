use http::StatusCode;
use salvo::{async_trait, writing::Text, Depot, Request, Response, Writer};

#[derive(Debug)]
pub enum ServerError {
    BadRequest(String),
    Internal(String),
}

impl ServerError {
    pub fn status_code(&self) -> StatusCode {
        match self {
            ServerError::BadRequest(_) => StatusCode::BAD_REQUEST,
            ServerError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

#[async_trait]
impl Writer for ServerError {
    async fn write(self, _req: &mut Request, _depot: &mut Depot, res: &mut Response) {
        res.status_code(self.status_code());
        match self {
            ServerError::BadRequest(msg) => {
                res.render(Text::Plain(format!("Bad request: {}", msg)));
            }
            ServerError::Internal(details) => {
                let mut message = "Internal Server Error".to_string();
                if !details.is_empty() {
                    message.push_str(": ");
                    message.push_str(&details);
                }
                res.render(Text::Plain(message));
            }
        }
    }
}
