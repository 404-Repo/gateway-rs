use salvo::http::StatusCode;
use salvo::prelude::*;
use tracing::error;

const TOO_MANY_REQUESTS_HTML: &str = include_str!("responses/429.html");
const METHOD_NOT_ALLOWED_HTML: &str = include_str!("responses/405.html");
const BAD_REQUEST_HTML: &str = include_str!("responses/400.html");
const INTERNAL_SERVER_ERROR_HTML: &str = include_str!("responses/500.html");
const NOT_FOUND_HTML: &str = include_str!("responses/404.html");

fn error_page_for(status: StatusCode) -> Option<&'static str> {
    match status {
        StatusCode::TOO_MANY_REQUESTS => Some(TOO_MANY_REQUESTS_HTML),
        StatusCode::METHOD_NOT_ALLOWED => Some(METHOD_NOT_ALLOWED_HTML),
        StatusCode::BAD_REQUEST => Some(BAD_REQUEST_HTML),
        StatusCode::INTERNAL_SERVER_ERROR => Some(INTERNAL_SERVER_ERROR_HTML),
        StatusCode::NOT_FOUND => Some(NOT_FOUND_HTML),
        _ => None,
    }
}

#[handler]
pub async fn custom_response(
    req: &Request,
    _depot: &mut Depot,
    res: &mut Response,
    ctrl: &mut FlowCtrl,
) {
    if let Some(status) = res.status_code {
        if status.is_client_error() || status.is_server_error() {
            let client_ip = match req.remote_addr() {
                salvo::conn::SocketAddr::IPv4(addr_v4) => addr_v4.ip().to_string(),
                salvo::conn::SocketAddr::IPv6(addr_v6) => addr_v6.ip().to_string(),
                _ => "unknown".to_string(),
            };
            let reason = if let salvo::http::ResBody::Error(e) = &res.body {
                e.detail.as_deref().unwrap_or(&e.brief).to_string()
            } else {
                "No specific reason available".to_string()
            };
            error!(
                "Error handling request from {}: {} {} - Status: {}, Reason: {}",
                client_ip,
                req.method(),
                req.uri().path(),
                status,
                reason
            );
        }

        if let Some(html) = error_page_for(status) {
            res.render(Text::Html(html));
            res.status_code(status);
            ctrl.skip_rest();
            return;
        }
    }
}
