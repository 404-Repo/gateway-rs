use salvo::http::StatusCode;
use salvo::prelude::*;
use tracing::error;

const TOO_MANY_REQUESTS_HTML: &str = include_str!("responses/429.html");
const METHOD_NOT_ALLOWED_HTML: &str = include_str!("responses/405.html");
const BAD_REQUEST_HTML: &str = include_str!("responses/400.html");
const INTERNAL_SERVER_ERROR_HTML: &str = include_str!("responses/500.html");
const NOT_FOUND_HTML: &str = include_str!("responses/404.html");

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
    }

    if let Some(StatusCode::TOO_MANY_REQUESTS) = res.status_code {
        res.render(Text::Html(TOO_MANY_REQUESTS_HTML));
        res.status_code(StatusCode::TOO_MANY_REQUESTS);
        ctrl.skip_rest();
        return;
    }
    if let Some(StatusCode::METHOD_NOT_ALLOWED) = res.status_code {
        res.render(Text::Html(METHOD_NOT_ALLOWED_HTML));
        res.status_code(StatusCode::METHOD_NOT_ALLOWED);
        ctrl.skip_rest();
        return;
    }
    if let Some(StatusCode::BAD_REQUEST) = res.status_code {
        res.render(Text::Html(BAD_REQUEST_HTML));
        res.status_code(StatusCode::BAD_REQUEST);
        ctrl.skip_rest();
        return;
    }
    if let Some(StatusCode::INTERNAL_SERVER_ERROR) = res.status_code {
        res.render(Text::Html(INTERNAL_SERVER_ERROR_HTML));
        res.status_code(StatusCode::INTERNAL_SERVER_ERROR);
        ctrl.skip_rest();
        return;
    }
    if let Some(StatusCode::NOT_FOUND) = res.status_code {
        res.render(Text::Html(NOT_FOUND_HTML));
        res.status_code(StatusCode::NOT_FOUND);
        ctrl.skip_rest();
        return;
    }
}
