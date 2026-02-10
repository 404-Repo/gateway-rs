use salvo::http::{StatusCode, header::ACCEPT};
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

fn log_error(status: StatusCode, client_ip: String, req: &Request, body: &salvo::http::ResBody) {
    let ip = &client_ip;
    let method = req.method();
    let path = req.uri().path();
    if let salvo::http::ResBody::Error(e) = body {
        let reason = e.detail.as_deref().unwrap_or(&e.brief);
        error!(
            "Error handling request from {ip}: {method} {path} - Status: {status}, Reason: {reason}"
        );
    } else {
        error!("Error handling request from {ip}: {method} {path} - Status: {status}");
    }
}

fn accepts_html(req: &Request) -> bool {
    req.headers()
        .get(ACCEPT)
        .and_then(|v| v.to_str().ok())
        .map(|accept| {
            accept.split(',').any(|value| {
                let media_type = value.split(';').next().unwrap_or("").trim();
                media_type.eq_ignore_ascii_case("text/html")
                    || media_type.eq_ignore_ascii_case("application/xhtml+xml")
            })
        })
        .unwrap_or(false)
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
            log_error(status, client_ip, req, &res.body);
        }

        if let Some(html) = error_page_for(status) {
            // Keep explicitly-written API payloads intact.
            let has_written_body = !res.body.is_none() && !res.body.is_error();
            if has_written_body || res.content_type().is_some() {
                return;
            }
            if !accepts_html(req) {
                return;
            }
            res.render(Text::Html(html));
            res.status_code(status);
            ctrl.skip_rest();
            return;
        }
    }
}
