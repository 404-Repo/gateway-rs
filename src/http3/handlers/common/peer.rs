use salvo::prelude::Request;

use crate::http3::client_ip::client_ip;
use crate::http3::state::HttpState;

pub(crate) fn request_ip(req: &Request, state: &HttpState) -> String {
    let cfg = state.config();
    if let Some(ip) = client_ip(req, cfg.trusted_proxy_cidrs()) {
        return ip.to_string();
    }
    match req.remote_addr() {
        salvo::conn::SocketAddr::IPv4(addr) => addr.ip().to_string(),
        salvo::conn::SocketAddr::IPv6(addr) => addr.ip().to_string(),
        _ => "unknown".to_owned(),
    }
}
