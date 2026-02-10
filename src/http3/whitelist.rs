use salvo::Request;
use std::collections::HashSet;
use std::net::IpAddr;
use std::sync::Arc;

use crate::http3::state::HttpState;

#[derive(Clone)]
pub struct RateLimitWhitelist {
    pub ips: Arc<HashSet<IpAddr>>,
}

pub fn is_whitelisted_ip(req: &Request, state: &HttpState) -> bool {
    let remote_ip = match req.remote_addr() {
        salvo::conn::SocketAddr::IPv4(addr) => Some(IpAddr::V4(*addr.ip())),
        salvo::conn::SocketAddr::IPv6(addr) => Some(IpAddr::V6(*addr.ip())),
        _ => None,
    };

    if let Some(ip) = remote_ip {
        return state.rate_limit_whitelist().ips.contains(&ip);
    }
    false
}
