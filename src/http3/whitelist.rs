use salvo::{Depot, Request};
use std::collections::HashSet;
use std::net::IpAddr;
use std::sync::Arc;

#[derive(Clone)]
pub struct AddTaskWhitelist {
    pub ips: Arc<HashSet<IpAddr>>,
}

pub fn is_whitelisted_ip(req: &Request, depot: &Depot) -> bool {
    let remote_ip = match req.remote_addr() {
        salvo::conn::SocketAddr::IPv4(addr) => Some(IpAddr::V4(*addr.ip())),
        salvo::conn::SocketAddr::IPv6(addr) => Some(IpAddr::V6(*addr.ip())),
        _ => None,
    };

    if let Some(ip) = remote_ip {
        if let Ok(whitelist) = depot.obtain::<AddTaskWhitelist>() {
            return whitelist.ips.contains(&ip);
        }
    }
    false
}
