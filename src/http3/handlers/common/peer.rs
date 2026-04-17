use salvo::prelude::Request;

pub(crate) fn request_ip(req: &Request) -> String {
    match req.remote_addr() {
        salvo::conn::SocketAddr::IPv4(addr) => addr.ip().to_string(),
        salvo::conn::SocketAddr::IPv6(addr) => addr.ip().to_string(),
        _ => "unknown".to_owned(),
    }
}
