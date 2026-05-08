use salvo::prelude::Request;
use std::net::IpAddr;

use crate::config::TrustedProxyRange;

pub(crate) fn remote_ip(req: &Request) -> Option<IpAddr> {
    match req.remote_addr() {
        salvo::conn::SocketAddr::IPv4(addr) => Some(IpAddr::V4(*addr.ip())),
        salvo::conn::SocketAddr::IPv6(addr) => Some(IpAddr::V6(*addr.ip())),
        _ => None,
    }
}

fn is_trusted_proxy(ip: IpAddr, trusted_proxy_cidrs: &[TrustedProxyRange]) -> bool {
    trusted_proxy_cidrs.iter().any(|net| net.contains(ip))
}

fn gke_client_ip_from_x_forwarded_for(header: &str) -> Option<IpAddr> {
    let mut parts = header.split(',').rev().map(str::trim);
    let _load_balancer_ip = parts.next()?.parse::<IpAddr>().ok()?;
    let client_ip = parts.next()?.parse::<IpAddr>().ok()?;

    Some(client_ip)
}

fn client_ip_from_parts(
    remote_ip: Option<IpAddr>,
    x_forwarded_for: Option<&str>,
    trusted_proxy_cidrs: &[TrustedProxyRange],
) -> Option<IpAddr> {
    let remote_ip = remote_ip?;
    if !is_trusted_proxy(remote_ip, trusted_proxy_cidrs) {
        return Some(remote_ip);
    }

    x_forwarded_for
        .and_then(gke_client_ip_from_x_forwarded_for)
        .or(Some(remote_ip))
}

pub(crate) fn client_ip(
    req: &Request,
    trusted_proxy_cidrs: &[TrustedProxyRange],
) -> Option<IpAddr> {
    client_ip_from_parts(
        remote_ip(req),
        req.headers()
            .get("x-forwarded-for")
            .and_then(|v| v.to_str().ok()),
        trusted_proxy_cidrs,
    )
}

#[cfg(test)]
mod tests {
    use super::client_ip_from_parts;
    use crate::config::TrustedProxyRange;
    use crate::config::parse_trusted_proxy_cidr;
    use std::net::IpAddr;

    fn ip(value: &str) -> IpAddr {
        value.parse().expect("ip")
    }

    fn cidr(value: &str) -> TrustedProxyRange {
        parse_trusted_proxy_cidr(value).expect("cidr")
    }

    #[test]
    fn ignores_forwarded_header_from_untrusted_remote() {
        let trusted = [cidr("10.0.0.0/8")];

        let resolved = client_ip_from_parts(
            Some(ip("192.0.2.10")),
            Some("198.51.100.99, 203.0.113.10"),
            &trusted,
        );

        assert_eq!(resolved, Some(ip("192.0.2.10")));
    }

    #[test]
    fn trusted_gke_header_uses_second_ip_from_right() {
        let trusted = [cidr("192.0.2.0/24")];

        let resolved = client_ip_from_parts(
            Some(ip("192.0.2.10")),
            Some("198.51.100.99, 203.0.113.20, 34.117.10.1"),
            &trusted,
        );

        assert_eq!(resolved, Some(ip("203.0.113.20")));
    }

    #[test]
    fn malformed_trusted_gke_header_falls_back_to_remote() {
        let trusted = [cidr("192.0.2.0/24")];

        let resolved = client_ip_from_parts(
            Some(ip("192.0.2.10")),
            Some("198.51.100.99, not-an-ip"),
            &trusted,
        );

        assert_eq!(resolved, Some(ip("192.0.2.10")));
    }
}
