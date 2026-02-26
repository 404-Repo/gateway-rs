use foldhash::HashSet as FoldHashSet;
use futures::stream::{self, StreamExt};
use salvo::Request;
use std::collections::HashSet;
use std::net::IpAddr;
use std::sync::Arc;
use tracing::warn;

use crate::common::resolve::lookup_all_host_ips;
use crate::http3::state::HttpState;

#[derive(Clone)]
pub struct RateLimitWhitelist {
    pub ips: Arc<HashSet<IpAddr>>,
}

const DNS_RESOLVE_CONCURRENCY: usize = 32;

async fn resolve_domains_best_effort(domains: Vec<String>, context: &str) -> HashSet<IpAddr> {
    let mut ips = HashSet::new();
    let mut resolutions = stream::iter(domains.into_iter())
        .map(|domain| async move {
            let result = lookup_all_host_ips(std::slice::from_ref(&domain)).await;
            (domain, result)
        })
        .buffer_unordered(DNS_RESOLVE_CONCURRENCY);

    while let Some((domain, result)) = resolutions.next().await {
        match result {
            Ok(resolved) => ips.extend(resolved),
            Err(err) => warn!("Failed to resolve {context} domain '{}': {}", domain, err),
        }
    }
    ips
}

pub async fn resolve_rate_limit_whitelist(entries: &FoldHashSet<String>) -> HashSet<IpAddr> {
    let mut ips = HashSet::new();
    let mut domains: Vec<String> = Vec::new();

    for entry in entries {
        if let Ok(ip) = entry.parse::<IpAddr>() {
            ips.insert(ip);
        } else {
            domains.push(entry.clone());
        }
    }

    if !domains.is_empty() {
        ips.extend(resolve_domains_best_effort(domains, "rate_limit_whitelist").await);
    }

    ips
}

pub async fn resolve_cluster_peer_ips(
    self_domain: &str,
    node_dns_names: &[String],
) -> HashSet<IpAddr> {
    let domains: Vec<String> = node_dns_names
        .iter()
        .filter(|d| d.as_str() != self_domain)
        .cloned()
        .collect();
    if domains.is_empty() {
        return HashSet::new();
    }

    resolve_domains_best_effort(domains, "cluster peer").await
}

pub fn is_whitelisted_ip(req: &Request, state: &HttpState) -> bool {
    let cfg = state.config();
    let remote_ip = match req.remote_addr() {
        salvo::conn::SocketAddr::IPv4(addr) => Some(IpAddr::V4(*addr.ip())),
        salvo::conn::SocketAddr::IPv6(addr) => Some(IpAddr::V6(*addr.ip())),
        _ => None,
    };

    if let Some(ip) = remote_ip {
        return cfg.rate_limit_whitelist().ips.contains(&ip);
    }
    false
}

#[cfg(test)]
mod tests {
    use super::{resolve_cluster_peer_ips, resolve_rate_limit_whitelist};
    use foldhash::HashSet as FoldHashSet;

    #[tokio::test]
    async fn whitelist_resolution_keeps_successful_domains_when_some_fail() {
        let mut entries: FoldHashSet<String> = FoldHashSet::default();
        entries.insert("localhost".to_string());
        entries.insert("definitely-invalid-hostname.invalid".to_string());

        let resolved = resolve_rate_limit_whitelist(&entries).await;
        assert!(
            !resolved.is_empty(),
            "expected at least localhost to resolve"
        );
    }

    #[tokio::test]
    async fn cluster_resolution_keeps_successful_domains_when_some_fail() {
        let domains = vec![
            "self.local".to_string(),
            "localhost".to_string(),
            "definitely-invalid-hostname.invalid".to_string(),
        ];

        let resolved = resolve_cluster_peer_ips("self.local", &domains).await;
        assert!(
            !resolved.is_empty(),
            "expected at least localhost to resolve"
        );
    }
}
