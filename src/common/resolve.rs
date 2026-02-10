use anyhow::Result;
use anyhow::anyhow;
use futures::future::try_join_all;
use std::net::IpAddr;

async fn lookup_host_ips(dns_name: &str) -> Result<Vec<IpAddr>> {
    let ips: Vec<IpAddr> = tokio::net::lookup_host((dns_name, 0))
        .await?
        .map(|addr| addr.ip())
        .collect();
    if ips.is_empty() {
        return Err(anyhow!("No IP address found for {dns_name}"));
    }
    Ok(ips)
}

/// Resolve all addresses for each host and return a flattened list.
pub async fn lookup_all_host_ips(dns_names: &[impl AsRef<str>]) -> Result<Vec<IpAddr>> {
    let futures = dns_names
        .iter()
        .map(|dns_name| async { lookup_host_ips(dns_name.as_ref()).await });
    let ip_lists = try_join_all(futures).await?;
    Ok(ip_lists.into_iter().flatten().collect())
}

/// Resolve exactly one address per host (the first resolver result).
pub async fn lookup_one_ip_per_host(dns_names: &[impl AsRef<str>]) -> Result<Vec<IpAddr>> {
    let futures = dns_names.iter().map(|dns_name| async {
        let ips = lookup_host_ips(dns_name.as_ref()).await?;
        let first_ip = *ips
            .first()
            .ok_or_else(|| anyhow!("No IP address found for {}", dns_name.as_ref()))?;
        Ok::<IpAddr, anyhow::Error>(first_ip)
    });
    let ips = try_join_all(futures).await?;
    Ok(ips)
}
