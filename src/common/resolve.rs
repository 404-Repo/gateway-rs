use anyhow::anyhow;
use anyhow::Result;
use futures::future::try_join_all;
use std::net::IpAddr;

pub async fn lookup_hosts_ips(dns_names: &[impl AsRef<str>]) -> Result<Vec<IpAddr>> {
    let futures = dns_names.iter().map(|dns_name| async {
        let addrs = tokio::net::lookup_host((dns_name.as_ref(), 0)).await?;
        let ip = addrs
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("No IP address found for {}", dns_name.as_ref()))?
            .ip();
        Ok::<IpAddr, anyhow::Error>(ip)
    });
    let ips = try_join_all(futures).await?;
    Ok(ips)
}
