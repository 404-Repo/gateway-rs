use anyhow::{Result, anyhow};
use regex::Regex;
use rustls::SupportedProtocolVersion;
use salvo::catcher::Catcher;
use salvo::conn::quinn::QuinnListener;
use salvo::conn::rustls::RustlsConfig;
use salvo::http::request::SecureMaxSize;
use salvo::prelude::*;
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::warn;

use crate::api::Task;
use crate::common::queue::DupQueue;
use crate::common::resolve::lookup_hosts_ips;
use crate::config::{ModelConfigStore, NodeConfig};
use crate::http3::handlers::admin::{
    admin_key_check, cluster_check, generic_key_read_handler, generic_key_update_handler,
};
use crate::http3::handlers::common::{
    api_or_generic_key_check, get_leader_handler, id_handler, metrics_handler, version_handler,
    write_handler,
};
use crate::http3::handlers::result::{add_result_handler, get_result_handler, get_status_handler};
use crate::http3::handlers::task::{add_task_handler, get_load_handler, get_tasks_handler};
use crate::http3::rate_limits::{
    RateLimitService, RateLimiters, enforce_rate_limit, prepare_rate_limit_context,
};
use crate::http3::response::custom_response;
use crate::http3::state::{HttpState, HttpStateInit};
use crate::http3::upload_limiter::ImageUploadLimiter;
use crate::http3::whitelist::AddTaskWhitelist;
use crate::metrics::Metrics;
use crate::raft::gateway_state::GatewayState;
use std::collections::HashSet;
use std::net::IpAddr;
use tokio_util::sync::CancellationToken;

// TLS version combinations
const TLS_V12_ONLY: &[&SupportedProtocolVersion] = &[&rustls::version::TLS12];
const TLS_V13_ONLY: &[&SupportedProtocolVersion] = &[&rustls::version::TLS13];
const TLS_V12_V13: &[&SupportedProtocolVersion] =
    &[&rustls::version::TLS12, &rustls::version::TLS13];

fn map_tls_versions_to_static(list: &[String]) -> &'static [&'static SupportedProtocolVersion] {
    let mut has12 = false;
    let mut has13 = false;
    for part in list {
        let normalized = part.to_lowercase();
        let trimmed = normalized.trim();
        match trimmed {
            "1.2" | "tls1.2" => has12 = true,
            "1.3" | "tls1.3" => has13 = true,
            _ => {}
        }
    }
    match (has12, has13) {
        (true, true) => TLS_V12_V13,
        (true, false) => TLS_V12_ONLY,
        (false, true) => TLS_V13_ONLY,
        (false, false) => TLS_V12_V13,
    }
}

pub struct Http3Server {
    join_handle: tokio::task::JoinHandle<()>,
}

async fn resolve_domain_ips(domains: &[&str]) -> Result<HashSet<IpAddr>> {
    let resolved = lookup_hosts_ips(domains).await?;
    Ok(resolved.into_iter().collect())
}

impl Http3Server {
    pub async fn run(
        config: Arc<NodeConfig>,
        model_store: Arc<ModelConfigStore>,
        tls_config: RustlsConfig,
        gateway_state: GatewayState,
        task_queue: DupQueue<Task>,
        metrics: Metrics,
        shutdown: CancellationToken,
    ) -> Result<Self> {
        let addr_str = format!("{}:{}", config.network.bind_ip, config.http.port);
        let addr: SocketAddr = addr_str
            .parse()
            .map_err(|e| anyhow!("Invalid listen address {}: {}", addr_str, e))?;

        let mut whitelist_ips: HashSet<IpAddr> = HashSet::new();
        let mut domains: Vec<&str> = Vec::new();
        for entry in &config.http.add_task_rate_limit_allowlist {
            if let Ok(ip) = entry.parse::<IpAddr>() {
                whitelist_ips.insert(ip);
            } else {
                domains.push(entry.as_str());
            }
        }
        if !domains.is_empty() {
            match resolve_domain_ips(&domains).await {
                Ok(resolved) => {
                    whitelist_ips.extend(resolved);
                }
                Err(e) => {
                    warn!(
                        "Failed to resolve some add_task_rate_limit_allowlist domains: {:?}",
                        e
                    );
                }
            }
        }
        let add_task_whitelist = AddTaskWhitelist {
            ips: Arc::new(whitelist_ips),
        };

        // Resolve peer IPs for the cluster_check hoop
        let mut cluster_ips: HashSet<IpAddr> = HashSet::new();
        let self_domain = &config.network.domain;
        let peer_domains: Vec<&str> = config
            .network
            .node_dns_names
            .iter()
            .map(|s| s.as_str())
            .filter(|d| d != self_domain)
            .collect();
        if !peer_domains.is_empty()
            && let Ok(resolved) = resolve_domain_ips(&peer_domains).await
        {
            cluster_ips.extend(resolved);
        }

        let prompt_regex = Regex::new(&config.prompt.allowed_pattern)
            .map_err(|e| anyhow!("Invalid prompt regex: {}", e))?;

        let (rate_limit_service, rate_limiters) = RateLimitService::new(&config.http);
        let image_upload_limiter =
            ImageUploadLimiter::new(config.http.max_concurrent_image_uploads);

        let state = HttpState::new(HttpStateInit {
            config: config.clone(),
            model_store,
            gateway_state: gateway_state.clone(),
            task_queue: task_queue.clone(),
            metrics: metrics.clone(),
            add_task_whitelist,
            cluster_ips,
            image_upload_limiter,
            prompt_regex,
            rate_limits: rate_limit_service,
        });

        let router = Self::setup_router(state, rate_limiters)?;

        let service = Service::new(router).catcher(Catcher::default().hoop(custom_response));

        // QUIC requires TLS1.3 per spec.
        const TLS13_ONLY: &[&SupportedProtocolVersion] = &[&rustls::version::TLS13];

        // Build TCP-specific TLS versions from config (defaults to 1.2,1.3) using static slices.
        let tcp_versions_static = map_tls_versions_to_static(&config.http.tls_versions);
        let tls_config_tcp = tls_config.clone().tls_versions(tcp_versions_static);
        let tls_config_quic = tls_config.tls_versions(TLS13_ONLY);

        let join_handle = tokio::spawn(async move {
            let tcp_listener = TcpListener::new(addr).rustls(tls_config_tcp.clone());
            let acceptor = QuinnListener::new(tls_config_quic, addr)
                .join(tcp_listener)
                .bind()
                .await;
            tokio::select! {
                _ = shutdown.cancelled() => {},
                _ = Server::new(acceptor).serve(service) => {},
            }
        });

        Ok(Self { join_handle })
    }

    fn setup_router(state: HttpState, rate_limiters: RateLimiters) -> Result<Router> {
        let compression = state.http_config().compression;
        let compression_lvl = state.http_config().compression_lvl;
        let request_size_limit = state.http_config().request_size_limit as usize;
        let add_task_size_limit = state.http_config().add_task_size_limit as usize;
        let request_file_size_limit = state.http_config().request_file_size_limit as usize;
        let raft_write_size_limit = state.http_config().raft_write_size_limit as usize;

        let size_limit_handler = || SecureMaxSize(request_size_limit);
        let add_task_size_limit_handler = || SecureMaxSize(add_task_size_limit);
        let file_size_limit_handler = || SecureMaxSize(request_file_size_limit);
        let raft_write_size_limit_handler = || SecureMaxSize(raft_write_size_limit);

        let router = if compression {
            Router::new().hoop(
                Compression::new()
                    .force_priority(true)
                    .enable_zstd(CompressionLevel::Precise(compression_lvl)),
            )
        } else {
            Router::new()
        };

        Ok(router
            .hoop(affix_state::inject(state))
            .hoop(prepare_rate_limit_context)
            .push(
                Router::with_path("/add_task")
                    .hoop(add_task_size_limit_handler())
                    .hoop(rate_limiters.unauthorized_only_limiter)
                    .hoop(api_or_generic_key_check)
                    .hoop(rate_limiters.generic_global_limiter)
                    .hoop(rate_limiters.generic_per_ip_limiter)
                    .hoop(enforce_rate_limit)
                    .post(add_task_handler),
            )
            .push(
                Router::with_path("/add_result")
                    .hoop(file_size_limit_handler())
                    .hoop(rate_limiters.result_limiter)
                    .post(add_result_handler),
            )
            .push(
                Router::with_path("/get_tasks")
                    .hoop(size_limit_handler())
                    .post(get_tasks_handler),
            )
            .push(
                Router::with_path("/get_load")
                    .hoop(size_limit_handler())
                    .hoop(rate_limiters.load_limiter)
                    .get(get_load_handler),
            )
            .push(
                Router::with_path("/get_leader")
                    .hoop(size_limit_handler())
                    .hoop(rate_limiters.leader_limiter)
                    .get(get_leader_handler),
            )
            .push(
                Router::with_path("/write")
                    .hoop(raft_write_size_limit_handler())
                    .hoop(cluster_check)
                    .hoop(admin_key_check)
                    .post(write_handler),
            )
            .push(
                Router::with_path("/get_version")
                    .hoop(size_limit_handler())
                    .hoop(rate_limiters.basic_limiter)
                    .get(version_handler),
            )
            .push(
                Router::with_path("/id")
                    .hoop(size_limit_handler())
                    .get(id_handler),
            )
            .push(
                Router::with_path("/get_result")
                    .hoop(size_limit_handler())
                    .hoop(api_or_generic_key_check)
                    .get(get_result_handler),
            )
            .push(
                Router::with_path("/get_status")
                    .hoop(size_limit_handler())
                    .hoop(rate_limiters.status_limiter)
                    .hoop(api_or_generic_key_check)
                    .get(get_status_handler),
            )
            .push(
                Router::with_path("/update_key")
                    .hoop(size_limit_handler())
                    .hoop(rate_limiters.update_limiter)
                    .hoop(admin_key_check)
                    .post(generic_key_update_handler),
            )
            .push(
                Router::with_path("/metrics")
                    .hoop(size_limit_handler())
                    .hoop(rate_limiters.metric_limiter)
                    .get(metrics_handler),
            )
            .push(
                Router::with_path("/get_key")
                    .hoop(size_limit_handler())
                    .hoop(rate_limiters.read_limiter)
                    .hoop(admin_key_check)
                    .get(generic_key_read_handler),
            ))
    }

    pub fn abort(&self) {
        self.join_handle.abort();
    }

    pub async fn wait(&mut self) -> Result<(), tokio::task::JoinError> {
        (&mut self.join_handle).await
    }
}
