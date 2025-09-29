use std::time::Duration;

use anyhow::{anyhow, Result};
use regex::Regex;
use rustls::SupportedProtocolVersion;
use salvo::catcher::Catcher;
use salvo::conn::quinn::QuinnListener;
use salvo::conn::rustls::RustlsConfig;
use salvo::http::request::SecureMaxSize;
use salvo::prelude::*;
use std::net::SocketAddr;
use std::sync::Arc;

use crate::api::Task;
use crate::bittensor::subnet_state::SubnetState;
use crate::common::queue::DupQueue;
use crate::common::resolve::lookup_hosts_ips;
use crate::config::NodeConfig;
use crate::http3::company_rate_limits::{company_rate_limit, CompanyRateLimiterStore};
use crate::http3::handlers::admin::{
    admin_key_check, generic_key_read_handler, generic_key_update_handler,
};
use crate::http3::handlers::common::{
    api_or_generic_key_check, get_leader_handler, id_handler, metrics_handler, version_handler,
    write_handler,
};
use crate::http3::handlers::result::{add_result_handler, get_result_handler, get_status_handler};
use crate::http3::handlers::task::{add_task_handler, get_load_handler, get_tasks_handler};
use crate::http3::response::custom_response;
use crate::http3::user_rate_limits::{prepare_rate_limit_context, RateLimits};
use crate::http3::whitelist::AddTaskWhitelist;
use crate::metrics::Metrics;
use crate::raft::gateway_state::GatewayState;
use std::collections::HashSet;
use std::net::IpAddr;

pub struct Http3Server {
    join_handle: tokio::task::JoinHandle<()>,
    subnet_state: SubnetState,
}

impl Http3Server {
    pub async fn run(
        config: Arc<NodeConfig>,
        tls_config: RustlsConfig,
        gateway_state: GatewayState,
        task_queue: DupQueue<Task>,
        metrics: Metrics,
    ) -> Result<Self> {
        let addr_str = format!("{}:{}", config.network.bind_ip, config.http.port);
        let addr: SocketAddr = addr_str
            .parse()
            .map_err(|e| anyhow!("Invalid listen address {}: {}", addr_str, e))?;

        let subnet_state = SubnetState::new(
            config.http.wss_bittensor.clone(),
            config.http.subnet_number,
            None,
            Duration::from_secs(config.http.subnet_poll_interval_sec),
            config.http.wss_max_message_size,
        );

        let mut whitelist_ips: HashSet<IpAddr> = HashSet::new();
        let mut domains: Vec<&str> = Vec::new();
        for entry in &config.http.add_task_whitelist {
            if let Ok(ip) = entry.parse::<IpAddr>() {
                whitelist_ips.insert(ip);
            } else {
                domains.push(entry.as_str());
            }
        }
        if !domains.is_empty() {
            match lookup_hosts_ips(&domains).await {
                Ok(resolved) => {
                    for ip in resolved {
                        whitelist_ips.insert(ip);
                    }
                }
                Err(e) => {
                    tracing::warn!("Failed to resolve some add_task_whitelist domains: {:?}", e);
                }
            }
        }
        let add_task_whitelist = AddTaskWhitelist {
            ips: Arc::new(whitelist_ips),
        };

        let router = Self::setup_router(
            config,
            &subnet_state,
            &gateway_state,
            &task_queue,
            &metrics,
            add_task_whitelist,
        )?;

        let service = Service::new(router).catcher(Catcher::default().hoop(custom_response));

        const TLS13_ONLY: &[&SupportedProtocolVersion] = &[&rustls::version::TLS13];
        let tls_config = tls_config.tls_versions(TLS13_ONLY);

        let join_handle = tokio::spawn(async move {
            let tcp_listener = TcpListener::new(addr).rustls(tls_config.clone());
            let acceptor = QuinnListener::new(tls_config, addr)
                .join(tcp_listener)
                .bind()
                .await;
            Server::new(acceptor).serve(service).await;
        });

        Ok(Self {
            join_handle,
            subnet_state,
        })
    }

    fn setup_router(
        config: Arc<NodeConfig>,
        subnet_state: &SubnetState,
        gateway_state: &GatewayState,
        task_queue: &DupQueue<Task>,
        metrics: &Metrics,
        add_task_whitelist: AddTaskWhitelist,
    ) -> Result<Router> {
        let http_config = &config.http;
        let prompt_config = &config.prompt;
        let image_config = &config.image;

        let prompt_regex = Regex::new(&prompt_config.allowed_pattern)
            .map_err(|e| anyhow!("Invalid prompt regex: {}", e))?;

        let rate_limits = RateLimits::new(http_config);

        let request_size_limit = http_config.request_size_limit as usize;
        let size_limit_handler = || SecureMaxSize(request_size_limit);
        let add_task_size_limit = http_config.add_task_size_limit as usize;
        let add_task_size_limit_handler = || SecureMaxSize(add_task_size_limit);
        let request_file_size_limit = http_config.request_file_size_limit as usize;
        let file_size_limit_handler = || SecureMaxSize(request_file_size_limit);
        let raft_write_size_limit = http_config.raft_write_size_limit as usize;
        let raft_write_size_limit_handler = || SecureMaxSize(raft_write_size_limit);

        let company_store =
            CompanyRateLimiterStore::new(http_config.company_rate_limiter_max_capacity);

        let router = if http_config.compression {
            Router::new().hoop(
                Compression::new()
                    .force_priority(true)
                    .enable_zstd(CompressionLevel::Precise(http_config.compression_lvl)),
            )
        } else {
            Router::new()
        };

        Ok(router
            .hoop(affix_state::inject(task_queue.clone()))
            .hoop(affix_state::inject(gateway_state.clone()))
            .hoop(affix_state::inject(subnet_state.clone()))
            .hoop(affix_state::inject(http_config.clone()))
            .hoop(affix_state::inject(add_task_whitelist))
            .hoop(affix_state::inject(config.network.node_id as usize))
            .hoop(affix_state::inject(metrics.clone()))
            .hoop(affix_state::inject(company_store))
            .hoop(affix_state::inject(prompt_config.clone()))
            .hoop(affix_state::inject(image_config.clone()))
            .hoop(affix_state::inject(prompt_regex))
            .hoop(prepare_rate_limit_context)
            .push(
                Router::with_path("/add_task")
                    .hoop(add_task_size_limit_handler())
                    .hoop(rate_limits.unauthorized_only_limiter)
                    .hoop(api_or_generic_key_check)
                    .hoop(rate_limits.generic_global_limiter)
                    .hoop(rate_limits.generic_per_ip_limiter)
                    .hoop(rate_limits.user_id_global_limiter)
                    .hoop(rate_limits.user_id_per_user_limiter)
                    .hoop(company_rate_limit)
                    .post(add_task_handler),
            )
            .push(
                Router::with_path("/add_result")
                    .hoop(file_size_limit_handler())
                    .hoop(rate_limits.result_limiter)
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
                    .hoop(rate_limits.load_limiter)
                    .get(get_load_handler),
            )
            .push(
                Router::with_path("/get_leader")
                    .hoop(size_limit_handler())
                    .hoop(rate_limits.leader_limiter)
                    .get(get_leader_handler),
            )
            .push(
                Router::with_path("/write")
                    .hoop(raft_write_size_limit_handler())
                    .hoop(admin_key_check)
                    .hoop(rate_limits.write_limiter)
                    .post(write_handler),
            )
            .push(
                Router::with_path("/get_version")
                    .hoop(size_limit_handler())
                    .hoop(rate_limits.basic_limiter)
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
                    .hoop(rate_limits.status_limiter)
                    .hoop(api_or_generic_key_check)
                    .get(get_status_handler),
            )
            .push(
                Router::with_path("/update_key")
                    .hoop(size_limit_handler())
                    .hoop(rate_limits.update_limiter)
                    .hoop(admin_key_check)
                    .post(generic_key_update_handler),
            )
            .push(
                Router::with_path("/metrics")
                    .hoop(size_limit_handler())
                    .hoop(rate_limits.metric_limiter)
                    .get(metrics_handler),
            )
            .push(
                Router::with_path("/get_key")
                    .hoop(size_limit_handler())
                    .hoop(rate_limits.read_limiter)
                    .hoop(admin_key_check)
                    .get(generic_key_read_handler),
            ))
    }

    pub fn abort(&self) {
        self.join_handle.abort();
        self.subnet_state.abort();
    }
}
