use anyhow::{Result, anyhow};
use rustls::SupportedProtocolVersion;
use salvo::catcher::Catcher;
use salvo::conn::quinn::QuinnListener;
use salvo::conn::rustls::RustlsConfig;
use salvo::http::request::SecureMaxSize;
use salvo::prelude::*;
use std::net::SocketAddr;
use std::sync::Arc;

use crate::api::Task;
use crate::common::queue::DupQueue;
use crate::config_runtime::RuntimeConfigStore;
use crate::http3::depot_ext::DepotExt;
use crate::http3::error::ServerError;
use crate::http3::handlers::admin::{
    admin_key_check, cluster_check, generic_key_read_handler, generic_key_update_handler,
};
use crate::http3::handlers::core::{
    api_or_generic_key_check, get_leader_handler, id_handler, metrics_handler, version_handler,
    write_handler,
};
use crate::http3::handlers::result::{add_result_handler, get_result_handler, get_status_handler};
use crate::http3::handlers::task::{add_task_handler, get_load_handler, get_tasks_handler};
use crate::http3::rate_limits::{
    basic_rate_limit, leader_rate_limit, load_rate_limit, metric_rate_limit,
    prepare_rate_limit_context, read_rate_limit, result_rate_limit, status_rate_limit,
    unauthorized_only_rate_limit, update_key_rate_limit,
};
use crate::http3::response::custom_response;
use crate::http3::state::{HttpState, HttpStateInit};
use crate::metrics::Metrics;
use crate::raft::gateway_state::GatewayState;
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

#[handler]
async fn request_size_limit(
    depot: &mut Depot,
    req: &mut Request,
    res: &mut Response,
    ctrl: &mut FlowCtrl,
) -> Result<(), ServerError> {
    let state = depot.require::<HttpState>()?.clone();
    let cfg = state.config();
    SecureMaxSize(cfg.http().request_size_limit as usize)
        .handle(req, depot, res, ctrl)
        .await;
    Ok(())
}

#[handler]
async fn add_task_size_limit(
    depot: &mut Depot,
    req: &mut Request,
    res: &mut Response,
    ctrl: &mut FlowCtrl,
) -> Result<(), ServerError> {
    let state = depot.require::<HttpState>()?.clone();
    let cfg = state.config();
    SecureMaxSize(cfg.http().add_task_size_limit as usize)
        .handle(req, depot, res, ctrl)
        .await;
    Ok(())
}

#[handler]
async fn request_file_size_limit(
    depot: &mut Depot,
    req: &mut Request,
    res: &mut Response,
    ctrl: &mut FlowCtrl,
) -> Result<(), ServerError> {
    let state = depot.require::<HttpState>()?.clone();
    let cfg = state.config();
    SecureMaxSize(cfg.http().request_file_size_limit as usize)
        .handle(req, depot, res, ctrl)
        .await;
    Ok(())
}

#[handler]
async fn raft_write_size_limit(
    depot: &mut Depot,
    req: &mut Request,
    res: &mut Response,
    ctrl: &mut FlowCtrl,
) -> Result<(), ServerError> {
    let state = depot.require::<HttpState>()?.clone();
    let cfg = state.config();
    SecureMaxSize(cfg.http().raft_write_size_limit as usize)
        .handle(req, depot, res, ctrl)
        .await;
    Ok(())
}

pub struct Http3Server {
    join_handle: tokio::task::JoinHandle<()>,
}

impl Http3Server {
    pub async fn run(
        config: Arc<RuntimeConfigStore>,
        tls_config: RustlsConfig,
        gateway_state: GatewayState,
        task_queue: DupQueue<Task>,
        metrics: Metrics,
        shutdown: CancellationToken,
    ) -> Result<Self> {
        let cfg = config.snapshot();
        let node_cfg = cfg.node();

        let addr_str = format!("{}:{}", node_cfg.network.bind_ip, node_cfg.http.port);
        let addr: SocketAddr = addr_str
            .parse()
            .map_err(|e| anyhow!("Invalid listen address {}: {}", addr_str, e))?;

        let state = HttpState::new(HttpStateInit {
            config: config.clone(),
            gateway_state: gateway_state.clone(),
            task_queue: task_queue.clone(),
            metrics: metrics.clone(),
        });

        let router = Self::setup_router(state)?;

        let service = Service::new(router).catcher(Catcher::default().hoop(custom_response));

        // QUIC requires TLS1.3 per spec.
        const TLS13_ONLY: &[&SupportedProtocolVersion] = &[&rustls::version::TLS13];

        // Build TCP-specific TLS versions from config (defaults to 1.2,1.3) using static slices.
        let tcp_versions_static = map_tls_versions_to_static(&node_cfg.http.tls_versions);
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

    fn setup_router(state: HttpState) -> Result<Router> {
        let cfg = state.config();
        let http_cfg = cfg.http();
        let compression = http_cfg.compression;
        let compression_lvl = http_cfg.compression_lvl;

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
                    .hoop(add_task_size_limit)
                    // Order matters:
                    // 1) cheap per-IP limiter (all requests, pre-parse)
                    // 2) unauthorized per-IP limiter
                    // 3) auth check
                    // 4) distributed subject limiter in handler after validation
                    .hoop(basic_rate_limit)
                    .hoop(unauthorized_only_rate_limit)
                    .hoop(api_or_generic_key_check)
                    .post(add_task_handler),
            )
            .push(
                Router::with_path("/add_result")
                    .hoop(request_file_size_limit)
                    .hoop(result_rate_limit)
                    .post(add_result_handler),
            )
            .push(
                Router::with_path("/get_tasks")
                    .hoop(request_size_limit)
                    .post(get_tasks_handler),
            )
            .push(
                Router::with_path("/get_load")
                    .hoop(request_size_limit)
                    .hoop(load_rate_limit)
                    .get(get_load_handler),
            )
            .push(
                Router::with_path("/get_leader")
                    .hoop(request_size_limit)
                    .hoop(leader_rate_limit)
                    .get(get_leader_handler),
            )
            .push(
                Router::with_path("/write")
                    .hoop(raft_write_size_limit)
                    .hoop(cluster_check)
                    .hoop(admin_key_check)
                    .post(write_handler),
            )
            .push(
                Router::with_path("/get_version")
                    .hoop(request_size_limit)
                    .hoop(basic_rate_limit)
                    .get(version_handler),
            )
            .push(
                Router::with_path("/id")
                    .hoop(request_size_limit)
                    .get(id_handler),
            )
            .push(
                Router::with_path("/get_result")
                    .hoop(request_size_limit)
                    .hoop(api_or_generic_key_check)
                    .get(get_result_handler),
            )
            .push(
                Router::with_path("/get_status")
                    .hoop(request_size_limit)
                    .hoop(status_rate_limit)
                    .hoop(api_or_generic_key_check)
                    .get(get_status_handler),
            )
            .push(
                Router::with_path("/update_key")
                    .hoop(request_size_limit)
                    .hoop(update_key_rate_limit)
                    .hoop(admin_key_check)
                    .post(generic_key_update_handler),
            )
            .push(
                Router::with_path("/metrics")
                    .hoop(request_size_limit)
                    .hoop(metric_rate_limit)
                    .get(metrics_handler),
            )
            .push(
                Router::with_path("/get_key")
                    .hoop(request_size_limit)
                    .hoop(read_rate_limit)
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
