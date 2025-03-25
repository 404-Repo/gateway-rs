use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use salvo::conn::quinn::QuinnListener;
use salvo::conn::rustls::RustlsConfig;
use salvo::http::request::SecureMaxSize;
use salvo::prelude::*;
use salvo::rate_limiter::{
    BasicQuota, FixedGuard, MokaStore, RateIssuer, RateLimiter, RemoteIpIssuer,
};
use tracing::{error, info};
use uuid::Uuid;

use crate::api::request::{AddTaskRequest, GatewayInfoExt, GetTasksRequest};
use crate::api::response::{GetTasksResponse, LeaderResponse, LoadResponse};
use crate::api::Task;
use crate::bittensor::crypto::verify_hotkey;
use crate::bittensor::subnet_state::SubnetState;
use crate::common::log::get_build_information;
use crate::common::queue::DupQueue;
use crate::config::HTTPConfig;
use crate::raft::gateway_state::GatewayState;

use super::error::ServerError;

pub type H3RateLimiter = RateLimiter<
    FixedGuard,
    MokaStore<<RemoteIpIssuer as RateIssuer>::Key, FixedGuard>,
    RemoteIpIssuer,
    BasicQuota,
>;

struct RateLimits {
    task_limiter: H3RateLimiter,
    load_limiter: H3RateLimiter,
    leader_limiter: H3RateLimiter,
}

// curl --http3 -X POST -H "Content-Type: application/json" -d '{"api_key": "XXXX", "prompt": "mechanic robot"}' -k https://gateway.404.xyz:4443/add_task
#[handler]
async fn add_task_handler(
    depot: &mut Depot,
    req: &mut Request,
    res: &mut Response,
) -> Result<(), ServerError> {
    let add_task = req
        .parse_json::<AddTaskRequest>()
        .await
        .map_err(|e| ServerError::BadRequest(e.to_string()))?;

    let task = Task {
        id: Uuid::new_v4(),
        prompt: add_task.prompt,
    };

    let gateway_state = depot.obtain::<GatewayState>().map_err(|e| {
        error!("Failed to obtain the state reader: {:?}", e);
        ServerError::Internal(format!("Failed to obtain the state reader: {:?}", e))
    })?;

    let api_key = match Uuid::from_str(&add_task.api_key) {
        Ok(uuid) => uuid,
        Err(e) => {
            error!("Failed to parse UUID: {:?}", e);
            return Err(ServerError::Internal(format!(
                "Failed to parse UUID: {:?}",
                e
            )));
        }
    };

    if !gateway_state.is_valid_api_key(&api_key) {
        error!("API key is not allowed");
        return Err(ServerError::Internal("API key is not allowed".to_string()));
    }

    let queue = depot.obtain::<Arc<DupQueue<Task>>>().map_err(|err_opt| {
        if let Some(err) = err_opt {
            error!("Failed to obtain the task queue: {:?}", err);
            ServerError::Internal(format!("Failed to obtain the task queue: {:?}", err))
        } else {
            error!("Failed to obtain the task queue: unknown error");
            ServerError::Internal("Failed to obtain the task queue: unknown error".into())
        }
    })?;

    let http_cfg = depot.obtain::<HTTPConfig>().map_err(|e| {
        error!("Failed to obtain the HTTPConfig: {:?}", e);
        ServerError::Internal(format!("Failed to obtain the HTTPConfig: {:?}", e))
    })?;

    if queue.len() >= http_cfg.max_task_queue_len {
        error!("Task queue is full: {} tasks", queue.len());
        return Err(ServerError::Internal("Task queue is full".to_string()));
    }

    queue.push(task.clone());
    info!("A new task has been pushed with ID: {}", task.id);
    res.render(Json(task));
    Ok(())
}

#[handler]
async fn get_tasks_handler(
    depot: &mut Depot,
    req: &mut Request,
    res: &mut Response,
) -> Result<(), ServerError> {
    let get_tasks = req
        .parse_json::<GetTasksRequest>()
        .await
        .map_err(|e| ServerError::BadRequest(e.to_string()))?;

    let http_cfg = depot.obtain::<HTTPConfig>().map_err(|e| {
        error!("Failed to obtain the HTTPConfig: {:?}", e);
        ServerError::Internal(format!("Failed to obtain the HTTPConfig: {:?}", e))
    })?;

    let subnet_state = depot.obtain::<Arc<SubnetState>>().map_err(|e| {
        error!("Failed to obtain the SubnetState: {:?}", e);
        ServerError::Internal(format!("Failed to obtain the SubnetState: {:?}", e))
    })?;

    subnet_state
        .validate_hotkey(&get_tasks.hotkey)
        .map_err(|e| {
            error!(
                "Hotkey {} is not registered in the subnet: {:?}",
                get_tasks.hotkey, e
            );
            ServerError::Internal(format!("Hotkey is not registered in the subnet: {:?}", e))
        })?;

    verify_hotkey(&get_tasks, http_cfg.signature_freshness_threshold).map_err(|e| {
        error!("Failed to verify GetTasksRequest: {:?}", e);
        ServerError::Internal(format!("Failed to verify GetTasksRequest: {:?}", e))
    })?;

    let gateway_state = depot.obtain::<GatewayState>().map_err(|e| {
        error!("Failed to obtain the state reader: {:?}", e);
        ServerError::Internal(format!("Failed to obtain the state reader: {:?}", e))
    })?;

    let queue = depot.obtain::<Arc<DupQueue<Task>>>().map_err(|err_opt| {
        if let Some(err) = err_opt {
            error!("Failed to obtain the task queue: {:?}", err);
            ServerError::Internal(format!("Failed to obtain the task queue: {:?}", err))
        } else {
            error!("Failed to obtain the task queue: unknown error");
            ServerError::Internal("Failed to obtain the task queue: unknown error".into())
        }
    })?;

    gateway_state.update_task_acquisition().map_err(|e| {
        ServerError::Internal(format!(
            "Failed to execute update_task_acquisition: {:?}",
            e
        ))
    })?;

    let gateways = gateway_state
        .gateways()
        .await
        .map_err(|e| ServerError::Internal(format!("Failed to obtain gateways: {:?}", e)))?;

    let tasks = queue.pop(get_tasks.requested_task_count, &get_tasks.hotkey);

    let response = GetTasksResponse { tasks, gateways };
    res.render(Json(response));
    Ok(())
}

// curl --http3 -X GET -k https://gateway.404.xyz:4443/get_load
#[handler]
async fn get_load_handler(
    depot: &mut Depot,
    _req: &mut Request,
    res: &mut Response,
) -> Result<(), ServerError> {
    let gateway_state = depot.obtain::<GatewayState>().map_err(|e| {
        error!("Failed to obtain GatewayState: {:?}", e);
        ServerError::Internal(format!("Failed to obtain GatewayState: {:?}", e))
    })?;

    let gateways = gateway_state.gateways().await.map_err(|e| {
        error!("Failed to obtain the gateways for get_load: {:?}", e);
        ServerError::Internal(format!(
            "Failed to obtain the gateways for get_load: {:?}",
            e
        ))
    })?;

    let load_response = LoadResponse { gateways };
    res.render(Json(load_response));
    Ok(())
}

#[handler]
async fn write_handler(
    depot: &mut Depot,
    req: &mut Request,
    res: &mut Response,
) -> Result<(), ServerError> {
    let gi = req
        .parse_json::<GatewayInfoExt>()
        .await
        .map_err(|e| ServerError::BadRequest(e.to_string()))?;

    let gateway_state = depot.obtain::<GatewayState>().map_err(|e| {
        error!("Failed to obtain GatewayState: {:?}", e);
        ServerError::Internal(format!("Failed to obtain GatewayState: {:?}", e))
    })?;

    if gi.cluster_name != gateway_state.cluster_name() {
        error!(
            "Unauthorized access to write handler from: {}",
            req.remote_addr().to_string()
        );
        return Err(ServerError::Unauthorized("Unauthorized access".to_string()));
    }

    gateway_state
        .set_gateway_info(gi.into())
        .await
        .map_err(|e| ServerError::Internal(format!("Failed to set gateway info: {:?}", e)))?;
    res.status_code(StatusCode::OK);
    res.render(Text::Plain("Ok"));
    Ok(())
}

#[handler]
async fn get_leader_handler(
    depot: &mut Depot,
    _req: &mut Request,
    res: &mut Response,
) -> Result<(), ServerError> {
    let gateway_state = depot.obtain::<GatewayState>().map_err(|e| {
        error!("Failed to obtain GatewayState: {:?}", e);
        ServerError::Internal(format!("Failed to obtain GatewayState: {:?}", e))
    })?;
    let leader_id = match gateway_state.leader().await {
        Some(id) => id,
        None => {
            error!("The leader is not elected");
            return Err(ServerError::Internal("The leader is not elected".into()));
        }
    };
    let gateway_info = gateway_state.gateway(leader_id).await.map_err(|e| {
        error!("Failed to obtain GatewayState: {:?}", e);
        ServerError::Internal(format!("Failed to obtain GatewayState: {:?}", e))
    })?;

    let response = LeaderResponse {
        leader_id,
        domain: gateway_info.domain,
        ip: gateway_info.ip,
        http_port: gateway_info.http_port,
    };
    res.render(Json(response));
    Ok(())
}

#[handler]
async fn version_handler(_depot: &mut Depot, _req: &mut Request, res: &mut Response) {
    let build_info = get_build_information(true);
    res.render(Text::Html(build_info));
}

pub struct Http3Server {
    join_handle: tokio::task::JoinHandle<()>,
    subnet_state: Arc<SubnetState>,
}

impl Http3Server {
    pub async fn run(
        addr: SocketAddr,
        tls_config: RustlsConfig,
        http_config: &HTTPConfig,
        gateway_state: GatewayState,
        task_queue: Arc<DupQueue<Task>>,
    ) -> Self {
        let load_limiter = Self::create_rate_limiter(http_config.load_rate_limit);
        let task_limiter = Self::create_rate_limiter(http_config.add_task_rate_limit);
        let leader_limiter = Self::create_rate_limiter(http_config.leader_rate_limit);

        let rate_limits = RateLimits {
            load_limiter,
            task_limiter,
            leader_limiter,
        };

        let subnet_state = Arc::new(SubnetState::new(
            http_config.wss_bittensor.clone(),
            http_config.subnet_number,
            None,
            Duration::from_secs(http_config.subnet_poll_interval_sec),
            http_config.wss_max_message_size,
        ));

        let router = Self::setup_router(
            task_queue,
            subnet_state.clone(),
            gateway_state,
            rate_limits,
            http_config.clone(),
        );

        let join_handle = tokio::spawn(async move {
            let tcp_listener = TcpListener::new(addr).rustls(tls_config.clone());
            let acceptor = QuinnListener::new(tls_config, addr)
                .join(tcp_listener)
                .bind()
                .await;
            Server::new(acceptor).serve(router).await;
        });

        Self {
            join_handle,
            subnet_state,
        }
    }

    fn create_rate_limiter(
        quota: usize,
    ) -> RateLimiter<
        FixedGuard,
        MokaStore<<RemoteIpIssuer as RateIssuer>::Key, FixedGuard>,
        RemoteIpIssuer,
        BasicQuota,
    > {
        RateLimiter::new(
            FixedGuard::new(),
            MokaStore::<<RemoteIpIssuer as RateIssuer>::Key, FixedGuard>::new(),
            RemoteIpIssuer,
            BasicQuota::per_minute(quota),
        )
    }

    fn setup_router(
        task_queue: Arc<DupQueue<Task>>,
        subnet_state: Arc<SubnetState>,
        gateway_state: GatewayState,
        rate_limits: RateLimits,
        config: HTTPConfig,
    ) -> Router {
        let size_limit_handler = SecureMaxSize(config.request_size_limit as usize);

        let router = if config.compression {
            Router::new().hoop(
                Compression::new()
                    .force_priority(true)
                    .enable_zstd(CompressionLevel::Precise(config.compression_lvl)),
            )
        } else {
            Router::new()
        };

        router
            .hoop(size_limit_handler)
            .hoop(affix_state::inject(task_queue))
            .hoop(affix_state::inject(gateway_state))
            .hoop(affix_state::inject(subnet_state))
            .hoop(affix_state::inject(config))
            .push(
                Router::with_path("/add_task")
                    .post(add_task_handler)
                    .hoop(rate_limits.task_limiter),
            )
            .push(Router::with_path("/get_tasks").post(get_tasks_handler))
            .push(
                Router::with_path("/get_load")
                    .get(get_load_handler)
                    .hoop(rate_limits.load_limiter),
            )
            .push(
                Router::with_path("/get_leader")
                    .get(get_leader_handler)
                    .hoop(rate_limits.leader_limiter),
            )
            .push(Router::with_path("/write").post(write_handler))
            .push(Router::with_path("/get_version").get(version_handler))
    }

    pub fn abort(&self) {
        self.join_handle.abort();
        self.subnet_state.abort();
    }
}
