// TODO: Remove this after the code is finalized
#![allow(dead_code)]

use std::net::SocketAddr;
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

use crate::api::request::{AddTaskRequest, GetTasksRequest};
use crate::api::response::{GatewayInfo, GetTasksResponse, LeaderResponse, LoadResponse};
use crate::api::Task;
use crate::bittensor::crypto::verify_hotkey;
use crate::bittensor::subnet_state::SubnetState;
use crate::common::queue::DupQueue;
use crate::config::HTTPConfig;
use crate::raft::gateway_state::GatewayState;

use super::error::ServerError;

const SUBNET_404GEN: u16 = 17;
const SUBNET_POLL_INTERVAL_SEC: u64 = 3600;

pub type H3RateLimiter = RateLimiter<
    FixedGuard,
    MokaStore<<RemoteIpIssuer as RateIssuer>::Key, FixedGuard>,
    RemoteIpIssuer,
    BasicQuota,
>;

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

    let queue = depot.obtain::<Arc<DupQueue<Task>>>().map_err(|err_opt| {
        if let Some(err) = err_opt {
            error!("Failed to obtain the task queue: {:?}", err);
            ServerError::Internal(format!("Failed to obtain the task queue: {:?}", err))
        } else {
            error!("Failed to obtain the task queue: unknown error");
            ServerError::Internal("Failed to obtain the task queue: unknown error".into())
        }
    })?;

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

    let tasks = queue.pop(get_tasks.requested_task_count, &get_tasks.hotkey);
    let membership = gateway_state.membership().await;
    let gateways = gateway_state
        .gateways(membership)
        .await
        .map_err(|e| ServerError::Internal(format!("Failed to obtain gateways: {:?}", e)))?;

    let response = GetTasksResponse { tasks, gateways };
    res.render(Json(response));
    Ok(())
}

// curl --http3 -X GET -k https://gateway.404.xyz:4443/load
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
    let membership = gateway_state.membership().await;
    let gateways = gateway_state.gateways(membership).await.unwrap_or_default();

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
        .parse_json::<GatewayInfo>()
        .await
        .map_err(|e| ServerError::BadRequest(e.to_string()))?;

    let gateway_state = depot.obtain::<GatewayState>().map_err(|e| {
        error!("Failed to obtain GatewayState: {:?}", e);
        ServerError::Internal(format!("Failed to obtain GatewayState: {:?}", e))
    })?;

    gateway_state
        .set_gateway_info(gi)
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

        let subnet_state = Arc::new(SubnetState::new(
            http_config.bittensor_wss.clone(),
            SUBNET_404GEN,
            None,
            Duration::from_secs(SUBNET_POLL_INTERVAL_SEC),
        ));

        let router = Self::setup_router(
            task_queue,
            subnet_state.clone(),
            gateway_state,
            task_limiter,
            load_limiter,
            leader_limiter,
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
        task_limiter: H3RateLimiter,
        load_limiter: H3RateLimiter,
        leader_limiter: H3RateLimiter,
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
                    .hoop(task_limiter),
            )
            .push(Router::with_path("/get_tasks").post(get_tasks_handler))
            .push(
                Router::with_path("/get_load")
                    .get(get_load_handler)
                    .hoop(load_limiter),
            )
            .push(
                Router::with_path("/get_leader")
                    .get(get_leader_handler)
                    .hoop(leader_limiter),
            )
            .push(Router::with_path("/write").post(write_handler))
    }

    pub fn abort(&self) {
        self.join_handle.abort();
        self.subnet_state.abort();
    }
}
