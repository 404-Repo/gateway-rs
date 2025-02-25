// TODO: Remove this after the code is finalized
#![allow(dead_code)]

use std::net::SocketAddr;
use std::sync::Arc;

use salvo::conn::quinn::QuinnListener;
use salvo::conn::rustls::RustlsConfig;
use salvo::http::request::SecureMaxSize;
use salvo::http::Method;
use salvo::prelude::*;
use salvo::rate_limiter::{
    BasicQuota, FixedGuard, MokaStore, RateIssuer, RateLimiter, RemoteIpIssuer,
};
use tracing::{error, info};
use uuid::Uuid;

use crate::api::request::{AddTaskRequest, GetTasksRequest};
use crate::api::response::{GatewayInfo, GetTasksResponse, LeaderResponse, LoadResponse};
use crate::api::Task;
use crate::common::queue::DupQueue;
use crate::config::HTTPConfig;
use crate::raft::gateway_state::GatewayState;

pub type H3RateLimiter = RateLimiter<
    FixedGuard,
    MokaStore<<RemoteIpIssuer as RateIssuer>::Key, FixedGuard>,
    RemoteIpIssuer,
    BasicQuota,
>;

// curl --http3 -X POST -H "Content-Type: application/json" -d '{"prompt": "mechanic robot"}' -k https://gateway.404.xyz:4443/add_task
#[handler]
async fn add_task_handler(depot: &mut Depot, req: &mut Request, res: &mut Response) {
    if req.method() != Method::POST {
        res.status_code(StatusCode::METHOD_NOT_ALLOWED);
        res.render(Text::Plain("Method Not Allowed"));
        return;
    }

    let add_task = match req.parse_json::<AddTaskRequest>().await {
        Ok(task) => task,
        Err(err) => {
            res.status_code(StatusCode::BAD_REQUEST);
            res.render(Text::Plain(format!("Bad Request: {}", err)));
            return;
        }
    };

    let task = Task {
        id: Uuid::new_v4(),
        prompt: add_task.prompt,
    };

    let queue = match depot.obtain::<Arc<DupQueue<Task>>>() {
        Ok(queue) => queue,
        Err(err_opt) => {
            if let Some(err) = err_opt {
                error!("Failed to obtain the task queue: {:?}", err);
            } else {
                error!("Failed to obtain the task queue: unknown error");
            }
            res.status_code(StatusCode::INTERNAL_SERVER_ERROR);
            res.render(Text::Plain(
                "Internal Server Error: Task queue unavailable.",
            ));
            return;
        }
    };

    queue.push(task.clone());
    info!("A new task has been pushed with ID: {}", task.id);
    res.render(Json(task));
}

#[handler]
async fn get_tasks_handler(depot: &mut Depot, req: &mut Request, res: &mut Response) {
    if req.method() != Method::POST {
        res.status_code(StatusCode::METHOD_NOT_ALLOWED);
        res.render(Text::Plain("Method Not Allowed"));
        return;
    }

    let get_tasks = match req.parse_json::<GetTasksRequest>().await {
        Ok(gt) => gt,
        Err(err) => {
            res.status_code(StatusCode::BAD_REQUEST);
            res.render(Text::Plain(format!("Bad Request: {}", err)));
            return;
        }
    };

    // TODO: verify hotkey
    let queue = match depot.obtain::<Arc<DupQueue<Task>>>() {
        Ok(queue) => queue,
        Err(err_opt) => {
            if let Some(err) = err_opt {
                error!("Failed to obtain the task queue: {:?}", err);
            } else {
                error!("Failed to obtain the task queue: unknown error");
            }
            res.status_code(StatusCode::INTERNAL_SERVER_ERROR);
            res.render(Text::Plain(
                "Internal Server Error: Task queue unavailable.",
            ));
            return;
        }
    };

    let tasks = queue.pop(get_tasks.requested_task_count, &get_tasks.hotkey);
    // TODO: obtain and use gateway info
    let response = GetTasksResponse {
        tasks,
        gateways: vec![],
    };
    res.render(Json(response));
}

// curl --http3 -X GET -k https://gateway.404.xyz:4443/load
#[handler]
async fn get_load_handler(depot: &mut Depot, req: &mut Request, res: &mut Response) {
    if req.method() == Method::GET {
        let gateway_state = match depot.obtain::<GatewayState>() {
            Ok(sr) => sr,
            Err(_) => {
                error!("Failed to obtain the raft");
                return;
            }
        };

        let membership = gateway_state.membership().await;
        let gateways = gateway_state.gateways(membership).await.unwrap_or_default();

        let load_response = LoadResponse { gateways };
        res.render(Json(load_response));
    } else {
        res.status_code(StatusCode::METHOD_NOT_ALLOWED);
        res.render(Text::Plain("Method Not Allowed"));
    }
}

#[handler]
async fn write_handler(depot: &mut Depot, req: &mut Request, res: &mut Response) {
    if req.method() != Method::POST {
        res.status_code(StatusCode::METHOD_NOT_ALLOWED);
        res.render(Text::Plain("Method Not Allowed"));
        return;
    }

    let gi = match req.parse_json::<GatewayInfo>().await {
        Ok(gt) => gt,
        Err(err) => {
            res.status_code(StatusCode::BAD_REQUEST);
            res.render(Text::Plain(format!("Bad Request: {}", err)));
            return;
        }
    };

    let gateway_state = match depot.obtain::<GatewayState>() {
        Ok(sr) => sr,
        Err(_) => {
            error!("Failed to obtain GatewayState");
            return;
        }
    };

    match gateway_state.set_gateway_info(gi).await {
        Ok(_) => {
            res.status_code(StatusCode::OK);
            res.render(Text::Plain("Ok"));
        }
        Err(e) => {
            res.status_code(StatusCode::INTERNAL_SERVER_ERROR);
            res.render(Text::Plain(format!("Error: {}", e)));
            return;
        }
    }
}

#[handler]
async fn get_leader_handler(depot: &mut Depot, req: &mut Request, res: &mut Response) {
    if req.method() == Method::GET {
        let gateway_state = match depot.obtain::<GatewayState>() {
            Ok(gt) => gt,
            Err(e) => {
                error!("Failed to obtain the state reader: {:?}", e);
                res.status_code(StatusCode::INTERNAL_SERVER_ERROR);
                res.render(Text::Plain("Internal Server Error"));
                return;
            }
        };

        let leader_id = gateway_state.leader().await.unwrap_or(0);

        match gateway_state.gateway(leader_id).await {
            Ok(gateway_info) => {
                let load_response = LeaderResponse {
                    leader_id,
                    domain: gateway_info.domain,
                    ip: gateway_info.ip,
                    http_port: gateway_info.http_port,
                };

                res.render(Json(load_response));
            }
            Err(e) => {
                error!("Failed to obtain gateway info: {:?}", e);
                res.status_code(StatusCode::INTERNAL_SERVER_ERROR);
                res.render(Text::Plain("Internal Server Error"));
            }
        }
    } else {
        res.status_code(StatusCode::METHOD_NOT_ALLOWED);
        res.render(Text::Plain("Method Not Allowed"));
    }
}

pub struct Http3Server {
    join_handle: tokio::task::JoinHandle<()>,
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

        let router = Self::setup_router(
            task_queue,
            gateway_state,
            task_limiter,
            load_limiter,
            leader_limiter,
            http_config,
        );

        let join_handle = tokio::spawn(async move {
            let tcp_listener = TcpListener::new(addr).rustls(tls_config.clone());
            let acceptor = QuinnListener::new(tls_config, addr)
                .join(tcp_listener)
                .bind()
                .await;
            Server::new(acceptor).serve(router).await;
        });

        Self { join_handle }
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
        gateway_state: GatewayState,
        task_limiter: H3RateLimiter,
        load_limiter: H3RateLimiter,
        leader_limiter: H3RateLimiter,
        config: &HTTPConfig,
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

    pub async fn stop(&self) {
        self.join_handle.abort();
    }
}
