// TODO: Remove this after the code is finalized
#![allow(dead_code)]

use std::net::SocketAddr;
use std::sync::Arc;

use salvo::conn::quinn::QuinnListener;
use salvo::conn::rustls::RustlsConfig;
use salvo::http::Method;
use salvo::prelude::*;
use salvo::rate_limiter::{BasicQuota, FixedGuard, MokaStore, RateLimiter, RemoteIpIssuer};
use salvo::size_limiter::MaxSize;
use tracing::{error, info};
use uuid::Uuid;

use crate::api::request::{AddTaskRequest, GetTasksRequest};
use crate::api::response::{GetTasksResponse, LoadResponse};
use crate::api::Task;
use crate::common::queue::DupQueue;
use crate::config::HTTPConfig;

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
async fn load_handler(req: &mut Request, res: &mut Response) {
    if req.method() == Method::GET {
        let load_response = LoadResponse {
            gateways: Vec::new(),
        };
        res.render(Json(load_response));
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
        task_queue: Arc<DupQueue<Task>>,
    ) -> Self {
        let load_limiter = RateLimiter::new(
            FixedGuard::new(),
            MokaStore::new(),
            RemoteIpIssuer,
            BasicQuota::per_minute(http_config.load_rate_limit),
        );

        let task_limiter = RateLimiter::new(
            FixedGuard::new(),
            MokaStore::new(),
            RemoteIpIssuer,
            BasicQuota::per_minute(http_config.add_task_rate_limit),
        );

        let size_limit_handler = MaxSize(http_config.request_size_limit);

        let router = Router::new()
            .hoop(size_limit_handler)
            .hoop(affix_state::inject(task_queue))
            .push(
                Router::with_path("/add_task")
                    .post(add_task_handler)
                    .hoop(task_limiter),
            )
            .push(Router::with_path("/get_tasks").post(get_tasks_handler))
            .push(
                Router::with_path("/load")
                    .get(load_handler)
                    .hoop(load_limiter),
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

    pub async fn stop(&self) {
        self.join_handle.abort();
    }
}
