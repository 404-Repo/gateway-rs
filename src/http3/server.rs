use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_zip::base::write::ZipFileWriter;
use async_zip::ZipEntryBuilder;
use futures::io::Cursor;
use http::HeaderValue;
use salvo::conn::quinn::QuinnListener;
use salvo::conn::rustls::RustlsConfig;
use salvo::http::request::SecureMaxSize;
use salvo::prelude::*;
use salvo::rate_limiter::{
    BasicQuota, FixedGuard, MokaStore, RateIssuer, RateLimiter, RemoteIpIssuer,
};
use tracing::{error, info};
use uuid::Uuid;

use crate::api::request::{
    AddTaskRequest, AddTaskResultRequest, GatewayInfoExt, GetTaskResultRequest, GetTaskStatus,
    GetTasksRequest, UpdateGenericKeyRequest,
};
use crate::api::response::{
    GenericKeyResponse, GetTaskStatusResponse, GetTasksResponse, LeaderResponse, LoadResponse,
};
use crate::api::Task;
use crate::bittensor::crypto::verify_hotkey;
use crate::bittensor::subnet_state::SubnetState;
use crate::common::log::get_build_information;
use crate::common::queue::DupQueue;
use crate::common::task::TaskResultScored;
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
    basic_limiter: H3RateLimiter,
    write_limiter: H3RateLimiter,
    read_limiter: H3RateLimiter,
    task_limiter: H3RateLimiter,
    load_limiter: H3RateLimiter,
    leader_limiter: H3RateLimiter,
}

// curl --http3 -X POST "https://gateway.404.xyz:4443" -H "Content-Type: application/json" -H "X-API-Key: 123e4567-e89b-12d3-a456-426614174000" -d '{"prompt": "mechanic robot"}'
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
async fn add_result_handler(
    depot: &mut Depot,
    req: &mut Request,
    res: &mut Response,
) -> Result<(), ServerError> {
    let add_result = req
        .parse_json::<AddTaskResultRequest>()
        .await
        .map_err(|e| ServerError::BadRequest(e.to_string()))?;

    let subnet_state = depot.obtain::<Arc<SubnetState>>().map_err(|e| {
        error!("Failed to obtain the SubnetState: {:?}", e);
        ServerError::Internal(format!("Failed to obtain the SubnetState: {:?}", e))
    })?;

    subnet_state
        .validate_hotkey(&add_result.validator_hotkey)
        .map_err(|e| {
            error!(
                "Hotkey {} is not registered in the subnet: {:?}",
                add_result.validator_hotkey, e
            );
            ServerError::Internal(format!("Hotkey is not registered in the subnet: {:?}", e))
        })?;

    let http_cfg = depot.obtain::<HTTPConfig>().map_err(|e| {
        error!("Failed to obtain the HTTPConfig: {:?}", e);
        ServerError::Internal(format!("Failed to obtain the HTTPConfig: {:?}", e))
    })?;

    verify_hotkey(
        &add_result.timestamp,
        &add_result.validator_hotkey,
        &add_result.signature,
        http_cfg.signature_freshness_threshold,
    )
    .map_err(|e| {
        error!("Failed to verify AddTaskRequest: {:?}", e);
        ServerError::Internal(format!("Failed to verify AddTaskRequest: {:?}", e))
    })?;

    let gateway_state = depot.obtain::<GatewayState>().map_err(|e| {
        error!("Failed to obtain the state reader: {:?}", e);
        ServerError::Internal(format!("Failed to obtain the state reader: {:?}", e))
    })?;

    let id = add_result.id;
    let result = TaskResultScored {
        validator_hotkey: add_result.validator_hotkey,
        miner_hotkey: add_result.miner_hotkey,
        asset: add_result.asset,
        score: add_result.score,
        instant: Instant::now(),
    };
    info!(
        "Received result for task {id} from validator {} and miner {}",
        result.validator_hotkey, result.miner_hotkey
    );

    gateway_state.task_manager().add_result(id, result).await;
    res.status_code(StatusCode::OK);
    res.render(Text::Plain("Ok"));
    Ok(())
}

#[handler]
async fn get_result_handler(
    depot: &mut Depot,
    req: &mut Request,
    res: &mut Response,
) -> Result<(), ServerError> {
    let get_task = req
        .parse_json::<GetTaskResultRequest>()
        .await
        .map_err(|e| ServerError::BadRequest(e.to_string()))?;

    let task_id = get_task.id;
    let all_results = get_task.all;

    let gateway_state = depot.obtain::<GatewayState>().map_err(|e| {
        error!("Failed to obtain the state reader: {:?}", e);
        ServerError::Internal(format!("Failed to obtain the state reader: {:?}", e))
    })?;

    let task_manager = gateway_state.task_manager();

    let results = task_manager.get_result(task_id).await;
    let results_vec =
        results.ok_or_else(|| ServerError::NotFound("Task ID not found".to_string()))?;

    if results_vec.is_empty() {
        return Err(ServerError::NotFound(
            "No results found for the task".to_string(),
        ));
    }

    if all_results {
        let mut buffer = Vec::new();
        let mut cursor = Cursor::new(&mut buffer);
        let mut zip_writer = ZipFileWriter::new(&mut cursor);

        for (i, file) in results_vec.into_iter().enumerate() {
            let filename = format!("{}.spz", i + 1);
            let entry =
                ZipEntryBuilder::new(filename.into(), async_zip::Compression::Stored).build();
            zip_writer
                .write_entry_whole(entry, &file.asset)
                .await
                .map_err(|e| {
                    ServerError::Internal(format!("Failed to write zip entry: {:?}", e))
                })?;
        }

        zip_writer
            .close()
            .await
            .map_err(|e| ServerError::Internal(format!("Failed to close zip archive: {:?}", e)))?;

        res.headers_mut()
            .insert("Content-Type", HeaderValue::from_static("application/zip"));
        res.headers_mut().insert(
            "Content-Disposition",
            HeaderValue::from_static("attachment; filename=\"results.zip\""),
        );
        res.body(buffer);
    } else {
        let best_result = results_vec
            .into_iter()
            .max_by(|a, b| {
                a.score
                    .partial_cmp(&b.score)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .ok_or_else(|| ServerError::Internal("Failed to find the best result".to_string()))?;

        res.headers_mut().insert(
            "Content-Type",
            HeaderValue::from_static("application/octet-stream"),
        );
        res.headers_mut().insert(
            "Content-Disposition",
            HeaderValue::from_static("attachment; filename=\"result.spz\""),
        );
        res.body(best_result.asset);
    }

    Ok(())
}

#[handler]
async fn get_status_handler(
    depot: &mut Depot,
    req: &mut Request,
    res: &mut Response,
) -> Result<(), ServerError> {
    let get_status = req
        .parse_json::<GetTaskStatus>()
        .await
        .map_err(|e| ServerError::BadRequest(e.to_string()))?;

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

    let status = gateway_state
        .task_manager()
        .get_status(get_status.id, queue.dup())
        .await;

    res.render(Json(GetTaskStatusResponse { status }));

    Ok(())
}

// curl --http3 -X POST https://gateway.404.xyz:4443/get_tasks \
//   -H "Content-Type: application/json" \
//   -d '{"validator_hotkey": "abc123", "signature": "somesignature", "timestamp": "2025-04-02T12:00:00Z", "requested_task_count": 10}'
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
        .validate_hotkey(&get_tasks.validator_hotkey)
        .map_err(|e| {
            error!(
                "Hotkey {} is not registered in the subnet: {:?}",
                get_tasks.validator_hotkey, e
            );
            ServerError::Internal(format!("Hotkey is not registered in the subnet: {:?}", e))
        })?;

    verify_hotkey(
        &get_tasks.timestamp,
        &get_tasks.validator_hotkey,
        &get_tasks.signature,
        http_cfg.signature_freshness_threshold,
    )
    .map_err(|e| {
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

    let tasks = queue.pop(get_tasks.requested_task_count, &get_tasks.validator_hotkey);

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

#[handler]
async fn id_handler(
    depot: &mut Depot,
    _req: &mut Request,
    res: &mut Response,
) -> Result<(), ServerError> {
    let node_id = depot.obtain::<usize>().map_err(|e| {
        error!("Failed to obtain node_id: {:?}", e);
        ServerError::Internal(format!("Failed to obtain node_id: {:?}", e))
    })?;

    res.render(node_id.to_string());
    Ok(())
}

#[handler]
async fn api_key_check(depot: &mut Depot, req: &mut Request, res: &mut Response) {
    if let Some(value) = req.headers().get("X-API-Key") {
        if let Ok(key_str) = value.to_str() {
            if let Ok(api_key) = Uuid::parse_str(key_str) {
                if let Ok(gateway_state) = depot.obtain::<GatewayState>() {
                    if gateway_state.is_valid_api_key(&api_key) {
                        return;
                    }
                } else {
                    res.status_code = Some(StatusCode::INTERNAL_SERVER_ERROR);
                    res.render("Internal error: Failed to obtain GatewayState");
                    return;
                }
            }
        }
    }
    res.status_code = Some(StatusCode::UNAUTHORIZED);
    res.render("Unauthorized: Invalid or missing API key");
}

#[handler]
async fn admin_key_check(depot: &mut Depot, req: &mut Request, res: &mut Response) {
    match depot.obtain::<HTTPConfig>() {
        Ok(http_cfg) => {
            if let Some(value) = req.headers().get("X-Admin-Key") {
                if let Ok(key_str) = value.to_str() {
                    if let Ok(key_uuid) = Uuid::parse_str(key_str) {
                        if key_uuid == http_cfg.admin_key {
                            return;
                        }
                    }
                }
            }
            res.status_code = Some(StatusCode::UNAUTHORIZED);
            res.render("Unauthorized: Invalid or missing admin key");
        }
        Err(_) => {
            res.status_code = Some(StatusCode::INTERNAL_SERVER_ERROR);
            res.render("Internal error: Failed to obtain configuration");
        }
    }
}

// curl --http3 -X GET "https://gateway.404.xyz:4443" -H "X-Admin-Key: b6c8597a-00e9-493a-b6cd-5dfc7244d46b"
#[handler]
async fn generic_key_update_handler(
    depot: &mut Depot,
    req: &mut Request,
    _res: &mut Response,
) -> Result<(), ServerError> {
    let ugk = req
        .parse_json::<UpdateGenericKeyRequest>()
        .await
        .map_err(|e| ServerError::BadRequest(e.to_string()))?;

    let gateway_state = depot.obtain::<GatewayState>().map_err(|e| {
        error!("Failed to obtain GatewayState: {:?}", e);
        ServerError::Internal(format!("Failed to obtain GatewayState: {:?}", e))
    })?;

    gateway_state
        .update_gateway_generic_key(Some(ugk.generic_key))
        .await
        .map_err(|e| {
            error!("Failed to update gateway generic key: {:?}", e);
            ServerError::Internal(format!("Failed to update gateway generic key: {:?}", e))
        })?;

    Ok(())
}

#[handler]
async fn generic_key_read_handler(
    depot: &mut Depot,
    _req: &mut Request,
    res: &mut Response,
) -> Result<(), ServerError> {
    let gateway_state = depot.obtain::<GatewayState>().map_err(|e| {
        error!("Failed to obtain GatewayState: {:?}", e);
        ServerError::Internal(format!("Failed to obtain GatewayState: {:?}", e))
    })?;

    if let Some(uuid) = gateway_state.generic_key().await {
        let response = GenericKeyResponse { generic_key: uuid };
        res.render(Json(response));
        Ok(())
    } else {
        error!("Generic key not found");
        Err(ServerError::Internal("Generic key not found".to_string()))
    }
}

pub struct Http3Server {
    join_handle: tokio::task::JoinHandle<()>,
    subnet_state: Arc<SubnetState>,
}

impl Http3Server {
    pub async fn run(
        node_id: usize,
        addr: SocketAddr,
        tls_config: RustlsConfig,
        http_config: &HTTPConfig,
        gateway_state: GatewayState,
        task_queue: Arc<DupQueue<Task>>,
    ) -> Self {
        let basic_limiter = Self::create_rate_limiter(http_config.basic_rate_limit);
        let write_limiter = Self::create_rate_limiter(http_config.write_rate_limit);
        let read_limiter = Self::create_rate_limiter(http_config.write_rate_limit);
        let load_limiter = Self::create_rate_limiter(http_config.load_rate_limit);
        let task_limiter = Self::create_rate_limiter(http_config.add_task_rate_limit);
        let leader_limiter = Self::create_rate_limiter(http_config.leader_rate_limit);

        let rate_limits = RateLimits {
            basic_limiter,
            write_limiter,
            read_limiter,
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
            node_id,
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
        node_id: usize,
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
            .hoop(affix_state::inject(node_id))
            .push(
                Router::with_path("/add_task")
                    .hoop(api_key_check)
                    .hoop(rate_limits.task_limiter)
                    .post(add_task_handler),
            )
            .push(Router::with_path("/add_result").post(add_result_handler))
            .push(Router::with_path("/get_tasks").post(get_tasks_handler))
            .push(
                Router::with_path("/get_load")
                    .hoop(rate_limits.load_limiter)
                    .get(get_load_handler),
            )
            .push(
                Router::with_path("/get_leader")
                    .hoop(rate_limits.leader_limiter)
                    .get(get_leader_handler),
            )
            .push(Router::with_path("/write").post(write_handler))
            .push(
                Router::with_path("/get_version")
                    .hoop(rate_limits.basic_limiter)
                    .get(version_handler),
            )
            .push(Router::with_path("/id").get(id_handler))
            .push(
                Router::with_path("/get_key")
                    .hoop(admin_key_check)
                    .hoop(rate_limits.read_limiter)
                    .get(generic_key_read_handler),
            )
            .push(
                Router::with_path("/get_result")
                    .hoop(api_key_check)
                    .get(get_result_handler),
            )
            .push(
                Router::with_path("/get_status")
                    .hoop(api_key_check)
                    .get(get_status_handler),
            )
            .push(
                Router::with_path("/update_key")
                    .hoop(admin_key_check)
                    .hoop(rate_limits.write_limiter)
                    .post(generic_key_update_handler),
            )
    }

    pub fn abort(&self) {
        self.join_handle.abort();
        self.subnet_state.abort();
    }
}
