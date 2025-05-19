use std::net::SocketAddr;
use std::str::FromStr;
use std::time::{Duration, Instant};

use async_zip::base::write::ZipFileWriter;
use async_zip::ZipEntryBuilder;
use futures::io::Cursor;
use futures::TryStreamExt;
use http::HeaderValue;
use multer::{Constraints, Multipart};
use prometheus::{Encoder as _, TextEncoder, TEXT_FORMAT};
use salvo::conn::quinn::QuinnListener;
use salvo::conn::rustls::RustlsConfig;
use salvo::http::request::SecureMaxSize;
use salvo::prelude::*;
use salvo::rate_limiter::{
    BasicQuota, FixedGuard, MokaStore, RateIssuer, RateLimiter, RemoteIpIssuer,
};

use tokio_util::codec::{BytesCodec, FramedRead};
use tokio_util::io::StreamReader;
use tracing::info;
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
use crate::metrics::Metrics;
use crate::raft::gateway_state::GatewayState;
use multer::SizeLimit;

use super::error::ServerError;

type H3RateLimiter = RateLimiter<
    FixedGuard,
    MokaStore<<RemoteIpIssuer as RateIssuer>::Key, FixedGuard>,
    RemoteIpIssuer,
    BasicQuota,
>;

type H3GenericKeyPerIpRateLimiter = RateLimiter<
    FixedGuard,
    MokaStore<<GenericKeyPerIpIssuer as RateIssuer>::Key, FixedGuard>,
    GenericKeyPerIpIssuer,
    BasicQuota,
>;
struct GenericKeyPerIpIssuer;

impl RateIssuer for GenericKeyPerIpIssuer {
    type Key = String;

    async fn issue(&self, req: &mut Request, depot: &Depot) -> Option<Self::Key> {
        let key_header = req.headers().get("X-API-Key")?.to_str().ok()?;
        let gs = depot.obtain::<GatewayState>().ok()?;
        let uuid = Uuid::from_str(key_header).ok()?;

        if gs.is_generic_key(&uuid).await {
            let socket_addr = req.remote_addr();
            let ip_str = match socket_addr {
                salvo::conn::SocketAddr::IPv4(addr) => addr.ip().to_string(),
                salvo::conn::SocketAddr::IPv6(addr) => addr.ip().to_string(),
                _ => return None,
            };
            return Some(format!("g:{}", ip_str));
        }
        None
    }
}

struct RateLimits {
    basic_limiter: H3RateLimiter,
    write_limiter: H3RateLimiter,
    update_limiter: H3RateLimiter,
    generic_limiter: H3GenericKeyPerIpRateLimiter,
    read_limiter: H3RateLimiter,
    task_limiter: H3RateLimiter,
    load_limiter: H3RateLimiter,
    leader_limiter: H3RateLimiter,
    metric_limiter: H3RateLimiter,
}

// curl --http3 -X POST "https://gateway.404.xyz:4443/add_task" -H "Content-Type: application/json" -H "X-API-Key: 123e4567-e89b-12d3-a456-426614174001" -d '{"prompt": "mechanic robot"}'
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

    let queue = depot.obtain::<DupQueue<Task>>().map_err(|err_opt| {
        if let Some(err) = err_opt {
            ServerError::Internal(format!("Failed to obtain the task queue: {:?}", err))
        } else {
            ServerError::Internal("Failed to obtain the task queue: unknown error".into())
        }
    })?;

    let http_cfg = depot
        .obtain::<HTTPConfig>()
        .map_err(|e| ServerError::Internal(format!("Failed to obtain the HTTPConfig: {:?}", e)))?;

    if queue.len() >= http_cfg.max_task_queue_len {
        return Err(ServerError::Internal("Task queue is full".to_string()));
    }

    let gateway_state = depot.obtain::<GatewayState>().map_err(|e| {
        ServerError::Internal(format!("Failed to obtain the state reader: {:?}", e))
    })?;

    queue.push(task.clone());
    info!("A new task has been pushed with ID: {}", task.id);
    gateway_state.task_manager().add_time(task.id);

    res.render(Json(task));
    Ok(())
}

// curl -X POST https://gateway.404.xyz:4443/add_result \
//   -H "Content-Type: multipart/form-data; boundary=----WebKitFormBoundary7MA4YWxkTrZu0gW" \
//   --form 'id="123e4567-e89b-12d3-a456-426614174001"' \
//   --form 'signature="signature"' \
//   --form 'timestamp="404_GATEWAY_1713096000"' \
//   --form 'validator_hotkey="validator_key_123"' \
//   --form 'miner_hotkey="miner_key_456"' \
//   --form 'asset=@/path/to/asset.spz' \
//   --form 'score="0.95"'
#[handler]
async fn add_result_handler(
    depot: &mut Depot,
    req: &mut Request,
    res: &mut Response,
) -> Result<(), ServerError> {
    let content_type = req
        .headers()
        .get("content-type")
        .and_then(|ct| ct.to_str().ok())
        .map(|s| s.to_string())
        .ok_or(ServerError::BadRequest("Missing Content-Type".into()))?;

    if !content_type
        .to_lowercase()
        .starts_with("multipart/form-data")
    {
        return Err(ServerError::BadRequest(
            "Invalid Content-Type, expected multipart/form-data".into(),
        ));
    }

    let parts: Vec<&str> = content_type.split(';').map(|s| s.trim()).collect();
    let boundary = parts
        .iter()
        .find(|&&part| part.to_lowercase().starts_with("boundary="))
        .and_then(|part| part.split('=').nth(1))
        .ok_or(ServerError::BadRequest(
            "Missing boundary in Content-Type".into(),
        ))?;

    let raw_stream = req
        .take_body()
        .into_stream()
        .map_err(|err| {
            std::io::Error::new(std::io::ErrorKind::Other, format!("Stream error: {}", err))
        })
        .and_then(|frame| async move {
            frame.into_data().map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::Other, "Frame data error".to_string())
            })
        });

    let stream_reader = StreamReader::new(raw_stream);
    let byte_stream = FramedRead::new(stream_reader, BytesCodec::new()).map_ok(|b| b.freeze());

    let http_cfg = depot
        .obtain::<HTTPConfig>()
        .map_err(|e| ServerError::Internal(format!("Failed to obtain the HTTPConfig: {:?}", e)))?;

    let constraints = Constraints::new()
        .allowed_fields(vec![
            "id",
            "signature",
            "timestamp",
            "validator_hotkey",
            "miner_hotkey",
            "asset",
            "score",
        ])
        .size_limit(SizeLimit::new().whole_stream(http_cfg.request_file_size_limit));

    let mut multipart = Multipart::with_constraints(byte_stream, boundary, constraints);

    let mut id = None;
    let mut signature = None;
    let mut timestamp = None;
    let mut validator_hotkey = None;
    let mut miner_hotkey = None;
    let mut asset = None;
    let mut score = None;

    while let Some(mut field) = multipart
        .next_field()
        .await
        .map_err(|e| ServerError::BadRequest(format!("Failed to read field: {}", e)))?
    {
        let name = field
            .name()
            .ok_or(ServerError::BadRequest("Field missing name".into()))?
            .to_string();

        match name.as_str() {
            "id" => {
                let id_str = field
                    .text()
                    .await
                    .map_err(|e| ServerError::BadRequest(format!("Failed to read id: {}", e)))?;
                id =
                    Some(Uuid::parse_str(&id_str).map_err(|e| {
                        ServerError::BadRequest(format!("Invalid id format: {}", e))
                    })?);
            }
            "signature" => {
                signature = Some(field.text().await.map_err(|e| {
                    ServerError::BadRequest(format!("Failed to read signature: {}", e))
                })?);
            }
            "timestamp" => {
                timestamp = Some(field.text().await.map_err(|e| {
                    ServerError::BadRequest(format!("Failed to read timestamp: {}", e))
                })?);
            }
            "validator_hotkey" => {
                validator_hotkey = Some(field.text().await.map_err(|e| {
                    ServerError::BadRequest(format!("Failed to read validator_hotkey: {}", e))
                })?);
            }
            "miner_hotkey" => {
                miner_hotkey = Some(field.text().await.map_err(|e| {
                    ServerError::BadRequest(format!("Failed to read miner_hotkey: {}", e))
                })?);
            }
            "asset" => {
                let mut content = Vec::new();
                while let Some(chunk) = field.chunk().await.map_err(|e| {
                    ServerError::BadRequest(format!("Failed to read asset chunk: {}", e))
                })? {
                    content.extend_from_slice(&chunk);
                }
                asset = Some(content);
            }
            "score" => {
                let score_text = field
                    .text()
                    .await
                    .map_err(|e| ServerError::BadRequest(format!("Failed to read score: {}", e)))?;
                score = Some(score_text.parse::<f32>().map_err(|e| {
                    ServerError::BadRequest(format!("Invalid score format: {}", e))
                })?);
            }
            _ => continue,
        }
    }

    let add_result = AddTaskResultRequest {
        id: id.ok_or(ServerError::BadRequest("Missing id field".into()))?,
        signature: signature.ok_or(ServerError::BadRequest("Missing signature field".into()))?,
        timestamp: timestamp.ok_or(ServerError::BadRequest("Missing timestamp field".into()))?,
        validator_hotkey: validator_hotkey.ok_or(ServerError::BadRequest(
            "Missing validator_hotkey field".into(),
        ))?,
        miner_hotkey: miner_hotkey
            .ok_or(ServerError::BadRequest("Missing miner_hotkey field".into()))?,
        asset: asset.ok_or(ServerError::BadRequest("Missing asset field".into()))?,
        score: score.ok_or(ServerError::BadRequest("Missing score field".into()))?,
    };

    let subnet_state = depot
        .obtain::<SubnetState>()
        .map_err(|e| ServerError::Internal(format!("Failed to obtain the SubnetState: {:?}", e)))?;

    subnet_state
        .validate_hotkey(&add_result.validator_hotkey)
        .map_err(|e| {
            ServerError::Internal(format!("Hotkey is not registered in the subnet: {:?}", e))
        })?;

    verify_hotkey(
        &add_result.timestamp,
        &add_result.validator_hotkey,
        &add_result.signature,
        http_cfg.signature_freshness_threshold,
    )
    .map_err(|e| ServerError::Internal(format!("Failed to verify AddTaskRequest: {:?}", e)))?;

    let gateway_state = depot.obtain::<GatewayState>().map_err(|e| {
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
        "Received result for task {} from validator {} and miner {}",
        id, &result.validator_hotkey, &result.miner_hotkey
    );

    let metrics = depot
        .obtain::<Metrics>()
        .map_err(|e| ServerError::Internal(format!("Failed to obtain Metrics: {:?}", e)))?;

    metrics.inc_task_completed(&result.validator_hotkey);

    let manager = gateway_state.task_manager();
    metrics.record_completion_time(&result.validator_hotkey, manager.get_time(id));
    manager.add_result(id, result).await;
    manager.remove_time(id);

    res.status_code(StatusCode::OK);
    res.render(Text::Plain("Ok"));
    Ok(())
}

// curl --http3 "https://gateway.404.xyz:4443/get_result?id=123e4567-e89b-12d3-a456-426614174000" \
//   -H "X-API-Key: 123e4567-e89b-12d3-a456-426614174001" \
//   -o result.spz

// curl --http3 "https://gateway.404.xyz:4443/get_result?id=123e4567-e89b-12d3-a456-426614174000&all=true" \
//   -H "X-API-Key: 123e4567-e89b-12d3-a456-426614174001" \
//   -o results.zip
#[handler]
pub async fn get_result_handler(
    depot: &mut Depot,
    req: &mut Request,
    res: &mut Response,
) -> Result<(), ServerError> {
    let get_task = req
        .parse_queries::<GetTaskResultRequest>()
        .map_err(|e| ServerError::BadRequest(e.to_string()))?;

    let task_manager = depot
        .obtain::<GatewayState>()
        .map_err(|e| ServerError::Internal(format!("Failed to obtain the state reader: {:?}", e)))?
        .task_manager();

    let mut results_vec = task_manager
        .get_result(get_task.id)
        .await
        .ok_or_else(|| ServerError::NotFound(format!("Task ID {} not found", get_task.id)))?;

    if results_vec.is_empty() {
        return Err(ServerError::NotFound(format!(
            "No results found for task ID {}",
            get_task.id
        )));
    }

    let metrics = depot
        .obtain::<Metrics>()
        .map_err(|e| ServerError::Internal(format!("Failed to obtain Metrics: {:?}", e)))?;

    let (content_type, content_disposition, body, best_validator) = if get_task.all {
        results_vec.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        let best_validator = results_vec
            .first()
            .map(|r| r.validator_hotkey.clone())
            .ok_or_else(|| {
                ServerError::Internal(format!(
                    "Failed to find best result for task ID {}",
                    get_task.id
                ))
            })?;

        let mut buffer = Vec::new();
        let mut zip_writer = ZipFileWriter::new(Cursor::new(&mut buffer));
        for (i, file) in results_vec.into_iter().enumerate() {
            let entry = ZipEntryBuilder::new(
                format!("{}.spz", i + 1).into(),
                async_zip::Compression::Stored,
            )
            .build();
            zip_writer
                .write_entry_whole(entry, &file.asset)
                .await
                .map_err(|e| {
                    ServerError::Internal(format!(
                        "Failed to write zip entry {} for task ID {}: {:?}",
                        i + 1,
                        get_task.id,
                        e
                    ))
                })?;
        }
        zip_writer.close().await.map_err(|e| {
            ServerError::Internal(format!(
                "Failed to close zip archive for task ID {}: {:?}",
                get_task.id, e
            ))
        })?;

        (
            "application/zip",
            "attachment; filename=\"results.zip\"",
            buffer,
            best_validator,
        )
    } else {
        let best = results_vec
            .into_iter()
            .max_by(|a, b| {
                a.score
                    .partial_cmp(&b.score)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .ok_or_else(|| {
                ServerError::Internal(format!(
                    "Failed to find the best result for task ID {}",
                    get_task.id
                ))
            })?;
        let best_validator = best.validator_hotkey.clone();
        (
            "application/octet-stream",
            "attachment; filename=\"result.spz\"",
            best.asset,
            best_validator,
        )
    };

    metrics.inc_best_task(&best_validator);

    res.headers_mut()
        .insert("Content-Type", HeaderValue::from_static(content_type));
    res.headers_mut().insert(
        "Content-Disposition",
        HeaderValue::from_static(content_disposition),
    );
    res.body(body);

    Ok(())
}

// curl --http3 https://gateway.404.xyz:4443/get_status -X GET -H "Content-Type: application/json" -d '{"id": "123e4567-e89b-12d3-a456-426614174000"}'
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
        ServerError::Internal(format!("Failed to obtain the state reader: {:?}", e))
    })?;

    let status = gateway_state.task_manager().get_status(get_status.id).await;

    res.render(Json(GetTaskStatusResponse { status }));

    Ok(())
}

// curl --http3 -X POST https://gateway.404.xyz:4443/get_tasks \
//   -H "Content-Type: application/json" \
//   -d '{"validator_hotkey": "abc123", "signature": "signatureinbase64", "timestamp": "404_GATEWAY_1743657200", "requested_task_count": 10}'
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

    let http_cfg = depot
        .obtain::<HTTPConfig>()
        .map_err(|e| ServerError::Internal(format!("Failed to obtain the HTTPConfig: {:?}", e)))?;

    let gateway_state = depot.obtain::<GatewayState>().map_err(|e| {
        ServerError::Internal(format!("Failed to obtain the state reader: {:?}", e))
    })?;

    let queue = depot.obtain::<DupQueue<Task>>().map_err(|err_opt| {
        if let Some(err) = err_opt {
            ServerError::Internal(format!("Failed to obtain the task queue: {:?}", err))
        } else {
            ServerError::Internal("Failed to obtain the task queue: unknown error".into())
        }
    })?;

    let metrics = depot
        .obtain::<Metrics>()
        .map_err(|e| ServerError::Internal(format!("Failed to obtain Metrics: {:?}", e)))?;

    let gateways = gateway_state
        .gateways()
        .await
        .map_err(|e| ServerError::Internal(format!("Failed to obtain gateways: {:?}", e)))?;

    let subnet_state = depot
        .obtain::<SubnetState>()
        .map_err(|e| ServerError::Internal(format!("Failed to obtain the SubnetState: {:?}", e)))?;

    subnet_state
        .validate_hotkey(&get_tasks.validator_hotkey)
        .map_err(|e| {
            ServerError::Internal(format!("Hotkey is not registered in the subnet: {:?}", e))
        })?;

    verify_hotkey(
        &get_tasks.timestamp,
        &get_tasks.validator_hotkey,
        &get_tasks.signature,
        http_cfg.signature_freshness_threshold,
    )
    .map_err(|e| ServerError::Internal(format!("Failed to verify GetTasksRequest: {:?}", e)))?;

    info!(
        "Validator {} has requested {} tasks.",
        get_tasks.validator_hotkey, get_tasks.requested_task_count
    );

    let mut tasks = Vec::new();
    for (t, d) in queue.pop(get_tasks.requested_task_count, &get_tasks.validator_hotkey) {
        tasks.push(t);
        if let Some(dur) = d {
            metrics.record_queue_time(dur.as_secs_f64());
        }
    }

    gateway_state.update_task_acquisition().map_err(|e| {
        ServerError::Internal(format!(
            "Failed to execute update_task_acquisition: {:?}",
            e
        ))
    })?;

    metrics.inc_tasks_received(&get_tasks.validator_hotkey, tasks.len());

    info!(
        "Validator {} received {} tasks",
        get_tasks.validator_hotkey,
        tasks.len()
    );

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
    let gateway_state = depot
        .obtain::<GatewayState>()
        .map_err(|e| ServerError::Internal(format!("Failed to obtain GatewayState: {:?}", e)))?;

    let gateways = gateway_state.gateways().await.map_err(|e| {
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

    let gateway_state = depot
        .obtain::<GatewayState>()
        .map_err(|e| ServerError::Internal(format!("Failed to obtain GatewayState: {:?}", e)))?;

    if gi.cluster_name != gateway_state.cluster_name() {
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
    let gateway_state = depot
        .obtain::<GatewayState>()
        .map_err(|e| ServerError::Internal(format!("Failed to obtain GatewayState: {:?}", e)))?;
    let leader_id = match gateway_state.leader().await {
        Some(id) => id,
        None => {
            return Err(ServerError::Internal("The leader is not elected".into()));
        }
    };
    let gateway_info = gateway_state
        .gateway(leader_id)
        .await
        .map_err(|e| ServerError::Internal(format!("Failed to obtain GatewayState: {:?}", e)))?;

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
    let node_id = depot
        .obtain::<usize>()
        .map_err(|e| ServerError::Internal(format!("Failed to obtain node_id: {:?}", e)))?;

    res.render(node_id.to_string());
    Ok(())
}

#[handler]
async fn api_key_check(depot: &mut Depot, req: &mut Request) -> Result<(), ServerError> {
    let gateway_state = depot
        .obtain::<GatewayState>()
        .map_err(|_| ServerError::Internal("Failed to obtain GatewayState".to_string()))?;

    let api_key = req
        .headers()
        .get("X-API-Key")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| Uuid::parse_str(s).ok());

    match api_key {
        Some(key) if gateway_state.is_valid_api_key(&key) => Ok(()),
        _ => Err(ServerError::Unauthorized(
            "Invalid or missing API key".to_string(),
        )),
    }
}

#[handler]
async fn generic_api_key_check(depot: &mut Depot, req: &mut Request) -> Result<(), ServerError> {
    let gateway_state = depot
        .obtain::<GatewayState>()
        .map_err(|_| ServerError::Internal("Failed to obtain GatewayState".to_string()))?;

    let api_key = req
        .headers()
        .get("X-API-Key")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| Uuid::parse_str(s).ok());

    match api_key {
        Some(key) if gateway_state.is_generic_key(&key).await => Ok(()),
        _ => Err(ServerError::Unauthorized(
            "Invalid or missing API key".to_string(),
        )),
    }
}

#[handler]
async fn admin_key_check(depot: &mut Depot, req: &mut Request) -> Result<(), ServerError> {
    let http_cfg = depot
        .obtain::<HTTPConfig>()
        .map_err(|_| ServerError::Internal("Failed to obtain HTTPConfig".to_string()))?;

    let is_admin = req
        .headers()
        .get("X-Admin-Key")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| Uuid::parse_str(s).ok())
        == Some(http_cfg.admin_key);

    if is_admin {
        Ok(())
    } else {
        Err(ServerError::Unauthorized(
            "Invalid or missing admin key".to_string(),
        ))
    }
}

// curl --http3 -X POST "https://gateway.404.xyz:4443/update_key" -H "X-Admin-Key: b6c8597a-00e9-493a-b6cd-5dfc7244d46b" -H "Content-Type: application/json" -d '{"generic_key": "6f3a2de1-f25d-4413-b0ad-4631eabbbb79"}'
#[handler]
async fn generic_key_update_handler(
    depot: &mut Depot,
    req: &mut Request,
    res: &mut Response,
) -> Result<(), ServerError> {
    let ugk = req
        .parse_json::<UpdateGenericKeyRequest>()
        .await
        .map_err(|e| ServerError::BadRequest(e.to_string()))?;

    let gateway_state = depot
        .obtain::<GatewayState>()
        .map_err(|e| ServerError::Internal(format!("Failed to obtain GatewayState: {:?}", e)))?;

    gateway_state
        .update_gateway_generic_key(Some(ugk.generic_key))
        .await
        .map_err(|e| {
            ServerError::Internal(format!("Failed to update gateway generic key: {:?}", e))
        })?;

    res.status_code(StatusCode::OK);
    res.render(Text::Plain("Ok"));
    Ok(())
}

// curl --http3 -X GET "https://gateway.404.xyz:4443/get_key" -H "X-Admin-Key: b6c8597a-00e9-493a-b6cd-5dfc7244d46b"
#[handler]
async fn generic_key_read_handler(
    depot: &mut Depot,
    _req: &mut Request,
    res: &mut Response,
) -> Result<(), ServerError> {
    let gateway_state = depot
        .obtain::<GatewayState>()
        .map_err(|e| ServerError::Internal(format!("Failed to obtain GatewayState: {:?}", e)))?;

    if let Some(uuid) = gateway_state.generic_key().await {
        let response = GenericKeyResponse { generic_key: uuid };
        res.render(Json(response));
        Ok(())
    } else {
        Err(ServerError::Internal("Generic key not found".to_string()))
    }
}

#[handler]
async fn metrics_handler(depot: &mut Depot, res: &mut Response) -> Result<(), ServerError> {
    let metrics = depot
        .obtain::<Metrics>()
        .map_err(|e| ServerError::Internal(format!("Failed to obtain Metrics: {:?}", e)))?;

    let metric_families = metrics.registry().gather();
    let encoder = TextEncoder::new();
    let mut buffer = Vec::new();

    encoder
        .encode(&metric_families, &mut buffer)
        .map_err(|e| ServerError::Internal(format!("Failed to encode metrics: {}", e)))?;

    res.headers_mut()
        .insert("Content-Type", HeaderValue::from_static(TEXT_FORMAT));
    res.status_code(StatusCode::OK);
    res.body(buffer);

    Ok(())
}

pub struct Http3Server {
    join_handle: tokio::task::JoinHandle<()>,
    subnet_state: SubnetState,
}

impl Http3Server {
    pub async fn run(
        node_id: usize,
        addr: SocketAddr,
        tls_config: RustlsConfig,
        http_config: &HTTPConfig,
        gateway_state: GatewayState,
        task_queue: DupQueue<Task>,
    ) -> Self {
        let basic_limiter = Self::create_rate_limiter(http_config.basic_rate_limit);
        let write_limiter = Self::create_rate_limiter(http_config.write_rate_limit);
        let update_limiter = Self::create_rate_limiter(http_config.update_key_rate_limit);
        let generic_limiter = H3GenericKeyPerIpRateLimiter::new(
            FixedGuard::new(),
            MokaStore::new(),
            GenericKeyPerIpIssuer,
            BasicQuota::per_minute(http_config.generic_key_rate_limit),
        );
        let read_limiter = Self::create_rate_limiter(http_config.write_rate_limit);
        let load_limiter = Self::create_rate_limiter(http_config.load_rate_limit);
        let task_limiter = Self::create_rate_limiter(http_config.add_task_rate_limit);
        let leader_limiter = Self::create_rate_limiter(http_config.leader_rate_limit);
        let metric_limiter = Self::create_rate_limiter(http_config.metric_rate_limit);

        let rate_limits = RateLimits {
            basic_limiter,
            write_limiter,
            update_limiter,
            generic_limiter,
            read_limiter,
            load_limiter,
            task_limiter,
            leader_limiter,
            metric_limiter,
        };

        let subnet_state = SubnetState::new(
            http_config.wss_bittensor.clone(),
            http_config.subnet_number,
            None,
            Duration::from_secs(http_config.subnet_poll_interval_sec),
            http_config.wss_max_message_size,
        );

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
        task_queue: DupQueue<Task>,
        subnet_state: SubnetState,
        gateway_state: GatewayState,
        rate_limits: RateLimits,
        config: HTTPConfig,
    ) -> Router {
        let request_size_limit = config.request_size_limit;
        let size_limit_handler = || SecureMaxSize(request_size_limit as usize);
        let file_size_limit_handler = SecureMaxSize(config.request_file_size_limit as usize);

        let metrics = Metrics::new(0.05).expect("Failed to create Metrics");

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
            .hoop(affix_state::inject(task_queue))
            .hoop(affix_state::inject(gateway_state))
            .hoop(affix_state::inject(subnet_state))
            .hoop(affix_state::inject(config))
            .hoop(affix_state::inject(node_id))
            .hoop(affix_state::inject(metrics))
            .push(
                Router::with_path("/add_task")
                    .hoop(size_limit_handler())
                    .hoop(api_key_check)
                    .hoop(rate_limits.task_limiter)
                    .post(add_task_handler),
            )
            .push(
                Router::with_path("/add_task_generic")
                    .hoop(size_limit_handler())
                    .hoop(generic_api_key_check)
                    .hoop(rate_limits.generic_limiter)
                    .post(add_task_handler),
            )
            .push(
                Router::with_path("/add_result")
                    .hoop(file_size_limit_handler)
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
                    .hoop(size_limit_handler())
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
                Router::with_path("/get_key")
                    .hoop(size_limit_handler())
                    .hoop(rate_limits.read_limiter)
                    .hoop(admin_key_check)
                    .get(generic_key_read_handler),
            )
            .push(
                Router::with_path("/get_result")
                    .hoop(size_limit_handler())
                    .hoop(api_key_check)
                    .get(get_result_handler),
            )
            .push(
                Router::with_path("/get_status")
                    .hoop(size_limit_handler())
                    .hoop(api_key_check)
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
    }

    pub fn abort(&self) {
        self.join_handle.abort();
        self.subnet_state.abort();
    }
}
