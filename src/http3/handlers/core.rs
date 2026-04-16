use crate::api::request::GatewayInfoExt;
use crate::api::response::{GatewayInfoRef, LeaderResponse};
use crate::common::log::get_build_information;
use crate::http3::depot_ext::DepotExt;
use crate::http3::error::ServerError;
use crate::http3::rate_limits::{RateLimitContext, ensure_api_key_context};
use crate::http3::state::HttpState;
use crate::raft::gateway_state::TaskLifecycleWriteRequest;
use crate::raft::store::Request as RaftRequest;
use http::HeaderValue;
use itoa::Buffer;
use prometheus::Encoder;
use salvo::prelude::*;
use serde_json::json;

pub const MULTIPART_PREFIX: &str = "multipart/form-data";
pub const BOUNDARY_PREFIX: &str = "boundary=";

#[handler]
pub async fn write_handler(
    depot: &mut Depot,
    req: &mut Request,
    res: &mut Response,
) -> Result<(), ServerError> {
    let state = depot.require::<HttpState>()?.clone();
    let cfg = state.config();
    let body = req
        .payload_with_max_size(cfg.http().raft_write_size_limit as usize)
        .await
        .map_err(|e| ServerError::BadRequest(e.to_string()))?;

    let gateway_state = state.gateway_state().clone();

    if let Ok(request) = rmp_serde::from_slice::<RaftRequest>(body.as_ref()) {
        match request.into_rate_limit_batch() {
            Ok(batch) => {
                gateway_state
                    .apply_rate_limit_mutation_batch(batch)
                    .await
                    .map_err(|e| {
                        ServerError::Internal(format!(
                            "Failed to apply rate limit mutations: {:?}",
                            e
                        ))
                    })?;
                res.status_code(StatusCode::OK);
                res.render(Text::Plain("Ok"));
                return Ok(());
            }
            Err(other) => {
                return Err(ServerError::BadRequest(format!(
                    "Unsupported raft write request: {:?}",
                    other
                )));
            }
        }
    }

    if let Ok(request) = rmp_serde::from_slice::<TaskLifecycleWriteRequest>(body.as_ref()) {
        gateway_state
            .apply_task_lifecycle_write(request)
            .await
            .map_err(|e| {
                ServerError::Internal(format!(
                    "Failed to apply generation lifecycle write: {:?}",
                    e
                ))
            })?;
        res.status_code(StatusCode::OK);
        res.render(Text::Plain("Ok"));
        return Ok(());
    }

    let gi: GatewayInfoExt = rmp_serde::from_slice(body.as_ref())
        .map_err(|e| ServerError::BadRequest(format!("Failed to parse msgpack: {}", e)))?;

    if !gateway_state.cluster_name_matches(&gi.cluster_name) {
        return Err(ServerError::Unauthorized("Unauthorized access".to_string()));
    }

    gateway_state
        .set_gateway_info(GatewayInfoRef {
            node_id: gi.node_id,
            domain: &gi.domain,
            ip: &gi.ip,
            name: &gi.name,
            http_port: gi.http_port,
            available_tasks: gi.available_tasks,
            last_task_acquisition: gi.last_task_acquisition,
            last_update: gi.last_update,
        })
        .await
        .map_err(|e| ServerError::Internal(format!("Failed to set gateway info: {:?}", e)))?;
    res.status_code(StatusCode::OK);
    res.render(Text::Plain("Ok"));
    Ok(())
}

#[handler]
pub async fn get_leader_handler(
    depot: &mut Depot,
    _req: &mut Request,
    res: &mut Response,
) -> Result<(), ServerError> {
    let state = depot.require::<HttpState>()?.clone();
    let gateway_state = state.gateway_state().clone();
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
pub async fn version_handler(_depot: &mut Depot, _req: &mut Request, res: &mut Response) {
    let build_info = get_build_information(true);
    res.render(Text::Html(build_info));
}

#[handler]
pub async fn id_handler(
    depot: &mut Depot,
    _req: &mut Request,
    res: &mut Response,
) -> Result<(), ServerError> {
    let state = depot.require::<HttpState>()?;
    let node_id = state.node_id();

    let mut buffer = Buffer::new();
    res.render(buffer.format(node_id).to_owned());
    Ok(())
}

#[handler]
pub async fn api_or_generic_key_check(
    depot: &mut Depot,
    req: &mut Request,
) -> Result<(), ServerError> {
    ensure_api_key_context(depot, req).await?;
    let context = depot.require::<RateLimitContext>()?;

    if req.headers().get("x-api-key").is_none() {
        return Err(ServerError::Unauthorized("Missing API key".to_string()));
    }

    if !context.key_is_uuid {
        return Err(ServerError::BadRequest(
            "Invalid API key format".to_string(),
        ));
    }

    if context.has_authorized_key() {
        Ok(())
    } else if context.auth_lookup_blocked {
        Err(ServerError::Json(
            StatusCode::TOO_MANY_REQUESTS,
            json!({
                "error": "invalid_api_key_rate_limit",
                "message": "Too many invalid API key attempts from this IP. Please retry later.",
            }),
        ))
    } else {
        Err(ServerError::Unauthorized("Invalid API key".to_string()))
    }
}

#[handler]
pub async fn metrics_handler(depot: &mut Depot, res: &mut Response) -> Result<(), ServerError> {
    let state = depot.require::<HttpState>()?.clone();
    let metrics = state.metrics();

    let metric_families = metrics.registry().gather();
    let encoder = prometheus::TextEncoder::new();
    let mut buffer = Vec::new();

    encoder
        .encode(&metric_families, &mut buffer)
        .map_err(|e| ServerError::Internal(format!("Failed to encode metrics: {}", e)))?;

    res.headers_mut().insert(
        "content-type",
        HeaderValue::from_static(prometheus::TEXT_FORMAT),
    );
    res.status_code(StatusCode::OK);
    res.body(buffer);

    Ok(())
}
