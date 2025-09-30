use crate::api::request::GatewayInfoExt;
use crate::api::response::{GatewayInfoRef, LeaderResponse};
use crate::common::log::get_build_information;
use crate::config::HTTPConfig;
use crate::http3::error::ServerError;
use crate::http3::user_rate_limits::RateLimitContext;
use crate::metrics::Metrics;
use crate::raft::gateway_state::GatewayState;
use http::HeaderValue;
use itoa::Buffer;
use prometheus::Encoder;
use salvo::prelude::*;

pub const MULTIPART_PREFIX: &str = "multipart/form-data";
pub const BOUNDARY_PREFIX: &str = "boundary=";

pub(crate) trait DepotExt {
    fn require<T: Send + Sync + 'static>(&self) -> Result<&T, ServerError>;
}

impl DepotExt for salvo::Depot {
    fn require<T: Send + Sync + 'static>(&self) -> Result<&T, ServerError> {
        self.obtain::<T>().map_err(|e| {
            ServerError::Internal(format!(
                "Failed to obtain {}: {:?}",
                std::any::type_name::<T>(),
                e
            ))
        })
    }
}

#[handler]
pub async fn write_handler(
    depot: &mut Depot,
    req: &mut Request,
    res: &mut Response,
) -> Result<(), ServerError> {
    let body = req
        .payload_with_max_size(depot.require::<HTTPConfig>()?.raft_write_size_limit as usize)
        .await
        .map_err(|e| ServerError::BadRequest(e.to_string()))?;
    let gi: GatewayInfoExt = rmp_serde::from_slice(body.as_ref())
        .map_err(|e| ServerError::BadRequest(format!("Failed to parse msgpack: {}", e)))?;

    let gateway_state = depot.require::<GatewayState>()?;

    if gi.cluster_name != gateway_state.cluster_name() {
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
    let gateway_state = depot.require::<GatewayState>()?;
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
    let node_id = *depot.require::<usize>()?;

    let mut buffer = Buffer::new();
    res.render(buffer.format(node_id).to_owned());
    Ok(())
}

#[handler]
pub async fn api_or_generic_key_check(
    depot: &mut Depot,
    req: &mut Request,
) -> Result<(), ServerError> {
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
    } else {
        Err(ServerError::Unauthorized("Invalid API key".to_string()))
    }
}

#[handler]
pub async fn metrics_handler(depot: &mut Depot, res: &mut Response) -> Result<(), ServerError> {
    let metrics = depot.require::<Metrics>()?;

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
