use anyhow::Result;
use salvo::prelude::*;
use std::net::IpAddr;
use uuid::Uuid;

use crate::api::request::UpdateGenericKeyRequest;
use crate::api::response::GenericKeyResponse;
use crate::http3::depot_ext::DepotExt;
use crate::http3::error::ServerError;
use crate::http3::state::HttpState;

// curl --http3 -X POST "https://gateway-eu.404.xyz:4443/update_key" -H "x-admin-key: b6c8597a-00e9-493a-b6cd-5dfc7244d46b" -H "content-type: application/json" -d '{"generic_key": "6f3a2de1-f25d-4413-b0ad-4631eabbbb79"}'
#[handler]
pub async fn generic_key_update_handler(
    depot: &mut Depot,
    req: &mut Request,
    res: &mut Response,
) -> Result<(), ServerError> {
    let state = depot.require::<HttpState>()?.clone();
    let ugk = req
        .parse_json::<UpdateGenericKeyRequest>()
        .await
        .map_err(|e| ServerError::BadRequest(e.to_string()))?;

    let gateway_state = state.gateway_state().clone();
    let current_node_id = state.node_id() as u64;
    let admin_key = req
        .headers()
        .get("x-admin-key")
        .and_then(|v| v.to_str().ok());

    gateway_state
        .update_gateway_generic_key(current_node_id, Some(ugk.generic_key), admin_key)
        .await
        .map_err(|e| {
            ServerError::Internal(format!("Failed to update gateway generic key: {:?}", e))
        })?;

    res.status_code(StatusCode::OK);
    res.render(Text::Plain("Ok"));
    Ok(())
}

// curl --http3 -X GET "https://gateway-eu.404.xyz:4443/get_key" -H "x-admin-key: b6c8597a-00e9-493a-b6cd-5dfc7244d46b"
#[handler]
pub async fn generic_key_read_handler(
    depot: &mut Depot,
    _req: &mut Request,
    res: &mut Response,
) -> Result<(), ServerError> {
    let state = depot.require::<HttpState>()?.clone();
    let gateway_state = state.gateway_state().clone();

    if let Some(uuid) = gateway_state.generic_key().await {
        let response = GenericKeyResponse { generic_key: uuid };
        res.render(Json(response));
        Ok(())
    } else {
        Err(ServerError::Internal("Generic key not found".to_string()))
    }
}

#[handler]
pub async fn admin_key_check(depot: &mut Depot, req: &mut Request) -> Result<(), ServerError> {
    let state = depot.require::<HttpState>()?.clone();
    let cfg = state.config();
    let http_cfg = cfg.http();

    let is_admin = req
        .headers()
        .get("x-admin-key")
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

#[handler]
pub async fn cluster_check(depot: &mut Depot, req: &mut Request) -> Result<(), ServerError> {
    let state = depot.require::<HttpState>()?;
    let remote_ip_opt: Option<IpAddr> = match req.remote_addr() {
        salvo::conn::SocketAddr::IPv4(addr) => Some(IpAddr::V4(*addr.ip())),
        salvo::conn::SocketAddr::IPv6(addr) => Some(IpAddr::V6(*addr.ip())),
        _ => None,
    };
    if let Some(ip) = remote_ip_opt
        && state.is_cluster_ip(&ip)
    {
        return Ok(());
    }
    Err(ServerError::Unauthorized("Not in cluster".to_string()))
}
