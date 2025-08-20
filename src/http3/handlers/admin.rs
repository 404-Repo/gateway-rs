use anyhow::Result;
use salvo::prelude::*;
use uuid::Uuid;

use crate::api::request::UpdateGenericKeyRequest;
use crate::api::response::GenericKeyResponse;
use crate::config::HTTPConfig;
use crate::http3::error::ServerError;
use crate::http3::handlers::common::DepotExt;
use crate::raft::gateway_state::GatewayState;

// curl --http3 -X POST "https://gateway-eu.404.xyz:4443/update_key" -H "x-admin-key: b6c8597a-00e9-493a-b6cd-5dfc7244d46b" -H "content-type: application/json" -d '{"generic_key": "6f3a2de1-f25d-4413-b0ad-4631eabbbb79"}'
#[handler]
pub async fn generic_key_update_handler(
    depot: &mut Depot,
    req: &mut Request,
    res: &mut Response,
) -> Result<(), ServerError> {
    let ugk = req
        .parse_json::<UpdateGenericKeyRequest>()
        .await
        .map_err(|e| ServerError::BadRequest(e.to_string()))?;

    let gateway_state = depot.require::<GatewayState>()?;
    let current_node_id = *depot.require::<usize>()? as u64;
    let admin_key = req
        .headers()
        .get("x-admin-key")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

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
    let gateway_state = depot.require::<GatewayState>()?;

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
    let http_cfg = depot.require::<HTTPConfig>()?;

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
