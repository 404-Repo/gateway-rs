use anyhow::Result;
use salvo::prelude::*;
use uuid::Uuid;

use crate::api::response::GenericKeyResponse;
use crate::http3::depot_ext::DepotExt;
use crate::http3::error::ServerError;
use crate::http3::state::HttpState;

#[handler]
pub async fn generic_key_update_handler(
    _depot: &mut Depot,
    _req: &mut Request,
    _res: &mut Response,
) -> Result<(), ServerError> {
    Err(ServerError::BadRequest(
        "Generic key is managed through gen admin settings and synced from PostgreSQL.".to_string(),
    ))
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

    if let Some(uuid) = gateway_state.generic_key() {
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
