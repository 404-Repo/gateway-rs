use salvo::prelude::*;
use tracing::error;

use crate::api::response::LoadResponse;
use crate::http3::depot_ext::DepotExt;
use crate::http3::error::ServerError;
use crate::http3::state::HttpState;

// curl --http3 -X GET -k https://gateway-eu.404.xyz:4443/get_load
#[handler]
pub async fn get_load_handler(
    depot: &mut Depot,
    _req: &mut Request,
    res: &mut Response,
) -> Result<(), ServerError> {
    let state = depot.require::<HttpState>()?.clone();
    let gateway_state = state.gateway_state().clone();

    let gateways = gateway_state.gateways().await.map_err(|e| {
        error!(error = ?e, "Failed to obtain gateways for get_load");
        ServerError::Internal(format!(
            "Failed to obtain the gateways for get_load: {:?}",
            e
        ))
    })?;

    let load_response = LoadResponse { gateways };
    res.render(Json(load_response));
    Ok(())
}
