use salvo::prelude::Request;

use crate::config::HTTPConfig;

pub fn normalize_origin<'a>(req: &'a Request, http_cfg: &HTTPConfig) -> &'a str {
    let origin = req
        .headers()
        .get("x-client-origin")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("api");
    if http_cfg.allowed_origins.contains(origin) {
        origin
    } else {
        "api"
    }
}
