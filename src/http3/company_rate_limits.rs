use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use foldhash::fast::RandomState;
use salvo::{handler, Depot, Request};
use scc::HashMap as SccHashMap;
use tracing::info;
use uuid::Uuid;

use crate::http3::error::ServerError;
use crate::raft::gateway_state::GatewayState;

#[derive(Clone, Debug)]
struct RateLimitInfo {
    hourly_requests: u64,
    daily_requests: u64,
    hour_epoch: u64,
    day_epoch: u64,
}

#[derive(Clone)]
pub struct CompanyRateLimiterStore {
    inner: Arc<SccHashMap<Uuid, RateLimitInfo, RandomState>>,
}

impl CompanyRateLimiterStore {
    pub fn new() -> Self {
        CompanyRateLimiterStore {
            inner: Arc::new(SccHashMap::with_capacity_and_hasher(
                4096,
                RandomState::default(),
            )),
        }
    }
}

#[handler]
pub async fn company_rate_limit(depot: &mut Depot, req: &mut Request) -> Result<(), ServerError> {
    let key_header = req
        .headers()
        .get("x-api-key")
        .and_then(|v| v.to_str().ok())
        .ok_or(ServerError::BadRequest("Missing API key".to_string()))?;

    let gs = depot
        .obtain::<GatewayState>()
        .map_err(|e| ServerError::Internal(format!("Failed to obtain GatewayState: {:?}", e)))?;

    if gs.is_company_key(key_header) {
        if let Some((cid, (company_name, hourly_limit, daily_limit))) =
            gs.get_company_rate_limits(key_header)
        {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|e| ServerError::Internal(format!("Time error: {}", e)))?
                .as_secs();
            let hour_epoch = now / 3600;
            let day_epoch = now / 86400;

            let store = depot
                .obtain::<CompanyRateLimiterStore>()
                .map_err(|e| ServerError::Internal(format!("RateLimiterStore missing: {:?}", e)))?;

            let mut allowed = true;

            store
                .inner
                .entry_async(cid)
                .await
                .and_modify(|rate_info| {
                    if rate_info.hour_epoch != hour_epoch {
                        rate_info.hourly_requests = 0;
                        rate_info.hour_epoch = hour_epoch;
                    }
                    if rate_info.day_epoch != day_epoch {
                        rate_info.daily_requests = 0;
                        rate_info.day_epoch = day_epoch;
                    }

                    if (hourly_limit > 0 && rate_info.hourly_requests >= hourly_limit)
                        || (daily_limit > 0 && rate_info.daily_requests >= daily_limit)
                    {
                        allowed = false;
                        return;
                    }

                    rate_info.hourly_requests += 1;
                    rate_info.daily_requests += 1;
                })
                .or_insert_with(|| RateLimitInfo {
                    hourly_requests: 1,
                    daily_requests: 1,
                    hour_epoch,
                    day_epoch,
                });

            if !allowed {
                info!("Rate limit exceeded for company {} ({})", company_name, cid);
                return Err(ServerError::TooManyRequests(
                    "Company rate limit exceeded".to_string(),
                ));
            }
        }
    }

    Ok(())
}
