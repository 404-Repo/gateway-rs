use crate::config::HTTPConfig;

use super::DistributedRateLimiter;

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct RateLimitPolicies {
    pub generic_global_daily_limit: u64,
    pub generic_per_ip_daily_limit: u64,
}

impl RateLimitPolicies {
    pub fn from_config(_cfg: &HTTPConfig) -> Self {
        Self::default()
    }
}

#[derive(Clone)]
pub struct RateLimitService {
    distributed: DistributedRateLimiter,
}

impl RateLimitService {
    pub fn new(http_config: &HTTPConfig) -> Self {
        Self {
            distributed: DistributedRateLimiter::new(
                http_config.distributed_rate_limiter_max_capacity,
            ),
        }
    }

    pub fn distributed(&self) -> &DistributedRateLimiter {
        &self.distributed
    }
}
