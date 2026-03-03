use crate::config::HTTPConfig;

use super::DistributedRateLimiter;

#[derive(Copy, Clone, Debug)]
pub struct RateLimitPolicies {
    pub user_hourly_limit: u64,
    pub generic_global_hourly_limit: u64,
    pub generic_per_ip_hourly_limit: u64,
}

impl RateLimitPolicies {
    pub fn from_config(cfg: &HTTPConfig) -> Self {
        Self {
            user_hourly_limit: cfg.add_task_authenticated_per_user_hourly_rate_limit as u64,
            generic_global_hourly_limit: cfg.add_task_generic_key_global_hourly_rate_limit as u64,
            generic_per_ip_hourly_limit: cfg.add_task_generic_key_per_ip_hourly_rate_limit as u64,
        }
    }
}

#[derive(Clone)]
pub struct RateLimitService {
    distributed: DistributedRateLimiter,
    policies: RateLimitPolicies,
}

impl RateLimitService {
    pub fn new(http_config: &HTTPConfig) -> Self {
        Self {
            distributed: DistributedRateLimiter::new(
                http_config.distributed_rate_limiter_max_capacity,
            ),
            policies: RateLimitPolicies::from_config(http_config),
        }
    }

    pub fn distributed(&self) -> &DistributedRateLimiter {
        &self.distributed
    }

    pub fn policies(&self) -> RateLimitPolicies {
        self.policies
    }
}
