use std::sync::Arc;

use crate::common::queue::TaskQueue;
use crate::config_runtime::{RuntimeConfigStore, RuntimeConfigView};
use crate::http3::rate_limits::{AdminKeyFailureLimiter, UnauthorizedDailyLimiter};
use crate::metrics::Metrics;
use crate::raft::gateway_state::GatewayState;

#[derive(Clone)]
pub struct HttpState {
    config: Arc<RuntimeConfigStore>,
    gateway_state: GatewayState,
    task_queue: TaskQueue,
    metrics: Metrics,
    unauthorized_daily_limiter: Arc<UnauthorizedDailyLimiter>,
    admin_key_failure_limiter: Arc<AdminKeyFailureLimiter>,
}

pub struct HttpStateInit {
    pub config: Arc<RuntimeConfigStore>,
    pub gateway_state: GatewayState,
    pub task_queue: TaskQueue,
    pub metrics: Metrics,
    pub unauthorized_daily_limiter: Arc<UnauthorizedDailyLimiter>,
    pub admin_key_failure_limiter: Arc<AdminKeyFailureLimiter>,
}

impl HttpState {
    pub fn new(init: HttpStateInit) -> Self {
        let HttpStateInit {
            config,
            gateway_state,
            task_queue,
            metrics,
            unauthorized_daily_limiter,
            admin_key_failure_limiter,
        } = init;
        Self {
            config,
            gateway_state,
            task_queue,
            metrics,
            unauthorized_daily_limiter,
            admin_key_failure_limiter,
        }
    }

    pub fn config(&self) -> RuntimeConfigView {
        self.config.snapshot()
    }

    pub fn node_id(&self) -> usize {
        self.config().node().network.node_id as usize
    }

    pub fn gateway_state(&self) -> &GatewayState {
        &self.gateway_state
    }

    pub fn task_queue(&self) -> &TaskQueue {
        &self.task_queue
    }

    pub fn metrics(&self) -> &Metrics {
        &self.metrics
    }

    pub fn unauthorized_daily_limiter(&self) -> &Arc<UnauthorizedDailyLimiter> {
        &self.unauthorized_daily_limiter
    }

    pub fn admin_key_failure_limiter(&self) -> &Arc<AdminKeyFailureLimiter> {
        &self.admin_key_failure_limiter
    }
}
