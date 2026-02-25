use std::net::IpAddr;
use std::sync::Arc;

use crate::api::Task;
use crate::common::queue::DupQueue;
use crate::config_runtime::{RuntimeConfigStore, RuntimeConfigView};
use crate::metrics::Metrics;
use crate::raft::gateway_state::GatewayState;

#[derive(Clone)]
pub struct HttpState {
    config: Arc<RuntimeConfigStore>,
    gateway_state: GatewayState,
    task_queue: DupQueue<Task>,
    metrics: Metrics,
}

pub struct HttpStateInit {
    pub config: Arc<RuntimeConfigStore>,
    pub gateway_state: GatewayState,
    pub task_queue: DupQueue<Task>,
    pub metrics: Metrics,
}

impl HttpState {
    pub fn new(init: HttpStateInit) -> Self {
        let HttpStateInit {
            config,
            gateway_state,
            task_queue,
            metrics,
        } = init;
        Self {
            config,
            gateway_state,
            task_queue,
            metrics,
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

    pub fn task_queue(&self) -> &DupQueue<Task> {
        &self.task_queue
    }

    pub fn metrics(&self) -> &Metrics {
        &self.metrics
    }

    pub fn is_cluster_ip(&self, ip: &IpAddr) -> bool {
        self.config().cluster_ips().contains(ip)
    }
}
