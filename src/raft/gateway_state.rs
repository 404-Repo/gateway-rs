use super::store::Request;
use super::{NodeId, Raft, StateMachineStore};
use crate::api::response::GatewayInfo;
use anyhow::Result;
use openraft::{BasicNode, RaftMetrics};
use serde_json;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{watch, RwLock};

#[derive(Debug)]
pub enum GatewayStateError {
    NotFound(String),
    DeserializeError(serde_json::Error),
}

impl fmt::Display for GatewayStateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GatewayStateError::NotFound(key) => {
                write!(f, "No gateway info found for key '{}'", key)
            }
            GatewayStateError::DeserializeError(e) => {
                write!(f, "Error deserializing gateway info: {}", e)
            }
        }
    }
}

impl std::error::Error for GatewayStateError {}

#[derive(Clone)]
pub struct GatewayState {
    state: Arc<StateMachineStore>,
    raft: Arc<RwLock<Raft>>,
    cluster_name: String,
    last_task_acquisition: Arc<AtomicU64>,
}

impl GatewayState {
    pub fn new(
        state: Arc<StateMachineStore>,
        raft: Arc<RwLock<Raft>>,
        cluster_name: String,
        last_task_acquisition: Arc<AtomicU64>,
    ) -> Self {
        Self {
            state,
            raft,
            cluster_name,
            last_task_acquisition,
        }
    }

    pub fn update_task_acquisition(&self) -> Result<()> {
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        self.last_task_acquisition
            .store(timestamp, Ordering::Relaxed);
        Ok(())
    }

    pub async fn leader(&self) -> Option<u64> {
        self.raft.read().await.metrics().borrow().current_leader
    }

    pub async fn metrics(&self) -> watch::Receiver<RaftMetrics<NodeId, BasicNode>> {
        self.raft.read().await.metrics()
    }

    pub async fn membership(&self) -> Vec<u64> {
        let membership_config = {
            let raft_guard = self.raft.read().await;
            raft_guard.metrics().borrow().membership_config.clone()
        };

        membership_config
            .membership()
            .nodes()
            .map(|(&id, _)| id)
            .collect()
    }

    pub async fn gateway(&self, n: u64) -> Result<GatewayInfo, GatewayStateError> {
        let key = n.to_string();
        let json_str = {
            self.state
                .get(&key)
                .await
                .ok_or_else(|| GatewayStateError::NotFound(key.clone()))?
        };

        serde_json::from_str::<GatewayInfo>(&json_str).map_err(GatewayStateError::DeserializeError)
    }

    pub async fn gateways(&self) -> Result<Vec<GatewayInfo>, GatewayStateError> {
        let (nodes, nodes_map) = {
            let nodes = self.membership().await;

            let nodes_map = { self.state.clone_map().await };

            (nodes, nodes_map)
        };

        nodes
            .into_iter()
            .map(|n| {
                let json_str = nodes_map
                    .get(&n.to_string())
                    .ok_or_else(|| GatewayStateError::NotFound(n.to_string()))?;
                serde_json::from_str::<GatewayInfo>(json_str)
                    .map_err(GatewayStateError::DeserializeError)
            })
            .collect()
    }

    pub async fn set_gateway_info(&self, info: GatewayInfo) -> Result<()> {
        let gateway_info_str = serde_json::to_string(&info)?;
        let key = info.node_id.to_string();

        let request = Request::Set {
            key,
            value: gateway_info_str,
        };

        {
            let raft_guard = self.raft.write().await;
            raft_guard.client_write(request).await?;
        }

        Ok(())
    }

    pub fn cluster_name(&self) -> &str {
        &self.cluster_name
    }
}
