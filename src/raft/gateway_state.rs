use super::store::Request;
use super::{NodeId, Raft, StateMachineStore};
use crate::api::response::GatewayInfo;
use crate::common::task::TaskManager;
use crate::db::ApiKeyValidator;
use anyhow::Result;
use openraft::{BasicNode, RaftMetrics};
use serde_json;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{watch, RwLock};
use uuid::Uuid;

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

impl std::error::Error for GatewayStateError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            GatewayStateError::NotFound(_) => None,
            GatewayStateError::DeserializeError(e) => Some(e),
        }
    }
}

#[derive(Clone)]
pub struct GatewayState {
    state: Arc<StateMachineStore>,
    raft: Arc<RwLock<Raft>>,
    cluster_name: String,
    last_task_acquisition: Arc<AtomicU64>,
    key_validator: Arc<ApiKeyValidator>,
    task_manager: Arc<TaskManager>,
}

impl GatewayState {
    pub fn new(
        state: Arc<StateMachineStore>,
        raft: Arc<RwLock<Raft>>,
        cluster_name: String,
        last_task_acquisition: Arc<AtomicU64>,
        key_validator_updater: Arc<ApiKeyValidator>,
        task_manager: Arc<TaskManager>,
    ) -> Self {
        Self {
            state,
            raft,
            cluster_name,
            last_task_acquisition,
            key_validator: key_validator_updater,
            task_manager,
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

    pub async fn update_gateway_generic_key(&self, new_key: Option<Uuid>) -> Result<()> {
        let raft_guard = self.raft.write().await;

        let key_to_set = if self.state.get("generic_key").await.is_none() {
            new_key.unwrap_or_else(Uuid::new_v4)
        } else if let Some(key) = new_key {
            key
        } else {
            return Ok(());
        };

        let serialized_key = serde_json::to_string(&key_to_set)
            .map_err(|e| anyhow::anyhow!("Failed to serialize UUID to JSON: {}", e))?;

        raft_guard
            .client_write(Request::Set {
                key: "generic_key".to_string(),
                value: serialized_key,
            })
            .await
            .map_err(|e| anyhow::anyhow!("Failed to complete client_write: {}", e))?;

        Ok(())
    }

    pub async fn generic_key(&self) -> Option<Uuid> {
        self.state
            .get("generic_key")
            .await
            .and_then(|s: String| serde_json::from_str::<Uuid>(&s).ok())
    }

    pub async fn is_generic_key(&self, api_key: &Uuid) -> bool {
        match self
            .state
            .get("generic_key")
            .await
            .and_then(|s: String| serde_json::from_str::<Uuid>(&s).ok())
        {
            Some(generic_key) => &generic_key == api_key,
            None => false,
        }
    }

    pub fn is_valid_api_key(&self, api_key: &str) -> bool {
        self.key_validator.is_valid_api_key(api_key)
    }

    pub fn is_company_key(&self, api_key: &str) -> bool {
        self.key_validator.is_company_key(api_key)
    }

    pub fn get_user_id(&self, api_key: &str) -> Option<Uuid> {
        self.key_validator.get_user_id(api_key)
    }

    pub fn get_company_rate_limits(&self, api_key: &str) -> Option<(Uuid, (String, u64, u64))> {
        self.key_validator.get_company_info_from_key(api_key)
    }

    pub fn cluster_name(&self) -> &str {
        &self.cluster_name
    }

    pub fn task_manager(&self) -> Arc<TaskManager> {
        self.task_manager.clone()
    }
}
