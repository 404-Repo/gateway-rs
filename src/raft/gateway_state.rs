use super::store::Request;
use super::{Raft, StateMachineStore};
use crate::api::response::GatewayInfo;
use anyhow::Result;
use serde_json;
use std::fmt;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug)]
pub enum StateReaderError {
    NotFound(String),
    DeserializeError(serde_json::Error),
}

impl fmt::Display for StateReaderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StateReaderError::NotFound(key) => write!(f, "No gateway info found for key '{}'", key),
            StateReaderError::DeserializeError(e) => {
                write!(f, "Error deserializing gateway info: {}", e)
            }
        }
    }
}

impl std::error::Error for StateReaderError {}

#[derive(Clone)]
pub struct GatewayState {
    state: Arc<StateMachineStore>,
    raft: Arc<RwLock<Raft>>,
}

impl GatewayState {
    pub fn new(state: Arc<StateMachineStore>, raft: Arc<RwLock<Raft>>) -> Self {
        Self { state, raft }
    }

    pub async fn leader(&self) -> Option<u64> {
        self.raft.read().await.metrics().borrow().current_leader
    }

    pub async fn membership(&self) -> Vec<u64> {
        self.raft
            .read()
            .await
            .metrics()
            .borrow()
            .membership_config
            .membership()
            .nodes()
            .map(|(&id, _)| id)
            .collect()
    }

    pub async fn gateway(&self, n: u64) -> Result<GatewayInfo, StateReaderError> {
        let guard = self.state.state_machine.read().await;
        let key = n.to_string();
        let json_str = guard
            .data
            .get(&key)
            .ok_or_else(|| StateReaderError::NotFound(key.clone()))?;
        serde_json::from_str::<GatewayInfo>(json_str).map_err(StateReaderError::DeserializeError)
    }

    pub async fn gateways(&self, nodes: Vec<u64>) -> Result<Vec<GatewayInfo>, StateReaderError> {
        let guard = self.state.state_machine.read().await;

        nodes
            .into_iter()
            .map(|n| {
                let key = n.to_string();
                let json_str = guard
                    .data
                    .get(&key)
                    .ok_or_else(|| StateReaderError::NotFound(key.clone()))?;
                serde_json::from_str::<GatewayInfo>(json_str)
                    .map_err(StateReaderError::DeserializeError)
            })
            .collect()
    }

    pub async fn set_gateway_info(&self, info: GatewayInfo) -> Result<()> {
        let gateway_info_str = serde_json::to_string(&info)?;

        let request = Request::Set {
            key: info.node_id.to_string(),
            value: gateway_info_str,
        };

        self.raft.write().await.client_write(request).await?;
        Ok(())
    }
}
