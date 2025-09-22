use serde::Deserialize;
use serde::Serialize;
use std::time::Instant;
use uuid::Uuid;

use super::response::GatewayInfo;
use crate::bittensor::hotkey::Hotkey;

#[derive(Debug, Clone, Deserialize)]
pub struct AddTaskRequest {
    pub prompt: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GetTasksRequest {
    pub validator_hotkey: Hotkey,
    pub signature: String,
    pub timestamp: String,
    pub requested_task_count: usize,
}

#[derive(Debug, Deserialize)]
pub struct GetTaskResultRequest {
    pub id: Uuid,
    #[serde(default)]
    pub all: bool,
    pub format: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct GetTaskStatus {
    pub id: Uuid,
}

#[derive(Deserialize)]
pub struct AddTaskResultRequest {
    pub validator_hotkey: Hotkey,
    pub miner_hotkey: Option<Hotkey>,
    pub miner_uid: Option<u32>,
    pub miner_rating: Option<f32>,
    pub asset: Option<Vec<u8>>,
    pub score: Option<f32>,
    pub reason: Option<String>,
    #[serde(skip_deserializing, default = "Instant::now")]
    pub instant: Instant,
}

impl AddTaskResultRequest {
    pub fn get_score(&self) -> Option<f32> {
        self.score
    }

    pub fn get_asset(&mut self) -> Option<Vec<u8>> {
        self.asset.take()
    }

    pub fn into_asset(self) -> Option<Vec<u8>> {
        self.asset
    }

    pub fn is_success(&self) -> bool {
        self.reason.is_none()
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct UpdateGenericKeyRequest {
    pub generic_key: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GatewayInfoExt {
    pub node_id: u64,
    pub domain: String,
    pub ip: String,
    pub name: String,
    pub http_port: u16,
    pub available_tasks: usize,
    // Used to check if the request is valid
    pub cluster_name: String,
    pub last_task_acquisition: u64,
    pub last_update: u64,
}

impl From<GatewayInfoExt> for GatewayInfo {
    fn from(info: GatewayInfoExt) -> Self {
        GatewayInfo {
            node_id: info.node_id,
            domain: info.domain,
            ip: info.ip,
            name: info.name,
            http_port: info.http_port,
            available_tasks: info.available_tasks,
            last_task_acquisition: info.last_task_acquisition,
            last_update: info.last_update,
        }
    }
}
