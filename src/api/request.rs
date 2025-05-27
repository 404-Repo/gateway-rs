use serde::Deserialize;
use serde::Serialize;
use std::time::Instant;
use uuid::Uuid;

use super::response::GatewayInfo;

#[derive(Debug, Clone, Deserialize)]
pub struct AddTaskRequest {
    pub prompt: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GetTasksRequest {
    pub validator_hotkey: String,
    pub signature: String,
    pub timestamp: String,
    pub requested_task_count: usize,
}

#[derive(Debug, Deserialize)]
pub struct GetTaskResultRequest {
    pub id: Uuid,
    #[serde(default)]
    pub all: bool,
}

#[derive(Debug, Deserialize)]
pub struct GetTaskStatus {
    pub id: Uuid,
}

#[derive(Deserialize)]
pub struct AddTaskResultRequest {
    pub validator_hotkey: String,
    pub miner_hotkey: Option<String>,
    pub result: TaskResultType,
    #[serde(skip_deserializing, default = "Instant::now")]
    pub instant: Instant,
}

impl AddTaskResultRequest {
    pub fn get_score(&self) -> Option<f32> {
        match &self.result {
            TaskResultType::Success { score, .. } => Some(*score),
            TaskResultType::Failure { .. } => None,
        }
    }

    pub fn get_asset(&mut self) -> Option<Vec<u8>> {
        if let TaskResultType::Success { asset, .. } = &mut self.result {
            Some(std::mem::take(asset))
        } else {
            None
        }
    }

    pub fn into_asset(self) -> Option<Vec<u8>> {
        match self.result {
            TaskResultType::Success { asset, .. } => Some(asset),
            TaskResultType::Failure { .. } => None,
        }
    }

    pub fn is_success(&self) -> bool {
        matches!(self.result, TaskResultType::Success { .. })
    }
}

#[derive(Deserialize)]
pub enum TaskResultType {
    Success { asset: Vec<u8>, score: f32 },
    Failure { reason: String },
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
