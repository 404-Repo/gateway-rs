use serde::Deserialize;
use serde::Serialize;
use std::sync::Arc;
use std::time::Instant;
use uuid::Uuid;

use super::response::GatewayInfo;
use crate::crypto::hotkey::Hotkey;

#[derive(Debug, Clone, Deserialize)]
pub struct AddTaskRequest {
    pub prompt: Option<String>,
    pub model: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum ModelFilter {
    One(String),
    Many(Vec<String>),
}

impl ModelFilter {
    pub fn to_vec(&self) -> Vec<String> {
        match self {
            ModelFilter::One(value) => vec![value.clone()],
            ModelFilter::Many(values) => values.clone(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct GetTasksRequest {
    #[serde(alias = "validator_hotkey")]
    pub worker_hotkey: Hotkey,
    pub worker_id: Arc<str>,
    pub signature: String,
    pub timestamp: String,
    pub requested_task_count: usize,
    pub model: ModelFilter,
}

#[derive(Debug, Deserialize)]
pub struct GetTaskResultRequest {
    pub id: Uuid,
    #[serde(default)]
    pub all: bool,
    // Legacy: format=ply is deprecated in favor of compress=0.
    pub format: Option<String>,
    // Accepts 1/0/true/false. Only applies to 3DGS output.
    pub compress: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct GetTaskStatus {
    pub id: Uuid,
}

#[derive(Deserialize)]
pub struct AddTaskResultRequest {
    #[serde(alias = "validator_hotkey")]
    pub worker_hotkey: Hotkey,
    pub worker_id: Arc<str>,
    pub asset: Option<Vec<u8>>,
    pub reason: Option<Arc<str>>,
    #[serde(skip_deserializing, default = "Instant::now")]
    pub instant: Instant,
}

impl AddTaskResultRequest {
    pub fn get_asset(&mut self) -> Option<Vec<u8>> {
        self.asset.take()
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

#[derive(Debug, Serialize)]
pub struct GatewayInfoExtRef<'a> {
    pub node_id: u64,
    pub domain: &'a str,
    pub ip: &'a str,
    pub name: &'a str,
    pub http_port: u16,
    pub available_tasks: usize,
    pub cluster_name: &'a str,
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
