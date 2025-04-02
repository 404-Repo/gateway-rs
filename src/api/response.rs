use serde::Deserialize;
use serde::Serialize;
use uuid::Uuid;

use crate::common::task::TaskStatus;

use super::Task;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GatewayInfo {
    pub node_id: u64,
    pub domain: String,
    pub ip: String,
    pub name: String,
    pub http_port: u16,
    pub available_tasks: usize,
    pub last_task_acquisition: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct LoadResponse {
    pub gateways: Vec<GatewayInfo>,
}

#[derive(Debug, Clone, Serialize)]
pub struct GenericKeyResponse {
    pub generic_key: Uuid,
}

// It provides a vector of tasks, as well as number of tasks available per gateway
#[derive(Debug, Clone, Serialize)]
pub struct GetTasksResponse {
    pub tasks: Vec<Task>,
    pub gateways: Vec<GatewayInfo>,
}

#[derive(Debug, Clone, Serialize)]
pub struct GetTaskStatusResponse {
    pub status: TaskStatus,
}

#[derive(Debug, Clone, Serialize)]
pub struct LeaderResponse {
    pub leader_id: u64,
    pub domain: String,
    pub ip: String,
    pub http_port: u16,
}
