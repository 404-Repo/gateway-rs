use serde::Deserialize;
use serde::Serialize;

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoadResponse {
    pub gateways: Vec<GatewayInfo>,
}

// It provides a vector of tasks, as well as number of tasks available per gateway
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetTasksResponse {
    pub tasks: Vec<Task>,
    pub gateways: Vec<GatewayInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaderResponse {
    pub leader_id: u64,
    pub domain: String,
    pub ip: String,
    pub http_port: u16,
}
