use serde::Deserialize;
use serde::Serialize;

use super::response::GatewayInfo;

// It should be confirmed that the caller has permission to add the task before proceeding with its use.
// The Gateway will assign a unique ID, which will be included in the response body.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddTaskRequest {
    pub api_key: String,
    pub prompt: String,
}

// Validator hotkey must be verified
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetTasksRequest {
    pub hotkey: String,
    pub signature: String,
    pub timestamp: String,
    pub requested_task_count: usize,
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
        }
    }
}
