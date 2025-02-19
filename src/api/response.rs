use serde::Deserialize;
use serde::Serialize;

use super::Task;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Gateway {
    // The identifier for this node.
    pub id: u64,
    // domain of the node
    pub domain: String,
    // Available tasks on this gateway
    pub available_tasks: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoadResponse {
    pub gateways: Vec<Gateway>,
}

// It provides a vector of tasks, as well as number of tasks available per gateway
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetTasksResponse {
    pub tasks: Vec<Task>,
    pub gateways: Vec<Gateway>,
}
