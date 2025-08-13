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
    pub last_update: u64,
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
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

impl From<TaskStatus> for GetTaskStatusResponse {
    fn from(value: TaskStatus) -> Self {
        let status_str = value.to_string();
        match value {
            TaskStatus::Failure { reason } => Self {
                status: status_str,
                reason: Some(reason),
            },
            _ => Self {
                status: status_str,
                reason: None,
            },
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct LeaderResponse {
    pub leader_id: u64,
    pub domain: String,
    pub ip: String,
    pub http_port: u16,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::task::TaskStatus;

    #[test]
    fn converts_no_result() {
        let resp: GetTaskStatusResponse = TaskStatus::NoResult.into();
        assert_eq!(resp.status, "NoResult");
        assert!(resp.reason.is_none());
        let json = serde_json::to_string(&resp).unwrap();
        assert_eq!(json, "{\"status\":\"NoResult\"}");
    }

    #[test]
    fn converts_success() {
        let resp: GetTaskStatusResponse = TaskStatus::Success.into();
        assert_eq!(resp.status, "Success");
        assert!(resp.reason.is_none());
        let json = serde_json::to_string(&resp).unwrap();
        assert_eq!(json, "{\"status\":\"Success\"}");
    }

    #[test]
    fn converts_partial_result() {
        let resp: GetTaskStatusResponse = TaskStatus::PartialResult(3).into();
        assert_eq!(resp.status, "PartialResult(3)");
        assert!(resp.reason.is_none());
        let json = serde_json::to_string(&resp).unwrap();
        assert_eq!(json, "{\"status\":\"PartialResult(3)\"}");
    }

    #[test]
    fn converts_failure_with_reason() {
        let resp: GetTaskStatusResponse = TaskStatus::Failure {
            reason: "boom".into(),
        }
        .into();
        assert_eq!(resp.status, "Failure");
        assert_eq!(resp.reason.as_deref(), Some("boom"));
        let json = serde_json::to_string(&resp).unwrap();
        assert_eq!(json, "{\"status\":\"Failure\",\"reason\":\"boom\"}");
    }
}
