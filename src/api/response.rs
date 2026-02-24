use serde::Serialize;
use uuid::Uuid;

use crate::task::TaskStatus;

use super::Task;
pub use super::gateway_info::{GatewayInfo, GatewayInfoRef};

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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worker_id: Option<String>,
}

impl From<TaskStatus> for GetTaskStatusResponse {
    fn from(status: TaskStatus) -> Self {
        let status_str = status.to_string();
        match status {
            TaskStatus::Failure { reason } => Self {
                status: status_str,
                reason: Some(reason.to_string()),
                worker_id: None,
            },
            TaskStatus::Success { worker_id } => Self {
                status: status_str,
                reason: None,
                worker_id: Some(worker_id.to_string()),
            },
            _ => Self {
                status: status_str,
                reason: None,
                worker_id: None,
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
    use crate::task::TaskStatus;
    use std::sync::Arc;

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
        let resp: GetTaskStatusResponse = TaskStatus::Success {
            worker_id: Arc::<str>::from("worker-123"),
        }
        .into();
        assert_eq!(resp.status, "Success");
        assert!(resp.reason.is_none());
        assert_eq!(resp.worker_id.as_deref(), Some("worker-123"));

        let json = serde_json::to_string(&resp).unwrap();
        assert_eq!(json, r#"{"status":"Success","worker_id":"worker-123"}"#);
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
            reason: Arc::<str>::from("boom"),
        }
        .into();
        assert_eq!(resp.status, "Failure");
        assert_eq!(resp.reason.as_deref(), Some("boom"));
        let json = serde_json::to_string(&resp).unwrap();
        assert_eq!(json, "{\"status\":\"Failure\",\"reason\":\"boom\"}");
    }

    #[test]
    fn serializes_gettasks_with_prompt() {
        use std::sync::Arc;
        use uuid::Uuid;

        let id = Uuid::parse_str("123e4567-e89b-12d3-a456-426614174000").unwrap();
        let task = Task {
            id,
            prompt: Some(Arc::new("mechanic robot".to_string())),
            image: None,
            model: Some("404-3dgs".to_string()),
            seed: 0,
        };

        let resp = GetTasksResponse {
            tasks: vec![task],
            gateways: vec![],
        };

        let json = serde_json::to_string(&resp).unwrap();
        assert_eq!(
            json,
            "{\"tasks\":[{\"id\":\"123e4567-e89b-12d3-a456-426614174000\",\"prompt\":\"mechanic robot\",\"model\":\"404-3dgs\",\"seed\":0}],\"gateways\":[]}"
        );
    }

    #[test]
    fn serializes_gettasks_with_image() {
        use bytes::Bytes;
        use uuid::Uuid;

        let id = Uuid::parse_str("123e4567-e89b-12d3-a456-426614174001").unwrap();
        let task = Task {
            id,
            prompt: None,
            image: Some(Bytes::from(vec![1u8, 2u8, 3u8])),
            model: Some("404-mesh".to_string()),
            seed: 0,
        };

        let resp = GetTasksResponse {
            tasks: vec![task],
            gateways: vec![],
        };

        let json = serde_json::to_string(&resp).unwrap();
        assert_eq!(
            json,
            "{\"tasks\":[{\"id\":\"123e4567-e89b-12d3-a456-426614174001\",\"image\":\"AQID\",\"model\":\"404-mesh\",\"seed\":0}],\"gateways\":[]}"
        );
    }
}
