use bytes::Bytes;
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;
use std::time::Instant;
use uuid::Uuid;

pub use super::gateway_info::{GatewayInfoExt, GatewayInfoExtRef};
use crate::crypto::hotkey::Hotkey;

#[derive(Debug, Clone, Deserialize)]
pub struct AddTaskRequest {
    pub seed: Option<SeedValue>,
    pub prompt: Option<String>,
    pub model: Option<String>,
    pub model_params: Option<Value>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum SeedValue {
    Signed(i32),
    Unsigned(u32),
}

impl SeedValue {
    pub fn into_i32(self) -> i32 {
        match self {
            SeedValue::Signed(value) => value,
            SeedValue::Unsigned(value) => value as i32,
        }
    }
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

#[derive(Clone)]
pub struct AddTaskResultRequest {
    pub worker_hotkey: Hotkey,
    pub worker_id: Arc<str>,
    pub assignment_token: Uuid,
    pub asset: Option<Bytes>,
    pub reason: Option<Arc<str>>,
    pub instant: Instant,
}

impl AddTaskResultRequest {
    pub fn get_asset(&mut self) -> Option<Bytes> {
        self.asset.take()
    }

    pub fn is_success(&self) -> bool {
        self.reason.is_none()
    }
}
