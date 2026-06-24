use bytes::Bytes;
use foldhash::{HashSet, fast::RandomState};
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;
use std::time::Instant;
use uuid::Uuid;

pub use super::gateway_info::{GatewayInfoExt, GatewayInfoExtRef};
use crate::crypto::hotkey::Hotkey;

pub const MAX_WORKER_TAG_LENGTH: usize = 32;

pub fn normalize_worker_tags(tags: &[String]) -> Result<Vec<String>, String> {
    let mut normalized = Vec::with_capacity(tags.len());
    let mut seen = HashSet::with_capacity_and_hasher(tags.len(), RandomState::default());
    for tag in tags {
        let tag = tag.trim().to_ascii_lowercase();
        if tag.is_empty() {
            return Err("worker_tags cannot contain empty tags".to_string());
        }
        if tag.len() > MAX_WORKER_TAG_LENGTH {
            return Err(format!(
                "worker_tags entries cannot exceed {MAX_WORKER_TAG_LENGTH} characters"
            ));
        }
        if !tag.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_' || byte == b'-'
        }) {
            return Err(
                "worker_tags entries may only contain lowercase letters, numbers, underscores, or dashes"
                    .to_string(),
            );
        }
        if seen.insert(tag.clone()) {
            normalized.push(tag);
        }
    }
    Ok(normalized)
}

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
    #[serde(default)]
    pub worker_tags: Vec<String>,
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
