use serde::Deserialize;
use serde::Serialize;

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
    pub requested_task_count: usize,
}
