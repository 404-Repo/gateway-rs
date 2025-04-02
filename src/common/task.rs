use foldhash::fast::RandomState;
use scc::HashMap;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[derive(Deserialize)]
pub struct TaskResultScored {
    pub validator_hotkey: String,
    pub miner_hotkey: String,
    pub asset: Vec<u8>,
    pub score: f32,

    #[serde(skip_deserializing, default = "Instant::now")]
    pub instant: Instant,
}

struct TaskManagerInner {
    completed: HashMap<Uuid, Vec<TaskResultScored>, RandomState>,
    expected_results: usize,
}

pub struct TaskManager {
    inner: Arc<TaskManagerInner>,
    cancel_token: CancellationToken,
}

#[derive(Debug, Clone, Serialize)]
pub enum TaskStatus {
    NoResults,
    PartialResult,
    Completed,
}

impl TaskManager {
    pub async fn new(
        initial_capacity: usize,
        expected_results: usize,
        cleanup_interval: Duration,
        result_lifetime: Duration,
    ) -> Arc<Self> {
        let inner = Arc::new(TaskManagerInner {
            completed: HashMap::with_capacity_and_hasher(initial_capacity, RandomState::default()),
            expected_results,
        });

        let cancel_token = CancellationToken::new();
        let token_child = cancel_token.child_token();
        let inner_clone = Arc::clone(&inner);

        task::spawn(async move {
            loop {
                tokio::select! {
                    _ = token_child.cancelled() => break,
                    _ = tokio::time::sleep(cleanup_interval) => {
                        let now = Instant::now();
                        inner_clone.completed.retain(|_, results| {
                            results.len() < inner_clone.expected_results ||
                            results.last().is_some_and(|last| now.duration_since(last.instant) < result_lifetime)
                        });
                    }
                }
            }
        });

        Arc::new(Self {
            inner,
            cancel_token,
        })
    }

    pub fn abort(&self) {
        self.cancel_token.cancel();
    }

    pub async fn add_result(&self, task_id: Uuid, result: TaskResultScored) {
        self.inner
            .completed
            .entry(task_id)
            .or_default()
            .push(result);
    }

    pub async fn get_status(&self, task_id: Uuid, expected_results: usize) -> TaskStatus {
        match self.inner.completed.get(&task_id) {
            Some(guard) if !guard.is_empty() => {
                if guard.len() < expected_results {
                    TaskStatus::PartialResult
                } else {
                    TaskStatus::Completed
                }
            }
            _ => TaskStatus::NoResults,
        }
    }

    pub async fn get_result(&self, task_id: Uuid) -> Option<Vec<TaskResultScored>> {
        self.inner
            .completed
            .remove(&task_id)
            .map(|(_, value)| value)
    }
}

impl Clone for TaskManager {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            cancel_token: self.cancel_token.clone(),
        }
    }
}
