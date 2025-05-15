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
    execution_time: HashMap<Uuid, Instant, RandomState>,
}

pub struct TaskManager {
    inner: Arc<TaskManagerInner>,
    cancel_token: CancellationToken,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub enum TaskStatus {
    NoResult,
    PartialResult(usize),
    Completed,
}

impl Default for TaskStatus {
    fn default() -> Self {
        Self::NoResult
    }
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
            execution_time: HashMap::with_capacity_and_hasher(
                initial_capacity,
                RandomState::default(),
            ),
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
                let count = guard.len();
                if count < expected_results {
                    TaskStatus::PartialResult(count)
                } else {
                    TaskStatus::Completed
                }
            }
            _ => TaskStatus::NoResult,
        }
    }

    pub async fn get_result(&self, task_id: Uuid) -> Option<Vec<TaskResultScored>> {
        self.inner
            .completed
            .remove(&task_id)
            .map(|(_, value)| value)
    }

    pub fn add_time(&self, task_id: Uuid) {
        let _ = self.inner.execution_time.insert(task_id, Instant::now());
    }

    pub fn get_time(&self, task_id: Uuid) -> f64 {
        if let Some(start) = self.inner.execution_time.get(&task_id) {
            start.elapsed().as_secs_f64()
        } else {
            0.0
        }
    }

    pub fn remove_time(&self, task_id: Uuid) {
        self.inner.execution_time.remove(&task_id);
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

#[tokio::test]
async fn test_cleanup() {
    const CLEANUP_INTERVAL: Duration = Duration::from_millis(200);
    const RESULT_LIFETIME: Duration = Duration::from_millis(400);
    const EXPECTED_RESULTS: usize = 2;
    const WAIT_DURATION: Duration = Duration::from_millis(600);

    fn make_result(validator: &str, miner: &str, score: f32, instant: Instant) -> TaskResultScored {
        TaskResultScored {
            validator_hotkey: validator.to_string(),
            miner_hotkey: miner.to_string(),
            asset: vec![],
            score,
            instant,
        }
    }

    let task_manager =
        TaskManager::new(10, EXPECTED_RESULTS, CLEANUP_INTERVAL, RESULT_LIFETIME).await;
    let now = Instant::now();

    // Task 1: Incomplete, old result
    let task_id1 = Uuid::new_v4();
    task_manager
        .add_result(
            task_id1,
            make_result("val1", "miner1", 0.5, now - Duration::from_millis(600)),
        )
        .await;

    // Task 2: Complete, last result will expire
    let task_id2 = Uuid::new_v4();
    task_manager
        .add_result(
            task_id2,
            make_result("val2a", "miner2a", 0.6, now - Duration::from_millis(600)),
        )
        .await;
    task_manager
        .add_result(
            task_id2,
            make_result("val2b", "miner2b", 0.7, now - Duration::from_millis(200)),
        )
        .await;

    // Task 3: Complete, last result recent but will expire
    let task_id3 = Uuid::new_v4();
    task_manager
        .add_result(
            task_id3,
            make_result("val3a", "miner3a", 0.8, now - Duration::from_millis(100)),
        )
        .await;
    task_manager
        .add_result(
            task_id3,
            make_result("val3b", "miner3b", 0.9, now - Duration::from_millis(100)),
        )
        .await;

    assert_eq!(
        task_manager.get_status(task_id1, EXPECTED_RESULTS).await,
        TaskStatus::PartialResult(1)
    );
    assert_eq!(
        task_manager.get_status(task_id2, EXPECTED_RESULTS).await,
        TaskStatus::Completed
    );
    assert_eq!(
        task_manager.get_status(task_id3, EXPECTED_RESULTS).await,
        TaskStatus::Completed
    );

    tokio::time::sleep(WAIT_DURATION).await;

    assert_eq!(
        task_manager.get_status(task_id1, EXPECTED_RESULTS).await,
        TaskStatus::PartialResult(1)
    );
    assert_eq!(
        task_manager.get_status(task_id2, EXPECTED_RESULTS).await,
        TaskStatus::NoResult
    );
    assert_eq!(
        task_manager.get_status(task_id3, EXPECTED_RESULTS).await,
        TaskStatus::NoResult
    );
}
