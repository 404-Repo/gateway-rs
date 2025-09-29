use bytes::Bytes;
use foldhash::fast::RandomState;
use scc::HashMap;
use serde::Serialize;
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::api::request::AddTaskResultRequest;
use crate::bittensor::hotkey::Hotkey;
use crate::metrics::Metrics;

struct TaskManagerInner {
    completed: HashMap<Uuid, Vec<AddTaskResultRequest>, RandomState>,
    expected_results: usize,
    execution_time: HashMap<Uuid, Instant, RandomState>,
    prompts: HashMap<Uuid, Arc<String>, RandomState>,
    images: HashMap<Uuid, Bytes, RandomState>,
    assigned_validators: HashMap<Uuid, Vec<Hotkey>, RandomState>,
    metrics: Metrics,
}

pub struct TaskManager {
    inner: Arc<TaskManagerInner>,
    cancel_token: CancellationToken,
    _cleanup_task: Arc<tokio::task::JoinHandle<()>>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub enum TaskStatus {
    NoResult,
    Failure {
        reason: String,
    },
    PartialResult(usize),
    Success {
        miner_hotkey: Option<Hotkey>,
        miner_uid: Option<u32>,
        miner_rating: Option<f32>,
    },
}

impl Default for TaskStatus {
    fn default() -> Self {
        Self::NoResult
    }
}

impl fmt::Display for TaskStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TaskStatus::NoResult => f.write_str("NoResult"),
            TaskStatus::Success { .. } => f.write_str("Success"),
            TaskStatus::Failure { .. } => f.write_str("Failure"),
            TaskStatus::PartialResult(n) => write!(f, "PartialResult({})", n),
        }
    }
}

impl TaskManager {
    pub async fn new(
        initial_capacity: usize,
        expected_results: usize,
        cleanup_interval: Duration,
        result_lifetime: Duration,
        metrics: Metrics,
    ) -> Self {
        let inner = Arc::new(TaskManagerInner {
            completed: HashMap::with_capacity_and_hasher(initial_capacity, RandomState::default()),
            expected_results,
            execution_time: HashMap::with_capacity_and_hasher(
                initial_capacity,
                RandomState::default(),
            ),
            prompts: HashMap::with_capacity_and_hasher(initial_capacity, RandomState::default()),
            images: HashMap::with_capacity_and_hasher(initial_capacity, RandomState::default()),
            assigned_validators: HashMap::with_capacity_and_hasher(
                initial_capacity,
                RandomState::default(),
            ),
            metrics,
        });

        let cancel_token = CancellationToken::new();
        let token_child = cancel_token.child_token();
        let handle = Self::spawn_cleanup(
            Arc::clone(&inner),
            token_child,
            cleanup_interval,
            result_lifetime,
        );

        Self {
            inner,
            cancel_token,
            _cleanup_task: Arc::new(handle),
        }
    }

    fn spawn_cleanup(
        inner: Arc<TaskManagerInner>,
        token_child: CancellationToken,
        cleanup_interval: Duration,
        result_lifetime: Duration,
    ) -> tokio::task::JoinHandle<()> {
        let inner_clone = Arc::clone(&inner);
        let result_lifetime_clone = result_lifetime;
        task::spawn(async move {
            let mut expired_ids: Vec<Uuid> = Vec::new();
            let mut timeouts_to_count: Vec<Hotkey> = Vec::new();
            loop {
                tokio::select! {
                    _ = token_child.cancelled() => break,
                    _ = tokio::time::sleep(cleanup_interval) => {
                        let now = Instant::now();
                        inner_clone.completed.retain_async(|_, results| {
                            results
                                .last()
                                .is_some_and(|last| now.duration_since(last.instant) < result_lifetime_clone)
                        }).await;

                        // Determine expired task ids based on execution_time and drop them
                        expired_ids.clear();
                        inner_clone
                            .execution_time
                            .retain_async(|task_id, start| {
                                let keep = now.duration_since(*start) < result_lifetime_clone;
                                if !keep {
                                    expired_ids.push(*task_id);
                                }
                                keep
                            }).await;

                        // For expired tasks, increment timeout failures for validators that did not submit any result
                        timeouts_to_count.clear();
                        for &task_id in &expired_ids {
                            if let Some((_k, validators)) = inner_clone.assigned_validators.remove_async(&task_id).await {
                                let had_results = inner_clone.completed.get_async(&task_id).await;
                                for validator in validators.iter() {
                                    let has_any = had_results
                                        .as_ref()
                                        .is_some_and(|vec| vec.iter().any(|r| r.validator_hotkey == *validator));
                                    if !has_any {
                                        timeouts_to_count.push(validator.clone());
                                    }
                                }
                            }
                        }

                        for hk in timeouts_to_count.drain(..) {
                            inner_clone.metrics.inc_timeout_failed(hk.as_ref()).await;
                        }

                        inner_clone
                            .prompts
                            .retain_async(|task_id, _| inner_clone.execution_time.contains_sync(task_id)).await;

                        inner_clone
                            .images
                            .retain_async(|task_id, _| inner_clone.execution_time.contains_sync(task_id)).await;
                    }
                }
            }
        })
    }

    pub fn abort(&self) {
        self.cancel_token.cancel();
    }

    pub async fn add_result(&self, task_id: Uuid, result: AddTaskResultRequest) {
        self.inner
            .completed
            .entry_async(task_id)
            .await
            .or_default()
            .push(result);
    }

    pub async fn get_status(&self, task_id: Uuid) -> TaskStatus {
        match self.inner.completed.get_async(&task_id).await {
            Some(results) if !results.is_empty() => {
                if let Some(reason) = results.iter().rev().find_map(|r| r.reason.clone()) {
                    return TaskStatus::Failure { reason };
                }
                let mut success_count: usize = 0;
                let mut best_index: Option<usize> = None;
                let mut best_score: f32 = f32::NEG_INFINITY;

                for (idx, result) in results.iter().enumerate() {
                    if result.is_success() {
                        success_count += 1;
                        let score = result.get_score().unwrap_or(0.0);
                        if score >= best_score {
                            best_score = score;
                            best_index = Some(idx);
                        }
                    }
                }

                if success_count < self.inner.expected_results {
                    TaskStatus::PartialResult(success_count)
                } else if let Some(index) = best_index {
                    let best = &results[index];
                    TaskStatus::Success {
                        miner_hotkey: best.miner_hotkey.clone(),
                        miner_uid: best.miner_uid,
                        miner_rating: best.miner_rating,
                    }
                } else {
                    TaskStatus::Failure {
                        reason: "No successful result found".to_string(),
                    }
                }
            }
            _ => TaskStatus::NoResult,
        }
    }

    pub async fn get_result(&self, task_id: Uuid) -> Option<Vec<AddTaskResultRequest>> {
        self.inner
            .completed
            .remove_async(&task_id)
            .await
            .map(|(_, value)| value)
    }

    pub async fn add_task(&self, task: crate::api::Task) {
        let _ = self
            .inner
            .execution_time
            .insert_async(task.id, Instant::now())
            .await;
        if let Some(prompt) = &task.prompt {
            let _ = self
                .inner
                .prompts
                .insert_async(task.id, Arc::clone(prompt))
                .await;
        }
        if let Some(image) = &task.image {
            let _ = self.inner.images.insert_async(task.id, image.clone()).await;
        }
    }

    pub async fn record_assignment(&self, task_id: Uuid, validator: Hotkey) {
        let mut set = self
            .inner
            .assigned_validators
            .entry_async(task_id)
            .await
            .or_default();
        if !set.contains(&validator) {
            set.push(validator);
        }
    }

    pub async fn get_prompt(&self, task_id: Uuid) -> Option<Arc<String>> {
        self.inner
            .prompts
            .get_async(&task_id)
            .await
            .map(|entry| Arc::clone(entry.get()))
    }

    pub async fn get_image(&self, task_id: Uuid) -> Option<Bytes> {
        self.inner
            .images
            .get_async(&task_id)
            .await
            .map(|entry| entry.get().clone())
    }

    pub async fn get_time(&self, task_id: Uuid) -> Option<f64> {
        self.inner
            .execution_time
            .get_async(&task_id)
            .await
            .map(|start| start.elapsed().as_secs_f64())
    }

    pub async fn remove_time(&self, task_id: Uuid) {
        self.inner.execution_time.remove_async(&task_id).await;
        self.inner.prompts.remove_async(&task_id).await;
        self.inner.images.remove_async(&task_id).await;
    }
}

impl Clone for TaskManager {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            cancel_token: self.cancel_token.clone(),
            _cleanup_task: Arc::clone(&self._cleanup_task),
        }
    }
}

impl Drop for TaskManager {
    fn drop(&mut self) {
        if Arc::strong_count(&self.inner) == 1 {
            self._cleanup_task.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use crate::api::request::AddTaskResultRequest;
    use crate::api::Task;
    use crate::bittensor::hotkey::Hotkey;
    use crate::common::task::TaskManager;
    use crate::common::task::TaskStatus;
    use bytes::Bytes;
    use std::time::Duration;
    use std::time::Instant;

    #[tokio::test]
    async fn test_cleanup() {
        const CLEANUP_INTERVAL: Duration = Duration::from_millis(200);
        const RESULT_LIFETIME: Duration = Duration::from_millis(400);
        const EXPECTED_RESULTS: usize = 2;
        const WAIT_DURATION: Duration = Duration::from_millis(600);

        fn make_result(
            validator: &str,
            miner: &str,
            score: f32,
            instant: Instant,
        ) -> AddTaskResultRequest {
            AddTaskResultRequest {
                validator_hotkey: validator.parse().unwrap(),
                miner_hotkey: Some(miner.parse().unwrap()),
                miner_uid: None,
                miner_rating: None,
                asset: Some(vec![]),
                score: Some(score),
                reason: None,
                instant,
            }
        }

        let task_manager = TaskManager::new(
            10,
            EXPECTED_RESULTS,
            CLEANUP_INTERVAL,
            RESULT_LIFETIME,
            crate::metrics::Metrics::new(0.05).unwrap(),
        )
        .await;
        let now = Instant::now();

        // Task 1: Incomplete, old result
        let task_id1 = Uuid::new_v4();
        task_manager
            .add_result(
                task_id1,
                make_result(
                    "5GTmkzxbXSFh8ApLU24fzWUu2asZs89V5eJnN3ufubTg9Pj7",
                    "5GTmkzxbXSFh8ApLU24fzWUu2asZs89V5eJnN3ufubTg9Pj7",
                    0.5,
                    now - Duration::from_millis(600),
                ),
            )
            .await;

        // Task 2: Complete, last result will expire
        let task_id2 = Uuid::new_v4();
        task_manager
            .add_result(
                task_id2,
                make_result(
                    "5GTmkzxbXSFh8ApLU24fzWUu2asZs89V5eJnN3ufubTg9Pj7",
                    "5GTmkzxbXSFh8ApLU24fzWUu2asZs89V5eJnN3ufubTg9Pj7",
                    0.6,
                    now - Duration::from_millis(600),
                ),
            )
            .await;
        task_manager
            .add_result(
                task_id2,
                make_result(
                    "5GTmkzxbXSFh8ApLU24fzWUu2asZs89V5eJnN3ufubTg9Pj7",
                    "5GTmkzxbXSFh8ApLU24fzWUu2asZs89V5eJnN3ufubTg9Pj7",
                    0.7,
                    now - Duration::from_millis(200),
                ),
            )
            .await;

        // Task 3: Complete, last result recent but will expire
        let task_id3 = Uuid::new_v4();
        task_manager
            .add_result(
                task_id3,
                make_result(
                    "5GTmkzxbXSFh8ApLU24fzWUu2asZs89V5eJnN3ufubTg9Pj7",
                    "5GTmkzxbXSFh8ApLU24fzWUu2asZs89V5eJnN3ufubTg9Pj7",
                    0.8,
                    now - Duration::from_millis(100),
                ),
            )
            .await;
        task_manager
            .add_result(
                task_id3,
                make_result(
                    "5GTmkzxbXSFh8ApLU24fzWUu2asZs89V5eJnN3ufubTg9Pj7",
                    "5GTmkzxbXSFh8ApLU24fzWUu2asZs89V5eJnN3ufubTg9Pj7",
                    0.9,
                    now - Duration::from_millis(100),
                ),
            )
            .await;

        assert_eq!(
            task_manager.get_status(task_id1).await,
            TaskStatus::PartialResult(1)
        );
        assert_ne!(
            task_manager.get_status(task_id2).await,
            TaskStatus::PartialResult(1)
        );
        assert_ne!(
            task_manager.get_status(task_id3).await,
            TaskStatus::PartialResult(1)
        );

        tokio::time::sleep(WAIT_DURATION).await;

        assert_eq!(
            task_manager.get_status(task_id1).await,
            TaskStatus::NoResult
        );
        assert_eq!(
            task_manager.get_status(task_id2).await,
            TaskStatus::NoResult
        );
        assert_eq!(
            task_manager.get_status(task_id3).await,
            TaskStatus::NoResult
        );
    }

    #[tokio::test]
    async fn test_timeout_increments_metric() {
        const CLEANUP_INTERVAL: Duration = Duration::from_millis(50);
        const RESULT_LIFETIME: Duration = Duration::from_millis(120);

        let metrics = crate::metrics::Metrics::new(0.05).unwrap();
        let task_manager =
            TaskManager::new(8, 1, CLEANUP_INTERVAL, RESULT_LIFETIME, metrics.clone()).await;

        let task_id = Uuid::new_v4();
        let task = Task {
            id: task_id,
            prompt: None,
            image: Some(Bytes::from_static(b"")),
        };
        task_manager.add_task(task).await;

        let validator: Hotkey = Hotkey::from_bytes(&[42u8; 32]);
        task_manager
            .record_assignment(task_id, validator.clone())
            .await;

        tokio::time::sleep(Duration::from_millis(
            RESULT_LIFETIME.as_millis() as u64 + 200,
        ))
        .await;

        let families = metrics.registry().gather();
        let mut found = false;
        for mf in families {
            if mf.name() == "timeout_failures_total" {
                for m in &mf.metric {
                    let matched = m
                        .label
                        .iter()
                        .any(|lp| lp.name() == "validator" && lp.value() == validator.to_string());
                    if matched && m.get_counter().as_ref().is_some_and(|c| c.value() >= 1.0) {
                        found = true;
                        break;
                    }
                }
            }
            if found {
                break;
            }
        }
        assert!(found, "timeout_failures_total for validator was not found");
    }
}
