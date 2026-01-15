use bytes::Bytes;
use foldhash::HashSet;
use foldhash::fast::RandomState;
use scc::{HashMap, hash_map::Entry};
use serde::Serialize;
use std::fmt;
use std::hash::Hash;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::api::request::AddTaskResultRequest;
use crate::crypto::hotkey::Hotkey;
use crate::metrics::{Metrics, TaskInProgressGuard};

struct TaskManagerInner {
    completed: HashMap<Uuid, Vec<AddTaskResultRequest>, RandomState>,
    expected_results: usize,
    execution_time: HashMap<Uuid, Instant, RandomState>,
    prompts: HashMap<Uuid, Arc<String>, RandomState>,
    images: HashMap<Uuid, Bytes, RandomState>,
    models: HashMap<Uuid, String, RandomState>,
    assigned_workers: HashMap<Uuid, HashSet<Hotkey>, RandomState>,
    in_progress: HashMap<(Uuid, Hotkey), TaskInProgressGuard, RandomState>,
    metrics: Metrics,
}

impl TaskManagerInner {
    fn new(initial_capacity: usize, expected_results: usize, metrics: Metrics) -> Self {
        Self {
            completed: Self::hash_map(initial_capacity),
            expected_results,
            execution_time: Self::hash_map(initial_capacity),
            prompts: Self::hash_map(initial_capacity),
            images: Self::hash_map(initial_capacity),
            models: Self::hash_map(initial_capacity),
            assigned_workers: Self::hash_map(initial_capacity),
            in_progress: Self::hash_map(initial_capacity),
            metrics,
        }
    }

    fn hash_map<K, V>(capacity: usize) -> HashMap<K, V, RandomState>
    where
        K: Eq + Hash,
    {
        HashMap::with_capacity_and_hasher(capacity, RandomState::default())
    }

    async fn finish_task(&self, task_id: Uuid, worker: &Hotkey) {
        let key = (task_id, worker.clone());
        let _ = self.in_progress.remove_async(&key).await;
    }

    async fn complete_assignment(&self, task_id: Uuid, worker: &Hotkey) -> bool {
        match self.assigned_workers.entry_async(task_id).await {
            Entry::Occupied(mut entry) => {
                entry.get_mut().remove(worker);
                if entry.get().is_empty() {
                    let _ = entry.remove_entry();
                    true
                } else {
                    false
                }
            }
            Entry::Vacant(_) => true,
        }
    }

    async fn cleanup_task_payload(&self, task_id: Uuid) {
        self.prompts.remove_async(&task_id).await;
        self.images.remove_async(&task_id).await;
    }

    async fn cleanup_task_metadata(&self, task_id: Uuid) {
        self.models.remove_async(&task_id).await;
    }
}

pub struct TaskManager {
    inner: Arc<TaskManagerInner>,
    cancel_token: CancellationToken,
    _cleanup_task: Arc<tokio::task::JoinHandle<()>>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Default)]
pub enum TaskStatus {
    #[default]
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

pub struct TaskResultBundle {
    pub results: Vec<AddTaskResultRequest>,
    pub model: Option<String>,
}

#[derive(Debug)]
pub enum AddResultError {
    NotAssigned,
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
        let inner = Arc::new(TaskManagerInner::new(
            initial_capacity,
            expected_results,
            metrics,
        ));

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
        task::spawn(async move {
            let mut expired_ids: Vec<Uuid> = Vec::new();
            let mut expired_results: Vec<Uuid> = Vec::new();
            loop {
                tokio::select! {
                _ = token_child.cancelled() => break,
                _ = tokio::time::sleep(cleanup_interval) => {
                    let now = Instant::now();
                    expired_results.clear();
                    inner.completed.retain_async(|task_id, results| {
                        let keep = results
                            .last()
                            .is_some_and(|last| now.duration_since(last.instant) < result_lifetime);
                        if !keep {
                            expired_results.push(*task_id);
                        }
                        keep
                    }).await;
                    for task_id in &expired_results {
                        inner.cleanup_task_metadata(*task_id).await;
                    }

                    // Determine expired task ids based on execution_time and drop them
                    expired_ids.clear();
                    inner
                        .execution_time
                            .retain_async(|task_id, start| {
                                let keep = now.duration_since(*start) < result_lifetime;
                                if !keep {
                                    expired_ids.push(*task_id);
                                }
                                keep
                            }).await;

                        // For expired tasks, increment timeout failures for workers that did not submit any result
                        for &task_id in &expired_ids {
                            if let Some((_, workers)) = inner.assigned_workers.remove_async(&task_id).await {
                                let had_results = inner.completed.get_async(&task_id).await;
                                for worker in workers {
                                    if !Self::has_result(&had_results, &worker) {
                                        inner.metrics.inc_timeout_failed(worker.as_ref()).await;
                                    }
                                    inner.finish_task(task_id, &worker).await;
                                }
                            }
                            inner.cleanup_task_payload(task_id).await;
                            inner.cleanup_task_metadata(task_id).await;
                        }
                    }
                }
            }
        })
    }

    fn has_result(
        results: &Option<impl std::ops::Deref<Target = Vec<AddTaskResultRequest>>>,
        worker: &Hotkey,
    ) -> bool {
        results
            .as_ref()
            .is_some_and(|vec| vec.iter().any(|r| &r.worker_hotkey == worker))
    }

    pub fn abort(&self) {
        self.cancel_token.cancel();
    }

    pub async fn add_result(
        &self,
        task_id: Uuid,
        result: AddTaskResultRequest,
    ) -> Result<bool, AddResultError> {
        let worker = result.worker_hotkey.clone();
        let is_assigned = self
            .inner
            .assigned_workers
            .read_async(&task_id, |_, workers| workers.contains(&worker))
            .await
            .unwrap_or(false);
        if !is_assigned {
            return Err(AddResultError::NotAssigned);
        }
        self.inner
            .completed
            .entry_async(task_id)
            .await
            .or_default()
            .push(result);
        self.inner.finish_task(task_id, &worker).await;
        Ok(self.inner.complete_assignment(task_id, &worker).await)
    }

    pub async fn get_status(&self, task_id: Uuid) -> TaskStatus {
        let Some(results) = self.inner.completed.get_async(&task_id).await else {
            return TaskStatus::NoResult;
        };

        if results.is_empty() {
            return TaskStatus::NoResult;
        }

        if let Some(reason) = results.iter().rev().find_map(|r| r.reason.clone()) {
            return TaskStatus::Failure { reason };
        }

        let (success_count, best_index) = results
            .iter()
            .enumerate()
            .filter(|(_, r)| r.is_success())
            .fold(
                (0, None),
                |(count, best): (usize, Option<(usize, f32)>), (idx, result)| {
                    let score = result.get_score().unwrap_or(0.0);
                    let new_best = match best {
                        Some((_, best_score)) if score < best_score => best,
                        _ => Some((idx, score)),
                    };
                    (count + 1, new_best)
                },
            );

        if success_count < self.inner.expected_results {
            TaskStatus::PartialResult(success_count)
        } else if let Some((index, _)) = best_index {
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

    pub async fn get_result(&self, task_id: Uuid) -> Option<TaskResultBundle> {
        let results = self
            .inner
            .completed
            .remove_async(&task_id)
            .await
            .map(|(_, value)| value)?;
        let model = self
            .inner
            .models
            .remove_async(&task_id)
            .await
            .map(|(_, value)| value);
        Some(TaskResultBundle { results, model })
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
        if let Some(model) = &task.model {
            let _ = self.inner.models.insert_async(task.id, model.clone()).await;
        }
    }

    #[cfg(test)]
    pub async fn get_model(&self, task_id: Uuid) -> Option<String> {
        self.inner
            .models
            .get_async(&task_id)
            .await
            .map(|entry| entry.get().clone())
    }

    pub async fn record_assignment(&self, task_id: Uuid, worker: Hotkey) {
        let should_track = {
            let mut entry = self
                .inner
                .assigned_workers
                .entry_async(task_id)
                .await
                .or_default();
            entry.get_mut().insert(worker.clone())
        };
        if should_track {
            let guard = self.inner.metrics.start_task(worker.as_ref()).await;
            let _ = self
                .inner
                .in_progress
                .insert_async((task_id, worker), guard)
                .await;
        }
    }

    pub async fn get_prompt(&self, task_id: Uuid) -> Option<Arc<String>> {
        self.inner
            .prompts
            .get_async(&task_id)
            .await
            .map(|e| Arc::clone(e.get()))
    }

    pub async fn get_image(&self, task_id: Uuid) -> Option<Bytes> {
        self.inner
            .images
            .get_async(&task_id)
            .await
            .map(|e| e.get().clone())
    }

    pub async fn get_time(&self, task_id: Uuid) -> Option<f64> {
        self.inner
            .execution_time
            .get_async(&task_id)
            .await
            .map(|e| e.elapsed().as_secs_f64())
    }

    pub async fn finalize_task(&self, task_id: Uuid) {
        self.inner.execution_time.remove_async(&task_id).await;
        self.inner.cleanup_task_payload(task_id).await;
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
mod tests;
