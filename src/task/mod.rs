use bytes::Bytes;
use foldhash::HashMap as FoldHashMap;
use foldhash::HashSet;
use foldhash::fast::RandomState;
use scc::{HashMap, hash_map::Entry};
use serde::Serialize;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::api::request::AddTaskResultRequest;
use crate::crypto::hotkey::Hotkey;
use crate::db::EventRecorder;
use crate::metrics::{Metrics, TaskInProgressGuard};

struct TaskManagerInner {
    tasks: HashMap<Uuid, TaskState, RandomState>,
    expected_results: usize,
    result_lifetime: Duration,
    metrics: Metrics,
    worker_event_recorder: Option<EventRecorder>,
    expiration_queue: scc::Queue<ExpirationEvent>,
}

struct TaskState {
    execution_start: Option<Instant>,
    task_expires_at: Option<Instant>,
    last_result_instant: Option<Instant>,
    results_expires_at: Option<Instant>,
    prompt: Option<Arc<String>>,
    image: Option<Bytes>,
    model: Option<String>,
    results: Vec<AddTaskResultRequest>,
    success_count: usize,
    last_failure_reason: Option<Arc<str>>,
    first_success_worker_id: Option<Arc<str>>,
    assigned_workers: HashSet<Hotkey>,
    assigned_worker_ids: FoldHashMap<Hotkey, Arc<str>>,
    in_progress: FoldHashMap<Hotkey, TaskInProgressGuard>,
    seed: Option<i32>,
}

#[derive(Copy, Clone, Eq, PartialEq)]
enum ExpirationKind {
    Task = 0,
    Results = 1,
}

struct ExpirationEvent {
    task_id: Uuid,
    when: Instant,
    kind: ExpirationKind,
}

impl TaskManagerInner {
    fn new(
        initial_capacity: usize,
        expected_results: usize,
        result_lifetime: Duration,
        expiration_queue: scc::Queue<ExpirationEvent>,
        metrics: Metrics,
        worker_event_recorder: Option<EventRecorder>,
    ) -> Self {
        Self {
            expected_results,
            result_lifetime,
            metrics,
            worker_event_recorder,
            tasks: HashMap::with_capacity_and_hasher(initial_capacity, RandomState::default()),
            expiration_queue,
        }
    }

    fn schedule_task_expiration(&self, task_id: Uuid, when: Instant) {
        self.expiration_queue.push(ExpirationEvent {
            task_id,
            when,
            kind: ExpirationKind::Task,
        });
    }

    fn schedule_results_expiration(&self, task_id: Uuid, when: Instant) {
        self.expiration_queue.push(ExpirationEvent {
            task_id,
            when,
            kind: ExpirationKind::Results,
        });
    }
}

impl TaskState {
    fn new(task: &crate::api::Task, now: Instant, result_lifetime: Duration) -> Self {
        Self {
            execution_start: Some(now),
            task_expires_at: Some(now + result_lifetime),
            last_result_instant: None,
            results_expires_at: None,
            prompt: task.prompt.as_ref().map(Arc::clone),
            image: task.image.clone(),
            model: task.model.clone(),
            results: Vec::new(),
            success_count: 0,
            last_failure_reason: None,
            first_success_worker_id: None,
            assigned_workers: HashSet::default(),
            assigned_worker_ids: FoldHashMap::default(),
            in_progress: FoldHashMap::default(),
            seed: Some(task.seed),
        }
    }

    fn empty() -> Self {
        Self {
            execution_start: None,
            task_expires_at: None,
            last_result_instant: None,
            results_expires_at: None,
            prompt: None,
            image: None,
            model: None,
            results: Vec::new(),
            success_count: 0,
            last_failure_reason: None,
            first_success_worker_id: None,
            assigned_workers: HashSet::default(),
            assigned_worker_ids: FoldHashMap::default(),
            in_progress: FoldHashMap::default(),
            seed: None,
        }
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
        reason: Arc<str>,
    },
    PartialResult(usize),
    Success {
        worker_id: Arc<str>,
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

#[derive(Debug, Clone)]
pub struct AddResultOutcome {
    pub completed: bool,
    pub worker_hotkey: Hotkey,
    pub worker_id: Arc<str>,
    pub reason: Option<Arc<str>>,
    pub is_success: bool,
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
        worker_event_recorder: Option<EventRecorder>,
    ) -> Self {
        let inner = Arc::new(TaskManagerInner::new(
            initial_capacity,
            expected_results,
            result_lifetime,
            scc::Queue::default(),
            metrics,
            worker_event_recorder,
        ));

        let cancel_token = CancellationToken::new();
        let token_child = cancel_token.child_token();
        let handle = Self::spawn_cleanup(Arc::clone(&inner), token_child, cleanup_interval);

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
    ) -> tokio::task::JoinHandle<()> {
        task::spawn(async move {
            let mut heap: BinaryHeap<Reverse<(Instant, u8, u128)>> = BinaryHeap::new();
            loop {
                tokio::select! {
                                _ = token_child.cancelled() => break,
                                _ = tokio::time::sleep(cleanup_interval) => {
                                    while let Some(entry) = inner.expiration_queue.pop() {
                                        let event = &**entry;
                                        heap.push(Reverse((event.when, event.kind as u8, event.task_id.as_u128())));
                                    }
                                    let now = Instant::now();
                                    loop {
                                        let next = match heap.peek().copied() {
                                            Some(Reverse((when, kind, id))) if when <= now => {
                                                heap.pop();
                                                Some((when, kind, id))
                                            }
                                            _ => None,
                                        };

                                        let Some((when, kind, id)) = next else {
                                            break;
                                        };
                                        let task_id = Uuid::from_u128(id);
                                        let is_task_expiration = kind == ExpirationKind::Task as u8;
                                        match inner.tasks.entry_async(task_id).await {
                                            Entry::Occupied(mut entry) => {
                                                let state = entry.get_mut();
                                                if is_task_expiration {
                                                    let Some(expires_at) = state.task_expires_at else {
                                                        continue;
                                                    };
                                                    if expires_at != when {
                                                        if expires_at > now {
                                                            heap.push(Reverse((
                                                                expires_at,
                                                                ExpirationKind::Task as u8,
                                                                task_id.as_u128(),
                                                            )));
                                                        }
                                                        continue;
                                                    }
                                                    if expires_at > now {
                                                        heap.push(Reverse((
                                                            expires_at,
                                                            ExpirationKind::Task as u8,
                                                            task_id.as_u128(),
                                                        )));
                                                        continue;
                                                    }
                                                    let task_kind = if state.image.is_some() {
                                                        "img3d"
                                                    } else if state.prompt.is_some() {
                                                        "txt3d"
                                                    } else {
                                                        "unknown"
                                                    };
                                                    let assigned_workers = std::mem::take(&mut state.assigned_workers);
                                                    let mut assigned_worker_ids =
                                                        std::mem::take(&mut state.assigned_worker_ids);
                                                    for worker in assigned_workers {
                                                        let worker_hotkey: &str = worker.as_ref();
                                                        inner.metrics
                                                            .inc_timeout_failed(worker_hotkey)
                                                            .await;
                                                        if let Some(recorder) = inner.worker_event_recorder.as_ref() {
                                                            let worker_id =
                                                                assigned_worker_ids.remove(&worker);
                                                            recorder.record_worker_event(
                                                                Some(task_id),
                                                                worker_id.as_deref(),
                                                                "timeout",
                                                                task_kind,
                                                                None,
                                                            );
                                                        }
                                                    }
                                                    state.in_progress.clear();
                                                    state.prompt = None;
                                                    state.image = None;
                                                    state.model = None;
                                                    state.execution_start = None;
                                                    state.task_expires_at = None;
                                                } else {
                                                    let Some(expires_at) = state.results_expires_at else {
                                                        continue;
                                                    };
                                                    if expires_at != when {
                                                        if expires_at > now {
                                                            heap.push(Reverse((
                                                                expires_at,
                                                                ExpirationKind::Results as u8,
                                                                task_id.as_u128(),
                                                            )));
                                                        }
                                                        continue;
                                                    }
                                                    if expires_at > now {
                                                        heap.push(Reverse((
                                                            expires_at,
                                                            ExpirationKind::Results as u8,
                                                            task_id.as_u128(),
                                                        )));
                                                        continue;
                                                    }
                                                    state.results.clear();
                                                    state.success_count = 0;
                                                    state.last_failure_reason = None;
                                                    state.first_success_worker_id = None;
                                                    state.last_result_instant = None;
                                                    state.results_expires_at = None;
                                                    state.model = None;
                                                }
                                            }
                                            Entry::Vacant(_) => {}
                                        }
                                    }
                                }
                }
            }
        })
    }

    pub fn abort(&self) {
        self.cancel_token.cancel();
    }

    pub async fn add_result(
        &self,
        task_id: Uuid,
        result: AddTaskResultRequest,
    ) -> Result<AddResultOutcome, AddResultError> {
        let outcome = AddResultOutcome {
            completed: false,
            worker_hotkey: result.worker_hotkey.clone(),
            worker_id: result.worker_id.clone(),
            reason: result.reason.clone(),
            is_success: result.is_success(),
        };
        let worker = &result.worker_hotkey;
        let (completed, results_expires_at) = match self.inner.tasks.entry_async(task_id).await {
            Entry::Occupied(mut entry) => {
                let state = entry.get_mut();
                if !state.assigned_workers.contains(worker) {
                    return Err(AddResultError::NotAssigned);
                }
                state.assigned_workers.remove(worker);
                state.assigned_worker_ids.remove(worker);
                state.in_progress.remove(worker);
                if result.is_success() {
                    state.success_count += 1;
                    if state.first_success_worker_id.is_none() {
                        state.first_success_worker_id = Some(result.worker_id.clone());
                    }
                } else if let Some(reason) = result.reason.as_ref() {
                    state.last_failure_reason = Some(Arc::clone(reason));
                }
                state.last_result_instant = Some(result.instant);
                state.results_expires_at = Some(result.instant + self.inner.result_lifetime);
                state.results.push(result);
                (state.assigned_workers.is_empty(), state.results_expires_at)
            }
            Entry::Vacant(_) => {
                return Err(AddResultError::NotAssigned);
            }
        };
        if let Some(when) = results_expires_at {
            self.inner.schedule_results_expiration(task_id, when);
        }
        Ok(AddResultOutcome {
            completed,
            ..outcome
        })
    }

    pub async fn get_status(&self, task_id: Uuid) -> TaskStatus {
        let Some(state) = self.inner.tasks.get_async(&task_id).await else {
            return TaskStatus::NoResult;
        };

        if state.results.is_empty() {
            return TaskStatus::NoResult;
        }

        if let Some(reason) = state.last_failure_reason.clone() {
            return TaskStatus::Failure { reason };
        }

        if state.success_count < self.inner.expected_results {
            TaskStatus::PartialResult(state.success_count)
        } else if let Some(worker_id) = state.first_success_worker_id.clone() {
            TaskStatus::Success { worker_id }
        } else {
            TaskStatus::Failure {
                reason: Arc::<str>::from("No successful result found"),
            }
        }
    }

    pub async fn get_result(&self, task_id: Uuid) -> Option<TaskResultBundle> {
        match self.inner.tasks.entry_async(task_id).await {
            Entry::Occupied(mut entry) => {
                let state = entry.get_mut();
                let results = std::mem::take(&mut state.results);
                if results.is_empty() {
                    return None;
                }
                state.success_count = 0;
                state.last_failure_reason = None;
                state.first_success_worker_id = None;
                state.last_result_instant = None;
                state.results_expires_at = None;
                let model = state.model.take();
                Some(TaskResultBundle { results, model })
            }
            Entry::Vacant(_) => None,
        }
    }

    pub async fn add_task(&self, task: crate::api::Task) {
        let now = Instant::now();
        match self.inner.tasks.entry_async(task.id).await {
            Entry::Occupied(mut entry) => {
                let state = entry.get_mut();
                state.prompt = task.prompt.as_ref().map(Arc::clone);
                state.image = task.image.clone();
                state.model = task.model.clone();
                state.execution_start = Some(now);
                state.task_expires_at = Some(now + self.inner.result_lifetime);
                state.seed = Some(task.seed);
            }
            Entry::Vacant(entry) => {
                entry.insert_entry(TaskState::new(&task, now, self.inner.result_lifetime));
            }
        }
        self.inner
            .schedule_task_expiration(task.id, now + self.inner.result_lifetime);
    }

    #[cfg(test)]
    pub async fn get_model(&self, task_id: Uuid) -> Option<String> {
        self.inner
            .tasks
            .get_async(&task_id)
            .await
            .and_then(|entry| entry.model.clone())
    }

    pub async fn record_assignment(&self, task_id: Uuid, worker: Hotkey, worker_id: Arc<str>) {
        let should_track = match self.inner.tasks.entry_async(task_id).await {
            Entry::Occupied(mut entry) => {
                let state = entry.get_mut();
                state
                    .assigned_worker_ids
                    .insert(worker.clone(), worker_id.clone());

                state.assigned_workers.insert(worker.clone())
            }
            Entry::Vacant(entry) => {
                let mut state = TaskState::empty();
                state.assigned_worker_ids.insert(worker.clone(), worker_id);
                let inserted = state.assigned_workers.insert(worker.clone());
                entry.insert_entry(state);
                inserted
            }
        };
        if !should_track {
            return;
        }

        let guard = self.inner.metrics.start_task(worker.as_ref()).await;
        if let Entry::Occupied(mut entry) = self.inner.tasks.entry_async(task_id).await {
            let state = entry.get_mut();
            if state.assigned_workers.contains(&worker) {
                state.in_progress.insert(worker, guard);
            }
        }
    }

    pub async fn get_prompt(&self, task_id: Uuid) -> Option<Arc<String>> {
        self.inner
            .tasks
            .get_async(&task_id)
            .await
            .and_then(|entry| entry.prompt.as_ref().map(Arc::clone))
    }

    pub async fn get_image(&self, task_id: Uuid) -> Option<Bytes> {
        self.inner
            .tasks
            .get_async(&task_id)
            .await
            .and_then(|entry| entry.image.clone())
    }

    #[cfg(feature = "test-support")]
    #[allow(dead_code)]
    pub async fn get_seed(&self, task_id: Uuid) -> Option<i32> {
        self.inner
            .tasks
            .get_async(&task_id)
            .await
            .and_then(|entry| entry.seed)
    }

    pub async fn get_time(&self, task_id: Uuid) -> Option<f64> {
        self.inner
            .tasks
            .get_async(&task_id)
            .await
            .and_then(|entry| {
                entry
                    .execution_start
                    .map(|start| start.elapsed().as_secs_f64())
            })
    }

    pub async fn finalize_task(&self, task_id: Uuid) {
        if let Entry::Occupied(mut entry) = self.inner.tasks.entry_async(task_id).await {
            let state = entry.get_mut();
            state.prompt = None;
            state.image = None;
            state.execution_start = None;
            state.task_expires_at = None;
        }
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
