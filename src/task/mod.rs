use bytes::Bytes;
use foldhash::HashMap as FoldHashMap;
use foldhash::HashSet;
use foldhash::fast::RandomState;
use scc::{HashMap, hash_map::Entry};
use serde::Serialize;
use serde_json::json;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task;
use tokio_util::sync::CancellationToken;
use tracing::warn;
use uuid::Uuid;

use crate::api::request::AddTaskResultRequest;
use crate::common::rate_limit_buffer::RateLimitMutationBuffer;
use crate::crypto::hotkey::Hotkey;
use crate::db::EventRecorder;
use crate::http3::rate_limits::RateLimitReservation;
use crate::metrics::{Metrics, TaskInProgressGuard, TaskKind};

const TASK_TIMED_OUT_REASON: &str = "Task timed out";
const PENDING_RESULT_COMMIT_GRACE: Duration = Duration::from_secs(5);
const RATE_LIMIT_COMPLETION_REQUEST_ID_XOR: u128 = 0x7b2f_4d91_a6c3_e805_19fe_42ac_55d0_3b71;

pub(crate) fn rate_limit_completion_request_id(task_id: Uuid) -> u128 {
    task_id.as_u128() ^ RATE_LIMIT_COMPLETION_REQUEST_ID_XOR
}

struct TaskManagerInnerInit {
    initial_capacity: usize,
    expected_results: usize,
    result_lifetime: Duration,
    expiration_queue: scc::Queue<ExpirationEvent>,
    rate_limit_mutation_queue: RateLimitMutationBuffer,
    metrics: Metrics,
    worker_event_recorder: Option<EventRecorder>,
}

pub struct TaskManagerInit {
    pub initial_capacity: usize,
    pub expected_results: usize,
    pub cleanup_interval: Duration,
    pub result_lifetime: Duration,
    pub rate_limit_mutation_queue: RateLimitMutationBuffer,
    pub metrics: Metrics,
    pub worker_event_recorder: Option<EventRecorder>,
}

struct TaskManagerInner {
    tasks: HashMap<Uuid, TaskState, RandomState>,
    expected_results: usize,
    result_lifetime: Duration,
    metrics: Metrics,
    worker_event_recorder: Option<EventRecorder>,
    expiration_queue: scc::Queue<ExpirationEvent>,
    rate_limit_mutation_queue: RateLimitMutationBuffer,
}

struct TaskState {
    execution_start: Option<Instant>,
    task_expires_at: Option<Instant>,
    last_result_instant: Option<Instant>,
    results_expires_at: Option<Instant>,
    task_kind: TaskKind,
    prompt: Option<Arc<String>>,
    image: Option<Bytes>,
    model: Option<String>,
    results: Vec<AddTaskResultRequest>,
    success_count: usize,
    last_failure_reason: Option<Arc<str>>,
    first_success_worker_id: Option<Arc<str>>,
    assigned_workers: HashSet<Hotkey>,
    assigned_worker_ids: FoldHashMap<Hotkey, Arc<str>>,
    assigned_assignment_tokens: FoldHashMap<Hotkey, Uuid>,
    in_progress: FoldHashMap<Hotkey, TaskInProgressGuard>,
    pending_results: FoldHashMap<Hotkey, AddTaskResultRequest>,
    seed: Option<i32>,
    finished_results_count: usize,
    rate_limit_reservation: Option<RateLimitReservation>,
}

fn task_kind_from_task(task: &crate::api::Task) -> TaskKind {
    if task.image.is_some() {
        TaskKind::ImageTo3D
    } else {
        TaskKind::TextTo3D
    }
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
    fn new(init: TaskManagerInnerInit) -> Self {
        let TaskManagerInnerInit {
            initial_capacity,
            expected_results,
            result_lifetime,
            expiration_queue,
            rate_limit_mutation_queue,
            metrics,
            worker_event_recorder,
        } = init;
        Self {
            expected_results,
            result_lifetime,
            metrics,
            worker_event_recorder,
            tasks: HashMap::with_capacity_and_hasher(initial_capacity, RandomState::default()),
            expiration_queue,
            rate_limit_mutation_queue,
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
            task_kind: task_kind_from_task(task),
            prompt: task.prompt.as_ref().map(Arc::clone),
            image: task.image.clone(),
            model: task.model.clone(),
            results: Vec::new(),
            success_count: 0,
            last_failure_reason: None,
            first_success_worker_id: None,
            assigned_workers: HashSet::default(),
            assigned_worker_ids: FoldHashMap::default(),
            assigned_assignment_tokens: FoldHashMap::default(),
            in_progress: FoldHashMap::default(),
            pending_results: FoldHashMap::default(),
            seed: Some(task.seed),
            finished_results_count: 0,
            rate_limit_reservation: None,
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

pub struct TaskActivityMetadata {
    pub task_kind: &'static str,
    pub model: Option<String>,
}

#[derive(Debug)]
pub enum AddResultError {
    NotAssigned,
    AlreadyStaged,
    PendingResultMissing,
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
    fn has_active_assignment(
        state: &TaskState,
        worker: &Hotkey,
        worker_id: &Arc<str>,
        assignment_token: Option<Uuid>,
    ) -> bool {
        if !state.assigned_workers.contains(worker) {
            return false;
        }
        if state
            .assigned_worker_ids
            .get(worker)
            .is_none_or(|stored_worker_id| stored_worker_id.as_ref() != worker_id.as_ref())
        {
            return false;
        }
        match assignment_token {
            Some(token) => state
                .assigned_assignment_tokens
                .get(worker)
                .is_some_and(|stored_token| *stored_token == token),
            None => true,
        }
    }

    fn apply_committed_result(
        state: &mut TaskState,
        result: AddTaskResultRequest,
        expected_results: usize,
        result_lifetime: Duration,
    ) -> (
        AddResultOutcome,
        Option<Instant>,
        Option<(RateLimitReservation, bool)>,
    ) {
        let outcome = AddResultOutcome {
            completed: false,
            worker_hotkey: result.worker_hotkey.clone(),
            worker_id: result.worker_id.clone(),
            reason: result.reason.clone(),
            is_success: result.is_success(),
        };
        let worker = &result.worker_hotkey;

        state.assigned_workers.remove(worker);
        state.assigned_worker_ids.remove(worker);
        state.assigned_assignment_tokens.remove(worker);
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
        state.results_expires_at = Some(result.instant + result_lifetime);
        state.results.push(result);
        state.finished_results_count = state.finished_results_count.saturating_add(1);

        let assignments_exhausted = state.assigned_workers.is_empty();
        let task_completed =
            assignments_exhausted && state.finished_results_count >= expected_results;
        let completed_reservation = if task_completed {
            Self::take_terminal_completion_reservation(state)
        } else {
            None
        };

        (
            AddResultOutcome {
                completed: task_completed,
                ..outcome
            },
            state.results_expires_at,
            completed_reservation,
        )
    }

    #[cfg(test)]
    pub async fn new(
        initial_capacity: usize,
        expected_results: usize,
        cleanup_interval: Duration,
        result_lifetime: Duration,
        metrics: Metrics,
        worker_event_recorder: Option<EventRecorder>,
    ) -> Self {
        Self::new_with_rate_limit_mutation_queue(TaskManagerInit {
            initial_capacity,
            expected_results,
            cleanup_interval,
            result_lifetime,
            rate_limit_mutation_queue: RateLimitMutationBuffer::default(),
            metrics,
            worker_event_recorder,
        })
        .await
    }

    pub async fn new_with_rate_limit_mutation_queue(init: TaskManagerInit) -> Self {
        let TaskManagerInit {
            initial_capacity,
            expected_results,
            cleanup_interval,
            result_lifetime,
            rate_limit_mutation_queue,
            metrics,
            worker_event_recorder,
        } = init;

        let inner = Arc::new(TaskManagerInner::new(TaskManagerInnerInit {
            initial_capacity,
            expected_results,
            result_lifetime,
            expiration_queue: scc::Queue::default(),
            rate_limit_mutation_queue,
            metrics,
            worker_event_recorder,
        }));

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
                                        let mut completed_reservation = None;
                                        let mut schedule_results_expiration = None;
                                        let mut timed_out_workers = Vec::new();
                                        let mut timed_out_task_kind = None;
                                        let mut timed_out_model = None;
                                        let mut timed_out_elapsed_secs = None;
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
                                                    if !state.pending_results.is_empty() {
                                                        let deferred_expires_at =
                                                            now + PENDING_RESULT_COMMIT_GRACE;
                                                        state.task_expires_at =
                                                            Some(deferred_expires_at);
                                                        heap.push(Reverse((
                                                            deferred_expires_at,
                                                            ExpirationKind::Task as u8,
                                                            task_id.as_u128(),
                                                        )));
                                                        continue;
                                                    }
                                                    let task_kind = state.task_kind.label();
                                                    let assigned_workers = std::mem::take(&mut state.assigned_workers);
                                                    let mut assigned_worker_ids =
                                                        std::mem::take(&mut state.assigned_worker_ids);
                                                    state.assigned_assignment_tokens.clear();
                                                    for worker in assigned_workers {
                                                        let worker_id = assigned_worker_ids.remove(&worker);
                                                        timed_out_workers.push((worker, worker_id));
                                                    }
                                                    timed_out_task_kind = Some(task_kind);
                                                    timed_out_model = state.model.clone();
                                                    timed_out_elapsed_secs = state
                                                        .execution_start
                                                        .map(|start| start.elapsed().as_secs_f64());
                                                    state.in_progress.clear();
                                                    state.prompt = None;
                                                    state.image = None;
                                                    state.model = None;
                                                    state.execution_start = None;
                                                    state.task_expires_at = None;
                                                    if state.success_count == 0
                                                        && state.last_failure_reason.is_none()
                                                    {
                                                        state.last_failure_reason =
                                                            Some(Arc::<str>::from(
                                                                TASK_TIMED_OUT_REASON,
                                                            ));
                                                    }
                                                    if let Some(completion) =
                                                        Self::take_terminal_completion_reservation(state)
                                                    {
                                                        completed_reservation = Some(completion);
                                                    }
                                                    if state
                                                        .results_expires_at
                                                        .is_none_or(|expires_at| expires_at <= now)
                                                    {
                                                        let terminal_expires_at =
                                                            now + inner.result_lifetime;
                                                        state.results_expires_at =
                                                            Some(terminal_expires_at);
                                                        schedule_results_expiration =
                                                            Some(terminal_expires_at);
                                                    }
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
                                                    state.finished_results_count = 0;
                                                    state.rate_limit_reservation = None;
                                                    if state.task_expires_at.is_none() {
                                                        let _ = entry.remove_entry();
                                                    }
                                                }
                                            }
                                            Entry::Vacant(_) => {}
                                        }
                                        if let Some(task_kind) = timed_out_task_kind {
                                            if let Some(elapsed_secs) = timed_out_elapsed_secs {
                                                warn!(
                                                    "Task {}, kind {}, timed out after {:.2}s",
                                                    task_id, task_kind, elapsed_secs
                                                );
                                            } else {
                                                warn!(
                                                    "Task {}, kind {}, timed out",
                                                    task_id, task_kind
                                                );
                                            }
                                            for (worker, worker_id) in timed_out_workers {
                                                inner.metrics.inc_timeout_failed(worker.as_ref()).await;
                                                if let Some(recorder) = inner.worker_event_recorder.as_ref() {
                                                    let metadata = json!({
                                                        "worker_hotkey": worker.as_ref(),
                                                    });
                                                    recorder.record_worker_event(
                                                        Some(task_id),
                                                        worker_id.as_deref(),
                                                        "timeout",
                                                        task_kind,
                                                        timed_out_model.as_deref(),
                                                        None,
                                                        Some(&metadata),
                                                    );
                                                }
                                            }
                                        }
                                        if let Some(when) = schedule_results_expiration {
                                            inner.schedule_results_expiration(task_id, when);
                                        }
                                        if let Some((reservation, success)) = completed_reservation {
                                            Self::emit_completion(inner.as_ref(), task_id, reservation, success)
                                                .await;
                                        }
                                    }
                                    inner.metrics.maybe_evict_stale_workers().await;
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
        let worker = &result.worker_hotkey;
        let assignment_token =
            (!result.assignment_token.is_nil()).then_some(result.assignment_token);
        let (outcome, results_expires_at, completed_reservation) = match self
            .inner
            .tasks
            .entry_async(task_id)
            .await
        {
            Entry::Occupied(mut entry) => {
                let state = entry.get_mut();
                if !Self::has_active_assignment(state, worker, &result.worker_id, assignment_token)
                {
                    return Err(AddResultError::NotAssigned);
                }
                if state.pending_results.contains_key(worker) {
                    return Err(AddResultError::AlreadyStaged);
                }
                let (outcome, results_expires_at, reservation) = Self::apply_committed_result(
                    state,
                    result,
                    self.inner.expected_results,
                    self.inner.result_lifetime,
                );
                (outcome, results_expires_at, reservation)
            }
            Entry::Vacant(_) => {
                return Err(AddResultError::NotAssigned);
            }
        };
        if let Some((reservation, success)) = completed_reservation {
            Self::emit_completion(self.inner.as_ref(), task_id, reservation, success).await;
        }
        if let Some(when) = results_expires_at {
            self.inner.schedule_results_expiration(task_id, when);
        }
        Ok(outcome)
    }

    pub async fn stage_result(
        &self,
        task_id: Uuid,
        result: AddTaskResultRequest,
    ) -> Result<(), AddResultError> {
        let worker = result.worker_hotkey.clone();
        let assignment_token =
            (!result.assignment_token.is_nil()).then_some(result.assignment_token);
        match self.inner.tasks.entry_async(task_id).await {
            Entry::Occupied(mut entry) => {
                let state = entry.get_mut();
                if !Self::has_active_assignment(state, &worker, &result.worker_id, assignment_token)
                {
                    return Err(AddResultError::NotAssigned);
                }
                if state.pending_results.contains_key(&worker) {
                    return Err(AddResultError::AlreadyStaged);
                }
                state.pending_results.insert(worker, result);
                Ok(())
            }
            Entry::Vacant(_) => Err(AddResultError::NotAssigned),
        }
    }

    pub async fn commit_staged_result(
        &self,
        task_id: Uuid,
        worker: &Hotkey,
    ) -> Result<AddResultOutcome, AddResultError> {
        let (outcome, results_expires_at, completed_reservation) =
            match self.inner.tasks.entry_async(task_id).await {
                Entry::Occupied(mut entry) => {
                    let state = entry.get_mut();
                    let Some(staged_result) = state.pending_results.get(worker) else {
                        return Err(AddResultError::PendingResultMissing);
                    };
                    let assignment_token = (!staged_result.assignment_token.is_nil())
                        .then_some(staged_result.assignment_token);
                    if !Self::has_active_assignment(
                        state,
                        worker,
                        &staged_result.worker_id,
                        assignment_token,
                    ) {
                        return Err(AddResultError::NotAssigned);
                    }
                    let Some(result) = state.pending_results.remove(worker) else {
                        return Err(AddResultError::PendingResultMissing);
                    };
                    let (outcome, results_expires_at, reservation) = Self::apply_committed_result(
                        state,
                        result,
                        self.inner.expected_results,
                        self.inner.result_lifetime,
                    );
                    (outcome, results_expires_at, reservation)
                }
                Entry::Vacant(_) => return Err(AddResultError::NotAssigned),
            };
        if let Some((reservation, success)) = completed_reservation {
            Self::emit_completion(self.inner.as_ref(), task_id, reservation, success).await;
        }
        if let Some(when) = results_expires_at {
            self.inner.schedule_results_expiration(task_id, when);
        }
        Ok(outcome)
    }

    pub async fn rollback_staged_result(&self, task_id: Uuid, worker: &Hotkey) {
        if let Entry::Occupied(mut entry) = self.inner.tasks.entry_async(task_id).await {
            entry.get_mut().pending_results.remove(worker);
        }
    }

    pub async fn get_status(&self, task_id: Uuid) -> TaskStatus {
        let Some(state) = self.inner.tasks.get_async(&task_id).await else {
            return TaskStatus::NoResult;
        };

        let expected_results = self.inner.expected_results;
        let exhausted_expected_results = state.finished_results_count >= expected_results;
        let can_still_receive_more_results =
            !state.assigned_workers.is_empty() || state.task_expires_at.is_some();

        if state.results.is_empty() {
            if !can_still_receive_more_results
                && let Some(reason) = state.last_failure_reason.clone()
            {
                return TaskStatus::Failure { reason };
            }
            return TaskStatus::NoResult;
        }

        if !exhausted_expected_results && can_still_receive_more_results {
            return TaskStatus::PartialResult(state.success_count);
        }

        if let Some(worker_id) = state.first_success_worker_id.clone() {
            TaskStatus::Success { worker_id }
        } else if let Some(reason) = state.last_failure_reason.clone() {
            TaskStatus::Failure { reason }
        } else {
            TaskStatus::Failure {
                reason: Arc::<str>::from("No successful result found"),
            }
        }
    }

    pub async fn get_result(&self, task_id: Uuid) -> Option<TaskResultBundle> {
        match self.inner.tasks.get_async(&task_id).await {
            Some(entry) => {
                if entry.results.is_empty() {
                    return None;
                }
                let results = entry.results.clone();
                let model = entry.model.clone();
                Some(TaskResultBundle { results, model })
            }
            None => None,
        }
    }

    #[cfg(test)]
    pub async fn add_task(&self, task: crate::api::Task) {
        self.add_task_with_rate_limit_reservation(&task, None).await;
    }

    pub async fn add_task_with_rate_limit_reservation(
        &self,
        task: &crate::api::Task,
        rate_limit_reservation: Option<RateLimitReservation>,
    ) {
        let now = Instant::now();
        match self.inner.tasks.entry_async(task.id).await {
            Entry::Occupied(mut entry) => {
                let state = entry.get_mut();
                state.task_kind = task_kind_from_task(task);
                state.prompt = task.prompt.as_ref().map(Arc::clone);
                state.image = task.image.clone();
                state.model = task.model.clone();
                state.execution_start = Some(now);
                state.task_expires_at = Some(now + self.inner.result_lifetime);
                state.seed = Some(task.seed);
                state.finished_results_count = 0;
                state.rate_limit_reservation = rate_limit_reservation;
            }
            Entry::Vacant(entry) => {
                let mut state = TaskState::new(task, now, self.inner.result_lifetime);
                state.rate_limit_reservation = rate_limit_reservation;
                entry.insert_entry(state);
            }
        }
        self.inner
            .schedule_task_expiration(task.id, now + self.inner.result_lifetime);
    }

    fn take_terminal_completion_reservation(
        state: &mut TaskState,
    ) -> Option<(RateLimitReservation, bool)> {
        state
            .rate_limit_reservation
            .take()
            .map(|reservation| (reservation, state.success_count > 0))
    }

    async fn emit_completion(
        inner: &TaskManagerInner,
        task_id: Uuid,
        reservation: RateLimitReservation,
        success: bool,
    ) {
        let request_id = rate_limit_completion_request_id(task_id);
        if success {
            reservation.rollback_pending_success().await;
            if let Some(batch) = reservation.success_batch_with_request_id(request_id) {
                inner.rate_limit_mutation_queue.push(batch).await;
            }
            return;
        }

        reservation.rollback_pending().await;
        if let Some(batch) = reservation.refund_batch_with_request_id(request_id) {
            inner.rate_limit_mutation_queue.push(batch).await;
        }
    }

    pub async fn get_model(&self, task_id: Uuid) -> Option<String> {
        self.inner
            .tasks
            .get_async(&task_id)
            .await
            .and_then(|entry| entry.model.clone())
    }

    pub async fn get_activity_metadata(&self, task_id: Uuid) -> Option<TaskActivityMetadata> {
        self.inner
            .tasks
            .get_async(&task_id)
            .await
            .map(|entry| TaskActivityMetadata {
                task_kind: entry.task_kind.label(),
                model: entry.model.clone(),
            })
    }

    pub async fn record_assignment(&self, task_id: Uuid, worker: Hotkey, worker_id: Arc<str>) {
        self.record_assignment_with_token(task_id, worker, worker_id, Uuid::nil())
            .await;
    }

    pub async fn record_assignment_with_token(
        &self,
        task_id: Uuid,
        worker: Hotkey,
        worker_id: Arc<str>,
        assignment_token: Uuid,
    ) {
        let should_track = match self.inner.tasks.entry_async(task_id).await {
            Entry::Occupied(mut entry) => {
                let state = entry.get_mut();
                state
                    .assigned_worker_ids
                    .insert(worker.clone(), worker_id.clone());
                if state
                    .assigned_assignment_tokens
                    .get(&worker)
                    .is_some_and(|stored_token| *stored_token != assignment_token)
                {
                    state.pending_results.remove(&worker);
                }
                state
                    .assigned_assignment_tokens
                    .insert(worker.clone(), assignment_token);

                state.assigned_workers.insert(worker.clone())
            }
            Entry::Vacant(_) => {
                warn!(
                    task_id = %task_id,
                    "record_assignment called for unknown task_id; skipping"
                );
                return;
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

    pub async fn is_assigned(&self, task_id: Uuid, worker: &Hotkey) -> bool {
        self.inner
            .tasks
            .get_async(&task_id)
            .await
            .is_some_and(|entry| entry.assigned_workers.contains(worker))
    }

    pub async fn is_assigned_with_token(
        &self,
        task_id: Uuid,
        worker: &Hotkey,
        assignment_token: Uuid,
    ) -> bool {
        self.inner
            .tasks
            .get_async(&task_id)
            .await
            .is_some_and(|entry| {
                entry
                    .assigned_worker_ids
                    .get(worker)
                    .is_some_and(|worker_id| {
                        Self::has_active_assignment(
                            &entry,
                            worker,
                            worker_id,
                            Some(assignment_token),
                        )
                    })
            })
    }

    pub async fn is_assigned_to_worker_id(
        &self,
        task_id: Uuid,
        worker: &Hotkey,
        worker_id: &Arc<str>,
    ) -> bool {
        self.inner
            .tasks
            .get_async(&task_id)
            .await
            .is_some_and(|entry| Self::has_active_assignment(&entry, worker, worker_id, None))
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

    #[cfg(test)]
    pub fn tracked_task_count(&self) -> usize {
        self.inner.tasks.len()
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
