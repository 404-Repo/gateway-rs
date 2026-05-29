use crossbeam_skiplist::SkipMap;
use foldhash::fast::RandomState;
use foldhash::{HashMap as FoldHashMap, HashSet as FoldHashSet};
use moka::sync::Cache;
use prometheus::IntGauge;
use scc::HashMap;
use scc::hash_map::Entry as SccEntry;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::task;
use uuid::Uuid;

use crate::api::{HasUuid, Task};
use crate::crypto::hotkey::Hotkey;

const SENTMAP_START_CAPACITY: usize = 1024;
const ACTIVE_ID_START_CAPACITY: usize = 1024;
const DEFAULT_DUP_COUNT: usize = 1;
const DEFAULT_TTL_SECS: u64 = 300;
const DEFAULT_CLEANUP_INTERVAL_SECS: u64 = 1;
// Soft aggregate targets for per-model routing caches. The budget is divided
// across configured model buckets, with a minimum of one entry per bucket, so
// pathological model counts can exceed this by requiring one cache slot each.
const EXHAUSTED_CACHE_SOFT_TOTAL_CAPACITY: u64 = 16_384;
const SCAN_CURSOR_CACHE_SOFT_TOTAL_CAPACITY: u64 = 16_384;
const MAX_CLEANUP_SCAN_PER_TICK: usize = 64;
const DEFAULT_RESERVATION_SCAN_CAP: usize = 512;
const ENTRY_COUNT_BITS: u64 = 30;
const ENTRY_COUNT_MASK: u64 = (1u64 << ENTRY_COUNT_BITS) - 1;
const ENTRY_LEASED_SHIFT: u64 = ENTRY_COUNT_BITS;
const ENTRY_RETIRED_FLAG: u64 = 1u64 << 61;
const MAX_DUP_SLOTS: usize = ENTRY_COUNT_MASK as usize;

type SentKey = (Uuid, usize);

#[derive(Clone, Eq, Hash, PartialEq)]
struct WorkerRoutingKey {
    tags: Arc<[String]>,
}

#[derive(Clone, Eq, Hash, PartialEq)]
struct ExhaustedKey {
    hotkey: Hotkey,
    routing: WorkerRoutingKey,
}

#[derive(Clone, Default)]
pub struct TaskRouting {
    required_worker_tags: Arc<[String]>,
}

#[derive(Copy, Clone, Default)]
pub struct WorkerRouting<'a> {
    tags: &'a [String],
}

impl TaskRouting {
    pub fn with_required_worker_tags(required_worker_tags: Vec<String>) -> Self {
        Self {
            required_worker_tags: Arc::from(required_worker_tags),
        }
    }

    fn matches_worker(&self, worker: WorkerRouting<'_>) -> bool {
        if self.required_worker_tags.is_empty() {
            worker.tags.is_empty()
        } else {
            self.required_worker_tags
                .iter()
                .any(|required| worker.tags.iter().any(|tag| tag == required))
        }
    }
}

impl<'a> WorkerRouting<'a> {
    pub fn from_tags(tags: &'a [String]) -> Self {
        Self { tags }
    }

    fn cache_key(self) -> WorkerRoutingKey {
        let mut tags = self.tags.to_vec();
        tags.sort();
        tags.dedup();
        WorkerRoutingKey {
            tags: Arc::from(tags),
        }
    }
}

impl ExhaustedKey {
    fn new(hotkey: &Hotkey, routing: WorkerRoutingKey) -> Self {
        Self {
            hotkey: hotkey.clone(),
            routing,
        }
    }
}

#[derive(Copy, Clone)]
struct ActiveTaskState {
    count: usize,
    generation: usize,
}

struct QueueEntry {
    item: Arc<Task>,
    routing: TaskRouting,
    state: AtomicU64,
    timestamp: Instant,
    generation: usize,
}

#[derive(Copy, Clone)]
struct LeaseAcquireOutcome {
    final_candidate: bool,
}

#[derive(Copy, Clone, Default)]
struct LeaseReleaseOutcome {
    generation_bump: bool,
    retire_now: bool,
}

impl QueueEntry {
    fn initial_state(available: usize) -> u64 {
        debug_assert!(available <= MAX_DUP_SLOTS);
        Self::pack_state(available, 0, false)
    }

    fn pack_state(available: usize, leased: usize, retired: bool) -> u64 {
        debug_assert!(available <= MAX_DUP_SLOTS);
        debug_assert!(leased <= MAX_DUP_SLOTS);
        let mut state = (available as u64) | ((leased as u64) << ENTRY_LEASED_SHIFT);
        if retired {
            state |= ENTRY_RETIRED_FLAG;
        }
        state
    }

    fn available_slots(state: u64) -> usize {
        (state & ENTRY_COUNT_MASK) as usize
    }

    fn leased_slots(state: u64) -> usize {
        ((state >> ENTRY_LEASED_SHIFT) & ENTRY_COUNT_MASK) as usize
    }

    fn is_retired(state: u64) -> bool {
        state & ENTRY_RETIRED_FLAG != 0
    }

    fn load_state(&self) -> u64 {
        self.state.load(Ordering::Acquire)
    }

    fn has_visible_capacity(&self) -> bool {
        let state = self.load_state();
        Self::available_slots(state) > 0 && !Self::is_retired(state)
    }

    fn is_expired(&self, ttl: Duration) -> bool {
        self.timestamp.elapsed() > ttl
    }

    fn try_acquire_lease(&self) -> Option<LeaseAcquireOutcome> {
        let mut state = self.load_state();
        loop {
            if Self::is_retired(state) {
                return None;
            }

            let available = Self::available_slots(state);
            let leased = Self::leased_slots(state);
            if available == 0 {
                return None;
            }

            let next = Self::pack_state(available - 1, leased + 1, false);
            match self
                .state
                .compare_exchange_weak(state, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => {
                    return Some(LeaseAcquireOutcome {
                        final_candidate: available == 1 && leased == 0,
                    });
                }
                Err(actual) => state = actual,
            }
        }
    }

    fn retire_if_idle(&self) -> bool {
        let mut state = self.load_state();
        loop {
            let leased = Self::leased_slots(state);
            if Self::is_retired(state) {
                return leased == 0;
            }

            let retire_now = leased == 0;
            let next = Self::pack_state(0, leased, true);
            match self
                .state
                .compare_exchange_weak(state, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return retire_now,
                Err(actual) => state = actual,
            }
        }
    }

    /// Retire the entry only if it is genuinely drained: no available capacity
    /// and nothing currently leased, observed atomically. Unlike
    /// [`retire_if_idle`], this never discards an available slot, so it is safe
    /// to call from the scan path where a concurrent rollback/requeue may have
    /// restored capacity after an earlier `available == 0` observation. Returns
    /// `true` only when this call performed the drained -> retired transition.
    fn retire_if_drained(&self) -> bool {
        let mut state = self.load_state();
        loop {
            if Self::is_retired(state) {
                return false;
            }
            if Self::available_slots(state) != 0 || Self::leased_slots(state) != 0 {
                return false;
            }

            let next = Self::pack_state(0, 0, true);
            match self
                .state
                .compare_exchange_weak(state, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return true,
                Err(actual) => state = actual,
            }
        }
    }

    fn finish_commit(&self) -> bool {
        let mut state = self.load_state();
        loop {
            let available = Self::available_slots(state);
            let leased = Self::leased_slots(state);
            let retired = Self::is_retired(state);
            if leased == 0 {
                return false;
            }

            let next_leased = leased - 1;
            let retire_now = next_leased == 0 && (retired || available == 0);
            let next = Self::pack_state(available, next_leased, retired || retire_now);
            match self
                .state
                .compare_exchange_weak(state, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return retire_now,
                Err(actual) => state = actual,
            }
        }
    }

    fn finish_rollback(&self, expired: bool) -> LeaseReleaseOutcome {
        let mut state = self.load_state();
        loop {
            let available = Self::available_slots(state);
            let leased = Self::leased_slots(state);
            let retired = Self::is_retired(state);
            if leased == 0 {
                return LeaseReleaseOutcome::default();
            }

            let next_leased = leased - 1;
            let (next_available, generation_bump, retire_now) = if expired || retired {
                (0, false, next_leased == 0)
            } else {
                (available + 1, available == 0, false)
            };
            let next = Self::pack_state(next_available, next_leased, retired || expired);
            match self
                .state
                .compare_exchange_weak(state, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => {
                    return LeaseReleaseOutcome {
                        generation_bump,
                        retire_now,
                    };
                }
                Err(actual) => state = actual,
            }
        }
    }
}

struct ModelBucket {
    entries: SkipMap<u64, Arc<QueueEntry>>,
    next_seq: AtomicU64,
    live: AtomicUsize,
    activity: AtomicUsize,
    generation: AtomicUsize,
    exhausted_by_worker: Cache<ExhaustedKey, usize, RandomState>,
    scan_cursor_by_worker: Cache<ExhaustedKey, u64, RandomState>,
}

#[cfg(test)]
impl Default for ModelBucket {
    fn default() -> Self {
        Self::with_cache_capacity(
            EXHAUSTED_CACHE_SOFT_TOTAL_CAPACITY,
            SCAN_CURSOR_CACHE_SOFT_TOTAL_CAPACITY,
        )
    }
}

impl ModelBucket {
    fn with_cache_capacity(exhausted_capacity: u64, scan_cursor_capacity: u64) -> Self {
        Self {
            entries: SkipMap::new(),
            next_seq: AtomicU64::new(0),
            live: AtomicUsize::new(0),
            activity: AtomicUsize::new(0),
            generation: AtomicUsize::new(0),
            exhausted_by_worker: Cache::builder()
                .max_capacity(exhausted_capacity.max(1))
                .build_with_hasher(RandomState::default()),
            scan_cursor_by_worker: Cache::builder()
                .max_capacity(scan_cursor_capacity.max(1))
                .build_with_hasher(RandomState::default()),
        }
    }
}

impl ModelBucket {
    #[cfg(test)]
    fn snapshot_entries(&self, limit: usize) -> Vec<(u64, Arc<QueueEntry>)> {
        self.entries
            .iter()
            .take(limit)
            .map(|entry| (*entry.key(), Arc::clone(entry.value())))
            .collect()
    }

    fn bump_generation(&self) {
        self.generation.fetch_add(1, Ordering::AcqRel);
    }

    fn clear_exhausted(&self, key: &ExhaustedKey) {
        self.exhausted_by_worker.invalidate(key);
    }

    fn scan_cursor(&self, key: &ExhaustedKey) -> u64 {
        self.scan_cursor_by_worker.get(key).unwrap_or(0)
    }

    fn update_scan_cursor(&self, key: &ExhaustedKey, seq: u64) {
        self.scan_cursor_by_worker.insert(key.clone(), seq);
    }

    fn rewind_scan_cursor(&self, key: &ExhaustedKey, seq: u64) {
        self.scan_cursor_by_worker.insert(key.clone(), seq);
    }

    fn enqueue_new(&self, entry: Arc<QueueEntry>) {
        let seq = self.next_seq.fetch_add(1, Ordering::Relaxed);
        self.entries.insert(seq, entry);
        self.live.fetch_add(1, Ordering::Relaxed);
        // Readers use the generation acquire-load as the publication point for
        // the widened live/activity state.
        self.activity.fetch_add(1, Ordering::AcqRel);
        self.bump_generation();
    }

    fn remove_entry(&self, seq: u64) -> bool {
        if self.entries.remove(&seq).is_none() {
            return false;
        }
        self.live.fetch_sub(1, Ordering::Relaxed);
        self.activity.fetch_add(1, Ordering::AcqRel);
        self.bump_generation();
        true
    }

    fn exhausted_generation(&self, key: &ExhaustedKey) -> Option<usize> {
        self.exhausted_by_worker.get(key)
    }

    fn mark_exhausted_if_stable(
        &self,
        key: &ExhaustedKey,
        generation: usize,
        start_activity: usize,
        local_activity: usize,
    ) {
        if self.generation.load(Ordering::Acquire) != generation {
            return;
        }
        if self.activity.load(Ordering::Acquire) != start_activity.wrapping_add(local_activity) {
            return;
        }

        self.exhausted_by_worker.insert(key.clone(), generation);
    }
}

struct Inner {
    dup: usize,
    default_model: Arc<str>,
    buckets: FoldHashMap<String, Arc<ModelBucket>>,
    #[allow(dead_code)]
    bucket_order: Vec<String>,
    sent: HashMap<SentKey, FoldHashSet<Hotkey>, RandomState>,
    active_ids: HashMap<Uuid, ActiveTaskState, RandomState>,
    ttl: Duration,
    len: AtomicUsize,
    queue_len_gauge: Option<IntGauge>,
    next_bucket: AtomicUsize,
    next_generation: AtomicUsize,
    reservation_scan_cap: AtomicUsize,
}

#[derive(Clone)]
pub struct TaskQueue {
    inner: Arc<Inner>,
}

pub struct TaskQueueReservation {
    inner: Arc<Inner>,
    committed: bool,
    _marker: PhantomData<Task>,
}

pub struct TaskQueueDelivery {
    inner: Arc<Inner>,
    bucket: Arc<ModelBucket>,
    seq: u64,
    entry: Option<Arc<QueueEntry>>,
    key: Option<SentKey>,
    hotkey: Option<Hotkey>,
    exhausted_key: ExhaustedKey,
    task: Option<Task>,
    duration: Option<Duration>,
}

pub struct TaskQueueBuilder {
    dup: usize,
    ttl: Duration,
    cleanup_interval: Duration,
    default_model: Option<String>,
    models: Vec<String>,
    queue_len_gauge: Option<IntGauge>,
    reservation_scan_cap: usize,
}

impl TaskQueueBuilder {
    pub fn dup(mut self, dup: usize) -> Self {
        self.dup = dup.max(1);
        self
    }

    pub fn ttl(mut self, ttl_secs: u64) -> Self {
        self.ttl = Duration::from_secs(ttl_secs);
        self
    }

    pub fn cleanup_interval(mut self, interval_secs: u64) -> Self {
        self.cleanup_interval = Duration::from_secs(interval_secs);
        self
    }

    pub fn default_model(mut self, model: impl Into<String>) -> Self {
        self.default_model = Some(model.into());
        self
    }

    pub fn models<I, S>(mut self, models: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.models = models.into_iter().map(Into::into).collect();
        self
    }

    pub fn queue_len_gauge(mut self, gauge: IntGauge) -> Self {
        self.queue_len_gauge = Some(gauge);
        self
    }

    pub fn reservation_scan_cap(mut self, cap: usize) -> Self {
        self.reservation_scan_cap = cap.max(1);
        self
    }

    pub fn build(self) -> TaskQueue {
        assert!(
            self.dup.max(1) <= MAX_DUP_SLOTS,
            "task queue dup exceeds supported internal slot count"
        );
        let default_model = self
            .default_model
            .filter(|value| !value.trim().is_empty())
            .expect("task queue requires a non-empty default_model");

        let mut seen = FoldHashSet::default();
        let mut bucket_order = Vec::new();
        bucket_order.push(default_model.clone());
        seen.insert(default_model.clone());

        for model in self.models {
            if !model.trim().is_empty() && seen.insert(model.clone()) {
                bucket_order.push(model);
            }
        }

        let bucket_count = bucket_order.len().max(1) as u64;
        let exhausted_cache_capacity = (EXHAUSTED_CACHE_SOFT_TOTAL_CAPACITY / bucket_count).max(1);
        let scan_cursor_cache_capacity =
            (SCAN_CURSOR_CACHE_SOFT_TOTAL_CAPACITY / bucket_count).max(1);
        let mut buckets = FoldHashMap::default();
        for model in &bucket_order {
            buckets.insert(
                model.clone(),
                Arc::new(ModelBucket::with_cache_capacity(
                    exhausted_cache_capacity,
                    scan_cursor_cache_capacity,
                )),
            );
        }

        let inner = Arc::new(Inner {
            dup: self.dup.max(1),
            default_model: Arc::from(default_model),
            buckets,
            bucket_order,
            sent: HashMap::with_capacity_and_hasher(SENTMAP_START_CAPACITY, RandomState::default()),
            active_ids: HashMap::with_capacity_and_hasher(
                ACTIVE_ID_START_CAPACITY,
                RandomState::default(),
            ),
            ttl: self.ttl,
            len: AtomicUsize::new(0),
            queue_len_gauge: self.queue_len_gauge,
            next_bucket: AtomicUsize::new(0),
            next_generation: AtomicUsize::new(1),
            reservation_scan_cap: AtomicUsize::new(self.reservation_scan_cap.max(1)),
        });
        inner.observe_len(0);
        let cleanup_interval = self.cleanup_interval;
        let weak_inner = Arc::downgrade(&inner);
        task::spawn(async move {
            while let Some(inner) = weak_inner.upgrade() {
                tokio::time::sleep(cleanup_interval).await;

                for bucket in inner.buckets.values() {
                    let scan_limit = bucket
                        .live
                        .load(Ordering::Acquire)
                        .min(MAX_CLEANUP_SCAN_PER_TICK);
                    for index_entry in bucket.entries.iter().take(scan_limit) {
                        let seq = *index_entry.key();
                        let entry = Arc::clone(index_entry.value());
                        drop(index_entry);

                        let state = entry.load_state();
                        let idle_without_capacity = QueueEntry::available_slots(state) == 0
                            && QueueEntry::leased_slots(state) == 0;
                        if entry.is_expired(inner.ttl) || idle_without_capacity {
                            let retired_now = entry.retire_if_idle();
                            let task_id = *entry.item.id();
                            let generation = entry.generation;
                            if retired_now {
                                inner.retire_entry(bucket.as_ref(), seq, task_id, generation);
                            }
                        }
                    }
                }
            }
        });
        TaskQueue { inner }
    }
}

impl TaskQueueReservation {
    pub fn push(mut self, task: Task) {
        self.inner.push_internal(task);
        self.committed = true;
    }

    pub fn push_with_routing(mut self, task: Task, routing: TaskRouting) {
        self.inner.push_internal_with_routing(task, routing);
        self.committed = true;
    }
}

impl TaskQueueDelivery {
    pub fn task(&self) -> &Task {
        self.task
            .as_ref()
            .expect("delivery owns a task until commit")
    }

    /// Returns a pre-commit duration hint for the final lease. The duration
    /// returned by `commit()` is authoritative because retirement happens there.
    pub fn duration(&self) -> Option<Duration> {
        self.duration
    }

    pub fn commit(mut self) -> (Task, Option<Duration>) {
        let entry = self
            .entry
            .take()
            .expect("delivery must own a leased entry before commit");
        let _key = self
            .key
            .take()
            .expect("delivery must own a dedupe key before commit");
        let task = self
            .task
            .take()
            .expect("delivery must own a task before commit");
        let generation = entry.generation;
        let retired_now = entry.finish_commit();

        if retired_now {
            self.inner
                .retire_entry(self.bucket.as_ref(), self.seq, task.id, generation);
        }

        let duration = retired_now.then(|| entry.timestamp.elapsed());
        (task, duration.or(self.duration))
    }

    pub fn rollback(mut self) {
        self.release_inner(false, true);
    }

    pub fn requeue_for_other_hotkeys(mut self) {
        self.release_inner(false, false);
    }

    pub fn retire(mut self) {
        self.release_inner(true, true);
    }

    fn release_inner(&mut self, force_retire: bool, clear_hotkey_delivery: bool) {
        let Some(entry) = self.entry.take() else {
            return;
        };
        let expired = force_retire || entry.is_expired(self.inner.ttl);
        if let Some(key) = self.key.take()
            && let Some(hotkey) = self.hotkey.take()
            && clear_hotkey_delivery
        {
            self.inner.clear_sent_for_hotkey(key, &hotkey);
            self.bucket.clear_exhausted(&self.exhausted_key);
            if !expired {
                self.bucket
                    .rewind_scan_cursor(&self.exhausted_key, self.seq);
            }
        }

        let outcome = entry.finish_rollback(expired);
        if outcome.generation_bump {
            self.bucket.bump_generation();
        }
        if outcome.retire_now {
            self.inner.retire_entry(
                self.bucket.as_ref(),
                self.seq,
                *entry.item.id(),
                entry.generation,
            );
        }
    }
}

impl Drop for TaskQueueReservation {
    fn drop(&mut self) {
        if !self.committed {
            self.inner.decrement_len();
        }
    }
}

impl Drop for TaskQueueDelivery {
    fn drop(&mut self) {
        self.release_inner(false, true);
    }
}

impl Inner {
    fn observe_len(&self, len: usize) {
        if let Some(gauge) = self.queue_len_gauge.as_ref() {
            gauge.set(len.try_into().unwrap_or(i64::MAX));
        }
    }

    fn increment_len(&self) {
        let len = self.len.fetch_add(1, Ordering::Relaxed) + 1;
        self.observe_len(len);
    }

    fn decrement_len(&self) {
        loop {
            let len = self.len.load(Ordering::Relaxed);
            if len == 0 {
                self.observe_len(0);
                return;
            }
            if self
                .len
                .compare_exchange_weak(len, len - 1, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                self.observe_len(len - 1);
                return;
            }
        }
    }

    fn try_increment_len(&self, max_len: usize) -> bool {
        loop {
            let len = self.len.load(Ordering::Relaxed);
            if len >= max_len {
                return false;
            }
            if self
                .len
                .compare_exchange_weak(len, len + 1, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                self.observe_len(len + 1);
                return true;
            }
        }
    }

    fn clear_sent_for_task_generation(&self, task_id: Uuid, generation: usize) {
        let _ = self.sent.remove_sync(&(task_id, generation));
    }

    fn clear_sent_for_hotkey(&self, key: SentKey, hotkey: &Hotkey) {
        if let SccEntry::Occupied(mut sent_entry) = self.sent.entry_sync(key) {
            let remove_entry = {
                let sent_hotkeys = sent_entry.get_mut();
                let _ = sent_hotkeys.remove(hotkey);
                sent_hotkeys.is_empty()
            };
            if remove_entry {
                let _ = sent_entry.remove_entry();
            }
        }
    }

    fn retire_entry(&self, bucket: &ModelBucket, seq: u64, task_id: Uuid, generation: usize) {
        if bucket.remove_entry(seq) {
            self.decrement_len();
            self.release_task_id(task_id, generation);
        }
    }

    fn acquire_task_generation(&self, task_id: Uuid) -> usize {
        match self.active_ids.entry_sync(task_id) {
            SccEntry::Occupied(mut entry) => {
                let state = entry.get_mut();
                state.count = state.count.saturating_add(1);
                state.generation
            }
            SccEntry::Vacant(entry) => {
                let generation = self.next_generation.fetch_add(1, Ordering::Relaxed);
                entry.insert_entry(ActiveTaskState {
                    count: 1,
                    generation,
                });
                generation
            }
        }
    }

    fn release_task_id(&self, task_id: Uuid, generation: usize) {
        let should_clear_sent = match self.active_ids.entry_sync(task_id) {
            SccEntry::Occupied(mut entry) => {
                let state = *entry.get();
                if state.generation != generation {
                    true
                } else if state.count > 1 {
                    entry.get_mut().count = state.count - 1;
                    false
                } else {
                    let _ = entry.remove_entry();
                    true
                }
            }
            SccEntry::Vacant(_) => true,
        };

        if should_clear_sent {
            self.clear_sent_for_task_generation(task_id, generation);
        }
    }

    fn normalize_task(&self, mut task: Task) -> (Task, String) {
        let model = task
            .model
            .clone()
            .unwrap_or_else(|| self.default_model.to_string());
        task.model = Some(model.clone());
        (task, model)
    }

    fn bucket_for_model(&self, model: &str) -> Arc<ModelBucket> {
        self.buckets
            .get(model)
            .cloned()
            .or_else(|| self.buckets.get(self.default_model.as_ref()).cloned())
            .expect("task queue must contain the default bucket")
    }

    fn push_internal(&self, task: Task) {
        self.push_internal_with_routing(task, TaskRouting::default());
    }

    fn push_internal_with_routing(&self, task: Task, routing: TaskRouting) {
        let (task, model) = self.normalize_task(task);
        let task_id = *task.id();
        let generation = self.acquire_task_generation(task_id);
        let entry = Arc::new(QueueEntry {
            item: Arc::new(task),
            routing,
            state: AtomicU64::new(QueueEntry::initial_state(self.dup)),
            timestamp: Instant::now(),
            generation,
        });
        self.bucket_for_model(&model).enqueue_new(entry);
    }
}

impl TaskQueue {
    pub fn builder() -> TaskQueueBuilder {
        TaskQueueBuilder {
            dup: DEFAULT_DUP_COUNT,
            ttl: Duration::from_secs(DEFAULT_TTL_SECS),
            cleanup_interval: Duration::from_secs(DEFAULT_CLEANUP_INTERVAL_SECS),
            default_model: None,
            models: Vec::new(),
            queue_len_gauge: None,
            reservation_scan_cap: DEFAULT_RESERVATION_SCAN_CAP,
        }
    }

    pub fn push(&self, task: Task) {
        self.inner.increment_len();
        self.inner.push_internal(task);
    }

    pub fn try_reserve(&self, max_len: usize) -> Option<TaskQueueReservation> {
        if !self.inner.try_increment_len(max_len) {
            return None;
        }
        Some(TaskQueueReservation {
            inner: Arc::clone(&self.inner),
            committed: false,
            _marker: PhantomData,
        })
    }

    #[allow(dead_code)]
    pub fn pop(&self, num: usize, hotkey: &Hotkey) -> Vec<(Task, Option<Duration>)> {
        self.pop_from_bucket_refs(
            num,
            hotkey,
            self.inner
                .bucket_order
                .iter()
                .filter_map(|name| self.inner.buckets.get(name).cloned()),
        )
    }

    #[allow(dead_code)]
    pub fn reserve(&self, num: usize, hotkey: &Hotkey) -> Vec<TaskQueueDelivery> {
        self.reserve_from_bucket_refs(
            num,
            hotkey,
            WorkerRouting::default(),
            self.inner
                .bucket_order
                .iter()
                .filter_map(|name| self.inner.buckets.get(name).cloned()),
        )
    }

    pub fn pop_for_models<S>(
        &self,
        num: usize,
        hotkey: &Hotkey,
        models: &[S],
    ) -> Vec<(Task, Option<Duration>)>
    where
        S: AsRef<str>,
    {
        self.pop_from_bucket_refs(
            num,
            hotkey,
            models
                .iter()
                .filter_map(|model| self.inner.buckets.get(model.as_ref()).cloned()),
        )
    }

    pub fn reserve_for_models<S>(
        &self,
        num: usize,
        hotkey: &Hotkey,
        models: &[S],
    ) -> Vec<TaskQueueDelivery>
    where
        S: AsRef<str>,
    {
        self.reserve_for_models_with_routing(num, hotkey, models, WorkerRouting::default())
    }

    pub fn reserve_for_models_with_routing<S>(
        &self,
        num: usize,
        hotkey: &Hotkey,
        models: &[S],
        routing: WorkerRouting<'_>,
    ) -> Vec<TaskQueueDelivery>
    where
        S: AsRef<str>,
    {
        self.reserve_from_bucket_refs(
            num,
            hotkey,
            routing,
            models
                .iter()
                .filter_map(|model| self.inner.buckets.get(model.as_ref()).cloned()),
        )
    }

    pub fn set_reservation_scan_cap(&self, cap: usize) {
        self.inner
            .reservation_scan_cap
            .store(cap.max(1), Ordering::Release);
    }

    fn pop_from_bucket_refs<I>(
        &self,
        num: usize,
        hotkey: &Hotkey,
        buckets_iter: I,
    ) -> Vec<(Task, Option<Duration>)>
    where
        I: IntoIterator<Item = Arc<ModelBucket>>,
    {
        self.reserve_from_bucket_refs(num, hotkey, WorkerRouting::default(), buckets_iter)
            .into_iter()
            .map(TaskQueueDelivery::commit)
            .collect()
    }

    fn reserve_from_bucket_refs<I>(
        &self,
        num: usize,
        hotkey: &Hotkey,
        routing: WorkerRouting<'_>,
        buckets_iter: I,
    ) -> Vec<TaskQueueDelivery>
    where
        I: IntoIterator<Item = Arc<ModelBucket>>,
    {
        if num == 0 {
            return Vec::new();
        }

        let buckets: Vec<Arc<ModelBucket>> = buckets_iter.into_iter().collect();
        if buckets.is_empty() {
            return Vec::new();
        }

        // The exhausted key only depends on the hotkey and worker routing, not
        // on the bucket, so compute it (and its routing-tag allocation) once per
        // reservation instead of once per bucket scan attempt.
        let exhausted_key = ExhaustedKey::new(hotkey, routing.cache_key());

        let mut result = Vec::with_capacity(num);
        while result.len() < num {
            let start = self.inner.next_bucket.fetch_add(1, Ordering::Relaxed) % buckets.len();
            let mut progressed = false;

            for offset in 0..buckets.len() {
                if result.len() >= num {
                    break;
                }
                let bucket = &buckets[(start + offset) % buckets.len()];
                if let Some(item) =
                    self.reserve_one_from_bucket(bucket, hotkey, routing, &exhausted_key)
                {
                    progressed = true;
                    result.push(item);
                }
            }

            if !progressed {
                break;
            }
        }

        result
    }

    fn reserve_one_from_bucket(
        &self,
        bucket: &Arc<ModelBucket>,
        hotkey: &Hotkey,
        routing: WorkerRouting<'_>,
        exhausted_key: &ExhaustedKey,
    ) -> Option<TaskQueueDelivery> {
        let generation = bucket.generation.load(Ordering::Acquire);
        if bucket.exhausted_generation(exhausted_key) == Some(generation) {
            return None;
        }

        let start_activity = bucket.activity.load(Ordering::Acquire);
        let live = bucket.live.load(Ordering::Acquire);
        let scan_cap = self.inner.reservation_scan_cap.load(Ordering::Acquire);
        let scan_limit = live.min(scan_cap);
        let local_activity = 0usize;

        if scan_limit == 0 {
            bucket.mark_exhausted_if_stable(
                exhausted_key,
                generation,
                start_activity,
                local_activity,
            );
            return None;
        }

        let start_seq = bucket.scan_cursor(exhausted_key);
        let mut scanned_any = false;
        let mut scanned_count = 0usize;

        for index_entry in bucket.entries.range(start_seq..) {
            scanned_any = true;
            scanned_count += 1;
            let seq = *index_entry.key();
            let entry = Arc::clone(index_entry.value());
            drop(index_entry);
            bucket.update_scan_cursor(exhausted_key, seq.saturating_add(1));

            if let Some(delivery) =
                self.reserve_scanned_entry(bucket, exhausted_key, hotkey, routing, seq, entry)
            {
                return Some(delivery);
            }
            if scanned_count >= scan_limit {
                break;
            }
        }

        if scanned_count < scan_limit && start_seq != 0 {
            for index_entry in bucket.entries.iter() {
                let seq = *index_entry.key();
                if seq >= start_seq {
                    break;
                }
                scanned_any = true;
                scanned_count += 1;
                let entry = Arc::clone(index_entry.value());
                drop(index_entry);
                bucket.update_scan_cursor(exhausted_key, seq.saturating_add(1));

                if let Some(delivery) =
                    self.reserve_scanned_entry(bucket, exhausted_key, hotkey, routing, seq, entry)
                {
                    return Some(delivery);
                }
                if scanned_count >= scan_limit {
                    break;
                }
            }
        }

        if !scanned_any {
            bucket.mark_exhausted_if_stable(
                exhausted_key,
                generation,
                start_activity,
                local_activity,
            );
            return None;
        }

        if live <= scan_cap && scanned_count >= live {
            bucket.mark_exhausted_if_stable(
                exhausted_key,
                generation,
                start_activity,
                local_activity,
            );
        }
        None
    }

    fn reserve_scanned_entry(
        &self,
        bucket: &Arc<ModelBucket>,
        exhausted_key: &ExhaustedKey,
        hotkey: &Hotkey,
        routing: WorkerRouting<'_>,
        seq: u64,
        entry: Arc<QueueEntry>,
    ) -> Option<TaskQueueDelivery> {
        if entry.is_expired(self.inner.ttl) {
            let task_id = *entry.item.id();
            let generation = entry.generation;
            let retired_now = entry.retire_if_idle();
            if retired_now {
                self.inner
                    .retire_entry(bucket.as_ref(), seq, task_id, generation);
            }
            return None;
        }

        if !entry.has_visible_capacity() {
            let task_id = *entry.item.id();
            let generation = entry.generation;
            // Retire only if the entry is genuinely drained (no available
            // capacity and nothing leased) as observed atomically. A concurrent
            // rollback/requeue can restore an available slot between the
            // has_visible_capacity() check above and this point; retiring then
            // would destroy a live, still-deliverable slot and strand the task.
            if entry.retire_if_drained() {
                self.inner
                    .retire_entry(bucket.as_ref(), seq, task_id, generation);
            }
            return None;
        }

        if !entry.routing.matches_worker(routing) {
            return None;
        }

        let key = (*entry.item.id(), entry.generation);
        let already_sent = match self.inner.sent.entry_sync(key) {
            SccEntry::Occupied(mut sent_entry) => {
                let sent_hotkeys = sent_entry.get_mut();
                !sent_hotkeys.insert(hotkey.clone())
            }
            SccEntry::Vacant(sent_entry) => {
                let mut sent_hotkeys = FoldHashSet::default();
                sent_hotkeys.insert(hotkey.clone());
                sent_entry.insert_entry(sent_hotkeys);
                false
            }
        };
        if already_sent {
            return None;
        }

        let Some(lease) = entry.try_acquire_lease() else {
            self.inner.clear_sent_for_hotkey(key, hotkey);
            return None;
        };

        bucket.clear_exhausted(exhausted_key);
        let task = (*entry.item).clone();
        let duration = lease.final_candidate.then(|| entry.timestamp.elapsed());
        Some(TaskQueueDelivery {
            inner: Arc::clone(&self.inner),
            bucket: Arc::clone(bucket),
            seq,
            entry: Some(entry),
            key: Some(key),
            hotkey: Some(hotkey.clone()),
            exhausted_key: exhausted_key.clone(),
            task: Some(task),
            duration,
        })
    }

    #[allow(dead_code)]
    pub fn dup(&self) -> usize {
        self.inner.dup
    }

    pub fn len(&self) -> usize {
        self.inner.len.load(Ordering::Relaxed)
    }

    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests;
