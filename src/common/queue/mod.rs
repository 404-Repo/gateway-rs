use foldhash::fast::RandomState;
use foldhash::{HashMap as FoldHashMap, HashSet as FoldHashSet};
use moka::sync::Cache;
use scc::HashMap;
use scc::hash_map::Entry as SccEntry;
use sdd::Queue;
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
const EXHAUSTED_CACHE_MAX_CAPACITY: u64 = 16_384;
const MAX_CLEANUP_SCAN_PER_TICK: usize = 64;
const ENTRY_COUNT_BITS: u64 = 30;
const ENTRY_COUNT_MASK: u64 = (1u64 << ENTRY_COUNT_BITS) - 1;
const ENTRY_LEASED_SHIFT: u64 = ENTRY_COUNT_BITS;
const ENTRY_QUEUED_FLAG: u64 = 1u64 << 60;
const ENTRY_RETIRED_FLAG: u64 = 1u64 << 61;
const MAX_DUP_SLOTS: usize = ENTRY_COUNT_MASK as usize;

type SentKey = (Uuid, usize);

#[derive(Copy, Clone)]
struct ActiveTaskState {
    count: usize,
    generation: usize,
}

struct QueueEntry {
    item: Arc<Task>,
    state: AtomicU64,
    timestamp: Instant,
    generation: usize,
}

#[derive(Copy, Clone)]
struct LeaseAcquireOutcome {
    available_after: usize,
    final_candidate: bool,
}

#[derive(Copy, Clone, Default)]
struct LeaseReleaseOutcome {
    generation_bump: bool,
    requeue: bool,
    retire_now: bool,
}

impl QueueEntry {
    fn initial_state(available: usize) -> u64 {
        debug_assert!(available <= MAX_DUP_SLOTS);
        Self::pack_state(available, 0, false, false)
    }

    fn pack_state(available: usize, leased: usize, queued: bool, retired: bool) -> u64 {
        debug_assert!(available <= MAX_DUP_SLOTS);
        debug_assert!(leased <= MAX_DUP_SLOTS);
        let mut state = (available as u64) | ((leased as u64) << ENTRY_LEASED_SHIFT);
        if queued {
            state |= ENTRY_QUEUED_FLAG;
        }
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

    fn is_queued(state: u64) -> bool {
        state & ENTRY_QUEUED_FLAG != 0
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

    fn clear_queued_flag(&self) {
        let mut state = self.load_state();
        loop {
            if !Self::is_queued(state) {
                return;
            }
            match self.state.compare_exchange_weak(
                state,
                state & !ENTRY_QUEUED_FLAG,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(actual) => state = actual,
            }
        }
    }

    fn try_mark_queued(&self) -> bool {
        let mut state = self.load_state();
        loop {
            if Self::is_retired(state)
                || Self::is_queued(state)
                || Self::available_slots(state) == 0
            {
                return false;
            }
            match self.state.compare_exchange_weak(
                state,
                state | ENTRY_QUEUED_FLAG,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(actual) => state = actual,
            }
        }
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

            let next = Self::pack_state(available - 1, leased + 1, Self::is_queued(state), false);
            match self
                .state
                .compare_exchange_weak(state, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => {
                    return Some(LeaseAcquireOutcome {
                        available_after: available - 1,
                        final_candidate: available == 1 && leased == 0,
                    });
                }
                Err(actual) => state = actual,
            }
        }
    }

    fn retire_after_pop(&self) -> bool {
        let mut state = self.load_state();
        loop {
            if Self::is_retired(state) {
                return false;
            }

            let leased = Self::leased_slots(state);
            let retire_now = leased == 0;
            let next = Self::pack_state(0, leased, false, retire_now);
            match self
                .state
                .compare_exchange_weak(state, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return retire_now,
                Err(actual) => state = actual,
            }
        }
    }

    fn finish_commit(&self) -> bool {
        let mut state = self.load_state();
        loop {
            let available = Self::available_slots(state);
            let leased = Self::leased_slots(state);
            let queued = Self::is_queued(state);
            let retired = Self::is_retired(state);
            if leased == 0 {
                return false;
            }

            let next_leased = leased - 1;
            let retire_now = !retired && available == 0 && next_leased == 0 && !queued;
            let next = Self::pack_state(available, next_leased, queued, retired || retire_now);
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
            let queued = Self::is_queued(state);
            let retired = Self::is_retired(state);
            if leased == 0 {
                return LeaseReleaseOutcome::default();
            }

            let next_leased = leased - 1;
            let (next_available, generation_bump, requeue, retire_now) = if expired {
                (0, false, false, !retired && next_leased == 0 && !queued)
            } else {
                (available + 1, available == 0, !queued, false)
            };
            let next = Self::pack_state(next_available, next_leased, queued, retired || retire_now);
            match self
                .state
                .compare_exchange_weak(state, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => {
                    return LeaseReleaseOutcome {
                        generation_bump,
                        requeue,
                        retire_now,
                    };
                }
                Err(actual) => state = actual,
            }
        }
    }
}

struct ModelBucket {
    q: Queue<Arc<QueueEntry>>,
    live: AtomicUsize,
    in_flight: AtomicUsize,
    activity: AtomicUsize,
    generation: AtomicUsize,
    exhausted_by_hotkey: Cache<Hotkey, usize, RandomState>,
}

impl Default for ModelBucket {
    fn default() -> Self {
        Self {
            q: Queue::default(),
            live: AtomicUsize::new(0),
            in_flight: AtomicUsize::new(0),
            activity: AtomicUsize::new(0),
            generation: AtomicUsize::new(0),
            exhausted_by_hotkey: Cache::builder()
                .max_capacity(EXHAUSTED_CACHE_MAX_CAPACITY)
                .build_with_hasher(RandomState::default()),
        }
    }
}

impl ModelBucket {
    fn pop_visible_entry(&self) -> Option<Arc<QueueEntry>> {
        self.pop_visible_entry_inner(|| {})
    }

    fn pop_visible_entry_inner<F: FnOnce()>(&self, after_pop: F) -> Option<Arc<QueueEntry>> {
        self.in_flight.fetch_add(1, Ordering::AcqRel);
        match self.q.pop() {
            Some(shared) => {
                let entry = (**shared).clone();
                entry.clear_queued_flag();
                after_pop();
                self.activity.fetch_add(1, Ordering::AcqRel);
                Some(entry)
            }
            None => {
                self.in_flight.fetch_sub(1, Ordering::AcqRel);
                None
            }
        }
    }

    #[cfg(test)]
    fn pop_visible_entry_with_hook<F: FnOnce()>(&self, after_pop: F) -> Option<Arc<QueueEntry>> {
        self.pop_visible_entry_inner(after_pop)
    }

    fn push_ready(&self, entry: Arc<QueueEntry>) -> bool {
        if !entry.try_mark_queued() {
            return false;
        }
        self.q.push(entry);
        self.activity.fetch_add(1, Ordering::AcqRel);
        true
    }

    fn finish_entry(&self, entry: Arc<QueueEntry>, requeue: bool) -> bool {
        let requeued = requeue && self.push_ready(entry);
        self.in_flight.fetch_sub(1, Ordering::AcqRel);
        requeued
    }

    fn bump_generation(&self) {
        self.generation.fetch_add(1, Ordering::AcqRel);
    }

    fn clear_exhausted_hotkey(&self, hotkey: &Hotkey) {
        self.exhausted_by_hotkey.invalidate(hotkey);
    }

    fn enqueue_new(&self, entry: Arc<QueueEntry>) {
        let _ = self.push_ready(entry);
        self.live.fetch_add(1, Ordering::Relaxed);
        // Readers use the generation acquire-load as the publication point for
        // the widened live/activity state.
        self.bump_generation();
    }

    fn exhausted_generation(&self, hotkey: &Hotkey) -> Option<usize> {
        self.exhausted_by_hotkey.get(hotkey)
    }

    fn mark_exhausted_if_stable(
        &self,
        hotkey: &Hotkey,
        generation: usize,
        start_activity: usize,
        local_activity: usize,
        start_in_flight: usize,
    ) {
        if start_in_flight != 0 {
            return;
        }
        if self.generation.load(Ordering::Acquire) != generation {
            return;
        }
        if self.in_flight.load(Ordering::Acquire) != 0 {
            return;
        }
        if self.activity.load(Ordering::Acquire) != start_activity.wrapping_add(local_activity) {
            return;
        }

        self.exhausted_by_hotkey.insert(hotkey.clone(), generation);
    }

    fn finish_live_entry(&self) {
        self.live.fetch_sub(1, Ordering::Relaxed);
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
    next_bucket: AtomicUsize,
    next_generation: AtomicUsize,
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
    entry: Option<Arc<QueueEntry>>,
    key: Option<SentKey>,
    hotkey: Option<Hotkey>,
    task: Task,
    duration: Option<Duration>,
}

pub struct TaskQueueBuilder {
    dup: usize,
    ttl: Duration,
    cleanup_interval: Duration,
    default_model: Option<String>,
    models: Vec<String>,
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

        let mut buckets = FoldHashMap::default();
        for model in &bucket_order {
            buckets.insert(model.clone(), Arc::new(ModelBucket::default()));
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
            next_bucket: AtomicUsize::new(0),
            next_generation: AtomicUsize::new(1),
        });
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
                    for _ in 0..scan_limit {
                        let Some(entry) = bucket.pop_visible_entry() else {
                            break;
                        };

                        if entry.is_expired(inner.ttl) || !entry.has_visible_capacity() {
                            let retired_now = entry.retire_after_pop();
                            let task_id = *entry.item.id();
                            let generation = entry.generation;
                            bucket.finish_entry(entry, false);
                            if retired_now {
                                inner.retire_entry(bucket.as_ref(), task_id, generation);
                            }
                        } else {
                            bucket.finish_entry(entry, true);
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
}

impl TaskQueueDelivery {
    pub fn task(&self) -> &Task {
        &self.task
    }

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
        let generation = entry.generation;
        let retired_now = entry.finish_commit();

        if retired_now {
            self.inner
                .retire_entry(self.bucket.as_ref(), self.task.id, generation);
        }

        let duration = retired_now.then(|| entry.timestamp.elapsed());
        (self.task.clone(), duration.or(self.duration))
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
        if let Some(key) = self.key.take()
            && let Some(hotkey) = self.hotkey.take()
            && clear_hotkey_delivery
        {
            self.inner.clear_sent_for_hotkey(key, &hotkey);
            self.bucket.clear_exhausted_hotkey(&hotkey);
        }

        let expired = force_retire || entry.is_expired(self.inner.ttl);
        let outcome = entry.finish_rollback(expired);
        if outcome.generation_bump {
            self.bucket.bump_generation();
        }
        if outcome.requeue {
            let _ = self.bucket.push_ready(Arc::clone(&entry));
        }
        if outcome.retire_now {
            self.inner
                .retire_entry(self.bucket.as_ref(), self.task.id, entry.generation);
        }
    }
}

impl Drop for TaskQueueReservation {
    fn drop(&mut self) {
        if !self.committed {
            self.inner.len.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

impl Drop for TaskQueueDelivery {
    fn drop(&mut self) {
        self.release_inner(false, true);
    }
}

impl Inner {
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

    fn retire_entry(&self, bucket: &ModelBucket, task_id: Uuid, generation: usize) {
        bucket.finish_live_entry();
        self.len.fetch_sub(1, Ordering::Relaxed);
        self.release_task_id(task_id, generation);
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
        let (task, model) = self.normalize_task(task);
        let task_id = *task.id();
        let generation = self.acquire_task_generation(task_id);
        let entry = Arc::new(QueueEntry {
            item: Arc::new(task),
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
        }
    }

    pub fn push(&self, task: Task) {
        self.inner.len.fetch_add(1, Ordering::Relaxed);
        self.inner.push_internal(task);
    }

    pub fn try_reserve(&self, max_len: usize) -> Option<TaskQueueReservation> {
        loop {
            let len = self.inner.len.load(Ordering::Relaxed);
            if len >= max_len {
                return None;
            }
            if self
                .inner
                .len
                .compare_exchange_weak(len, len + 1, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                return Some(TaskQueueReservation {
                    inner: Arc::clone(&self.inner),
                    committed: false,
                    _marker: PhantomData,
                });
            }
        }
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
        self.reserve_from_bucket_refs(
            num,
            hotkey,
            models
                .iter()
                .filter_map(|model| self.inner.buckets.get(model.as_ref()).cloned()),
        )
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
        self.reserve_from_bucket_refs(num, hotkey, buckets_iter)
            .into_iter()
            .map(TaskQueueDelivery::commit)
            .collect()
    }

    fn reserve_from_bucket_refs<I>(
        &self,
        num: usize,
        hotkey: &Hotkey,
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

        let mut result = Vec::with_capacity(num);
        while result.len() < num {
            let start = self.inner.next_bucket.fetch_add(1, Ordering::Relaxed) % buckets.len();
            let mut progressed = false;

            for offset in 0..buckets.len() {
                if result.len() >= num {
                    break;
                }
                let bucket = &buckets[(start + offset) % buckets.len()];
                if let Some(item) = self.reserve_one_from_bucket(bucket, hotkey) {
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
    ) -> Option<TaskQueueDelivery> {
        let generation = bucket.generation.load(Ordering::Acquire);
        if bucket.exhausted_generation(hotkey) == Some(generation) {
            return None;
        }

        let start_activity = bucket.activity.load(Ordering::Acquire);
        let start_in_flight = bucket.in_flight.load(Ordering::Acquire);
        let scan_limit = bucket.live.load(Ordering::Acquire);
        let mut local_activity = 0usize;

        for _ in 0..scan_limit {
            let Some(entry) = bucket.pop_visible_entry() else {
                bucket.mark_exhausted_if_stable(
                    hotkey,
                    generation,
                    start_activity,
                    local_activity,
                    start_in_flight,
                );
                return None;
            };
            local_activity = local_activity.wrapping_add(1);

            if entry.is_expired(self.inner.ttl) || !entry.has_visible_capacity() {
                let task_id = *entry.item.id();
                let generation = entry.generation;
                let retired_now = entry.retire_after_pop();
                bucket.finish_entry(entry, false);
                if retired_now {
                    self.inner
                        .retire_entry(bucket.as_ref(), task_id, generation);
                }
                continue;
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
                if bucket.finish_entry(entry, true) {
                    local_activity = local_activity.wrapping_add(1);
                }
                continue;
            }

            let Some(lease) = entry.try_acquire_lease() else {
                self.inner.clear_sent_for_hotkey(key, hotkey);
                let task_id = *entry.item.id();
                let generation = entry.generation;
                let retired_now = entry.retire_after_pop();
                bucket.finish_entry(entry, false);
                if retired_now {
                    self.inner
                        .retire_entry(bucket.as_ref(), task_id, generation);
                }
                continue;
            };

            bucket.clear_exhausted_hotkey(hotkey);
            let _ = bucket.finish_entry(Arc::clone(&entry), lease.available_after > 0);
            let task = (*entry.item).clone();
            let duration = lease.final_candidate.then(|| entry.timestamp.elapsed());
            return Some(TaskQueueDelivery {
                inner: Arc::clone(&self.inner),
                bucket: Arc::clone(bucket),
                entry: Some(entry),
                key: Some(key),
                hotkey: Some(hotkey.clone()),
                task,
                duration,
            });
        }

        bucket.mark_exhausted_if_stable(
            hotkey,
            generation,
            start_activity,
            local_activity,
            start_in_flight,
        );
        None
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
