mod snapshot_persistence;

use openraft::BasicNode;
use openraft::Entry;
use openraft::EntryPayload;
use openraft::LogId;
use openraft::Membership;
use openraft::RaftSnapshotBuilder;
use openraft::RaftTypeConfig;
use openraft::SnapshotMeta;
use openraft::StorageError;
use openraft::StorageIOError;
use openraft::StoredMembership;
use openraft::storage::RaftStateMachine;
use openraft::storage::Snapshot;
use portable_atomic::AtomicU128;
use sdd::{AtomicOwned, Guard, Owned, Tag};
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::{Cursor, Error as IoError};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tokio::task::spawn_blocking;
use tracing::info;
use uuid::Uuid;

use crate::common::fs::write_atomic;
use crate::raft::NodeId;
use crate::raft::TypeConfig;

use foldhash::HashMap as FoldHashMap;
use foldhash::fast::RandomState;
use scc::hash_cache::Entry as CacheEntry;
use scc::{HashCache, HashMap};
use snapshot_persistence::SnapshotPersistence;

pub type LogStore = crate::raft::memstore::log_store::LogStore<TypeConfig>;

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct RateLimitSnapshot {
    pub day_epoch: u64,
    #[serde(default)]
    pub active_limits: Vec<(RateLimitKey, u64)>,
    pub day_limits: Vec<(RateLimitKey, u64)>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, Eq, PartialEq, Hash, PartialOrd, Ord)]
pub enum Subject {
    User,
    Company,
    GenericGlobal,
    GenericIp,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, Eq, PartialEq, Hash, PartialOrd, Ord)]
pub struct RateLimitKey {
    pub subject: Subject,
    pub id: u128,
}

impl RateLimitKey {
    pub const fn new(subject: Subject, id: u128) -> Self {
        Self { subject, id }
    }
}

pub const fn rate_limit_key(subject: Subject, id: u128) -> RateLimitKey {
    RateLimitKey::new(subject, id)
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub struct RateLimitDelta {
    pub subject: Subject,
    pub id: u128,
    pub day_epoch: u64,
    #[serde(default)]
    pub add_active: u32,
    pub add_day: u32,
}

impl RateLimitDelta {
    pub const fn key(&self) -> RateLimitKey {
        rate_limit_key(self.subject, self.id)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub struct RateLimitMutation {
    pub subject: Subject,
    pub id: u128,
    pub day_epoch: u64,
    #[serde(default)]
    pub active_delta: i64,
    pub day_delta: i64,
}

impl RateLimitMutation {
    pub const fn key(&self) -> RateLimitKey {
        rate_limit_key(self.subject, self.id)
    }

    pub const fn from_delta(delta: RateLimitDelta) -> Self {
        Self {
            subject: delta.subject,
            id: delta.id,
            day_epoch: delta.day_epoch,
            active_delta: delta.add_active as i64,
            day_delta: delta.add_day as i64,
        }
    }

    pub const fn from_refund(refund: RateLimitRefund) -> Self {
        Self {
            subject: refund.subject,
            id: refund.id,
            day_epoch: refund.day_epoch,
            active_delta: -(refund.sub_active as i64),
            day_delta: -(refund.sub_day as i64),
        }
    }

    pub const fn refund_from_delta(delta: RateLimitDelta) -> Self {
        Self {
            subject: delta.subject,
            id: delta.id,
            day_epoch: delta.day_epoch,
            active_delta: -(delta.add_active as i64),
            day_delta: -(delta.add_day as i64),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq)]
pub struct RateLimitMutationBatch {
    pub request_id: u128,
    pub mutations: Vec<RateLimitMutation>,
}

impl RateLimitMutationBatch {
    pub const fn new(request_id: u128, mutations: Vec<RateLimitMutation>) -> Self {
        Self {
            request_id,
            mutations,
        }
    }

    pub fn with_generated_request_id(mutations: Vec<RateLimitMutation>) -> Option<Self> {
        (!mutations.is_empty()).then(|| Self::new(Uuid::new_v4().as_u128(), mutations))
    }

    pub fn is_empty(&self) -> bool {
        self.mutations.is_empty()
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub struct RateLimitRefund {
    pub subject: Subject,
    pub id: u128,
    pub day_epoch: u64,
    #[serde(default)]
    pub sub_active: u32,
    pub sub_day: u32,
}

impl RateLimitRefund {
    pub const fn key(&self) -> RateLimitKey {
        rate_limit_key(self.subject, self.id)
    }

    pub const fn from_delta(delta: RateLimitDelta) -> Self {
        Self {
            subject: delta.subject,
            id: delta.id,
            day_epoch: delta.day_epoch,
            sub_active: delta.add_active,
            sub_day: delta.add_day,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Request {
    Set {
        request_id: u128,
        key: String,
        value: Vec<u8>,
    },
    RateLimitMutations {
        request_id: u128,
        mutations: Vec<RateLimitMutation>,
    },
    RateLimitDeltas {
        request_id: u128,
        deltas: Vec<RateLimitDelta>,
    },
    RateLimitRefunds {
        request_id: u128,
        refunds: Vec<RateLimitRefund>,
    },
}

impl Request {
    pub fn request_id(&self) -> u128 {
        match self {
            Request::Set { request_id, .. } => *request_id,
            Request::RateLimitMutations { request_id, .. } => *request_id,
            Request::RateLimitDeltas { request_id, .. } => *request_id,
            Request::RateLimitRefunds { request_id, .. } => *request_id,
        }
    }

    pub fn into_rate_limit_batch(self) -> Result<RateLimitMutationBatch, Self> {
        match self {
            Request::RateLimitMutations {
                request_id,
                mutations,
            } => Ok(RateLimitMutationBatch::new(request_id, mutations)),
            Request::RateLimitDeltas { request_id, deltas } => Ok(RateLimitMutationBatch::new(
                request_id,
                deltas
                    .into_iter()
                    .map(RateLimitMutation::from_delta)
                    .collect(),
            )),
            Request::RateLimitRefunds {
                request_id,
                refunds,
            } => Ok(RateLimitMutationBatch::new(
                request_id,
                refunds
                    .into_iter()
                    .map(RateLimitMutation::from_refund)
                    .collect(),
            )),
            other => Err(other),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Response;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct StoredSnapshot {
    pub meta: SnapshotMeta<NodeId, BasicNode>,

    /// The data of the state machine at the time of this snapshot.
    pub data: Vec<u8>,
}

#[derive(Debug, Clone)]
struct CurrentSnapshotRef {
    meta: SnapshotMeta<NodeId, BasicNode>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub(crate) struct SnapshotPayload {
    pub data: BTreeMap<String, Vec<u8>>,
    pub rate_limits: RateLimitSnapshot,
}

#[derive(Debug, Clone, Default)]
pub struct SnapshotKv(Arc<BTreeMap<String, Arc<[u8]>>>);

impl SnapshotKv {
    pub(super) fn from_owned_map(data: BTreeMap<String, Vec<u8>>) -> Self {
        let data = data
            .into_iter()
            .map(|(key, value)| (key, Arc::<[u8]>::from(value)))
            .collect();
        Self(Arc::new(data))
    }

    pub(super) fn to_owned_map(&self) -> BTreeMap<String, Vec<u8>> {
        self.0
            .iter()
            .map(|(key, value)| (key.clone(), value.as_ref().to_vec()))
            .collect()
    }

    pub(crate) fn insert(&mut self, key: String, value: impl Into<Arc<[u8]>>) -> Option<Arc<[u8]>> {
        let mut next = (*self.0).clone();
        let prev = next.insert(key, value.into());
        self.0 = Arc::new(next);
        prev
    }
}

impl Deref for SnapshotKv {
    type Target = BTreeMap<String, Arc<[u8]>>;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

impl SnapshotPayload {
    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, rmp_serde::decode::Error> {
        rmp_serde::from_slice(bytes)
    }
}

/// Data contained in the Raft state machine.
#[derive(Debug, Default, Clone)]
pub struct StateMachineData {
    pub last_applied_log: Option<LogId<NodeId>>,

    pub last_membership: StoredMembership<NodeId, BasicNode>,

    /// Application data.
    pub data: SnapshotKv,
}

/// Defines a state machine for the Raft cluster. This state machine represents a copy of the
/// data for this node. Additionally, it is responsible for storing the last snapshot of the data.
#[derive(Debug)]
pub struct StateMachineStore {
    /// The Raft state machine.
    pub state_machine: RwLock<StateMachineData>,

    /// Used in identifier for snapshot.
    ///
    /// Note that concurrently created snapshots and snapshots created on different nodes
    /// are not guaranteed to have sequential `snapshot_idx` values, but this does not matter for
    /// correctness.
    snapshot_idx: AtomicU64,

    /// The last received snapshot.
    current_snapshot: AtomicOwned<Arc<CurrentSnapshotRef>>,
    current_snapshot_data_path: PathBuf,

    /// Optional on-disk persistence configuration.
    persistence: Option<Arc<SnapshotPersistence>>,

    /// Concurrent request-id deduplication cache to avoid re-applying idempotent requests.
    request_dedupe: HashCache<u128, Instant, RandomState>,

    /// Concurrent hash maps for rate limits, swapped atomically during snapshot installs.
    rate_limits_active: AtomicOwned<HashMap<RateLimitKey, u64, RandomState>>,
    rate_limits_day: AtomicOwned<HashMap<RateLimitKey, u64, RandomState>>,
    day_epoch: AtomicU64,

    /// Guard to synchronize apply and snapshot building for consistent cuts.
    snapshot_guard: tokio::sync::Mutex<()>,

    /// Cached generic-key UUID as u128 (0 = absent). Updated only on
    /// Set{"generic_key"} applies and snapshot installs — avoids acquiring the state_machine
    /// RwLock on every request.
    cached_generic_key: AtomicU128,
}

#[cfg(test)]
const REQUEST_DEDUPE_TTL: Duration = Duration::from_millis(50);
#[cfg(not(test))]
const REQUEST_DEDUPE_TTL: Duration = Duration::from_secs(600);

impl Default for StateMachineStore {
    fn default() -> Self {
        StateMachineStore {
            state_machine: RwLock::new(StateMachineData::default()),
            snapshot_idx: AtomicU64::new(0),
            current_snapshot: AtomicOwned::null(),
            current_snapshot_data_path: std::env::temp_dir().join(format!(
                "gateway-raft-current-snapshot-{}.bin",
                Uuid::new_v4()
            )),
            persistence: None,
            request_dedupe: HashCache::with_capacity_and_hasher(512, 1024, RandomState::default()),
            rate_limits_active: AtomicOwned::new(HashMap::with_capacity_and_hasher(
                4096,
                RandomState::default(),
            )),
            rate_limits_day: AtomicOwned::new(HashMap::with_capacity_and_hasher(
                4096,
                RandomState::default(),
            )),
            day_epoch: AtomicU64::new(0),
            snapshot_guard: tokio::sync::Mutex::new(()),
            cached_generic_key: AtomicU128::new(0),
        }
    }
}

impl StateMachineStore {
    fn build_rate_limit_map(
        new_limits: Vec<(RateLimitKey, u64)>,
    ) -> HashMap<RateLimitKey, u64, RandomState> {
        let capacity = new_limits.len().max(4096);
        let map = HashMap::with_capacity_and_hasher(capacity, RandomState::default());
        for (key, value) in new_limits {
            let _ = map.insert_sync(key, value);
        }
        map
    }

    fn current_rate_limits_day<'guard>(
        &self,
        guard: &'guard Guard,
    ) -> Option<&'guard HashMap<RateLimitKey, u64, RandomState>> {
        self.rate_limits_day.load(Ordering::Acquire, guard).as_ref()
    }

    fn current_rate_limits_active<'guard>(
        &self,
        guard: &'guard Guard,
    ) -> Option<&'guard HashMap<RateLimitKey, u64, RandomState>> {
        self.rate_limits_active
            .load(Ordering::Acquire, guard)
            .as_ref()
    }

    fn sync_rate_limits(&self, snapshot: RateLimitSnapshot) {
        let active_map = Self::build_rate_limit_map(snapshot.active_limits);
        let day_map = Self::build_rate_limit_map(snapshot.day_limits);
        let _ = self
            .rate_limits_active
            .swap((Some(Owned::new(active_map)), Tag::None), Ordering::AcqRel);
        let _ = self
            .rate_limits_day
            .swap((Some(Owned::new(day_map)), Tag::None), Ordering::AcqRel);
        self.day_epoch.store(snapshot.day_epoch, Ordering::Release);
    }

    pub fn with_persistence<P: AsRef<Path>>(
        dir: P,
        retention: usize,
        log_store_path: Option<PathBuf>,
    ) -> anyhow::Result<Self> {
        let persistence = Arc::new(SnapshotPersistence::new(dir, retention, log_store_path)?);
        let (state_machine_data, rate_limits_data, snapshot_idx, current_snapshot) =
            persistence.load_latest()?;

        let day_epoch = rate_limits_data.day_epoch;
        let rate_limits_active = Self::build_rate_limit_map(rate_limits_data.active_limits);
        let rate_limits_day = Self::build_rate_limit_map(rate_limits_data.day_limits);
        let snapshot_data_path = persistence.dir.join("current_snapshot_payload.bin");
        if let Some(snapshot) = current_snapshot.as_ref() {
            write_atomic(&snapshot_data_path, &snapshot.data)?;
        }

        let generic_key_u128 = Self::extract_generic_key_u128(&state_machine_data.data);

        Ok(Self {
            state_machine: RwLock::new(state_machine_data),
            snapshot_idx: AtomicU64::new(snapshot_idx),
            current_snapshot: match current_snapshot {
                Some(snapshot) => AtomicOwned::new(Arc::new(CurrentSnapshotRef {
                    meta: snapshot.meta,
                })),
                None => AtomicOwned::null(),
            },
            current_snapshot_data_path: snapshot_data_path,
            persistence: Some(persistence),
            request_dedupe: HashCache::with_capacity_and_hasher(256, 1024, RandomState::default()),
            rate_limits_active: AtomicOwned::new(rate_limits_active),
            rate_limits_day: AtomicOwned::new(rate_limits_day),
            day_epoch: AtomicU64::new(day_epoch),
            snapshot_guard: tokio::sync::Mutex::new(()),
            cached_generic_key: AtomicU128::new(generic_key_u128),
        })
    }

    fn extract_generic_key_u128(data: &SnapshotKv) -> u128 {
        data.get("generic_key")
            .and_then(|v| rmp_serde::from_slice::<Uuid>(v.as_ref()).ok())
            .map_or(0, |uuid| uuid.as_u128())
    }

    fn store_cached_generic_key(&self, raw: u128) {
        self.cached_generic_key.store(raw, Ordering::Release);
    }

    /// Returns the cached generic-key without acquiring any locks.
    pub fn get_cached_generic_key(&self) -> Option<Uuid> {
        let raw = self.cached_generic_key.load(Ordering::Acquire);
        (raw != 0).then(|| Uuid::from_u128(raw))
    }

    async fn with_snapshot_state_machine_read<T, F>(&self, f: F) -> T
    where
        F: FnOnce(&StateMachineData) -> T,
    {
        let _snapshot_guard = self.snapshot_guard.lock().await;
        let state_machine = self.state_machine.read().await;
        f(&state_machine)
    }

    async fn with_snapshot_state_machine_write<T, F>(&self, f: F) -> T
    where
        F: FnOnce(&mut StateMachineData) -> T,
    {
        let _snapshot_guard = self.snapshot_guard.lock().await;
        let mut state_machine = self.state_machine.write().await;
        f(&mut state_machine)
    }

    async fn is_duplicate_request(&self, request_id: u128) -> bool {
        let now = Instant::now();
        match self.request_dedupe.entry_async(request_id).await {
            CacheEntry::Occupied(mut entry) => {
                let seen_at = *entry.get();
                let is_dup = now.duration_since(seen_at) <= REQUEST_DEDUPE_TTL;
                *entry.get_mut() = now;
                is_dup
            }
            CacheEntry::Vacant(entry) => {
                entry.put_entry(now);
                false
            }
        }
    }

    pub async fn get_raw(&self, key: &str) -> Option<Vec<u8>> {
        let sm = self.state_machine.read().await;
        sm.data.get(key).map(|value| value.as_ref().to_vec())
    }

    pub async fn get_rate_limit_usage(&self, key: &RateLimitKey, day_epoch: u64) -> (u64, u64) {
        let guard = Guard::new();
        let active = self
            .current_rate_limits_active(&guard)
            .and_then(|map| map.read_sync(key, |_, v| *v))
            .unwrap_or(0);

        let day = {
            let current = self.day_epoch.load(Ordering::Acquire);
            if current != day_epoch {
                0
            } else {
                let value = self
                    .current_rate_limits_day(&guard)
                    .and_then(|map| map.read_sync(key, |_, v| *v))
                    .unwrap_or(0);
                if self.day_epoch.load(Ordering::Acquire) != current {
                    0
                } else {
                    value
                }
            }
        };

        (active, day)
    }

    async fn persist_snapshot(
        &self,
        snapshot: Arc<StoredSnapshot>,
    ) -> Result<(), StorageError<NodeId>> {
        if let Some(persistence) = &self.persistence {
            let persistence = Arc::clone(persistence);
            let signature = snapshot.meta.signature();
            match spawn_blocking(move || persistence.store(snapshot.as_ref())).await {
                Ok(Ok(())) => {}
                Ok(Err(err)) => {
                    return Err(StorageIOError::write_snapshot(Some(signature), &err).into());
                }
                Err(join_err) => {
                    let io_err = IoError::other(format!(
                        "task join error while persisting snapshot: {join_err}"
                    ));
                    return Err(StorageIOError::write_snapshot(Some(signature), &io_err).into());
                }
            }
        }
        Ok(())
    }

    async fn apply_blank(&self, log_id: LogId<NodeId>) {
        self.with_snapshot_state_machine_write(|sm| {
            sm.last_applied_log = Some(log_id);
        })
        .await;
    }

    async fn apply_membership(&self, log_id: LogId<NodeId>, mem: Membership<NodeId, BasicNode>) {
        self.with_snapshot_state_machine_write(|sm| {
            sm.last_applied_log = Some(log_id);
            sm.last_membership = StoredMembership::new(Some(log_id), mem);
        })
        .await;
    }

    /// Apply rate-limit mutations to the concurrent maps. Caller must hold `snapshot_guard`.
    fn apply_rate_limit_mutations_maps(&self, mutations: Vec<RateLimitMutation>) {
        let guard = Guard::new();
        let mut day_aggregated: FoldHashMap<(Subject, u128, u64), i64> =
            FoldHashMap::with_capacity_and_hasher(mutations.len(), RandomState::default());
        let mut active_aggregated: FoldHashMap<(Subject, u128), i64> =
            FoldHashMap::with_capacity_and_hasher(mutations.len(), RandomState::default());
        let mut min_day_epoch = u64::MAX;
        let mut max_day_epoch = 0u64;
        for mutation in mutations {
            if mutation.active_delta != 0 {
                active_aggregated
                    .entry((mutation.subject, mutation.id))
                    .and_modify(|acc| {
                        *acc = acc.saturating_add(mutation.active_delta);
                    })
                    .or_insert(mutation.active_delta);
            }
            if mutation.day_delta != 0 {
                let key = (mutation.subject, mutation.id, mutation.day_epoch);
                day_aggregated
                    .entry(key)
                    .and_modify(|acc| {
                        *acc = acc.saturating_add(mutation.day_delta);
                    })
                    .or_insert(mutation.day_delta);
                min_day_epoch = min_day_epoch.min(mutation.day_epoch);
                max_day_epoch = max_day_epoch.max(mutation.day_epoch);
            }
        }

        let mut day_updates: Vec<_> = day_aggregated
            .into_iter()
            .filter(|(_, delta)| *delta != 0)
            .map(|((subject, id, day_epoch), day_delta)| (day_epoch, subject, id, day_delta))
            .collect();

        if min_day_epoch != max_day_epoch {
            day_updates.sort_by_key(|(epoch, _, _, _)| *epoch);
        }

        for ((subject, id), active_delta) in active_aggregated {
            if active_delta == 0 {
                continue;
            }
            if let Some(map) = self.current_rate_limits_active(&guard) {
                let key = rate_limit_key(subject, id);
                if active_delta > 0 {
                    let add = active_delta as u64;
                    map.entry_sync(key)
                        .and_modify(|v| {
                            *v = v.saturating_add(add);
                        })
                        .or_insert(add);
                } else {
                    let sub = active_delta.unsigned_abs();
                    let _ = map.entry_sync(key).and_modify(|v| {
                        *v = v.saturating_sub(sub);
                    });
                }
            }
        }

        let mut current_day_epoch = self.day_epoch.load(Ordering::Relaxed);
        for (day_epoch, subject, id, day_delta) in day_updates {
            if day_epoch > current_day_epoch {
                let new_map = HashMap::with_capacity_and_hasher(4096, RandomState::default());
                let _ = self
                    .rate_limits_day
                    .swap((Some(Owned::new(new_map)), Tag::None), Ordering::AcqRel);
                self.day_epoch.store(day_epoch, Ordering::Release);
                current_day_epoch = day_epoch;
            }
            if day_epoch == current_day_epoch
                && let Some(map) = self.current_rate_limits_day(&guard)
            {
                let key = rate_limit_key(subject, id);
                if day_delta > 0 {
                    let add = day_delta as u64;
                    map.entry_sync(key)
                        .and_modify(|v| {
                            *v = v.saturating_add(add);
                        })
                        .or_insert(add);
                } else {
                    let sub = day_delta.unsigned_abs();
                    let _ = map.entry_sync(key).and_modify(|v| {
                        *v = v.saturating_sub(sub);
                    });
                }
            }
        }
    }
}

impl RaftSnapshotBuilder<TypeConfig> for Arc<StateMachineStore> {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        // Collect Arc-backed data under locks and serialize outside to minimize critical section.
        let (data, rate_limits, last_applied_log, last_membership) = self
            .with_snapshot_state_machine_read(|state_machine| {
                let guard = Guard::new();
                let mut active_limits = Vec::new();
                if let Some(map) = self.current_rate_limits_active(&guard) {
                    map.iter_sync(|k, v| {
                        active_limits.push((*k, *v));
                        true
                    });
                }
                let mut day_limits = Vec::new();
                if let Some(map) = self.current_rate_limits_day(&guard) {
                    map.iter_sync(|k, v| {
                        day_limits.push((*k, *v));
                        true
                    });
                }
                let rate_limits = RateLimitSnapshot {
                    day_epoch: self.day_epoch.load(Ordering::Acquire),
                    active_limits,
                    day_limits,
                };
                (
                    state_machine.data.clone(),
                    rate_limits,
                    state_machine.last_applied_log,
                    state_machine.last_membership.clone(),
                )
            })
            .await;

        let payload = SnapshotPayload {
            data: data.to_owned_map(),
            rate_limits,
        };
        let snapshot_bytes =
            rmp_serde::to_vec(&payload).map_err(|e| StorageIOError::read_state_machine(&e))?;

        let snapshot_idx = self.snapshot_idx.fetch_add(1, Ordering::Relaxed) + 1;
        let snapshot_id = if let Some(last) = last_applied_log {
            format!("{}-{}-{}", last.leader_id, last.index, snapshot_idx)
        } else {
            format!("--{}", snapshot_idx)
        };

        let meta = SnapshotMeta {
            last_log_id: last_applied_log,
            last_membership,
            snapshot_id,
        };

        let snapshot = Arc::new(StoredSnapshot {
            meta: meta.clone(),
            data: snapshot_bytes,
        });

        self.persist_snapshot(Arc::clone(&snapshot)).await?;
        write_atomic(&self.current_snapshot_data_path, &snapshot.data)
            .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;

        let _ = self.current_snapshot.swap(
            (
                Some(Owned::new(Arc::new(CurrentSnapshotRef {
                    meta: meta.clone(),
                }))),
                Tag::None,
            ),
            Ordering::Release,
        );

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(snapshot.data.clone())),
        })
    }
}

impl RaftStateMachine<TypeConfig> for Arc<StateMachineStore> {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, BasicNode>), StorageError<NodeId>>
    {
        let state_machine = self.state_machine.read().await;
        Ok((
            state_machine.last_applied_log,
            state_machine.last_membership.clone(),
        ))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<Response>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
    {
        enum Classified {
            Blank(LogId<NodeId>),
            Membership(LogId<NodeId>, Membership<NodeId, BasicNode>),
            Set(LogId<NodeId>, Request),
            RateLimit(LogId<NodeId>, Request),
        }

        let classified: Vec<Classified> = entries
            .into_iter()
            .map(|entry| match entry.payload {
                EntryPayload::Blank => Classified::Blank(entry.log_id),
                EntryPayload::Membership(mem) => Classified::Membership(entry.log_id, mem),
                EntryPayload::Normal(request) => {
                    if matches!(&request, Request::Set { .. }) {
                        Classified::Set(entry.log_id, request)
                    } else {
                        Classified::RateLimit(entry.log_id, request)
                    }
                }
            })
            .collect();

        let count = classified.len();
        let mut res = Vec::with_capacity(count);

        // Pre-scan to find batch boundaries (indices where rate-limit runs start/end).
        // Then process in a single owned pass using drain or indexed removal.
        // Simplest correct approach: convert to VecDeque and pop from front.
        let mut queue = std::collections::VecDeque::from(classified);

        while let Some(item) = queue.pop_front() {
            match item {
                Classified::Blank(log_id) => {
                    self.apply_blank(log_id).await;
                    res.push(Response);
                }
                Classified::Membership(log_id, mem) => {
                    self.apply_membership(log_id, mem).await;
                    res.push(Response);
                }
                Classified::Set(log_id, request) => {
                    let request_id = request.request_id();
                    let is_dup = self.is_duplicate_request(request_id).await;
                    self.with_snapshot_state_machine_write(|sm| {
                        if !is_dup
                            && let Request::Set {
                                ref key, ref value, ..
                            } = request
                        {
                            if key == "generic_key" {
                                let cached = rmp_serde::from_slice::<Uuid>(value)
                                    .ok()
                                    .map_or(0, |uuid| uuid.as_u128());
                                self.store_cached_generic_key(cached);
                            }
                            sm.data.insert(key.clone(), value.clone());
                        }
                        sm.last_applied_log = Some(log_id);
                    })
                    .await;
                    res.push(Response);
                }
                Classified::RateLimit(log_id, request) => {
                    // Batch this entry with any consecutive rate-limit entries.
                    // Collect all mutations into a single Vec to aggregate across the
                    // whole batch — one set of FoldHashMap allocations, one apply pass.
                    let mut all_mutations = Vec::new();
                    let mut last_log_id = log_id;

                    // Finish any async dedupe work before entering the snapshot critical section.
                    let request_id = request.request_id();
                    let is_dup = self.is_duplicate_request(request_id).await;
                    if !is_dup {
                        let batch = request
                            .into_rate_limit_batch()
                            .expect("rate-limit request should convert");
                        all_mutations.extend(batch.mutations);
                    }
                    res.push(Response);

                    // Drain consecutive rate-limit entries.
                    while let Some(Classified::RateLimit(..)) = queue.front() {
                        if let Some(Classified::RateLimit(lid, req)) = queue.pop_front() {
                            last_log_id = lid;
                            let rid = req.request_id();
                            let is_dup = self.is_duplicate_request(rid).await;
                            if !is_dup {
                                let batch = req
                                    .into_rate_limit_batch()
                                    .expect("rate-limit request should convert");
                                all_mutations.extend(batch.mutations);
                            }
                            res.push(Response);
                        }
                    }

                    self.with_snapshot_state_machine_write(|sm| {
                        // Apply all mutations in one pass.
                        if !all_mutations.is_empty() {
                            self.apply_rate_limit_mutations_maps(all_mutations);
                        }

                        // Single state_machine write for the entire batch.
                        sm.last_applied_log = Some(last_log_id);
                    })
                    .await;
                }
            }
        }
        Ok(res)
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<<TypeConfig as RaftTypeConfig>::SnapshotData>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, BasicNode>,
        snapshot: Box<<TypeConfig as RaftTypeConfig>::SnapshotData>,
    ) -> Result<(), StorageError<NodeId>> {
        info!(
            { snapshot_size = snapshot.get_ref().len() },
            "decoding snapshot for installation"
        );

        let new_snapshot = StoredSnapshot {
            meta: meta.clone(),
            data: snapshot.into_inner(),
        };

        let payload = SnapshotPayload::decode(&new_snapshot.data)
            .map_err(|e| StorageIOError::read_snapshot(Some(new_snapshot.meta.signature()), &e))?;
        let new_snapshot = Arc::new(new_snapshot);
        self.persist_snapshot(Arc::clone(&new_snapshot)).await?;
        write_atomic(&self.current_snapshot_data_path, &new_snapshot.data)
            .map_err(|e| StorageIOError::write_snapshot(Some(new_snapshot.meta.signature()), &e))?;

        // Update the state machine and rate limits under the snapshot guard.
        self.with_snapshot_state_machine_write(|state_machine| {
            let SnapshotPayload {
                data,
                rate_limits: new_rate_limits,
            } = payload;

            let updated_state_machine = StateMachineData {
                last_applied_log: meta.last_log_id,
                last_membership: meta.last_membership.clone(),
                data: SnapshotKv::from_owned_map(data),
            };

            self.store_cached_generic_key(StateMachineStore::extract_generic_key_u128(
                &updated_state_machine.data,
            ));

            *state_machine = updated_state_machine;
            self.sync_rate_limits(new_rate_limits);
        })
        .await;

        // Update current snapshot atomically
        let _ = self.current_snapshot.swap(
            (
                Some(Owned::new(Arc::new(CurrentSnapshotRef {
                    meta: new_snapshot.meta.clone(),
                }))),
                Tag::None,
            ),
            Ordering::Release,
        );
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        let current_meta = {
            let guard = Guard::new();
            let ptr = self.current_snapshot.load(Ordering::Acquire, &guard);
            ptr.as_ref().map(|snapshot_ref| snapshot_ref.meta.clone())
        };

        if let Some(meta) = current_meta {
            // Snapshot payload is stored in a standalone file and loaded on demand.
            // This keeps memory usage bounded and avoids stale in-memory payload state,
            // at the cost of file I/O per call to get_current_snapshot().
            let data = tokio::fs::read(&self.current_snapshot_data_path)
                .await
                .map_err(|e| StorageIOError::read_snapshot(Some(meta.signature()), &e))?;
            Ok(Some(Snapshot {
                meta,
                snapshot: Box::new(Cursor::new(data)),
            }))
        } else {
            Ok(None)
        }
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }
}

#[cfg(test)]
#[path = "../tests/store_state_machine.rs"]
mod tests;
