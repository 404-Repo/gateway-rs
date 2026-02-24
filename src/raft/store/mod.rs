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
use sdd::{AtomicOwned, Guard, Owned, Tag};
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::{Cursor, Error as IoError};
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
    pub hour_epoch: u64,
    pub day_epoch: u64,
    pub hour_limits: Vec<(RateLimitKey, u64)>,
    pub day_limits: Vec<(RateLimitKey, u64)>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, Eq, PartialEq, Hash, PartialOrd, Ord)]
pub enum Subject {
    User,
    Company,
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
    pub hour_epoch: u64,
    pub day_epoch: u64,
    pub add_hour: u32,
    pub add_day: u32,
}

impl RateLimitDelta {
    pub const fn key(&self) -> RateLimitKey {
        rate_limit_key(self.subject, self.id)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Request {
    Set {
        request_id: u128,
        key: String,
        value: Vec<u8>,
    },
    RateLimitDeltas {
        request_id: u128,
        deltas: Vec<RateLimitDelta>,
    },
}

impl Request {
    pub fn request_id(&self) -> u128 {
        match self {
            Request::Set { request_id, .. } => *request_id,
            Request::RateLimitDeltas { request_id, .. } => *request_id,
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
pub(super) struct SnapshotPayload {
    pub data: BTreeMap<String, Vec<u8>>,
    pub rate_limits: RateLimitSnapshot,
}

#[derive(Serialize)]
struct SnapshotPayloadRef<'a> {
    data: &'a BTreeMap<String, Vec<u8>>,
    rate_limits: &'a RateLimitSnapshot,
}

impl SnapshotPayload {
    fn decode(bytes: &[u8]) -> Result<Self, rmp_serde::decode::Error> {
        rmp_serde::from_slice(bytes)
    }
}

/// Data contained in the Raft state machine.
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct StateMachineData {
    pub last_applied_log: Option<LogId<NodeId>>,

    pub last_membership: StoredMembership<NodeId, BasicNode>,

    /// Application data.
    pub data: BTreeMap<String, Vec<u8>>,
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
    rate_limits_hour: AtomicOwned<HashMap<RateLimitKey, u64, RandomState>>,
    rate_limits_day: AtomicOwned<HashMap<RateLimitKey, u64, RandomState>>,
    hour_epoch: AtomicU64,
    day_epoch: AtomicU64,

    /// Guard to synchronize apply and snapshot building for consistent cuts.
    snapshot_guard: tokio::sync::Mutex<()>,
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
            rate_limits_hour: AtomicOwned::new(HashMap::with_capacity_and_hasher(
                4096,
                RandomState::default(),
            )),
            rate_limits_day: AtomicOwned::new(HashMap::with_capacity_and_hasher(
                4096,
                RandomState::default(),
            )),
            hour_epoch: AtomicU64::new(0),
            day_epoch: AtomicU64::new(0),
            snapshot_guard: tokio::sync::Mutex::new(()),
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

    fn current_rate_limits_hour<'guard>(
        &self,
        guard: &'guard Guard,
    ) -> Option<&'guard HashMap<RateLimitKey, u64, RandomState>> {
        self.rate_limits_hour
            .load(Ordering::Acquire, guard)
            .as_ref()
    }

    fn current_rate_limits_day<'guard>(
        &self,
        guard: &'guard Guard,
    ) -> Option<&'guard HashMap<RateLimitKey, u64, RandomState>> {
        self.rate_limits_day.load(Ordering::Acquire, guard).as_ref()
    }

    fn sync_rate_limits(&self, snapshot: RateLimitSnapshot) {
        let hour_map = Self::build_rate_limit_map(snapshot.hour_limits);
        let day_map = Self::build_rate_limit_map(snapshot.day_limits);
        let _ = self
            .rate_limits_hour
            .swap((Some(Owned::new(hour_map)), Tag::None), Ordering::AcqRel);
        let _ = self
            .rate_limits_day
            .swap((Some(Owned::new(day_map)), Tag::None), Ordering::AcqRel);
        self.hour_epoch
            .store(snapshot.hour_epoch, Ordering::Release);
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

        let hour_epoch = rate_limits_data.hour_epoch;
        let day_epoch = rate_limits_data.day_epoch;
        let rate_limits_hour = Self::build_rate_limit_map(rate_limits_data.hour_limits);
        let rate_limits_day = Self::build_rate_limit_map(rate_limits_data.day_limits);
        let snapshot_data_path = persistence.dir.join("current_snapshot_payload.bin");
        if let Some(snapshot) = current_snapshot.as_ref() {
            write_atomic(&snapshot_data_path, &snapshot.data)?;
        }

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
            rate_limits_hour: AtomicOwned::new(rate_limits_hour),
            rate_limits_day: AtomicOwned::new(rate_limits_day),
            hour_epoch: AtomicU64::new(hour_epoch),
            day_epoch: AtomicU64::new(day_epoch),
            snapshot_guard: tokio::sync::Mutex::new(()),
        })
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
        sm.data.get(key).cloned()
    }

    pub async fn get_rate_limit_usage(
        &self,
        key: &RateLimitKey,
        hour_epoch: u64,
        day_epoch: u64,
    ) -> (u64, u64) {
        let guard = Guard::new();
        let hour = {
            let current = self.hour_epoch.load(Ordering::Acquire);
            if current != hour_epoch {
                0
            } else {
                let value = self
                    .current_rate_limits_hour(&guard)
                    .and_then(|map| map.read_sync(key, |_, v| *v))
                    .unwrap_or(0);
                if self.hour_epoch.load(Ordering::Acquire) != current {
                    0
                } else {
                    value
                }
            }
        };

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

        (hour, day)
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
        let _sg = self.snapshot_guard.lock().await;
        let mut sm = self.state_machine.write().await;
        sm.last_applied_log = Some(log_id);
    }

    async fn apply_membership(&self, log_id: LogId<NodeId>, mem: Membership<NodeId, BasicNode>) {
        let _sg = self.snapshot_guard.lock().await;
        let mut sm = self.state_machine.write().await;
        sm.last_applied_log = Some(log_id);
        sm.last_membership = StoredMembership::new(Some(log_id), mem);
    }

    async fn apply_request(&self, log_id: LogId<NodeId>, request: Request, is_dup: bool) {
        let _sg = self.snapshot_guard.lock().await;
        match request {
            Request::Set { key, value, .. } => {
                let mut sm = self.state_machine.write().await;
                if !is_dup {
                    sm.data.insert(key, value);
                }
                sm.last_applied_log = Some(log_id);
            }
            Request::RateLimitDeltas { deltas, .. } => {
                if !is_dup {
                    let guard = Guard::new();
                    let mut aggregated: FoldHashMap<(Subject, u128, u64, u64), (u64, u64)> =
                        FoldHashMap::with_capacity_and_hasher(deltas.len(), RandomState::default());
                    for d in deltas {
                        let key = (d.subject, d.id, d.hour_epoch, d.day_epoch);
                        aggregated
                            .entry(key)
                            .and_modify(|acc| {
                                acc.0 = acc.0.saturating_add(d.add_hour as u64);
                                acc.1 = acc.1.saturating_add(d.add_day as u64);
                            })
                            .or_insert((d.add_hour as u64, d.add_day as u64));
                    }

                    let mut hour_updates = Vec::with_capacity(aggregated.len());
                    let mut day_updates = Vec::with_capacity(aggregated.len());
                    for ((subject, id, hour_epoch, day_epoch), (add_hour, add_day)) in aggregated {
                        if add_hour > 0 {
                            hour_updates.push((hour_epoch, subject, id, add_hour));
                        }
                        if add_day > 0 {
                            day_updates.push((day_epoch, subject, id, add_day));
                        }
                    }

                    hour_updates.sort_by_key(|(epoch, _, _, _)| *epoch);
                    day_updates.sort_by_key(|(epoch, _, _, _)| *epoch);

                    let mut current_hour_epoch = self.hour_epoch.load(Ordering::Relaxed);
                    for (hour_epoch, subject, id, add_hour) in hour_updates {
                        if hour_epoch > current_hour_epoch {
                            let new_map =
                                HashMap::with_capacity_and_hasher(4096, RandomState::default());
                            let _ = self
                                .rate_limits_hour
                                .swap((Some(Owned::new(new_map)), Tag::None), Ordering::AcqRel);
                            self.hour_epoch.store(hour_epoch, Ordering::Release);
                            current_hour_epoch = hour_epoch;
                        }
                        if hour_epoch == current_hour_epoch
                            && let Some(map) = self.current_rate_limits_hour(&guard)
                        {
                            let key = rate_limit_key(subject, id);
                            map.entry_sync(key)
                                .and_modify(|v| {
                                    *v = v.saturating_add(add_hour);
                                })
                                .or_insert(add_hour);
                        }
                    }

                    let mut current_day_epoch = self.day_epoch.load(Ordering::Relaxed);
                    for (day_epoch, subject, id, add_day) in day_updates {
                        if day_epoch > current_day_epoch {
                            let new_map =
                                HashMap::with_capacity_and_hasher(4096, RandomState::default());
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
                            map.entry_sync(key)
                                .and_modify(|v| {
                                    *v = v.saturating_add(add_day);
                                })
                                .or_insert(add_day);
                        }
                    }
                }

                let mut sm = self.state_machine.write().await;
                sm.last_applied_log = Some(log_id);
            }
        }
    }
}

impl RaftSnapshotBuilder<TypeConfig> for Arc<StateMachineStore> {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        let (snapshot_bytes, last_applied_log, last_membership) = {
            // Ensure a consistent cut between state_machine and rate_limits
            let _sg = self.snapshot_guard.lock().await;
            let state_machine = self.state_machine.read().await;

            // Collect rate limits from the concurrent HashMap
            let guard = Guard::new();
            let mut hour_limits = Vec::new();
            if let Some(map) = self.current_rate_limits_hour(&guard) {
                map.iter_sync(|k, v| {
                    hour_limits.push((*k, *v));
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
                hour_epoch: self.hour_epoch.load(Ordering::Acquire),
                day_epoch: self.day_epoch.load(Ordering::Acquire),
                hour_limits,
                day_limits,
            };
            let payload = SnapshotPayloadRef {
                data: &state_machine.data,
                rate_limits: &rate_limits,
            };
            let snapshot_bytes =
                rmp_serde::to_vec(&payload).map_err(|e| StorageIOError::read_state_machine(&e))?;
            (
                snapshot_bytes,
                state_machine.last_applied_log,
                state_machine.last_membership.clone(),
            )
        };

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
            data: snapshot_bytes.clone(),
        });

        self.persist_snapshot(Arc::clone(&snapshot)).await?;
        write_atomic(&self.current_snapshot_data_path, &snapshot_bytes)
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
            snapshot: Box::new(Cursor::new(snapshot_bytes)),
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
        let mut res = Vec::new();

        for entry in entries {
            let log_id = entry.log_id;

            match entry.payload {
                EntryPayload::Blank => self.apply_blank(log_id).await,
                EntryPayload::Normal(request) => {
                    let request_id = request.request_id();
                    let is_dup = self.is_duplicate_request(request_id).await;
                    self.apply_request(log_id, request, is_dup).await;
                }
                EntryPayload::Membership(mem) => self.apply_membership(log_id, mem).await,
            }
            res.push(Response);
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
        {
            let _sg = self.snapshot_guard.lock().await;

            let SnapshotPayload {
                data,
                rate_limits: new_rate_limits,
            } = payload;

            let updated_state_machine = StateMachineData {
                last_applied_log: meta.last_log_id,
                last_membership: meta.last_membership.clone(),
                data,
            };

            {
                let mut state_machine = self.state_machine.write().await;
                *state_machine = updated_state_machine;
            }

            self.sync_rate_limits(new_rate_limits);
        }

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
