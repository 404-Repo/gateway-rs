mod persistence;

use openraft::storage::RaftStateMachine;
use openraft::storage::Snapshot;
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
use sdd::{AtomicOwned, Guard, Owned, Tag};
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::{Cursor, Error as IoError};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tokio::task::spawn_blocking;
use tracing::info;

use crate::raft::NodeId;
use crate::raft::TypeConfig;

use foldhash::fast::RandomState;
use persistence::SnapshotPersistence;
use scc::hash_cache::Entry as CacheEntry;
use scc::{HashCache, HashMap};

pub type LogStore = crate::raft::memstore::log_store::LogStore<TypeConfig>;

#[derive(Serialize, Deserialize, Debug, Clone, Copy, Default)]
pub struct RateLimitWindow {
    pub hour_epoch: u64,
    pub day_epoch: u64,
    pub hour: u64,
    pub day: u64,
}

impl RateLimitWindow {
    fn from_delta(delta: &RateLimitDelta) -> Self {
        Self {
            hour_epoch: delta.hour_epoch,
            day_epoch: delta.day_epoch,
            hour: delta.add_hour as u64,
            day: delta.add_day as u64,
        }
    }

    fn apply_delta(&mut self, delta: &RateLimitDelta) {
        let mut add_hour = delta.add_hour as u64;
        match delta.hour_epoch.cmp(&self.hour_epoch) {
            std::cmp::Ordering::Greater => {
                self.hour_epoch = delta.hour_epoch;
                self.hour = 0;
            }
            std::cmp::Ordering::Less => {
                add_hour = 0;
            }
            std::cmp::Ordering::Equal => {}
        }

        let mut add_day = delta.add_day as u64;
        match delta.day_epoch.cmp(&self.day_epoch) {
            std::cmp::Ordering::Greater => {
                self.day_epoch = delta.day_epoch;
                self.day = 0;
            }
            std::cmp::Ordering::Less => {
                add_day = 0;
            }
            std::cmp::Ordering::Equal => {}
        }

        self.hour = self.hour.saturating_add(add_hour);
        self.day = self.day.saturating_add(add_day);
    }
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
    pub add_hour: u16,
    pub add_day: u16,
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

#[derive(Serialize, Deserialize, Debug, Clone)]
pub(super) struct SnapshotPayload {
    pub data: BTreeMap<String, Vec<u8>>,
    pub rate_limits: BTreeMap<RateLimitKey, RateLimitWindow>,
}

#[derive(Serialize)]
struct SnapshotPayloadRef<'a> {
    data: &'a BTreeMap<String, Vec<u8>>,
    rate_limits: &'a BTreeMap<RateLimitKey, RateLimitWindow>,
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
    current_snapshot: AtomicOwned<StoredSnapshot>,

    /// Optional on-disk persistence configuration.
    persistence: Option<Arc<SnapshotPersistence>>,

    /// Concurrent request-id deduplication cache to avoid re-applying idempotent requests.
    request_dedupe: HashCache<u128, Instant, RandomState>,

    /// Concurrent hash map for rate limits, swapped atomically during snapshot installs.
    rate_limits: AtomicOwned<HashMap<RateLimitKey, RateLimitWindow, RandomState>>,

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
            persistence: None,
            request_dedupe: HashCache::with_capacity_and_hasher(512, 1024, RandomState::default()),
            rate_limits: AtomicOwned::new(HashMap::with_capacity_and_hasher(
                4096,
                RandomState::default(),
            )),
            snapshot_guard: tokio::sync::Mutex::new(()),
        }
    }
}

impl StateMachineStore {
    fn build_rate_limit_map(
        new_limits: BTreeMap<RateLimitKey, RateLimitWindow>,
    ) -> HashMap<RateLimitKey, RateLimitWindow, RandomState> {
        let capacity = new_limits.len().max(4096);
        let map = HashMap::with_capacity_and_hasher(capacity, RandomState::default());
        for (key, value) in new_limits {
            let _ = map.insert_sync(key, value);
        }
        map
    }

    fn current_rate_limits<'guard>(
        &self,
        guard: &'guard Guard,
    ) -> Option<&'guard HashMap<RateLimitKey, RateLimitWindow, RandomState>> {
        self.rate_limits.load(Ordering::Acquire, guard).as_ref()
    }

    fn sync_rate_limits(&self, new_limits: BTreeMap<RateLimitKey, RateLimitWindow>) {
        let map = Self::build_rate_limit_map(new_limits);
        let _ = self
            .rate_limits
            .swap((Some(Owned::new(map)), Tag::None), Ordering::AcqRel);
    }

    pub fn with_persistence<P: AsRef<Path>>(
        dir: P,
        retention: usize,
        log_store_path: Option<PathBuf>,
    ) -> anyhow::Result<Self> {
        let persistence = Arc::new(SnapshotPersistence::new(dir, retention, log_store_path)?);
        let (state_machine_data, rate_limits_data, snapshot_idx, current_snapshot) =
            persistence.load_latest()?;

        let rate_limits = Self::build_rate_limit_map(rate_limits_data);

        Ok(Self {
            state_machine: RwLock::new(state_machine_data),
            snapshot_idx: AtomicU64::new(snapshot_idx),
            current_snapshot: match current_snapshot {
                Some(snapshot) => AtomicOwned::new(snapshot),
                None => AtomicOwned::null(),
            },
            persistence: Some(persistence),
            request_dedupe: HashCache::with_capacity_and_hasher(256, 1024, RandomState::default()),
            rate_limits: AtomicOwned::new(rate_limits),
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

    pub async fn get_rate_limit_window(&self, key: &RateLimitKey) -> Option<RateLimitWindow> {
        let guard = Guard::new();
        self.current_rate_limits(&guard)
            .and_then(|map| map.read_sync(key, |_, v| *v))
    }

    async fn persist_snapshot(
        &self,
        snapshot: &StoredSnapshot,
    ) -> Result<(), StorageError<NodeId>> {
        if let Some(persistence) = &self.persistence {
            let persistence = Arc::clone(persistence);
            let to_store = snapshot.clone();
            match spawn_blocking(move || persistence.store(&to_store)).await {
                Ok(Ok(())) => {}
                Ok(Err(err)) => {
                    return Err(StorageIOError::write_snapshot(
                        Some(snapshot.meta.signature()),
                        &err,
                    )
                    .into());
                }
                Err(join_err) => {
                    let io_err = IoError::other(format!(
                        "task join error while persisting snapshot: {join_err}"
                    ));
                    return Err(StorageIOError::write_snapshot(
                        Some(snapshot.meta.signature()),
                        &io_err,
                    )
                    .into());
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
                    let map = self
                        .current_rate_limits(&guard)
                        .expect("rate limit map should be initialized");
                    for d in deltas {
                        let key = d.key();
                        map.entry_sync(key)
                            .and_modify(|w| {
                                w.apply_delta(&d);
                            })
                            .or_insert(RateLimitWindow::from_delta(&d));
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
            let mut rate_limits = BTreeMap::new();
            let guard = Guard::new();
            if let Some(map) = self.current_rate_limits(&guard) {
                map.iter_sync(|k, v| {
                    rate_limits.insert(*k, *v);
                    true
                });
            }

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

        let snapshot = StoredSnapshot {
            meta: meta.clone(),
            data: snapshot_bytes.clone(),
        };

        self.persist_snapshot(&snapshot).await?;

        let _ = self
            .current_snapshot
            .swap((Some(Owned::new(snapshot)), Tag::None), Ordering::Release);

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
        self.persist_snapshot(&new_snapshot).await?;

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
            (Some(Owned::new(new_snapshot)), Tag::None),
            Ordering::Release,
        );
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        let guard = Guard::new();
        let ptr = self.current_snapshot.load(Ordering::Acquire, &guard);
        if let Some(snapshot_ref) = ptr.as_ref() {
            let data = snapshot_ref.data.clone();
            Ok(Some(Snapshot {
                meta: snapshot_ref.meta.clone(),
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
mod tests {
    use crate::raft::memstore::persistence::PersistedLogState;
    use crate::raft::memstore::persistence::TypeConfigLogPersistence;
    use crate::raft::memstore::persistence::LOG_STORE_ARCHIVE_PREFIX;
    use crate::raft::test_utils::unique_path;
    use crate::raft::TypeConfig;

    use super::*;
    use anyhow::Result;
    use openraft::storage::RaftSnapshotBuilder;
    use openraft::LeaderId;
    use openraft::LogId;
    use openraft::RaftLogId;
    use std::fs;
    use std::io::Cursor;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::time::Duration;
    use tempfile::{tempdir, TempDir};

    fn blank_entry(leader: LeaderId<u64>, idx: u64) -> Entry<TypeConfig> {
        Entry {
            log_id: LogId::new(leader, idx),
            payload: EntryPayload::Blank,
        }
    }

    fn prepare_snapshot_dir() -> Result<(TempDir, PathBuf)> {
        let dir = tempdir()?;
        let log_store_path = unique_path(dir.path(), "state_store_log_", ".bin");
        fs::write(&log_store_path, b"log-store-state")?;
        Ok((dir, log_store_path))
    }

    async fn write_snapshot_with_state(
        store: &Arc<StateMachineStore>,
        key: &str,
        value: Vec<u8>,
        log_index: u64,
    ) -> Result<()> {
        {
            let mut sm = store.state_machine.write().await;
            sm.data.insert(key.to_string(), value);
            sm.last_applied_log = Some(LogId::new(LeaderId::new(1, 1), log_index));
        }

        let mut builder = Arc::clone(store);
        builder.build_snapshot().await?;
        Ok(())
    }

    fn collect_sorted_snapshots(dir: &Path) -> Result<Vec<(PathBuf, u64)>> {
        let mut snapshots: Vec<(PathBuf, u64)> = fs::read_dir(dir)?
            .filter_map(|entry| {
                let entry = entry.ok()?;
                let path = entry.path();
                if !path.is_file() {
                    return None;
                }
                SnapshotPersistence::extract_snapshot_idx(&path).map(|idx| (path, idx))
            })
            .collect();
        snapshots.sort_by_key(|(_, idx)| *idx);
        Ok(snapshots)
    }

    fn collect_sorted_log_archives(dir: &Path) -> Result<Vec<(PathBuf, u64)>> {
        let mut archives: Vec<(PathBuf, u64)> = fs::read_dir(dir)?
            .filter_map(|entry| {
                let entry = entry.ok()?;
                let path = entry.path();
                if !path.is_file() {
                    return None;
                }
                SnapshotPersistence::extract_log_store_idx(&path).map(|idx| (path, idx))
            })
            .collect();
        archives.sort_by_key(|(_, idx)| *idx);
        Ok(archives)
    }

    #[tokio::test]
    async fn persists_and_recovers_state_machine() -> Result<()> {
        let (temp_dir, log_store_path) = prepare_snapshot_dir()?;
        let dir_path = temp_dir.path();

        let store = Arc::new(StateMachineStore::with_persistence(
            dir_path,
            2,
            Some(log_store_path.clone()),
        )?);

        write_snapshot_with_state(&store, "key", vec![1, 2, 3], 42).await?;
        drop(store);

        let restored =
            StateMachineStore::with_persistence(dir_path, 2, Some(log_store_path.clone()))?;
        let sm = restored.state_machine.read().await;
        assert_eq!(sm.data.get("key"), Some(&vec![1, 2, 3]));
        assert_eq!(
            sm.last_applied_log,
            Some(LogId::new(LeaderId::new(1, 1), 42))
        );

        let archived_log_stores: Vec<_> = fs::read_dir(dir_path)?
            .filter_map(|entry| {
                let entry = entry.ok()?;
                let name = entry.file_name();
                let name = name.to_string_lossy();
                if name.starts_with(LOG_STORE_ARCHIVE_PREFIX) {
                    Some(entry.path())
                } else {
                    None
                }
            })
            .collect();
        assert_eq!(archived_log_stores.len(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn loads_first_valid_snapshot_when_newer_corrupted() -> Result<()> {
        let (temp_dir, log_store_path) = prepare_snapshot_dir()?;
        let dir_path = temp_dir.path();

        let store = Arc::new(StateMachineStore::with_persistence(
            dir_path,
            5,
            Some(log_store_path.clone()),
        )?);

        write_snapshot_with_state(&store, "key", vec![1, 2, 3], 10).await?;
        write_snapshot_with_state(&store, "key", vec![4, 5, 6], 20).await?;
        drop(store);

        let snapshots = collect_sorted_snapshots(dir_path)?;
        assert!(snapshots.len() >= 2, "expected at least two snapshot files");
        let latest_path = snapshots
            .last()
            .map(|(path, _)| path.clone())
            .expect("should have at least one snapshot file");

        fs::write(latest_path, b"corrupted snapshot contents")?;

        let restored =
            StateMachineStore::with_persistence(dir_path, 5, Some(log_store_path.clone()))?;
        let sm = restored.state_machine.read().await;
        assert_eq!(sm.data.get("key"), Some(&vec![1, 2, 3]));
        assert_eq!(
            sm.last_applied_log,
            Some(LogId::new(LeaderId::new(1, 1), 10))
        );

        Ok(())
    }

    #[tokio::test]
    async fn prunes_old_snapshots_and_log_archives() -> Result<()> {
        let (temp_dir, log_store_path) = prepare_snapshot_dir()?;
        let dir_path = temp_dir.path();

        let store = Arc::new(StateMachineStore::with_persistence(
            dir_path,
            2,
            Some(log_store_path.clone()),
        )?);

        for i in 0..3 {
            fs::write(&log_store_path, format!("log-store-state-{i}").as_bytes())?;
            write_snapshot_with_state(&store, &format!("key{i}"), vec![i as u8], (i + 1) as u64)
                .await?;
        }

        drop(store);

        let snapshots = collect_sorted_snapshots(dir_path)?;
        assert_eq!(snapshots.len(), 2);
        assert_eq!(
            snapshots.iter().map(|(_, idx)| *idx).collect::<Vec<_>>(),
            vec![2, 3]
        );

        let log_archives = collect_sorted_log_archives(dir_path)?;
        assert_eq!(log_archives.len(), 2);
        assert_eq!(
            log_archives.iter().map(|(_, idx)| *idx).collect::<Vec<_>>(),
            vec![2, 3]
        );

        Ok(())
    }

    #[tokio::test]
    async fn load_latest_returns_empty_when_no_files() -> Result<()> {
        let temp_dir = tempdir()?;
        let dir_path = temp_dir.path();

        let store = StateMachineStore::with_persistence(dir_path, 3, None)?;
        let sm = store.state_machine.read().await;
        assert!(sm.data.is_empty());
        assert!(sm.last_applied_log.is_none());

        drop(sm);
        drop(store);

        let mut entries = fs::read_dir(dir_path)?;
        assert!(entries.next().is_none());

        Ok(())
    }

    #[tokio::test]
    async fn archive_log_store_is_noop_when_missing_source() -> Result<()> {
        let temp_dir = tempdir()?;
        let dir_path = temp_dir.path();

        let missing_log_path = unique_path(dir_path, "missing_log_store_", ".bin");
        let store = Arc::new(StateMachineStore::with_persistence(
            dir_path,
            2,
            Some(missing_log_path.clone()),
        )?);

        write_snapshot_with_state(&store, "key", vec![42], 1).await?;

        drop(store);

        let snapshots = collect_sorted_snapshots(dir_path)?;
        assert_eq!(snapshots.len(), 1);
        assert!(!missing_log_path.exists());

        let log_archives = collect_sorted_log_archives(dir_path)?;
        assert!(log_archives.is_empty());

        Ok(())
    }

    fn decode_log_store_archive(path: &Path) -> anyhow::Result<PersistedLogState> {
        let raw = fs::read(path)?;
        let decoded = zstd::stream::decode_all(Cursor::new(raw))?;
        TypeConfigLogPersistence::decode_diff_bytes(&decoded)
    }

    #[tokio::test]
    async fn log_store_archive_contains_full_state() -> Result<()> {
        let temp_dir = tempdir()?;
        let dir_path = temp_dir.path();

        let log_store_path = unique_path(dir_path, "state_store_full_", ".bin");
        let persistence = TypeConfigLogPersistence::new(&log_store_path)?;

        let store = Arc::new(StateMachineStore::with_persistence(
            dir_path,
            5,
            Some(log_store_path.clone()),
        )?);

        let leader = LeaderId::new(1, 1);

        for idx in 0..1_000u64 {
            let entry = blank_entry(leader, idx);
            persistence.append_append(&[entry])?;
        }

        write_snapshot_with_state(&store, "first", vec![1], 999).await?;

        let archives_after_first = collect_sorted_log_archives(dir_path)?;
        assert_eq!(archives_after_first.len(), 1);
        let first_archive_path = archives_after_first.last().unwrap().0.clone();
        let first_state = decode_log_store_archive(&first_archive_path)?;
        assert_eq!(first_state.log.len(), 1_000);
        assert_eq!(
            first_state
                .log
                .last()
                .expect("log store should contain entries")
                .get_log_id()
                .index,
            999
        );

        for idx in 1_000..2_000u64 {
            let entry = blank_entry(leader, idx);
            persistence.append_append(&[entry])?;
        }

        write_snapshot_with_state(&store, "second", vec![2], 1_999).await?;

        let archives_after_second = collect_sorted_log_archives(dir_path)?;
        assert_eq!(archives_after_second.len(), 2);
        let second_archive_path = archives_after_second.last().unwrap().0.clone();
        let second_state = decode_log_store_archive(&second_archive_path)?;
        assert_eq!(second_state.log.len(), 2_000);
        assert_eq!(
            second_state
                .log
                .first()
                .expect("log store should contain entries")
                .get_log_id()
                .index,
            0
        );
        assert_eq!(
            second_state
                .log
                .last()
                .expect("log store should contain entries")
                .get_log_id()
                .index,
            1_999
        );

        Ok(())
    }

    #[test]
    fn extract_snapshot_idx_handles_underscored_timestamp() {
        let path = Path::new("snapshot_1-1-42-3_2025_10_01_T_12_34_56Z.bin.zst");
        assert_eq!(SnapshotPersistence::extract_snapshot_idx(path), Some(3));
    }

    #[test]
    fn extract_log_store_idx_handles_underscored_timestamp() {
        let path = Path::new("log_store_1-1-42-3_2025_10_01_T_12_34_56Z.bin.zst");
        assert_eq!(SnapshotPersistence::extract_log_store_idx(path), Some(3));
    }

    #[tokio::test]
    async fn request_dedupe_expires_after_ttl() -> Result<()> {
        let store = StateMachineStore::default();
        let request_id = 4242u128;

        assert!(!store.is_duplicate_request(request_id).await);
        assert!(store.is_duplicate_request(request_id).await);

        tokio::time::sleep(REQUEST_DEDUPE_TTL + Duration::from_millis(10)).await;

        assert!(!store.is_duplicate_request(request_id).await);

        Ok(())
    }
}
