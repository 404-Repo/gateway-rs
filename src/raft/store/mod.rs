mod persistence;

use openraft::storage::RaftStateMachine;
use openraft::storage::Snapshot;
use openraft::BasicNode;
use openraft::Entry;
use openraft::EntryPayload;
use openraft::LogId;
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
use tokio::sync::RwLock;
use tokio::task::spawn_blocking;

use crate::raft::NodeId;
use crate::raft::TypeConfig;

use persistence::SnapshotPersistence;

pub type LogStore = crate::raft::memstore::log_store::LogStore<TypeConfig>;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Request {
    Set { key: String, value: Vec<u8> },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Response;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct StoredSnapshot {
    pub meta: SnapshotMeta<NodeId, BasicNode>,

    /// The data of the state machine at the time of this snapshot.
    pub data: Vec<u8>,
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
}

impl Default for StateMachineStore {
    fn default() -> Self {
        StateMachineStore {
            state_machine: RwLock::new(StateMachineData::default()),
            snapshot_idx: AtomicU64::new(0),
            current_snapshot: AtomicOwned::null(),
            persistence: None,
        }
    }
}

impl StateMachineStore {
    pub fn with_persistence<P: AsRef<Path>>(
        dir: P,
        retention: usize,
        log_store_path: Option<PathBuf>,
    ) -> anyhow::Result<Self> {
        let persistence = Arc::new(SnapshotPersistence::new(dir, retention, log_store_path)?);
        let (state_machine_data, snapshot_idx, current_snapshot) = persistence.load_latest()?;

        Ok(Self {
            state_machine: RwLock::new(state_machine_data),
            snapshot_idx: AtomicU64::new(snapshot_idx),
            current_snapshot: match current_snapshot {
                Some(snapshot) => AtomicOwned::new(snapshot),
                None => AtomicOwned::null(),
            },
            persistence: Some(persistence),
        })
    }

    pub async fn get_raw(&self, key: &str) -> Option<Vec<u8>> {
        let sm = self.state_machine.read().await;
        sm.data.get(key).cloned()
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
}

impl RaftSnapshotBuilder<TypeConfig> for Arc<StateMachineStore> {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        let (data, last_applied_log, last_membership) = {
            let state_machine = self.state_machine.read().await;
            let data = rmp_serde::to_vec(&state_machine.data)
                .map_err(|e| StorageIOError::read_state_machine(&e))?;
            (
                data,
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
            data: data.clone(),
        };

        self.persist_snapshot(&snapshot).await?;

        let _ = self
            .current_snapshot
            .swap((Some(Owned::new(snapshot)), Tag::None), Ordering::Release);

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
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
        let mut res = Vec::new(); //No `with_capacity`; do not know `len` of iterator

        let mut sm = self.state_machine.write().await;

        for entry in entries {
            tracing::debug!(%entry.log_id, "replicate to sm");

            sm.last_applied_log = Some(entry.log_id);

            match entry.payload {
                EntryPayload::Blank => {}
                EntryPayload::Normal(Request::Set { key, value }) => {
                    sm.data.insert(key, value);
                }
                EntryPayload::Membership(mem) => {
                    sm.last_membership = StoredMembership::new(Some(entry.log_id), mem);
                }
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
        tracing::info!(
            { snapshot_size = snapshot.get_ref().len() },
            "decoding snapshot for installation"
        );

        let new_snapshot = StoredSnapshot {
            meta: meta.clone(),
            data: snapshot.into_inner(),
        };

        let updated_state_machine_data = rmp_serde::from_slice(&new_snapshot.data)
            .map_err(|e| StorageIOError::read_snapshot(Some(new_snapshot.meta.signature()), &e))?;
        self.persist_snapshot(&new_snapshot).await?;

        // Update the state machine.
        let updated_state_machine = StateMachineData {
            last_applied_log: meta.last_log_id,
            last_membership: meta.last_membership.clone(),
            data: updated_state_machine_data,
        };
        {
            let mut state_machine = self.state_machine.write().await;
            *state_machine = updated_state_machine;
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
    use tempfile::{tempdir, TempDir};

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
        Ok(TypeConfigLogPersistence::decode_diff_bytes(&decoded)?)
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
            let entry = Entry {
                log_id: LogId::new(leader.clone(), idx),
                payload: EntryPayload::Blank,
            };
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
            let entry = Entry {
                log_id: LogId::new(leader.clone(), idx),
                payload: EntryPayload::Blank,
            };
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
}
