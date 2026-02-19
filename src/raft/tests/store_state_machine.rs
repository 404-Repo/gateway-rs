use crate::raft::TypeConfig;
use crate::raft::memstore::persistence::LOG_STORE_ARCHIVE_PREFIX;
use crate::raft::memstore::persistence::PersistedLogState;
use crate::raft::memstore::persistence::TypeConfigLogPersistence;
use crate::raft::test_utils::unique_path;

use super::*;
use anyhow::Result;
use openraft::LeaderId;
use openraft::LogId;
use openraft::RaftLogId;
use openraft::storage::RaftSnapshotBuilder;
use openraft::storage::RaftStateMachine;
use std::fs;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tempfile::{TempDir, tempdir};

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

    let restored = StateMachineStore::with_persistence(dir_path, 2, Some(log_store_path.clone()))?;
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

    let restored = StateMachineStore::with_persistence(dir_path, 5, Some(log_store_path.clone()))?;
    let sm = restored.state_machine.read().await;
    assert_eq!(sm.data.get("key"), Some(&vec![1, 2, 3]));
    assert_eq!(
        sm.last_applied_log,
        Some(LogId::new(LeaderId::new(1, 1), 10))
    );

    Ok(())
}

#[tokio::test]
async fn fails_startup_when_all_snapshots_are_unreadable() -> Result<()> {
    let (temp_dir, log_store_path) = prepare_snapshot_dir()?;
    let dir_path = temp_dir.path();

    let store = Arc::new(StateMachineStore::with_persistence(
        dir_path,
        5,
        Some(log_store_path.clone()),
    )?);
    write_snapshot_with_state(&store, "key", vec![1, 2, 3], 10).await?;
    drop(store);

    let snapshots = collect_sorted_snapshots(dir_path)?;
    assert_eq!(snapshots.len(), 1);
    fs::write(&snapshots[0].0, b"corrupted snapshot contents")?;

    let err = StateMachineStore::with_persistence(dir_path, 5, Some(log_store_path))
        .expect_err("startup should fail when all snapshots are unreadable");
    assert!(
        err.to_string()
            .contains("no valid snapshot could be loaded from"),
        "unexpected error: {err}"
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

#[tokio::test]
async fn get_current_snapshot_fails_when_payload_file_missing() -> Result<()> {
    let (temp_dir, log_store_path) = prepare_snapshot_dir()?;
    let dir_path = temp_dir.path();

    let store = Arc::new(StateMachineStore::with_persistence(
        dir_path,
        2,
        Some(log_store_path),
    )?);
    write_snapshot_with_state(&store, "key", vec![9, 9, 9], 7).await?;

    assert!(store.current_snapshot_data_path.exists());
    fs::remove_file(&store.current_snapshot_data_path)?;

    let mut state_machine = Arc::clone(&store);
    let result = state_machine.get_current_snapshot().await;
    assert!(
        result.is_err(),
        "expected get_current_snapshot() to fail when snapshot payload file is missing"
    );

    Ok(())
}
