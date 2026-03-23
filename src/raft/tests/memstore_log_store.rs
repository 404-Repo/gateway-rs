use super::*;
use crate::raft::TypeConfig;
use crate::raft::test_utils::unique_path;
use openraft::Entry;
use openraft::EntryPayload;
use openraft::LeaderId;
use std::fs;
use std::path::Path;
use tempfile::tempdir;
use tokio::sync::oneshot;

fn blank_entry(leader: LeaderId<u64>, idx: u64) -> Entry<TypeConfig> {
    Entry {
        log_id: LogId::new(leader, idx),
        payload: EntryPayload::Blank,
    }
}

fn log_has_index(inner: &LogStoreInner<TypeConfig>, index: u64) -> bool {
    if inner.log.is_empty() {
        return false;
    }
    let base = inner.base_index;
    let last_exclusive = base + inner.log.len() as u64;
    index >= base && index < last_exclusive
}

fn count_diff_records(path: &Path) -> anyhow::Result<usize> {
    let raw = fs::read(path)?;
    // Header format is: 8-byte magic + 4-byte version
    if raw.len() < 12 {
        anyhow::bail!("diff file is shorter than header");
    }
    let mut pos = 12usize;
    let mut count = 0usize;
    while pos + 4 <= raw.len() {
        let mut len_bytes = [0u8; 4];
        len_bytes.copy_from_slice(&raw[pos..pos + 4]);
        pos += 4;
        let rec_len = u32::from_le_bytes(len_bytes) as usize;
        if pos + rec_len > raw.len() {
            break;
        }
        pos += rec_len;
        count += 1;
    }
    Ok(count)
}

#[tokio::test]
async fn persists_and_recovers_log_store() -> anyhow::Result<()> {
    let tmp_dir = tempdir()?;
    let log_path = unique_path(tmp_dir.path(), "log_store_persist_recover_", ".bin");

    let log_store = LogStore::with_persistence(&log_path)?;

    // Mutate in-memory and persist per-op without cloning the entire store
    let log_id = LogId::new(LeaderId::new(1, 1), 1);
    let entry = blank_entry(LeaderId::new(1, 1), 1);
    {
        let mut guard = log_store.inner.lock().await;
        guard
            .append(vec![entry.clone()])
            .map_err(|e| anyhow::anyhow!(e))?;
        guard.committed = Some(log_id);
        guard.last_purged_log_id = Some(LogId::new(LeaderId::new(1, 1), 0));
        guard.vote = Some(Vote::new(1, 1));
    }
    // Persist each change as diff ops
    log_store
        .persist_if_needed(
            log_store.persist_op_if_needed(Some(super::PersistOp::Append(Arc::new(vec![entry])))),
        )
        .await?;
    log_store
        .persist_if_needed(
            log_store.persist_op_if_needed(Some(super::PersistOp::CommittedSet(Some(log_id)))),
        )
        .await?;
    log_store
        .persist_if_needed(
            log_store.persist_op_if_needed(Some(super::PersistOp::PurgeTo(LogId::new(
                LeaderId::new(1, 1),
                0,
            )))),
        )
        .await?;
    log_store
        .persist_if_needed(
            log_store.persist_op_if_needed(Some(super::PersistOp::VoteSet(Some(Vote::new(1, 1))))),
        )
        .await?;

    drop(log_store);

    let restored = LogStore::with_persistence(&log_path)?;
    let guard = restored.inner.lock().await;
    assert_eq!(guard.log.len(), 1);
    assert!(log_has_index(&guard, 1));
    assert_eq!(guard.committed.unwrap().index, 1);
    assert_eq!(guard.last_purged_log_id.unwrap().index, 0);
    assert_eq!(guard.vote.unwrap().leader_id().node_id, 1);

    Ok(())
}

#[tokio::test]
async fn recovers_from_archive_when_primary_corrupted() -> anyhow::Result<()> {
    let tmp_dir = tempdir()?;
    let log_path = unique_path(tmp_dir.path(), "log_store_recover_archive_", ".bin");
    let persistence = TypeConfigLogPersistence::new(&log_path)?;

    let log_id = LogId::new(LeaderId::new(1, 1), 7);
    let entry = blank_entry(LeaderId::new(1, 1), 7);
    // Write using diff-only append methods
    persistence.append_append(&[entry])?;
    persistence.append_committed(&Some(log_id))?;

    let archive_path = unique_path(tmp_dir.path(), "log_store_backup_test_", ".bin.zst");
    let archive_bytes = fs::read(&log_path)?;
    fs::write(&archive_path, &archive_bytes)?;

    fs::write(&log_path, b"corrupted-log-store-bytes")?;

    let restored = LogStore::with_persistence(&log_path)?;
    let guard = restored.inner.lock().await;
    assert_eq!(guard.log.len(), 1);
    assert!(log_has_index(&guard, log_id.index));
    assert_eq!(guard.committed.unwrap(), log_id);

    Ok(())
}

#[tokio::test]
async fn truncate_removes_boundary_entry() -> anyhow::Result<()> {
    let mut inner = LogStoreInner::<TypeConfig>::default();
    let leader = LeaderId::new(1, 1);
    let entry1 = blank_entry(leader, 5);
    let entry2 = blank_entry(leader, 6);
    inner.append(vec![entry1.clone(), entry2])?;

    inner.truncate(entry1.log_id)?;

    assert!(!log_has_index(&inner, entry1.log_id.index));
    assert!(!log_has_index(&inner, entry1.log_id.index + 1));

    Ok(())
}

#[tokio::test]
async fn purge_rewrites_persistent_log_store() -> anyhow::Result<()> {
    let tmp_dir = tempdir()?;
    let log_path = unique_path(tmp_dir.path(), "log_store_purge_rewrites_", ".bin");

    let log_store = LogStore::with_persistence(&log_path)?;
    let leader = LeaderId::new(1, 1);

    let entry1 = blank_entry(leader, 1);
    let entry2 = blank_entry(leader, 2);

    {
        let mut guard = log_store.inner.lock().await;
        guard
            .append(vec![entry1.clone(), entry2.clone()])
            .map_err(|e| anyhow::anyhow!(e))?;
    }

    log_store
        .persist_if_needed(
            log_store.persist_op_if_needed(Some(super::PersistOp::Append(Arc::new(vec![
                entry1.clone(),
                entry2.clone(),
            ])))),
        )
        .await?;

    let size_after_append = fs::metadata(&log_path)?.len();

    let purge_id = entry1.log_id;
    {
        let mut guard = log_store.inner.lock().await;
        guard.purge(purge_id)?;
    }
    log_store
        .persist_if_needed(
            log_store.persist_op_if_needed(Some(super::PersistOp::PurgeTo(purge_id))),
        )
        .await?;

    let size_after_purge = fs::metadata(&log_path)?.len();
    assert!(
        size_after_purge < size_after_append,
        "expected purge to rewrite log store file ({} !< {})",
        size_after_purge,
        size_after_append
    );

    drop(log_store);

    let restored = LogStore::with_persistence(&log_path)?;
    let guard = restored.inner.lock().await;
    assert_eq!(guard.last_purged_log_id, Some(purge_id));
    assert!(!log_has_index(&guard, purge_id.index));
    assert!(log_has_index(&guard, purge_id.index + 1));

    Ok(())
}

#[tokio::test]
async fn append_rejects_gap_without_persisting_invalid_entry() -> anyhow::Result<()> {
    let tmp_dir = tempdir()?;
    let log_path = unique_path(tmp_dir.path(), "log_store_gap_reject_", ".bin");
    let leader = LeaderId::new(1, 1);
    let first = blank_entry(leader, 1);
    let invalid_gap = blank_entry(leader, 3);

    let log_store = LogStore::with_persistence(&log_path)?;
    log_store.append_batch(vec![first.clone()]).await?;

    let err = log_store
        .append_batch(vec![invalid_gap])
        .await
        .expect_err("append with a gap must fail");
    assert!(err.to_string().contains("log gap detected"));

    drop(log_store);

    let restored = LogStore::with_persistence(&log_path)?;
    let guard = restored.inner.lock().await;
    assert_eq!(guard.log.len(), 1);
    assert!(log_has_index(&guard, 1));
    assert!(!log_has_index(&guard, 3));

    Ok(())
}

#[tokio::test]
async fn append_is_immediately_recoverable_without_background_flush() -> anyhow::Result<()> {
    let tmp_dir = tempdir()?;
    let log_path = unique_path(tmp_dir.path(), "log_store_immediate_recover_", ".bin");
    let leader = LeaderId::new(1, 1);
    let entry = blank_entry(leader, 11);

    let log_store = LogStore::with_persistence(&log_path)?;
    log_store
        .persist_if_needed(
            log_store.persist_op_if_needed(Some(super::PersistOp::Append(Arc::new(vec![
                entry.clone(),
            ])))),
        )
        .await?;

    let persisted = TypeConfigLogPersistence::new(&log_path)?
        .load()?
        .expect("persisted log state should exist after append");
    assert_eq!(persisted.log.len(), 1);
    assert_eq!(persisted.log[0].log_id.index, entry.log_id.index);

    Ok(())
}

#[test]
fn fails_startup_when_persisted_log_is_not_contiguous() -> anyhow::Result<()> {
    let tmp_dir = tempdir()?;
    let log_path = unique_path(tmp_dir.path(), "log_store_non_contiguous_", ".bin");
    let leader = LeaderId::new(1, 1);
    let state = PersistedLogState {
        last_purged_log_id: None,
        committed: None,
        vote: None,
        log: vec![blank_entry(leader, 1), blank_entry(leader, 3)],
    };
    let bytes = TypeConfigLogPersistence::encode_state_as_bytes(&state)?;
    fs::write(&log_path, bytes)?;

    let err = LogStore::with_persistence(&log_path)
        .expect_err("log store startup should fail when persisted log entries are not contiguous");
    assert!(err.to_string().contains("not contiguous"));

    Ok(())
}

#[tokio::test]
async fn process_batch_merges_adjacent_append_ops() -> anyhow::Result<()> {
    let tmp_dir = tempdir()?;
    let log_path = unique_path(tmp_dir.path(), "log_store_merge_batch_", ".bin");
    let persistence = TypeConfigLogPersistence::new(&log_path)?;

    let leader = LeaderId::new(1, 1);
    let (tx1, rx1) = oneshot::channel::<Result<(), Box<StorageError<NodeId>>>>();
    let (tx2, rx2) = oneshot::channel::<Result<(), Box<StorageError<NodeId>>>>();
    let (tx3, rx3) = oneshot::channel::<Result<(), Box<StorageError<NodeId>>>>();
    let batch = vec![
        (
            PersistOp::Append(Arc::new(vec![blank_entry(leader, 1)])),
            tx1,
        ),
        (
            PersistOp::Append(Arc::new(vec![blank_entry(leader, 2)])),
            tx2,
        ),
        (
            PersistOp::Append(Arc::new(vec![blank_entry(leader, 3)])),
            tx3,
        ),
    ];

    LogStore::<TypeConfig>::process_persist_batch(&persistence, batch);

    let r1 = rx1.await?;
    let r2 = rx2.await?;
    let r3 = rx3.await?;
    assert!(r1.is_ok());
    assert!(r2.is_ok());
    assert!(r3.is_ok());

    assert_eq!(
        count_diff_records(&log_path)?,
        1,
        "adjacent append ops should produce one persisted append record"
    );

    let restored = LogStore::with_persistence(&log_path)?;
    let guard = restored.inner.lock().await;
    assert_eq!(guard.log.len(), 3);
    assert!(log_has_index(&guard, 1));
    assert!(log_has_index(&guard, 2));
    assert!(log_has_index(&guard, 3));

    Ok(())
}

#[tokio::test]
async fn process_batch_preserves_non_append_boundaries() -> anyhow::Result<()> {
    let tmp_dir = tempdir()?;
    let log_path = unique_path(tmp_dir.path(), "log_store_merge_boundary_", ".bin");
    let persistence = TypeConfigLogPersistence::new(&log_path)?;

    let leader = LeaderId::new(1, 1);
    let (tx1, rx1) = oneshot::channel::<Result<(), Box<StorageError<NodeId>>>>();
    let (tx2, rx2) = oneshot::channel::<Result<(), Box<StorageError<NodeId>>>>();
    let (tx3, rx3) = oneshot::channel::<Result<(), Box<StorageError<NodeId>>>>();
    let batch = vec![
        (
            PersistOp::Append(Arc::new(vec![blank_entry(leader, 1)])),
            tx1,
        ),
        (PersistOp::VoteSet(Some(Vote::new(1, 1))), tx2),
        (
            PersistOp::Append(Arc::new(vec![blank_entry(leader, 2)])),
            tx3,
        ),
    ];

    LogStore::<TypeConfig>::process_persist_batch(&persistence, batch);

    let r1 = rx1.await?;
    let r2 = rx2.await?;
    let r3 = rx3.await?;
    assert!(r1.is_ok());
    assert!(r2.is_ok());
    assert!(r3.is_ok());

    assert_eq!(
        count_diff_records(&log_path)?,
        3,
        "append ops separated by non-append ops must not be merged"
    );

    let restored = LogStore::with_persistence(&log_path)?;
    let guard = restored.inner.lock().await;
    assert_eq!(guard.log.len(), 2);
    assert!(log_has_index(&guard, 1));
    assert!(log_has_index(&guard, 2));
    assert_eq!(guard.vote, Some(Vote::new(1, 1)));

    Ok(())
}
