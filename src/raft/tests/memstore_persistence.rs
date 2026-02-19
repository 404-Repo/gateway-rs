use super::*;
use crate::raft::TypeConfig;
use crate::raft::test_utils::unique_path;
use openraft::EntryPayload;
use openraft::LeaderId;
use openraft::LogId;
use openraft::Vote;
use tempfile::tempdir;

fn blank_entry(leader: LeaderId<u64>, idx: u64) -> Entry<TypeConfig> {
    Entry {
        log_id: LogId::new(leader, idx),
        payload: EntryPayload::Blank,
    }
}

#[test]
fn purge_rewrites_log_store_to_trimmed_state() -> anyhow::Result<()> {
    let tmp_dir = tempdir()?;
    let path = unique_path(tmp_dir.path(), "persist_log_store_", ".bin");

    let persistence = TypeConfigLogPersistence::new(&path)?;
    let leader = LeaderId::new(1, 1);

    for idx in 1..=2_000u64 {
        let entry = blank_entry(leader, idx);
        persistence.append_append(&[entry])?;
    }

    let size_after_append = std::fs::metadata(&path)?.len();

    let purge_id = LogId::new(leader, 1_000);
    persistence.append_purge_to(&purge_id)?;

    let size_after_purge = std::fs::metadata(&path)?.len();
    assert!(
        size_after_purge < size_after_append,
        "expected purge to shrink log store file ({} !< {})",
        size_after_purge,
        size_after_append
    );

    let reloaded = TypeConfigLogPersistence::new(&path)?;
    let persisted = reloaded
        .load()?
        .expect("log store should deserialize after purge");

    assert_eq!(persisted.last_purged_log_id, Some(purge_id));
    assert!(
        persisted
            .log
            .iter()
            .all(|entry| entry.get_log_id().index > purge_id.index)
    );

    Ok(())
}

#[test]
fn recover_from_truncated_tail_record() -> anyhow::Result<()> {
    let tmp_dir = tempdir()?;
    let path = unique_path(tmp_dir.path(), "persist_log_store_truncated_", ".bin");

    let mut buf = Vec::new();
    TypeConfigLogPersistence::write_diff_header(&mut buf);
    TypeConfigLogPersistence::append_record(&mut buf, &DiffRecord::VoteSet(Some(Vote::new(1, 1))))?;

    let mut truncated = Vec::new();
    let committed = LogId::new(LeaderId::new(1, 1), 42);
    TypeConfigLogPersistence::append_record(
        &mut truncated,
        &DiffRecord::CommittedSet(Some(committed)),
    )?;
    truncated.truncate(truncated.len().saturating_sub(1));
    buf.extend_from_slice(&truncated);

    std::fs::write(&path, &buf)?;

    let persistence = TypeConfigLogPersistence::new(&path)?;
    let persisted = persistence
        .load()?
        .expect("log store should recover after truncated tail");

    assert!(persisted.vote.is_some());
    assert!(persisted.committed.is_none());

    Ok(())
}

#[test]
fn corrupted_file_is_reinitialized_on_append() -> anyhow::Result<()> {
    let tmp_dir = tempdir()?;
    let path = unique_path(tmp_dir.path(), "persist_log_store_corrupt_", ".bin");

    std::fs::write(&path, b"not-a-log-store")?;

    let persistence = TypeConfigLogPersistence::new(&path)?;
    persistence.append_vote(&Some(Vote::new(2, 2)))?;

    let reloaded = TypeConfigLogPersistence::new(&path)?;
    let persisted = reloaded
        .load()?
        .expect("log store should deserialize after corruption recovery");

    assert!(persisted.vote.is_some());

    Ok(())
}

#[test]
fn restores_from_older_archive_when_newest_archive_is_corrupted() -> anyhow::Result<()> {
    let tmp_dir = tempdir()?;
    let primary_path = unique_path(tmp_dir.path(), "persist_log_store_primary_", ".bin");

    let leader = LeaderId::new(1, 1);
    let old_state = PersistedLogState {
        last_purged_log_id: None,
        committed: Some(LogId::new(leader, 1)),
        vote: Some(Vote::new(1, 1)),
        log: vec![blank_entry(leader, 1)],
    };
    let old_encoded = TypeConfigLogPersistence::encode_state_as_bytes(&old_state)?;
    let old_archive = tmp_dir.path().join(format!(
        "{LOG_STORE_ARCHIVE_PREFIX}T1-N1-1-1_2025_10_01_T_12_34_56Z{LOG_STORE_ARCHIVE_SUFFIX}"
    ));
    let old_compressed = zstd::stream::encode_all(Cursor::new(old_encoded), 1)?;
    std::fs::write(&old_archive, old_compressed)?;

    let latest_archive = tmp_dir.path().join(format!(
        "{LOG_STORE_ARCHIVE_PREFIX}T1-N1-2-2_2025_10_01_T_12_34_57Z{LOG_STORE_ARCHIVE_SUFFIX}"
    ));
    std::fs::write(&latest_archive, b"corrupted-archive-bytes")?;

    let persistence = TypeConfigLogPersistence::new(&primary_path)?;
    let restored = persistence
        .load()?
        .expect("expected restore from older valid archive");

    assert_eq!(restored.log.len(), 1);
    assert_eq!(restored.log[0].log_id.index, 1);
    assert_eq!(restored.committed, Some(LogId::new(leader, 1)));
    assert_eq!(restored.vote, Some(Vote::new(1, 1)));

    let reloaded_primary = TypeConfigLogPersistence::new(&primary_path)?
        .load()?
        .expect("primary should be rewritten after archive restore");
    assert_eq!(reloaded_primary.log.len(), 1);
    assert_eq!(reloaded_primary.log[0].log_id.index, 1);

    Ok(())
}
