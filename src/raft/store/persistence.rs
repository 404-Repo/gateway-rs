use std::collections::BTreeMap;
use std::fs;
use std::io::{Cursor, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use anyhow::{anyhow, Context};
use chrono::Utc;
use tracing::warn;

use crate::raft::memstore::persistence::{
    TypeConfigLogPersistence, LOG_STORE_ARCHIVE_PREFIX, LOG_STORE_ARCHIVE_SUFFIX,
};
use crate::raft::SNAPSHOT_COMPRESSION_LVL;

use super::{StateMachineData, StoredSnapshot};

pub(crate) const SNAPSHOT_FILE_PREFIX: &str = "snapshot_";

#[derive(Debug)]
pub(crate) struct SnapshotPersistence {
    pub(crate) dir: PathBuf,
    pub(crate) retention: usize,
    pub(crate) lock: Mutex<()>,
    pub(crate) log_store_path: Option<PathBuf>,
}

impl SnapshotPersistence {
    pub(crate) fn new<P: AsRef<Path>>(
        dir: P,
        retention: usize,
        log_store_path: Option<PathBuf>,
    ) -> anyhow::Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        let retention = retention.max(1);
        if let Some(parent) = dir.parent() {
            if !parent.exists() {
                fs::create_dir_all(parent).with_context(|| {
                    format!("Failed to create snapshot parent dir: {:?}", parent)
                })?;
            }
        }
        if !dir.exists() {
            fs::create_dir_all(&dir)
                .with_context(|| format!("Failed to create snapshot directory: {:?}", dir))?;
        }
        Ok(Self {
            dir,
            retention,
            lock: Mutex::new(()),
            log_store_path,
        })
    }

    pub(crate) fn store(&self, snapshot: &StoredSnapshot) -> std::io::Result<()> {
        let _guard = self
            .lock
            .lock()
            .expect("snapshot persistence lock poisoned");
        if !self.dir.exists() {
            fs::create_dir_all(&self.dir)?;
        }
        let timestamp = Utc::now().format("%Y_%m_%d_T_%H_%M_%SZ").to_string();
        let file_name = format!(
            "{SNAPSHOT_FILE_PREFIX}{}_{timestamp}.bin.zst",
            snapshot.meta.snapshot_id
        );
        let path = self.dir.join(&file_name);
        let tmp_path = path.with_extension("tmp");
        let encoded = rmp_serde::to_vec(snapshot)
            .map_err(|e| std::io::Error::other(format!("encode snapshot: {e}")))?;
        let compressed = zstd::stream::encode_all(Cursor::new(&encoded), SNAPSHOT_COMPRESSION_LVL)
            .map_err(|e| std::io::Error::other(format!("compress snapshot: {e}")))?;
        {
            let mut file = fs::File::create(&tmp_path)?;
            file.write_all(&compressed)?;
            file.sync_all()?;
        }
        fs::rename(&tmp_path, &path)?;
        let final_file = fs::File::open(&path)?;
        final_file.sync_all()?;
        if let Some(parent) = path.parent() {
            let dir = fs::File::open(parent)?;
            dir.sync_all()?;
        }
        self.archive_log_store(snapshot, &timestamp)?;
        self.prune_locked()?;
        Ok(())
    }

    pub(crate) fn load_latest(
        &self,
    ) -> anyhow::Result<(StateMachineData, u64, Option<StoredSnapshot>)> {
        let _guard = self
            .lock
            .lock()
            .expect("snapshot persistence lock poisoned");
        let files = self.collect_snapshot_files()?;
        let had_files = !files.is_empty();

        for (path, idx) in files.into_iter().rev() {
            match Self::read_snapshot_file(&path) {
                Ok((state_machine, stored)) => {
                    return Ok((state_machine, idx, Some(stored)));
                }
                Err(err) => {
                    warn!(
                        error = ?err,
                        path = %path.display(),
                        "failed to load snapshot, falling back to older one"
                    );
                }
            }
        }

        if had_files {
            warn!("no valid snapshot could be loaded from disk; starting from empty state");
        }

        Ok((StateMachineData::default(), 0, None))
    }

    fn read_snapshot_file(path: &Path) -> anyhow::Result<(StateMachineData, StoredSnapshot)> {
        let buf =
            fs::read(path).with_context(|| format!("Failed to read snapshot file: {:?}", path))?;
        let decoded = zstd::stream::decode_all(Cursor::new(&buf))
            .map_err(|e| anyhow!("decompress snapshot {:?}: {e}", path))?;
        let stored: StoredSnapshot = rmp_serde::from_slice(&decoded)
            .map_err(|e| anyhow!("decode snapshot {:?}: {e}", path))?;
        let data_map: BTreeMap<String, Vec<u8>> = rmp_serde::from_slice(&stored.data)
            .map_err(|e| anyhow!("decode snapshot state {:?}: {e}", path))?;
        let state_machine = StateMachineData {
            last_applied_log: stored.meta.last_log_id,
            last_membership: stored.meta.last_membership.clone(),
            data: data_map,
        };
        Ok((state_machine, stored))
    }

    fn prune_locked(&self) -> std::io::Result<()> {
        self.prune_file_set(self.collect_snapshot_files()?)?;
        self.prune_file_set(self.collect_log_store_files()?)?;
        Ok(())
    }

    fn prune_file_set(&self, mut files: Vec<(PathBuf, u64)>) -> std::io::Result<()> {
        if files.len() <= self.retention {
            return Ok(());
        }

        files.sort_by_key(|(_, idx)| *idx);
        while files.len() > self.retention {
            if let Some((path, _)) = files.first().cloned() {
                fs::remove_file(path)?;
                files.remove(0);
            }
        }
        let dir = fs::File::open(&self.dir)?;
        dir.sync_all()?;
        Ok(())
    }

    pub(crate) fn collect_snapshot_files(&self) -> std::io::Result<Vec<(PathBuf, u64)>> {
        if !self.dir.exists() {
            return Ok(Vec::new());
        }

        let mut entries = Vec::new();
        for entry in fs::read_dir(&self.dir)? {
            let entry = entry?;
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            if let Some(idx) = Self::extract_snapshot_idx(&path) {
                entries.push((path, idx));
            }
        }
        entries.sort_by_key(|(_, idx)| *idx);
        Ok(entries)
    }

    pub(crate) fn collect_log_store_files(&self) -> std::io::Result<Vec<(PathBuf, u64)>> {
        if !self.dir.exists() {
            return Ok(Vec::new());
        }

        let mut entries = Vec::new();
        for entry in fs::read_dir(&self.dir)? {
            let entry = entry?;
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            if let Some(idx) = Self::extract_log_store_idx(&path) {
                entries.push((path, idx));
            }
        }
        entries.sort_by_key(|(_, idx)| *idx);
        Ok(entries)
    }

    pub(crate) fn extract_snapshot_idx(path: &Path) -> Option<u64> {
        let file_name = path.file_name()?.to_str()?;
        let rest = file_name
            .strip_suffix(".bin.zst")?
            .strip_prefix(SNAPSHOT_FILE_PREFIX)?;

        let snapshot_id = rest.split_once('_').map(|(id, _ts)| id).unwrap_or(rest);

        Self::parse_snapshot_idx(snapshot_id)
    }

    pub(crate) fn extract_log_store_idx(path: &Path) -> Option<u64> {
        let file_name = path.file_name()?.to_str()?;
        let rest = file_name
            .strip_suffix(LOG_STORE_ARCHIVE_SUFFIX)?
            .strip_prefix(LOG_STORE_ARCHIVE_PREFIX)?;

        let snapshot_id = rest.split_once('_').map(|(id, _ts)| id).unwrap_or(rest);

        Self::parse_snapshot_idx(snapshot_id)
    }

    fn archive_log_store(&self, snapshot: &StoredSnapshot, timestamp: &str) -> std::io::Result<()> {
        let Some(base_path) = self.log_store_path.as_ref() else {
            return Ok(());
        };

        if !base_path.exists() {
            return Ok(());
        }

        let raw = fs::read(base_path)?;
        let prepared = self.prepare_archive_log_store_bytes(&raw)?;
        // compress archived log store to keep snapshot archives small
        let data = zstd::stream::encode_all(Cursor::new(&prepared), SNAPSHOT_COMPRESSION_LVL)
            .map_err(|e| std::io::Error::other(format!("compress log store for archive: {e}")))?;
        let archive_name = format!(
            "{LOG_STORE_ARCHIVE_PREFIX}{}_{timestamp}{LOG_STORE_ARCHIVE_SUFFIX}",
            snapshot.meta.snapshot_id
        );
        let archive_path = self.dir.join(&archive_name);
        let tmp_path = archive_path.with_extension("tmp");
        {
            let mut file = fs::File::create(&tmp_path)?;
            file.write_all(&data)?;
            file.sync_all()?;
        }
        fs::rename(&tmp_path, &archive_path)?;
        let final_file = fs::File::open(&archive_path)?;
        final_file.sync_all()?;
        let dir = fs::File::open(&self.dir)?;
        dir.sync_all()?;
        Ok(())
    }

    fn prepare_archive_log_store_bytes(&self, raw: &[u8]) -> std::io::Result<Vec<u8>> {
        match TypeConfigLogPersistence::decode_diff_bytes(raw) {
            Ok(state) => TypeConfigLogPersistence::encode_state_as_bytes(&state)
                .map_err(|e| std::io::Error::other(format!("encode log store for archive: {e}"))),
            Err(err) => {
                warn!(
                    error = %err,
                    "failed to decode log store while archiving; storing raw bytes"
                );
                Ok(raw.to_vec())
            }
        }
    }

    /// Extracts the monotonically increasing snapshot index from a snapshot_id.
    ///
    /// Snapshot IDs are generated in `store::StateMachineStore::build_snapshot()` as:
    /// - With a last applied log: `"{leader_id}-{last_index}-{snapshot_idx}"`
    ///   where `leader_id`'s Display implementation is `"T{term}-N{node_id}"` (e.g. `"T2-N1"`).
    ///   Example: `"T2-N1-10999-11"` where `10999` is the last log index and `11` is the snapshot index.
    /// - Without a last applied log: `"--{snapshot_idx}"`.
    ///
    /// Contract: the final non-empty `-`-separated segment is always the `snapshot_idx` (u64).
    /// Callers strip any filename timestamp suffixes (appended with `_`) before passing here.
    fn parse_snapshot_idx(snapshot_id: &str) -> Option<u64> {
        snapshot_id
            .rsplit('-')
            .find(|segment| !segment.is_empty())
            .and_then(|segment| segment.parse::<u64>().ok())
    }
}

#[cfg(test)]
mod snapshot_id_tests {
    use super::SnapshotPersistence;

    #[test]
    fn parses_idx_from_full_snapshot_id_with_leader_parts() {
        // leader_id Display is "T{term}-N{node_id}", separated with '-'
        let id = "T3-N1-42-7"; // term=3, node=1, last_index=42, snapshot_idx=7
        assert_eq!(SnapshotPersistence::parse_snapshot_idx(id), Some(7));
    }

    #[test]
    fn parses_idx_from_minimal_snapshot_id_without_last_log() {
        let id = "--12"; // no last log, only snapshot_idx=12
        assert_eq!(SnapshotPersistence::parse_snapshot_idx(id), Some(12));
    }

    #[test]
    fn returns_none_when_no_trailing_number() {
        let id = "invalid-format-"; // trailing hyphen, no number
        assert_eq!(SnapshotPersistence::parse_snapshot_idx(id), None);
    }

    #[test]
    fn parses_idx_from_real_world_example() {
        let id = "T2-N1-10999-11";
        assert_eq!(SnapshotPersistence::parse_snapshot_idx(id), Some(11));
    }
}
