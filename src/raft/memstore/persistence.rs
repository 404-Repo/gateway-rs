use std::collections::BTreeMap;
use std::fs;
use std::io::Cursor;
use std::io::Read;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex as StdMutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context};
use openraft::{Entry, LogId, RaftLogId, Vote};
use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::common::fs::write_atomic;
use crate::raft::archive;
use crate::raft::{NodeId, TypeConfig};
use std::fs::OpenOptions;
use std::io::BufWriter;

pub(crate) const LOG_STORE_ARCHIVE_PREFIX: &str = "log_store_";
pub(crate) const LOG_STORE_ARCHIVE_SUFFIX: &str = ".bin.zst";

const LOG_STORE_MAGIC: [u8; 8] = *b"LOGDIF01";
const LOG_STORE_VERSION: u32 = 1;
const LOG_STORE_HEADER_LEN: usize = LOG_STORE_MAGIC.len() + std::mem::size_of::<u32>();
// Defaults are defined in config::PersistenceConfig. These are only used to seed the
// internal config when callers use `new()` instead of `with_config()` (e.g., tests).
const DEFAULT_LOG_STORE_COMPACTION_THRESHOLD_BYTES: u64 = 8 * 1024 * 1024;
const DEFAULT_LOG_STORE_COMPACTION_OPS: u64 = 4096;

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub(crate) struct PersistedLogState {
    pub(crate) last_purged_log_id: Option<LogId<NodeId>>,
    pub(crate) committed: Option<LogId<NodeId>>,
    pub(crate) vote: Option<Vote<NodeId>>,
    pub(crate) log: Vec<Entry<TypeConfig>>,
}

#[derive(Clone, Debug, Default)]
struct PersistedLogCache {
    last_purged_log_id: Option<LogId<NodeId>>,
    committed: Option<LogId<NodeId>>,
    vote: Option<Vote<NodeId>>,
    log: BTreeMap<u64, Entry<TypeConfig>>,
}

impl From<PersistedLogState> for PersistedLogCache {
    fn from(state: PersistedLogState) -> Self {
        let mut log = BTreeMap::new();
        for entry in state.log {
            log.insert(entry.get_log_id().index, entry);
        }
        Self {
            last_purged_log_id: state.last_purged_log_id,
            committed: state.committed,
            vote: state.vote,
            log,
        }
    }
}

impl From<&PersistedLogCache> for PersistedLogState {
    fn from(cache: &PersistedLogCache) -> Self {
        let log = cache.log.values().cloned().collect();
        Self {
            last_purged_log_id: cache.last_purged_log_id,
            committed: cache.committed,
            vote: cache.vote,
            log,
        }
    }
}

impl PersistedLogCache {
    fn apply(&mut self, record: DiffRecord) {
        match record {
            DiffRecord::Full(state) => {
                *self = PersistedLogCache::from(state);
            }
            DiffRecord::VoteSet(vote) => {
                self.vote = vote;
            }
            DiffRecord::CommittedSet(committed) => {
                self.committed = committed;
            }
            DiffRecord::PurgeTo(id) => {
                let should_update = self
                    .last_purged_log_id
                    .as_ref()
                    .map(|existing| existing < &id)
                    .unwrap_or(true);
                if should_update {
                    self.last_purged_log_id = Some(id);
                }
                if let Some(next_index) = id.index.checked_add(1) {
                    let mut higher = self.log.split_off(&next_index);
                    std::mem::swap(&mut self.log, &mut higher);
                } else {
                    self.log.clear();
                }
            }
            DiffRecord::TruncateFrom(start) => {
                let _removed = self.log.split_off(&start);
            }
            DiffRecord::Append(entries) => {
                for entry in entries {
                    self.log.insert(entry.get_log_id().index, entry);
                }
            }
        }
    }
}

pub(crate) struct TypeConfigLogPersistence {
    path: PathBuf,
    lock: StdMutex<()>,
    ops_since_compaction: AtomicU64,
    bytes_since_compaction: AtomicU64,
    compaction_threshold_bytes: u64,
    compaction_ops: u64,
    // Buffered append handle, created lazily and rotated on full rewrites
    append_handle: StdMutex<Option<BufWriter<fs::File>>>,
}

impl TypeConfigLogPersistence {
    // Extract monotonic snapshot/log index from a log-store archive file name.
    // Mirrors logic used by snapshot persistence: the last non-empty '-' separated
    // segment before any timestamp suffix `_...` is the numeric snapshot index.
    fn extract_log_store_idx_from_path(path: &Path) -> Option<u64> {
        archive::extract_idx_from_path(path, LOG_STORE_ARCHIVE_PREFIX, LOG_STORE_ARCHIVE_SUFFIX)
    }
    fn ensure_append_handle_opened(&self) -> std::io::Result<()> {
        let mut guard = self.append_handle.lock().unwrap();
        if guard.is_none() {
            *guard = Some(BufWriter::new(self.open_append_file()?));
        }
        Ok(())
    }

    fn reopen_append_handle_locked(&self) -> std::io::Result<()> {
        let mut guard = self.append_handle.lock().unwrap();
        *guard = Some(BufWriter::new(self.open_append_file()?));
        Ok(())
    }

    fn open_append_file(&self) -> std::io::Result<fs::File> {
        let mut opts = OpenOptions::new();
        opts.append(true).create(true);
        opts.open(&self.path)
    }

    pub(crate) fn new<P: AsRef<Path>>(path: P) -> anyhow::Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            if !parent.exists() {
                fs::create_dir_all(parent).context(format!(
                    "Failed to create log store parent dir: {:?}",
                    parent
                ))?;
            }
        }
        Ok(Self {
            path,
            lock: StdMutex::new(()),
            ops_since_compaction: AtomicU64::new(0),
            bytes_since_compaction: AtomicU64::new(0),
            compaction_threshold_bytes: DEFAULT_LOG_STORE_COMPACTION_THRESHOLD_BYTES,
            compaction_ops: DEFAULT_LOG_STORE_COMPACTION_OPS,
            append_handle: StdMutex::new(None),
        })
    }

    pub(crate) fn with_thresholds<P: AsRef<Path>>(
        path: P,
        compaction_threshold_bytes: u64,
        compaction_ops: u64,
    ) -> anyhow::Result<Self> {
        let mut this = Self::new(path)?;
        this.compaction_threshold_bytes = compaction_threshold_bytes;
        this.compaction_ops = compaction_ops;
        Ok(this)
    }

    pub(crate) fn load(&self) -> anyhow::Result<Option<PersistedLogState>> {
        let _guard = self.lock.lock().expect("log persistence lock poisoned");

        let mut primary_err: Option<anyhow::Error> = None;

        if self.path.exists() {
            match Self::read_log_store_file(&self.path) {
                Ok((persisted, _)) => {
                    self.ops_since_compaction.store(0, Ordering::Relaxed);
                    self.bytes_since_compaction.store(0, Ordering::Relaxed);
                    return Ok(Some(persisted));
                }
                Err(err) => {
                    warn!(
                        error = %err,
                        path = %self.path.display(),
                        "failed to load primary log store file; will probe archives"
                    );
                    primary_err = Some(err);
                }
            }
        }

        if let Some(restored) = self.restore_from_archives()? {
            self.ops_since_compaction.store(0, Ordering::Relaxed);
            self.bytes_since_compaction.store(0, Ordering::Relaxed);
            return Ok(Some(restored));
        }

        if let Some(err) = primary_err {
            return Err(anyhow!(
                "failed to load log store from {:?} and no usable archive was found: {err}",
                self.path
            ));
        }

        Ok(None)
    }

    fn read_log_store_file(path: &Path) -> anyhow::Result<(PersistedLogState, Vec<u8>)> {
        let data = fs::read(path).context(format!("Failed to read log store file: {:?}", path))?;

        if Self::is_diff_format(&data) {
            let persisted = Self::decode_diff_bytes(&data)
                .with_context(|| format!("Failed to replay diff log file: {:?}", path))?;
            return Ok((persisted, data));
        }

        let decoded = zstd::stream::decode_all(Cursor::new(&data))
            .with_context(|| format!("Failed to decode log store archive: {:?}", path))?;
        if Self::is_diff_format(&decoded) {
            let persisted = Self::decode_diff_bytes(&decoded)
                .with_context(|| format!("Failed to replay compressed diff archive: {:?}", path))?;
            return Ok((persisted, decoded));
        }

        Err(anyhow!(
            "unsupported log store format at {:?}: expected diff or zstd-compressed diff",
            path
        ))
    }

    fn restore_from_archives(&self) -> anyhow::Result<Option<PersistedLogState>> {
        let archives = self.collect_archive_candidates()?;
        // Prefer newest by parsed log index, fall back to mtime.
        for archive_path in archives.into_iter() {
            match Self::read_log_store_file(&archive_path) {
                Ok((persisted, raw_bytes)) => {
                    warn!(
                        path = %archive_path.display(),
                        "restoring log store from archive"
                    );
                    Self::write_bytes_atomic(&self.path, &raw_bytes)?;
                    // rotate append handle to point to the new file
                    self.reopen_append_handle_locked()?;
                    return Ok(Some(persisted));
                }
                Err(err) => {
                    warn!(
                        error = %err,
                        path = %archive_path.display(),
                        "failed to read log store archive; trying older one"
                    );
                }
            }
        }
        Ok(None)
    }

    fn collect_archive_candidates(&self) -> anyhow::Result<Vec<PathBuf>> {
        let Some(parent) = self.path.parent() else {
            return Ok(Vec::new());
        };

        let mut entries_mtime: Vec<(Duration, PathBuf)> = Vec::new();
        let mut entries_with_idx: Vec<(u64, PathBuf)> = Vec::new();
        let matching = archive::collect_matching_files(
            parent,
            LOG_STORE_ARCHIVE_PREFIX,
            LOG_STORE_ARCHIVE_SUFFIX,
        )
        .with_context(|| format!("Failed to list log store archive directory: {:?}", parent))?;
        for path in matching {
            if path == self.path || !path.is_file() {
                continue;
            }
            if let Some(idx) = Self::extract_log_store_idx_from_path(&path) {
                entries_with_idx.push((idx, path));
            } else {
                let modified = fs::metadata(&path)
                    .and_then(|m| m.modified())
                    .unwrap_or(SystemTime::UNIX_EPOCH);
                let order_key = modified
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or(Duration::ZERO);
                entries_mtime.push((order_key, path));
            }
        }
        // Prefer by highest log index; then by mtime for those without index
        entries_with_idx.sort_by_key(|(idx, _)| *idx);
        entries_with_idx.reverse();
        entries_mtime.sort_by_key(|(dur, _)| *dur);
        entries_mtime.reverse();
        let mut out = Vec::new();
        out.extend(entries_with_idx.into_iter().map(|(_, p)| p));
        out.extend(entries_mtime.into_iter().map(|(_, p)| p));
        Ok(out)
    }

    fn write_bytes_atomic(path: &Path, data: &[u8]) -> anyhow::Result<()> {
        write_atomic(path, data).context(format!(
            "Failed to write log store atomically to {:?}",
            path
        ))
    }

    // ---- Append-only per-op API used by LogStore to avoid cloning full state ----

    pub(crate) fn append_vote(&self, vote: &Option<Vote<NodeId>>) -> std::io::Result<()> {
        self.persist_record(DiffRecord::VoteSet(*vote))
    }

    pub(crate) fn append_committed(
        &self,
        committed: &Option<LogId<NodeId>>,
    ) -> std::io::Result<()> {
        self.persist_record(DiffRecord::CommittedSet(*committed))
    }

    pub(crate) fn append_purge_to(&self, id: &LogId<NodeId>) -> std::io::Result<()> {
        self.persist_record(DiffRecord::PurgeTo(*id))
    }

    pub(crate) fn append_truncate_from(&self, start: u64) -> std::io::Result<()> {
        self.persist_record(DiffRecord::TruncateFrom(start))
    }

    pub(crate) fn append_append(&self, entries: &[Entry<TypeConfig>]) -> std::io::Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        self.persist_record(DiffRecord::Append(entries.to_vec()))
    }

    fn persist_record(&self, record: DiffRecord) -> std::io::Result<()> {
        let _guard = self.lock.lock().expect("log persistence lock poisoned");
        self.ensure_initialized_file()?;

        let rewrite_after = matches!(record, DiffRecord::PurgeTo(_) | DiffRecord::TruncateFrom(_));

        if rewrite_after {
            // Rebuild current state from disk, apply the record, then rewrite
            let current = self.read_current_state().map_err(|e| {
                std::io::Error::other(format!("read current state for rewrite: {e}"))
            })?;
            let mut cache = PersistedLogCache::from(current);
            cache.apply(record);
            let snapshot: PersistedLogState = (&cache).into();
            self.ops_since_compaction.store(0, Ordering::Relaxed);
            self.bytes_since_compaction.store(0, Ordering::Relaxed);
            self.rewrite_full_locked(&snapshot)
        } else {
            let mut buf = Vec::with_capacity(256);
            Self::append_record(&mut buf, &record)
                .map_err(|e| std::io::Error::other(format!("encode diff record: {e}")))?;
            let appended_len = buf.len();
            self.append_and_sync(&buf)?;

            self.maybe_compact_locked(appended_len)
        }
    }

    fn maybe_compact_locked(&self, appended_bytes: usize) -> std::io::Result<()> {
        let ops = self.ops_since_compaction.fetch_add(1, Ordering::Relaxed) + 1;
        let appended = appended_bytes as u64;
        let bytes = self
            .bytes_since_compaction
            .fetch_add(appended, Ordering::Relaxed)
            + appended;

        let file_len = fs::metadata(&self.path).map(|m| m.len()).unwrap_or(0);

        let should_compact = file_len >= self.compaction_threshold_bytes
            || ops >= self.compaction_ops
            || bytes >= self.compaction_threshold_bytes;

        if should_compact {
            self.ops_since_compaction.store(0, Ordering::Relaxed);
            self.bytes_since_compaction.store(0, Ordering::Relaxed);
            let state = self.read_current_state().map_err(|e| {
                std::io::Error::other(format!("read current state for compaction: {e}"))
            })?;
            self.rewrite_full_locked(&state)
        } else {
            Ok(())
        }
    }

    fn rewrite_full_locked(&self, state: &PersistedLogState) -> std::io::Result<()> {
        let buf = Self::encode_state_as_bytes(state)
            .map_err(|e| std::io::Error::other(format!("encode full diff record: {e}")))?;
        Self::write_bytes_atomic(&self.path, &buf)
            .map_err(|e| std::io::Error::other(format!("rewrite log store: {e}")))?;
        // Rotate append handle to the new file written by atomic rename
        self.reopen_append_handle_locked()?;
        Ok(())
    }

    fn ensure_initialized_file(&self) -> std::io::Result<()> {
        if self.path.exists() {
            let mut header = [0u8; LOG_STORE_HEADER_LEN];
            if let Ok(mut f) = fs::File::open(&self.path) {
                if let Ok(n) = f.read(&mut header) {
                    if n >= LOG_STORE_HEADER_LEN && Self::is_diff_format(&header) {
                        self.ensure_append_handle_opened()?;
                        return Ok(());
                    }
                }
            }
            // File exists but is not in diff format
            if let Ok((state, _raw)) = Self::read_log_store_file(&self.path) {
                self.rewrite_full_locked(&state)?;
                return Ok(());
            }
        }
        // Initialize a fresh diff file with just the header
        let mut buf = Vec::with_capacity(256);
        Self::write_diff_header(&mut buf);
        Self::write_bytes_atomic(&self.path, &buf)
            .map_err(|e| std::io::Error::other(format!("write diff header: {e}")))?;
        // Open append handle after initialization
        self.reopen_append_handle_locked()?;
        Ok(())
    }

    fn append_and_sync(&self, buf: &[u8]) -> std::io::Result<()> {
        {
            let mut guard = self.append_handle.lock().unwrap();
            if guard.is_none() {
                *guard = Some(BufWriter::new(self.open_append_file()?));
            }
            let writer = guard.as_mut().unwrap();
            writer.write_all(buf)?;
            writer.flush()?;
            // fdatasync (sync_data) is sufficient for appends
            writer.get_ref().sync_data()?;
        }
        Ok(())
    }

    fn is_diff_format(bytes: &[u8]) -> bool {
        if bytes.len() < LOG_STORE_HEADER_LEN {
            return false;
        }
        if bytes[..LOG_STORE_MAGIC.len()] != LOG_STORE_MAGIC {
            return false;
        }
        let mut version_bytes = [0u8; 4];
        version_bytes.copy_from_slice(&bytes[LOG_STORE_MAGIC.len()..LOG_STORE_HEADER_LEN]);
        u32::from_le_bytes(version_bytes) == LOG_STORE_VERSION
    }

    fn write_diff_header(buf: &mut Vec<u8>) {
        buf.extend_from_slice(&LOG_STORE_MAGIC);
        buf.extend_from_slice(&LOG_STORE_VERSION.to_le_bytes());
    }

    fn append_record<T: Serialize>(buf: &mut Vec<u8>, rec: &T) -> anyhow::Result<()> {
        let payload = rmp_serde::to_vec(rec).map_err(|e| anyhow!("encode diff record: {e}"))?;
        let len = payload.len() as u32;
        buf.extend_from_slice(&len.to_le_bytes());
        buf.extend_from_slice(&payload);
        Ok(())
    }

    pub(crate) fn encode_state_as_bytes(state: &PersistedLogState) -> anyhow::Result<Vec<u8>> {
        let mut buf = Vec::with_capacity(1024);
        Self::write_diff_header(&mut buf);
        let record = DiffRecord::Full(state.clone());
        Self::append_record(&mut buf, &record)?;
        Ok(buf)
    }

    pub(crate) fn decode_diff_bytes(bytes: &[u8]) -> anyhow::Result<PersistedLogState> {
        if bytes.len() < LOG_STORE_HEADER_LEN {
            return Err(anyhow!("not a diff-format file"));
        }
        if bytes[..LOG_STORE_MAGIC.len()] != LOG_STORE_MAGIC {
            return Err(anyhow!("unexpected log store magic"));
        }
        let mut version_bytes = [0u8; 4];
        version_bytes.copy_from_slice(&bytes[LOG_STORE_MAGIC.len()..LOG_STORE_HEADER_LEN]);
        let version = u32::from_le_bytes(version_bytes);
        if version != LOG_STORE_VERSION {
            return Err(anyhow!(
                "unsupported log store version: expected {}, found {}",
                LOG_STORE_VERSION,
                version
            ));
        }
        let mut pos = LOG_STORE_HEADER_LEN; // after magic + version
        let mut cache = PersistedLogCache::default();
        while pos + 4 <= bytes.len() {
            let mut len_arr = [0u8; 4];
            len_arr.copy_from_slice(&bytes[pos..pos + 4]);
            pos += 4;
            let len = u32::from_le_bytes(len_arr) as usize;
            if pos + len > bytes.len() {
                // Treat an incomplete trailing record as a torn write and stop replay.
                break;
            }
            let payload = &bytes[pos..pos + len];
            pos += len;
            let rec: DiffRecord =
                rmp_serde::from_slice(payload).map_err(|e| anyhow!("decode diff record: {e}"))?;
            cache.apply(rec);
        }
        Ok((&cache).into())
    }

    // Read the current state from disk (primary or compressed), defaulting to empty state
    fn read_current_state(&self) -> anyhow::Result<PersistedLogState> {
        if self.path.exists() {
            let (persisted, _raw) = Self::read_log_store_file(&self.path)?;
            Ok(persisted)
        } else {
            Ok(PersistedLogState::default())
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
enum DiffRecord {
    Full(PersistedLogState),
    VoteSet(Option<Vote<NodeId>>),
    CommittedSet(Option<LogId<NodeId>>),
    PurgeTo(LogId<NodeId>),
    TruncateFrom(u64),
    Append(Vec<Entry<TypeConfig>>),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::test_utils::unique_path;
    use crate::raft::TypeConfig;
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
        assert!(persisted
            .log
            .iter()
            .all(|entry| entry.get_log_id().index > purge_id.index));

        Ok(())
    }

    #[test]
    fn recover_from_truncated_tail_record() -> anyhow::Result<()> {
        let tmp_dir = tempdir()?;
        let path = unique_path(tmp_dir.path(), "persist_log_store_truncated_", ".bin");

        let mut buf = Vec::new();
        TypeConfigLogPersistence::write_diff_header(&mut buf);
        TypeConfigLogPersistence::append_record(
            &mut buf,
            &DiffRecord::VoteSet(Some(Vote::new(1, 1))),
        )?;

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
}
