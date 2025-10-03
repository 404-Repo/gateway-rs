use std::collections::BTreeMap;
use std::fs;
use std::io::Cursor;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex as StdMutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context};
use openraft::{Entry, LogId, RaftLogId, Vote};
use serde::{Deserialize, Serialize};

use crate::raft::{NodeId, TypeConfig};

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
                let keys: Vec<u64> = self.log.range(..=id.index).map(|(k, _)| *k).collect();
                for key in keys {
                    self.log.remove(&key);
                }
            }
            DiffRecord::TruncateFrom(start) => {
                let keys: Vec<u64> = self.log.range(start..).map(|(k, _)| *k).collect();
                for key in keys {
                    self.log.remove(&key);
                }
            }
            DiffRecord::Append(entries) => {
                for entry in entries {
                    self.log.insert(entry.get_log_id().index, entry);
                }
            }
        }
    }

    fn is_empty(&self) -> bool {
        self.last_purged_log_id.is_none()
            && self.committed.is_none()
            && self.vote.is_none()
            && self.log.is_empty()
    }
}

pub(crate) struct TypeConfigLogPersistence {
    path: PathBuf,
    lock: StdMutex<()>,
    state: StdMutex<PersistedLogCache>,
    ops_since_compaction: AtomicU64,
    bytes_since_compaction: AtomicU64,
    compaction_threshold_bytes: u64,
    compaction_ops: u64,
}

impl TypeConfigLogPersistence {
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
        let cache = if path.exists() {
            match Self::read_log_store_file(&path) {
                Ok((persisted, _raw)) => PersistedLogCache::from(persisted),
                Err(err) => {
                    tracing::warn!(
                        error = %err,
                        path = %path.display(),
                        "failed to pre-load log store; will rely on runtime recovery"
                    );
                    PersistedLogCache::default()
                }
            }
        } else {
            PersistedLogCache::default()
        };

        Ok(Self {
            path,
            lock: StdMutex::new(()),
            state: StdMutex::new(cache),
            ops_since_compaction: AtomicU64::new(0),
            bytes_since_compaction: AtomicU64::new(0),
            compaction_threshold_bytes: DEFAULT_LOG_STORE_COMPACTION_THRESHOLD_BYTES,
            compaction_ops: DEFAULT_LOG_STORE_COMPACTION_OPS,
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
                    *self.state.lock().unwrap() = PersistedLogCache::from(persisted.clone());
                    self.ops_since_compaction.store(0, Ordering::Relaxed);
                    self.bytes_since_compaction.store(0, Ordering::Relaxed);
                    return Ok(Some(persisted));
                }
                Err(err) => {
                    tracing::warn!(
                        error = %err,
                        path = %self.path.display(),
                        "failed to load primary log store file; will probe archives"
                    );
                    primary_err = Some(err);
                }
            }
        }

        if let Some(restored) = self.restore_from_archives()? {
            *self.state.lock().unwrap() = PersistedLogCache::from(restored.clone());
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
        for archive_path in archives.into_iter().rev() {
            match Self::read_log_store_file(&archive_path) {
                Ok((persisted, raw_bytes)) => {
                    tracing::warn!(
                        path = %archive_path.display(),
                        "restoring log store from archive"
                    );
                    Self::write_bytes_atomic(&self.path, &raw_bytes)?;
                    return Ok(Some(persisted));
                }
                Err(err) => {
                    tracing::warn!(
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

        let mut entries: Vec<(Duration, PathBuf)> = Vec::new();
        for entry in fs::read_dir(parent)
            .with_context(|| format!("Failed to list log store archive directory: {:?}", parent))?
        {
            let entry = entry?;
            let path = entry.path();
            if path == self.path || !path.is_file() {
                continue;
            }
            let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            if !name.starts_with(LOG_STORE_ARCHIVE_PREFIX)
                || !name.ends_with(LOG_STORE_ARCHIVE_SUFFIX)
            {
                continue;
            }
            let modified = entry
                .metadata()
                .and_then(|m| m.modified())
                .unwrap_or(SystemTime::UNIX_EPOCH);
            let order_key = modified
                .duration_since(UNIX_EPOCH)
                .unwrap_or(Duration::ZERO);
            entries.push((order_key, path));
        }

        entries.sort_by_key(|(order, _)| *order);
        Ok(entries.into_iter().map(|(_, path)| path).collect())
    }

    fn write_bytes_atomic(path: &Path, data: &[u8]) -> anyhow::Result<()> {
        if let Some(parent) = path.parent() {
            if !parent.exists() {
                fs::create_dir_all(parent).context(format!(
                    "Failed to create log store directory for recovery: {:?}",
                    parent
                ))?;
            }
        }

        let tmp_path = path.with_extension("tmp.recover");
        {
            let mut file = fs::File::create(&tmp_path).context(format!(
                "Failed to create temporary recovered log store file: {:?}",
                tmp_path
            ))?;

            file.write_all(data).context(format!(
                "Failed to write temporary recovered log store file: {:?}",
                tmp_path
            ))?;
            file.sync_all().context(format!(
                "Failed to sync temporary recovered log store file: {:?}",
                tmp_path
            ))?;
        }
        fs::rename(&tmp_path, path).context(format!(
            "Failed to replace log store with recovered archive at {:?}",
            path
        ))?;
        if let Some(parent) = path.parent() {
            let dir = fs::File::open(parent).context(format!(
                "Failed to open log store directory for syncing: {:?}",
                parent
            ))?;
            dir.sync_all()
                .context(format!("Failed to sync log store directory: {:?}", parent))?;
        }
        let final_file = fs::File::open(path).context(format!(
            "Failed to reopen recovered log store file for syncing: {:?}",
            path
        ))?;
        final_file.sync_all().context(format!(
            "Failed to sync recovered log store file: {:?}",
            path
        ))?;
        Ok(())
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
            let snapshot = {
                let mut cache = self.state.lock().unwrap();
                cache.apply(record);
                cache.clone()
            };
            self.ops_since_compaction.store(0, Ordering::Relaxed);
            self.bytes_since_compaction.store(0, Ordering::Relaxed);
            self.rewrite_full_locked(&snapshot)
        } else {
            let mut buf = Vec::with_capacity(256);
            Self::append_record(&mut buf, &record)
                .map_err(|e| std::io::Error::other(format!("encode diff record: {e}")))?;
            let appended_len = buf.len();
            self.append_and_sync(&buf)?;

            {
                let mut cache = self.state.lock().unwrap();
                cache.apply(record);
            }

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
            let snapshot = self.state.lock().unwrap().clone();
            self.rewrite_full_locked(&snapshot)
        } else {
            Ok(())
        }
    }

    fn rewrite_full_locked(&self, cache: &PersistedLogCache) -> std::io::Result<()> {
        let full_state: PersistedLogState = cache.into();
        let buf = Self::encode_state_as_bytes(&full_state)
            .map_err(|e| std::io::Error::other(format!("encode full diff record: {e}")))?;
        Self::write_bytes_atomic(&self.path, &buf)
            .map_err(|e| std::io::Error::other(format!("rewrite log store: {e}")))?;
        if let Some(parent) = self.path.parent() {
            let dir = fs::File::open(parent)?;
            dir.sync_all()?;
        }
        Ok(())
    }

    fn ensure_initialized_file(&self) -> std::io::Result<()> {
        if self.path.exists() {
            let is_diff = fs::read(&self.path)
                .ok()
                .map(|b| Self::is_diff_format(&b))
                .unwrap_or(false);
            if is_diff {
                return Ok(());
            }
        }
        let snapshot = self.state.lock().unwrap().clone();
        let buf = if snapshot.is_empty() {
            let mut buf = Vec::with_capacity(256);
            Self::write_diff_header(&mut buf);
            buf
        } else {
            let state: PersistedLogState = (&snapshot).into();
            Self::encode_state_as_bytes(&state)
                .map_err(|e| std::io::Error::other(format!("encode initial diff record: {e}")))?
        };
        Self::write_bytes_atomic(&self.path, &buf)
            .map_err(|e| std::io::Error::other(format!("write diff header: {e}")))?;
        Ok(())
    }

    fn append_and_sync(&self, buf: &[u8]) -> std::io::Result<()> {
        let mut file = fs::OpenOptions::new().append(true).open(&self.path)?;
        file.write_all(buf)?;
        file.sync_all()?;
        if let Some(parent) = self.path.parent() {
            let dir = fs::File::open(parent)?;
            dir.sync_all()?;
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
                return Err(anyhow!("truncated diff record"));
            }
            let payload = &bytes[pos..pos + len];
            pos += len;
            let rec: DiffRecord =
                rmp_serde::from_slice(payload).map_err(|e| anyhow!("decode diff record: {e}"))?;
            cache.apply(rec);
        }
        Ok((&cache).into())
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
    use openraft::EntryPayload;
    use openraft::LeaderId;
    use tempfile::tempdir;

    #[test]
    fn purge_rewrites_log_store_to_trimmed_state() -> anyhow::Result<()> {
        let tmp_dir = tempdir()?;
        let path = tmp_dir.path().join("log_store.bin");

        let persistence = TypeConfigLogPersistence::new(&path)?;
        let leader = LeaderId::new(1, 1);

        for idx in 1..=2_000u64 {
            let entry = Entry {
                log_id: LogId::new(leader.clone(), idx),
                payload: EntryPayload::Blank,
            };
            persistence.append_append(&[entry])?;
        }

        let size_after_append = std::fs::metadata(&path)?.len();

        let purge_id = LogId::new(leader.clone(), 1_000);
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

        assert_eq!(persisted.last_purged_log_id, Some(purge_id.clone()));
        assert!(persisted
            .log
            .iter()
            .all(|entry| entry.get_log_id().index > purge_id.index));

        Ok(())
    }
}
