use std::collections::VecDeque;
use std::fmt;
use std::fmt::Debug;
use std::io::Error as IoError;
use std::ops::{Bound, RangeBounds};
use std::path::Path;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use openraft::LogId;
use openraft::LogState;
use openraft::RaftLogId;
use openraft::RaftTypeConfig;
use openraft::StorageError;
use openraft::StorageIOError;
use openraft::Vote;

use super::persistence::{PersistedLogState, TypeConfigLogPersistence};
use crate::raft::TypeConfig;
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::oneshot;
use tracing::warn;

/// RaftLogStore implementation with a in-memory storage
pub struct LogStore<C: RaftTypeConfig> {
    inner: Arc<AsyncMutex<LogStoreInner<C>>>,
    worker: Option<Arc<PersistenceWorker<C>>>,
}

pub enum PersistOp<C: RaftTypeConfig> {
    VoteSet(Option<Vote<C::NodeId>>),
    CommittedSet(Option<LogId<C::NodeId>>),
    PurgeTo(LogId<C::NodeId>),
    TruncateFrom(u64),
    Append(Vec<C::Entry>),
}

type PersistFn<C> = dyn Fn(PersistOp<C>) -> Result<(), Box<StorageError<<C as RaftTypeConfig>::NodeId>>>
    + Send
    + Sync;

enum PersistMsg<C: RaftTypeConfig> {
    Op(
        PersistOp<C>,
        oneshot::Sender<Result<(), Box<StorageError<C::NodeId>>>>,
    ),
    Shutdown,
}

struct PersistenceWorker<C: RaftTypeConfig> {
    tx: std::sync::mpsc::Sender<PersistMsg<C>>,
    handle: Option<thread::JoinHandle<()>>,
}

impl<C: RaftTypeConfig> PersistenceWorker<C> {
    fn send_op(
        &self,
        op: PersistOp<C>,
        resp_tx: oneshot::Sender<Result<(), Box<StorageError<C::NodeId>>>>,
    ) -> Result<(), Box<StorageError<C::NodeId>>> {
        self.tx.send(PersistMsg::Op(op, resp_tx)).map_err(|_| {
            Box::new(
                StorageIOError::write_logs(&IoError::other("persistence worker is not available"))
                    .into(),
            )
        })
    }
}

impl<C: RaftTypeConfig> Drop for PersistenceWorker<C> {
    fn drop(&mut self) {
        let _ = self.tx.send(PersistMsg::Shutdown);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl<C: RaftTypeConfig> Clone for LogStore<C> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            worker: self.worker.clone(),
        }
    }
}

impl<C: RaftTypeConfig> fmt::Debug for LogStore<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LogStore").finish()
    }
}

impl<C: RaftTypeConfig> Default for LogStore<C> {
    fn default() -> Self {
        Self {
            inner: Arc::new(AsyncMutex::new(LogStoreInner::default())),
            worker: None,
        }
    }
}

#[derive(Debug)]
pub struct LogStoreInner<C: RaftTypeConfig> {
    /// The last purged log id.
    last_purged_log_id: Option<LogId<C::NodeId>>,

    /// The Raft log.
    log: VecDeque<C::Entry>,
    base_index: u64,

    /// The commit log id.
    committed: Option<LogId<C::NodeId>>,

    /// The current granted vote.
    vote: Option<Vote<C::NodeId>>,
}

impl<C: RaftTypeConfig> Default for LogStoreInner<C> {
    fn default() -> Self {
        Self {
            last_purged_log_id: None,
            log: VecDeque::new(),
            base_index: 0,
            committed: None,
            vote: None,
        }
    }
}

impl<C: RaftTypeConfig> LogStoreInner<C> {
    fn ensure_not_compacted(&self, index: u64) -> Result<(), Box<StorageError<C::NodeId>>> {
        if let Some(last_purged) = &self.last_purged_log_id
            && index <= last_purged.index
        {
            return Err(Box::new(self.compacted_error(Some(index))));
        }
        Ok(())
    }

    fn missing_log_error(&self, message: impl Into<String>) -> StorageError<C::NodeId> {
        let err = IoError::new(std::io::ErrorKind::NotFound, message.into());
        StorageIOError::read_logs(&err).into()
    }

    fn compacted_error(&self, requested_index: Option<u64>) -> StorageError<C::NodeId> {
        let last_purged = self.last_purged_log_id.clone();
        let first_log_id = self.log.front().map(|entry| entry.get_log_id());
        let message = match (requested_index, last_purged) {
            (Some(idx), Some(last)) => format!(
                "log entry at index {idx} has been compacted (last purged: {:?}, first available: {:?})",
                last, first_log_id
            ),
            (None, Some(last)) => format!(
                "requested log range overlaps compacted logs (last purged: {:?}, first available: {:?})",
                last, first_log_id
            ),
            (Some(idx), None) => {
                format!("log entry at index {idx} is unavailable; no compaction metadata recorded")
            }
            (None, None) => {
                "requested log range is unavailable; compaction metadata missing".to_string()
            }
        };

        let io_err = IoError::other(message);
        StorageIOError::read_logs(&io_err).into()
    }

    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug>(
        &mut self,
        range: RB,
    ) -> Result<Vec<C::Entry>, StorageError<C::NodeId>>
    where
        C::Entry: Clone,
    {
        let start_index = match range.start_bound() {
            Bound::Included(&s) => Some(s),
            Bound::Excluded(&s) => s.checked_add(1),
            Bound::Unbounded => None,
        };
        let end_index = match range.end_bound() {
            Bound::Included(&e) => e.checked_add(1),
            Bound::Excluded(&e) => Some(e),
            Bound::Unbounded => None,
        };

        let expected_len = match (start_index, end_index) {
            (Some(s), Some(e)) if e > s => Some(e - s),
            (Some(_), Some(_)) => Some(0),
            _ => None,
        };

        if let Some(start) = start_index {
            self.ensure_not_compacted(start).map_err(|e| *e)?;
        }

        if self.log.is_empty() {
            if let (Some(start), Some(len)) = (start_index, expected_len)
                && len > 0
            {
                return Err(self.missing_log_error(format!("missing log entry at index {start}")));
            }
            return Ok(Vec::new());
        }

        let base = self.base_index;
        let log_len = self.log.len() as u64;
        let last_exclusive = base + log_len;

        if let Some(start) = start_index {
            if start < base {
                return Err(self.missing_log_error(format!("missing log entry at index {start}")));
            }
            if start >= last_exclusive {
                return Err(self.missing_log_error(format!("missing log entry at index {start}")));
            }
        }

        if let Some(end) = end_index
            && end > last_exclusive
        {
            let start = start_index.unwrap_or(base);
            if start < last_exclusive {
                return Err(self.missing_log_error(format!(
                    "missing log entries in range [{start}, {end}) starting at index {last_exclusive}"
                )));
            }
            return Err(self.missing_log_error(format!("missing log entry at index {start}")));
        }

        let slice_start = start_index.unwrap_or(base);
        let slice_end = end_index.unwrap_or(last_exclusive);
        if slice_start >= slice_end {
            return Ok(Vec::new());
        }

        let offset_start = (slice_start - base) as usize;
        let offset_len = (slice_end - slice_start) as usize;
        let mut response = match expected_len {
            Some(len) => Vec::with_capacity(len as usize),
            None => Vec::with_capacity(offset_len),
        };
        response.extend(self.log.iter().skip(offset_start).take(offset_len).cloned());

        Ok(response)
    }

    async fn get_log_state(&mut self) -> Result<LogState<C>, StorageError<C::NodeId>> {
        let last = self.log.back().map(|ent| ent.get_log_id());

        let last_purged = &self.last_purged_log_id;

        let last = match last {
            None => last_purged.as_ref(),
            Some(x) => Some(x),
        };

        Ok(LogState {
            last_purged_log_id: last_purged.clone(),
            last_log_id: last.cloned(),
        })
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<C::NodeId>>,
    ) -> Result<(), StorageError<C::NodeId>> {
        self.committed = committed;
        Ok(())
    }

    async fn read_committed(
        &mut self,
    ) -> Result<Option<LogId<C::NodeId>>, StorageError<C::NodeId>> {
        Ok(self.committed.clone())
    }

    async fn save_vote(&mut self, vote: &Vote<C::NodeId>) -> Result<(), StorageError<C::NodeId>> {
        self.vote = Some(vote.clone());
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<C::NodeId>>, StorageError<C::NodeId>> {
        Ok(self.vote.clone())
    }

    async fn append<I>(&mut self, entries: I) -> Result<(), StorageError<C::NodeId>>
    where
        I: IntoIterator<Item = C::Entry>,
    {
        for entry in entries {
            let idx = entry.get_log_id().index;
            if self.log.is_empty() {
                self.base_index = idx;
                self.log.push_back(entry);
                continue;
            }

            let base = self.base_index;
            let next_index = base + self.log.len() as u64;
            if idx == next_index {
                self.log.push_back(entry);
            } else if idx + 1 == base {
                self.base_index = idx;
                self.log.push_front(entry);
            } else if idx >= base && idx < next_index {
                let offset = (idx - base) as usize;
                if let Some(slot) = self.log.get_mut(offset) {
                    *slot = entry;
                } else {
                    let io_err = IoError::other(format!(
                        "log index {idx} maps outside of log window [{base}, {next_index})"
                    ));
                    return Err(StorageIOError::write_logs(&io_err).into());
                }
            } else {
                let io_err = IoError::other(format!(
                    "log gap detected while appending index {idx} (window [{base}, {next_index}))"
                ));
                return Err(StorageIOError::write_logs(&io_err).into());
            }
        }
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<C::NodeId>) -> Result<(), StorageError<C::NodeId>> {
        if self.log.is_empty() {
            self.base_index = log_id.index;
            return Ok(());
        }

        let base = self.base_index;
        let last_exclusive = base + self.log.len() as u64;
        if log_id.index <= base {
            self.log.clear();
            self.base_index = log_id.index;
        } else if log_id.index < last_exclusive {
            let new_len = (log_id.index - base) as usize;
            self.log.truncate(new_len);
        }

        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<C::NodeId>) -> Result<(), StorageError<C::NodeId>> {
        {
            let ld = &mut self.last_purged_log_id;
            if let Some(current) = ld.as_ref()
                && log_id < *current
            {
                let io_err = IoError::other(format!(
                    "purge log id {:?} is older than last purged {:?}",
                    log_id, current
                ));
                return Err(StorageIOError::write_logs(&io_err).into());
            }
            *ld = Some(log_id.clone());
        }

        {
            if self.log.is_empty() {
                return Ok(());
            }

            let base = self.base_index;
            let last_exclusive = base + self.log.len() as u64;
            let purge_index = log_id.index;
            if purge_index < base {
                return Ok(());
            }

            let next_index = purge_index.saturating_add(1);
            if next_index >= last_exclusive {
                self.log.clear();
                self.base_index = next_index;
                return Ok(());
            }

            let remove_count = (next_index - base) as usize;
            for _ in 0..remove_count {
                self.log.pop_front();
            }
            self.base_index = next_index;
        }

        Ok(())
    }
}

impl<C: RaftTypeConfig> LogStore<C> {
    fn persist_op_if_needed(&self, op: Option<PersistOp<C>>) -> Option<PersistOp<C>> {
        // Persist only if worker sender exists
        if self.worker.is_some() { op } else { None }
    }

    async fn persist_if_needed(
        &self,
        op: Option<PersistOp<C>>,
    ) -> Result<(), StorageError<C::NodeId>> {
        let Some(op) = op else {
            return Ok(());
        };

        if let Some(worker) = &self.worker {
            let (resp_tx, resp_rx) = oneshot::channel::<Result<(), Box<StorageError<C::NodeId>>>>();

            match worker.send_op(op, resp_tx) {
                Ok(()) => {}
                Err(boxed_err) => return Err(*boxed_err),
            }

            match resp_rx.await {
                Ok(res) => match res {
                    Ok(()) => Ok(()),
                    Err(boxed_err) => Err(*boxed_err),
                },
                Err(recv_err) => {
                    let io_err = IoError::other(format!(
                        "persistence worker dropped response channel: {recv_err}"
                    ));
                    Err(StorageIOError::write_logs(&io_err).into())
                }
            }
        } else {
            let io_err = IoError::other("persistence worker is not available");
            Err(StorageIOError::write_logs(&io_err).into())
        }
    }
}

impl PersistedLogState {
    fn into_inner(self) -> LogStoreInner<TypeConfig> {
        let mut log = VecDeque::with_capacity(self.log.len());
        let mut base_index = 0;
        for entry in self.log {
            let idx = entry.get_log_id().index;
            if log.is_empty() {
                base_index = idx;
            } else {
                let expected = base_index + log.len() as u64;
                debug_assert!(
                    idx == expected,
                    "persisted log is not contiguous (expected {expected}, got {idx})"
                );
            }
            log.push_back(entry);
        }
        LogStoreInner {
            last_purged_log_id: self.last_purged_log_id,
            log,
            base_index,
            committed: self.committed,
            vote: self.vote,
        }
    }
}

impl LogStore<TypeConfig> {
    fn from_persistence_instance(
        persistence: Arc<TypeConfigLogPersistence>,
        flush_interval: Duration,
    ) -> anyhow::Result<Self> {
        let initial_inner = if let Some(persisted) = persistence.load()? {
            persisted.into_inner()
        } else {
            LogStoreInner::default()
        };

        let persistence_fn: Arc<PersistFn<TypeConfig>> = {
            let persistence = Arc::clone(&persistence);
            Arc::new(move |op: PersistOp<TypeConfig>| {
                use PersistOp::*;
                let io_res = match op {
                    VoteSet(v) => persistence.append_vote(&v),
                    CommittedSet(c) => persistence.append_committed(&c),
                    PurgeTo(id) => persistence.append_purge_to(&id),
                    TruncateFrom(start) => persistence.append_truncate_from(start),
                    Append(entries) => persistence.append_append(&entries),
                };
                io_res.map_err(|e| Box::new(StorageIOError::write_logs(&e).into()))
            })
        };

        // Create a single background worker thread to handle all persistence ops
        let (op_tx, op_rx): (
            std::sync::mpsc::Sender<PersistMsg<TypeConfig>>,
            std::sync::mpsc::Receiver<PersistMsg<TypeConfig>>,
        ) = std::sync::mpsc::channel();

        let worker_fn = Arc::clone(&persistence_fn);
        let persistence_flush = Arc::clone(&persistence);
        let handle = thread::spawn(move || {
            loop {
                match op_rx.recv_timeout(flush_interval) {
                    Ok(PersistMsg::Op(op, resp_tx)) => {
                        let res = (worker_fn)(op);
                        let _ = resp_tx.send(res);
                    }
                    Ok(PersistMsg::Shutdown) => break,
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                        if let Err(err) = persistence_flush.flush_if_dirty() {
                            warn!("log persistence flush failed: {}", err);
                        }
                    }
                    Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
                }
            }
        });

        let worker = PersistenceWorker {
            tx: op_tx,
            handle: Some(handle),
        };

        Ok(Self {
            inner: Arc::new(AsyncMutex::new(initial_inner)),
            worker: Some(Arc::new(worker)),
        })
    }

    #[cfg(test)]
    pub fn with_persistence<P: AsRef<Path>>(path: P) -> anyhow::Result<Self> {
        let persistence = Arc::new(TypeConfigLogPersistence::new(path)?);
        Self::from_persistence_instance(persistence, Duration::from_millis(200))
    }

    pub fn with_persistence_and_thresholds<P: AsRef<Path>>(
        path: P,
        compaction_threshold_bytes: u64,
        compaction_ops: u64,
        flush_interval_ms: u64,
    ) -> anyhow::Result<Self> {
        let persistence = Arc::new(TypeConfigLogPersistence::with_thresholds(
            path,
            compaction_threshold_bytes,
            compaction_ops,
        )?);
        Self::from_persistence_instance(
            persistence,
            Duration::from_millis(flush_interval_ms.max(1)),
        )
    }
}

mod impl_log_store {
    use std::fmt::Debug;
    use std::ops::RangeBounds;

    use openraft::LogId;
    use openraft::LogState;
    use openraft::RaftLogReader;
    use openraft::RaftTypeConfig;
    use openraft::StorageError;
    use openraft::Vote;
    use openraft::storage::LogFlushed;
    use openraft::storage::RaftLogStorage;

    use crate::raft::memstore::log_store::LogStore;

    impl<C: RaftTypeConfig> RaftLogReader<C> for LogStore<C>
    where
        C::Entry: Clone,
    {
        async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug>(
            &mut self,
            range: RB,
        ) -> Result<Vec<C::Entry>, StorageError<C::NodeId>> {
            let mut inner = self.inner.lock().await;
            inner.try_get_log_entries(range).await
        }
    }

    impl<C: RaftTypeConfig> RaftLogStorage<C> for LogStore<C>
    where
        C::Entry: Clone,
    {
        type LogReader = Self;

        async fn get_log_state(&mut self) -> Result<LogState<C>, StorageError<C::NodeId>> {
            let mut inner = self.inner.lock().await;
            inner.get_log_state().await
        }

        async fn save_committed(
            &mut self,
            committed: Option<LogId<C::NodeId>>,
        ) -> Result<(), StorageError<C::NodeId>> {
            // Persist first, if it fails, don't update memory
            let op =
                self.persist_op_if_needed(Some(super::PersistOp::CommittedSet(committed.clone())));
            self.persist_if_needed(op).await?;
            let mut inner = self.inner.lock().await;
            inner.save_committed(committed).await
        }

        async fn read_committed(
            &mut self,
        ) -> Result<Option<LogId<C::NodeId>>, StorageError<C::NodeId>> {
            let mut inner = self.inner.lock().await;
            inner.read_committed().await
        }

        async fn save_vote(
            &mut self,
            vote: &Vote<C::NodeId>,
        ) -> Result<(), StorageError<C::NodeId>> {
            let op = self.persist_op_if_needed(Some(super::PersistOp::VoteSet(Some(vote.clone()))));
            self.persist_if_needed(op).await?;
            let mut inner = self.inner.lock().await;
            inner.save_vote(vote).await
        }

        async fn read_vote(&mut self) -> Result<Option<Vote<C::NodeId>>, StorageError<C::NodeId>> {
            let mut inner = self.inner.lock().await;
            inner.read_vote().await
        }

        async fn append<I>(
            &mut self,
            entries: I,
            callback: LogFlushed<C>,
        ) -> Result<(), StorageError<C::NodeId>>
        where
            I: IntoIterator<Item = C::Entry>,
        {
            // Collect entries once. Persist first, then update memory.
            let batch: Vec<C::Entry> = entries.into_iter().collect();
            let op = self.persist_op_if_needed(Some(super::PersistOp::Append(batch.clone())));
            match self.persist_if_needed(op).await {
                Ok(()) => {
                    let mut inner = self.inner.lock().await;
                    inner.append(batch).await?;
                    callback.log_io_completed(Ok(()));
                    Ok(())
                }
                Err(e) => {
                    let error_message = e.to_string();
                    callback.log_io_completed(Err(std::io::Error::other(error_message)));
                    Err(e)
                }
            }
        }

        async fn truncate(
            &mut self,
            log_id: LogId<C::NodeId>,
        ) -> Result<(), StorageError<C::NodeId>> {
            let op = self.persist_op_if_needed(Some(super::PersistOp::TruncateFrom(log_id.index)));
            self.persist_if_needed(op).await?;
            let mut inner = self.inner.lock().await;
            inner.truncate(log_id).await
        }

        async fn purge(&mut self, log_id: LogId<C::NodeId>) -> Result<(), StorageError<C::NodeId>> {
            let op = self.persist_op_if_needed(Some(super::PersistOp::PurgeTo(log_id.clone())));
            self.persist_if_needed(op).await?;
            let mut inner = self.inner.lock().await;
            inner.purge(log_id).await
        }

        async fn get_log_reader(&mut self) -> Self::LogReader {
            self.clone()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::TypeConfig;
    use crate::raft::test_utils::unique_path;
    use openraft::Entry;
    use openraft::EntryPayload;
    use openraft::LeaderId;
    use std::fs;
    use std::time::{Duration, Instant};
    use tempfile::tempdir;
    use tokio::time::sleep;

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
                .await
                .map_err(|e| anyhow::anyhow!(e))?;
            guard.committed = Some(log_id);
            guard.last_purged_log_id = Some(LogId::new(LeaderId::new(1, 1), 0));
            guard.vote = Some(Vote::new(1, 1));
        }
        // Persist each change as diff ops
        log_store
            .persist_if_needed(
                log_store.persist_op_if_needed(Some(super::PersistOp::Append(vec![entry]))),
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
                log_store
                    .persist_op_if_needed(Some(super::PersistOp::VoteSet(Some(Vote::new(1, 1))))),
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
        inner.append(vec![entry1.clone(), entry2]).await?;

        inner.truncate(entry1.log_id).await?;

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
                .await
                .map_err(|e| anyhow::anyhow!(e))?;
        }

        log_store
            .persist_if_needed(
                log_store.persist_op_if_needed(Some(super::PersistOp::Append(vec![
                    entry1.clone(),
                    entry2.clone(),
                ]))),
            )
            .await?;

        let size_after_append = fs::metadata(&log_path)?.len();

        let purge_id = entry1.log_id;
        {
            let mut guard = log_store.inner.lock().await;
            guard.purge(purge_id).await?;
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
    async fn periodic_flush_persists_append() -> anyhow::Result<()> {
        let tmp_dir = tempdir()?;
        let log_path = unique_path(tmp_dir.path(), "log_store_periodic_flush_", ".bin");

        let log_store = LogStore::with_persistence(&log_path)?;
        let start_flushes = TypeConfigLogPersistence::flush_count();

        let entry = blank_entry(LeaderId::new(1, 1), 1);
        log_store
            .persist_if_needed(
                log_store.persist_op_if_needed(Some(super::PersistOp::Append(vec![entry]))),
            )
            .await?;

        let deadline = Instant::now() + Duration::from_secs(2);
        while TypeConfigLogPersistence::flush_count() == start_flushes {
            if Instant::now() >= deadline {
                anyhow::bail!("periodic flush did not trigger within timeout");
            }
            sleep(Duration::from_millis(50)).await;
        }

        Ok(())
    }
}
