use std::collections::BTreeMap;
use std::fmt;
use std::fmt::Debug;
use std::io::Error as IoError;
use std::ops::{Bound, RangeBounds};
use std::path::Path;
use std::sync::Arc;
use std::thread;

use openraft::LogId;
use openraft::LogState;
use openraft::RaftLogId;
use openraft::RaftTypeConfig;
use openraft::StorageError;
use openraft::StorageIOError;
use openraft::Vote;

use super::persistence::{PersistedLogState, TypeConfigLogPersistence};
use crate::raft::TypeConfig;
use tokio::sync::oneshot;
use tokio::sync::Mutex as AsyncMutex;

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
    log: BTreeMap<u64, C::Entry>,

    /// The commit log id.
    committed: Option<LogId<C::NodeId>>,

    /// The current granted vote.
    vote: Option<Vote<C::NodeId>>,
}

impl<C: RaftTypeConfig> Default for LogStoreInner<C> {
    fn default() -> Self {
        Self {
            last_purged_log_id: None,
            log: BTreeMap::new(),
            committed: None,
            vote: None,
        }
    }
}

impl<C: RaftTypeConfig> LogStoreInner<C> {
    fn ensure_not_compacted(&self, index: u64) -> Result<(), Box<StorageError<C::NodeId>>> {
        if let Some(last_purged) = &self.last_purged_log_id {
            if index <= last_purged.index {
                return Err(Box::new(self.compacted_error(Some(index))));
            }
        }
        Ok(())
    }

    fn missing_log_error(&self, message: impl Into<String>) -> StorageError<C::NodeId> {
        let err = IoError::new(std::io::ErrorKind::NotFound, message.into());
        StorageIOError::read_logs(&err).into()
    }

    fn compacted_error(&self, requested_index: Option<u64>) -> StorageError<C::NodeId> {
        let last_purged = self.last_purged_log_id.clone();
        let first_log_id = self.log.values().next().map(|entry| entry.get_log_id());
        let message = match (requested_index, last_purged) {
            (Some(idx), Some(last)) => format!(
                "log entry at index {idx} has been compacted (last purged: {:?}, first available: {:?})",
                last, first_log_id
            ),
            (None, Some(last)) => format!(
                "requested log range overlaps compacted logs (last purged: {:?}, first available: {:?})",
                last, first_log_id
            ),
            (Some(idx), None) => format!(
                "log entry at index {idx} is unavailable; no compaction metadata recorded"
            ),
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

        let mut response = match expected_len {
            Some(len) => Vec::with_capacity(len as usize),
            None => Vec::new(),
        };
        for (_, val) in self.log.range(range.clone()) {
            response.push(val.clone());
        }

        if let Some(start) = start_index {
            self.ensure_not_compacted(start).map_err(|e| *e)?;
        }

        if response.is_empty() {
            if let (Some(start), Some(len)) = (start_index, expected_len) {
                if len > 0 {
                    return Err(
                        self.missing_log_error(format!("missing log entry at index {start}"))
                    );
                }
            }
            return Ok(response);
        }

        if let Some(start) = start_index {
            let mut expected = start;
            for entry in &response {
                let idx = entry.get_log_id().index;
                if idx != expected {
                    self.ensure_not_compacted(expected).map_err(|e| *e)?;
                    return Err(self.missing_log_error(format!(
                        "log gap detected at index {expected}, found {idx}"
                    )));
                }
                expected = expected.saturating_add(1);
            }

            if let Some(end) = end_index {
                if expected < end {
                    self.ensure_not_compacted(expected).map_err(|e| *e)?;
                    return Err(self.missing_log_error(format!(
                        "missing log entries in range [{start}, {end}) starting at index {expected}"
                    )));
                }
            }
        }

        Ok(response)
    }

    async fn get_log_state(&mut self) -> Result<LogState<C>, StorageError<C::NodeId>> {
        let last = self.log.iter().next_back().map(|(_, ent)| ent.get_log_id());

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
            self.log.insert(entry.get_log_id().index, entry);
        }
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<C::NodeId>) -> Result<(), StorageError<C::NodeId>> {
        let _removed = self.log.split_off(&log_id.index);

        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<C::NodeId>) -> Result<(), StorageError<C::NodeId>> {
        {
            let ld = &mut self.last_purged_log_id;
            if let Some(current) = ld.as_ref() {
                if log_id < *current {
                    let io_err = IoError::other(format!(
                        "purge log id {:?} is older than last purged {:?}",
                        log_id, current
                    ));
                    return Err(StorageIOError::write_logs(&io_err).into());
                }
            }
            *ld = Some(log_id.clone());
        }

        {
            // Keep only entries with index > log_id.index by splitting and assigning
            if let Some(next_index) = log_id.index.checked_add(1) {
                let mut higher = self.log.split_off(&next_index);
                std::mem::swap(&mut self.log, &mut higher);
            } else {
                self.log.clear();
            }
        }

        Ok(())
    }
}

impl<C: RaftTypeConfig> LogStore<C> {
    fn persist_op_if_needed(&self, op: Option<PersistOp<C>>) -> Option<PersistOp<C>> {
        // Persist only if worker sender exists
        if self.worker.is_some() {
            op
        } else {
            None
        }
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
        let mut map = BTreeMap::new();
        for entry in self.log {
            map.insert(entry.get_log_id().index, entry);
        }
        LogStoreInner {
            last_purged_log_id: self.last_purged_log_id,
            log: map,
            committed: self.committed,
            vote: self.vote,
        }
    }
}

impl LogStore<TypeConfig> {
    fn from_persistence_instance(
        persistence: Arc<TypeConfigLogPersistence>,
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
        let handle = thread::spawn(move || loop {
            match op_rx.recv() {
                Ok(PersistMsg::Op(op, resp_tx)) => {
                    let res = (worker_fn)(op);
                    let _ = resp_tx.send(res);
                }
                Ok(PersistMsg::Shutdown) => break,
                Err(_) => break,
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
        Self::from_persistence_instance(persistence)
    }

    pub fn with_persistence_and_thresholds<P: AsRef<Path>>(
        path: P,
        compaction_threshold_bytes: u64,
        compaction_ops: u64,
    ) -> anyhow::Result<Self> {
        let persistence = Arc::new(TypeConfigLogPersistence::with_thresholds(
            path,
            compaction_threshold_bytes,
            compaction_ops,
        )?);
        Self::from_persistence_instance(persistence)
    }
}

mod impl_log_store {
    use std::fmt::Debug;
    use std::ops::RangeBounds;

    use openraft::storage::LogFlushed;
    use openraft::storage::RaftLogStorage;
    use openraft::LogId;
    use openraft::LogState;
    use openraft::RaftLogReader;
    use openraft::RaftTypeConfig;
    use openraft::StorageError;
    use openraft::Vote;

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
    use crate::raft::test_utils::unique_path;
    use openraft::Entry;
    use openraft::EntryPayload;
    use openraft::LeaderId;
    use std::fs;
    use tempfile::tempdir;

    #[tokio::test]
    async fn persists_and_recovers_log_store() -> anyhow::Result<()> {
        let tmp_dir = tempdir()?;
        let log_path = unique_path(tmp_dir.path(), "log_store_persist_recover_", ".bin");

        let log_store = LogStore::with_persistence(&log_path)?;

        // Mutate in-memory and persist per-op without cloning the entire store
        let log_id = LogId::new(LeaderId::new(1, 1), 1);
        let entry = Entry {
            log_id,
            payload: EntryPayload::Blank,
        };
        {
            let mut guard = log_store.inner.lock().await;
            guard.log.insert(1, entry.clone());
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
        assert!(guard.log.get(&1).is_some());
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
        let entry = Entry {
            log_id,
            payload: EntryPayload::Blank,
        };
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
        assert!(guard.log.get(&log_id.index).is_some());
        assert_eq!(guard.committed.unwrap(), log_id);

        Ok(())
    }

    #[tokio::test]
    async fn truncate_removes_boundary_entry() -> anyhow::Result<()> {
        let mut inner = LogStoreInner::<TypeConfig>::default();
        let leader = LeaderId::new(1, 1);
        let entry1 = Entry {
            log_id: LogId::new(leader.clone(), 5),
            payload: EntryPayload::Blank,
        };
        let entry2 = Entry {
            log_id: LogId::new(leader.clone(), 6),
            payload: EntryPayload::Blank,
        };
        inner.log.insert(entry1.log_id.index, entry1.clone());
        inner.log.insert(entry2.log_id.index, entry2);

        inner.truncate(entry1.log_id).await?;

        assert!(!inner.log.contains_key(&entry1.log_id.index));
        assert!(!inner.log.contains_key(&(entry1.log_id.index + 1)));

        Ok(())
    }

    #[tokio::test]
    async fn purge_rewrites_persistent_log_store() -> anyhow::Result<()> {
        let tmp_dir = tempdir()?;
        let log_path = unique_path(tmp_dir.path(), "log_store_purge_rewrites_", ".bin");

        let log_store = LogStore::with_persistence(&log_path)?;
        let leader = LeaderId::new(1, 1);

        let entry1 = Entry {
            log_id: LogId::new(leader.clone(), 1),
            payload: EntryPayload::Blank,
        };
        let entry2 = Entry {
            log_id: LogId::new(leader.clone(), 2),
            payload: EntryPayload::Blank,
        };

        {
            let mut guard = log_store.inner.lock().await;
            guard.log.insert(entry1.log_id.index, entry1.clone());
            guard.log.insert(entry2.log_id.index, entry2.clone());
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

        let purge_id = entry1.log_id.clone();
        {
            let mut guard = log_store.inner.lock().await;
            guard.purge(purge_id.clone()).await?;
        }
        log_store
            .persist_if_needed(
                log_store.persist_op_if_needed(Some(super::PersistOp::PurgeTo(purge_id.clone()))),
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
        assert_eq!(guard.last_purged_log_id, Some(purge_id.clone()));
        assert!(guard.log.get(&purge_id.index).is_none());
        assert!(guard.log.get(&(purge_id.index + 1)).is_some());

        Ok(())
    }
}
