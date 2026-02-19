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
use crate::raft::NodeId;
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

enum PersistMsg<C: RaftTypeConfig> {
    Op(
        PersistOp<C>,
        oneshot::Sender<Result<(), Box<StorageError<C::NodeId>>>>,
    ),
    Shutdown,
}

type PersistOpResult = Result<(), Box<StorageError<NodeId>>>;
type PersistResultTx = oneshot::Sender<PersistOpResult>;
type PersistBatchItem = (PersistOp<TypeConfig>, PersistResultTx);

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

    fn validate_append_batch(
        &self,
        entries: &[C::Entry],
    ) -> Result<(), Box<StorageError<C::NodeId>>> {
        let mut base = self.base_index;
        let mut len = self.log.len() as u64;
        let mut has_data = !self.log.is_empty();

        for entry in entries {
            let idx = entry.get_log_id().index;
            if !has_data {
                base = idx;
                len = 1;
                has_data = true;
                continue;
            }

            let next_index = base + len;
            let is_prepend = idx.checked_add(1) == Some(base);
            if idx == next_index || is_prepend {
                if is_prepend {
                    base = idx;
                }
                len += 1;
            } else if idx >= base && idx < next_index {
                // Overwrite in-place is allowed.
            } else {
                let io_err = IoError::other(format!(
                    "log gap detected while appending index {idx} (window [{base}, {next_index}))"
                ));
                return Err(Box::new(StorageIOError::write_logs(&io_err).into()));
            }
        }

        Ok(())
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
            } else if idx.checked_add(1) == Some(base) {
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

    async fn append_batch(&self, batch: Vec<C::Entry>) -> Result<(), StorageError<C::NodeId>>
    where
        C::Entry: Clone,
    {
        {
            let inner = self.inner.lock().await;
            inner.validate_append_batch(&batch).map_err(|e| *e)?;
        }

        let op = self.persist_op_if_needed(Some(PersistOp::Append(batch.clone())));
        self.persist_if_needed(op).await?;

        let mut inner = self.inner.lock().await;
        inner.append(batch).await
    }
}

impl PersistedLogState {
    fn try_into_inner(self) -> anyhow::Result<LogStoreInner<TypeConfig>> {
        let mut log = VecDeque::with_capacity(self.log.len());
        let mut base_index = 0;
        let mut prev_index: Option<u64> = None;
        for entry in self.log {
            let idx = entry.get_log_id().index;
            if log.is_empty() {
                base_index = idx;
            } else {
                let Some(prev) = prev_index else {
                    return Err(anyhow::anyhow!("persisted log contiguity check failed"));
                };
                let Some(expected) = prev.checked_add(1) else {
                    return Err(anyhow::anyhow!(
                        "persisted log contains index overflow after {prev}"
                    ));
                };
                if idx != expected {
                    return Err(anyhow::anyhow!(
                        "persisted log is not contiguous (expected {expected}, got {idx})"
                    ));
                }
            }
            prev_index = Some(idx);
            log.push_back(entry);
        }
        Ok(LogStoreInner {
            last_purged_log_id: self.last_purged_log_id,
            log,
            base_index,
            committed: self.committed,
            vote: self.vote,
        })
    }
}

impl LogStore<TypeConfig> {
    const MAX_PERSIST_BATCH: usize = 128;

    fn persist_single_op(
        persistence: &TypeConfigLogPersistence,
        op: PersistOp<TypeConfig>,
    ) -> PersistOpResult {
        use PersistOp::*;

        let io_res = match op {
            VoteSet(v) => persistence.append_vote(&v),
            CommittedSet(c) => persistence.append_committed(&c),
            PurgeTo(id) => persistence.append_purge_to(&id),
            TruncateFrom(start) => persistence.append_truncate_from(start),
            Append(entries) => persistence.append_append(&entries),
        };
        io_res.map_err(|e| Box::new(StorageIOError::write_logs(&e).into()))
    }

    fn send_op_result(tx: PersistResultTx, result: PersistOpResult) {
        let _ = tx.send(result);
    }

    fn send_op_error(tx: PersistResultTx, message: &str) {
        let io_err = IoError::other(message.to_string());
        let _ = tx.send(Err(Box::new(StorageIOError::write_logs(&io_err).into())));
    }

    fn process_persist_batch(persistence: &TypeConfigLogPersistence, batch: Vec<PersistBatchItem>) {
        let mut iter = batch.into_iter().peekable();

        while let Some((op, tx)) = iter.next() {
            match op {
                PersistOp::Append(mut merged_entries) => {
                    let mut responders = vec![tx];

                    while let Some((PersistOp::Append(_), _)) = iter.peek() {
                        if let Some((PersistOp::Append(next_entries), next_tx)) = iter.next() {
                            merged_entries.extend(next_entries);
                            responders.push(next_tx);
                        } else {
                            break;
                        }
                    }

                    let persist_result =
                        Self::persist_single_op(persistence, PersistOp::Append(merged_entries));
                    match persist_result {
                        Ok(()) => {
                            for responder in responders {
                                Self::send_op_result(responder, Ok(()));
                            }
                        }
                        Err(err) => {
                            let err_msg = err.to_string();
                            for responder in responders {
                                Self::send_op_error(responder, &err_msg);
                            }
                        }
                    }
                }
                other => {
                    let res = Self::persist_single_op(persistence, other);
                    Self::send_op_result(tx, res);
                }
            }
        }
    }

    fn from_persistence_instance(
        persistence: Arc<TypeConfigLogPersistence>,
        flush_interval: Duration,
    ) -> anyhow::Result<Self> {
        let initial_inner = if let Some(persisted) = persistence.load()? {
            persisted.try_into_inner()?
        } else {
            LogStoreInner::default()
        };

        // Create a single background worker thread to handle all persistence ops
        let (op_tx, op_rx): (
            std::sync::mpsc::Sender<PersistMsg<TypeConfig>>,
            std::sync::mpsc::Receiver<PersistMsg<TypeConfig>>,
        ) = std::sync::mpsc::channel();

        let persistence_flush = Arc::clone(&persistence);
        let persistence_for_ops = Arc::clone(&persistence);
        let handle = thread::spawn(move || {
            loop {
                match op_rx.recv_timeout(flush_interval) {
                    Ok(PersistMsg::Op(op, resp_tx)) => {
                        let mut batch = vec![(op, resp_tx)];
                        let mut should_shutdown = false;

                        while batch.len() < Self::MAX_PERSIST_BATCH {
                            match op_rx.try_recv() {
                                Ok(PersistMsg::Op(next_op, next_tx)) => {
                                    batch.push((next_op, next_tx));
                                }
                                Ok(PersistMsg::Shutdown) => {
                                    should_shutdown = true;
                                    break;
                                }
                                Err(std::sync::mpsc::TryRecvError::Empty) => break,
                                Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                                    should_shutdown = true;
                                    break;
                                }
                            }
                        }

                        Self::process_persist_batch(&persistence_for_ops, batch);

                        if should_shutdown {
                            if let Err(err) = persistence_flush.flush_if_dirty() {
                                warn!("log persistence flush failed during shutdown: {}", err);
                            }
                            break;
                        }
                    }
                    Ok(PersistMsg::Shutdown) => {
                        if let Err(err) = persistence_flush.flush_if_dirty() {
                            warn!("log persistence flush failed during shutdown: {}", err);
                        }
                        break;
                    }
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                        if let Err(err) = persistence_flush.flush_if_dirty() {
                            warn!("log persistence flush failed: {}", err);
                        }
                    }
                    Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                        if let Err(err) = persistence_flush.flush_if_dirty() {
                            warn!(
                                "log persistence flush failed after worker channel disconnect: {}",
                                err
                            );
                        }
                        break;
                    }
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
            let batch: Vec<C::Entry> = entries.into_iter().collect();
            match self.append_batch(batch).await {
                Ok(()) => {
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
#[path = "../tests/memstore_log_store.rs"]
mod tests;
