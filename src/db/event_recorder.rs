use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use chrono::Utc;
use tokio_util::sync::CancellationToken;
use tracing::error;
use uuid::Uuid;

use crate::db::{ActivityEventRow, EventSink, EventSinkHandle, WorkerEventRow};

const MAX_RETRY_ATTEMPTS: u32 = 8;

#[derive(Clone)]
enum EventRow {
    Activity(ActivityEventRow),
    Worker(WorkerEventRow),
}

#[derive(Clone)]
struct QueuedEventRow {
    row: EventRow,
    retry_attempts: u32,
    next_retry_at: Instant,
}

impl QueuedEventRow {
    fn ready(row: EventRow) -> Self {
        Self {
            row,
            retry_attempts: 0,
            next_retry_at: Instant::now(),
        }
    }
}

#[derive(Clone)]
pub struct EventRecorder<S: EventSink = EventSinkHandle> {
    sink: Arc<S>,
    gateway_name: Arc<str>,
    queue: Arc<scc::Queue<QueuedEventRow>>,
    capacity: usize,
    dropped: Arc<AtomicUsize>,
    dead_lettered: Arc<AtomicUsize>,
}

impl<S: EventSink + 'static> EventRecorder<S> {
    pub fn new(
        sink: Arc<S>,
        gateway_name: Arc<str>,
        flush_interval: Duration,
        capacity: usize,
        shutdown: CancellationToken,
    ) -> Self {
        let capacity = capacity.max(1);
        let recorder = Self {
            sink,
            gateway_name,
            queue: Arc::new(scc::Queue::default()),
            capacity,
            dropped: Arc::new(AtomicUsize::new(0)),
            dead_lettered: Arc::new(AtomicUsize::new(0)),
        };
        recorder.spawn_flusher(flush_interval, shutdown);
        recorder
    }

    #[allow(clippy::too_many_arguments)]
    pub fn record_activity(
        &self,
        user_id: Option<i64>,
        user_email: Option<&str>,
        company_id: Option<Uuid>,
        company_name: Option<&str>,
        action: &str,
        client_origin: &str,
        task_kind: &str,
        model: Option<&str>,
        task_id: Option<Uuid>,
    ) {
        let row = ActivityEventRow {
            user_id,
            user_email: user_email.map(|v| v.to_string()),
            company_id,
            company_name: company_name.map(|v| v.to_string()),
            action: action.to_string(),
            client_origin: client_origin.to_string(),
            task_kind: task_kind.to_string(),
            model: model.map(|v| v.to_string()),
            gateway_name: self.gateway_name.to_string(),
            task_id,
            created_at: Utc::now(),
        };
        self.enqueue(EventRow::Activity(row));
    }

    #[allow(clippy::too_many_arguments)]
    pub fn record_worker_event(
        &self,
        task_id: Option<Uuid>,
        worker_id: Option<&str>,
        action: &str,
        task_kind: &str,
        reason: Option<&str>,
    ) {
        let row = WorkerEventRow {
            task_id,
            worker_id: worker_id.map(|v| v.to_string()),
            action: action.to_string(),
            task_kind: task_kind.to_string(),
            reason: reason.map(|v| v.to_string()),
            gateway_name: self.gateway_name.to_string(),
            created_at: Utc::now(),
        };
        self.enqueue(EventRow::Worker(row));
    }

    fn spawn_flusher(&self, flush_interval: Duration, shutdown: CancellationToken) {
        let sink = Arc::clone(&self.sink);
        let queue = Arc::clone(&self.queue);
        let capacity = self.capacity;
        let dropped = Arc::clone(&self.dropped);
        let dead_lettered = Arc::clone(&self.dead_lettered);
        let token = shutdown.child_token();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(flush_interval);
            loop {
                tokio::select! {
                    _ = token.cancelled() => {
                        let _ = Self::flush_once(
                            &sink,
                            &queue,
                            capacity,
                            &dropped,
                            &dead_lettered,
                            flush_interval,
                            true,
                        ).await;
                        break;
                    }
                    _ = interval.tick() => {
                        let _ = Self::flush_once(
                            &sink,
                            &queue,
                            capacity,
                            &dropped,
                            &dead_lettered,
                            flush_interval,
                            false,
                        ).await;
                    }
                }
            }
        });
    }

    fn enqueue(&self, row: EventRow) {
        Self::enqueue_with_limit(
            &self.queue,
            self.capacity,
            &self.dropped,
            QueuedEventRow::ready(row),
        );
    }

    fn enqueue_with_limit(
        queue: &Arc<scc::Queue<QueuedEventRow>>,
        capacity: usize,
        dropped: &Arc<AtomicUsize>,
        row: QueuedEventRow,
    ) {
        if queue.len() >= capacity {
            let dropped_count = dropped.fetch_add(1, Ordering::Relaxed) + 1;
            if dropped_count == 1 || dropped_count.is_multiple_of(100) {
                error!(
                    dropped = dropped_count,
                    capacity, "Event queue is full; dropping events"
                );
            }
            return;
        }
        queue.push(row);
    }

    fn retry_delay(flush_interval: Duration, retry_attempts: u32) -> Duration {
        let base_ms = (flush_interval.as_millis().max(1)).min(u128::from(u64::MAX)) as u64;
        let exponent = retry_attempts.saturating_sub(1).min(6);
        let backoff_ms = base_ms.saturating_mul(1u64 << exponent);
        let jitter_ms = rand::random_range(0..=base_ms);
        Duration::from_millis(backoff_ms.saturating_add(jitter_ms))
    }

    fn requeue_failed_rows(
        queue: &Arc<scc::Queue<QueuedEventRow>>,
        capacity: usize,
        dropped: &Arc<AtomicUsize>,
        dead_lettered: &Arc<AtomicUsize>,
        rows: Vec<QueuedEventRow>,
        flush_interval: Duration,
        kind: &'static str,
    ) {
        for mut queued in rows {
            queued.retry_attempts = queued.retry_attempts.saturating_add(1);
            if queued.retry_attempts > MAX_RETRY_ATTEMPTS {
                let dead_lettered_count = dead_lettered.fetch_add(1, Ordering::Relaxed) + 1;
                error!(
                    dead_lettered = dead_lettered_count,
                    kind,
                    retry_attempts = queued.retry_attempts,
                    "Dropping event after repeated flush failures"
                );
                continue;
            }
            queued.next_retry_at =
                Instant::now() + Self::retry_delay(flush_interval, queued.retry_attempts);
            Self::enqueue_with_limit(queue, capacity, dropped, queued);
        }
    }

    async fn flush_once(
        sink: &Arc<S>,
        queue: &Arc<scc::Queue<QueuedEventRow>>,
        capacity: usize,
        dropped: &Arc<AtomicUsize>,
        dead_lettered: &Arc<AtomicUsize>,
        flush_interval: Duration,
        force_ready: bool,
    ) -> anyhow::Result<()> {
        let mut activity_entries: Vec<QueuedEventRow> = Vec::new();
        let mut worker_entries: Vec<QueuedEventRow> = Vec::new();
        let mut deferred_entries: Vec<QueuedEventRow> = Vec::new();
        let now = Instant::now();
        while let Some(entry) = queue.pop() {
            let queued = (**entry).clone();
            if !force_ready && queued.next_retry_at > now {
                deferred_entries.push(queued);
                continue;
            }
            match &queued.row {
                EventRow::Activity(_) => activity_entries.push(queued),
                EventRow::Worker(_) => worker_entries.push(queued),
            }
        }
        for queued in deferred_entries {
            Self::enqueue_with_limit(queue, capacity, dropped, queued);
        }

        if !activity_entries.is_empty() {
            let activity_rows: Vec<ActivityEventRow> = activity_entries
                .iter()
                .filter_map(|queued| match &queued.row {
                    EventRow::Activity(row) => Some(row.clone()),
                    EventRow::Worker(_) => None,
                })
                .collect();
            if let Err(e) = sink.record_activity_events_batch(&activity_rows).await {
                error!(error = ?e, rows = activity_rows.len(), "Failed to flush activity events");
                Self::requeue_failed_rows(
                    queue,
                    capacity,
                    dropped,
                    dead_lettered,
                    activity_entries,
                    flush_interval,
                    "activity",
                );
            }
        }
        if !worker_entries.is_empty() {
            let worker_rows: Vec<WorkerEventRow> = worker_entries
                .iter()
                .filter_map(|queued| match &queued.row {
                    EventRow::Activity(_) => None,
                    EventRow::Worker(row) => Some(row.clone()),
                })
                .collect();
            if let Err(e) = sink.record_worker_events_batch(&worker_rows).await {
                error!(error = ?e, rows = worker_rows.len(), "Failed to flush worker events");
                Self::requeue_failed_rows(
                    queue,
                    capacity,
                    dropped,
                    dead_lettered,
                    worker_entries,
                    flush_interval,
                    "worker",
                );
            }
        }
        Ok(())
    }

    #[cfg(feature = "test-support")]
    #[allow(dead_code)]
    pub async fn flush_once_for_test(&self) {
        let _ = Self::flush_once(
            &self.sink,
            &self.queue,
            self.capacity,
            &self.dropped,
            &self.dead_lettered,
            Duration::from_millis(1),
            true,
        )
        .await;
    }
}
