use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Utc;
use tokio_util::sync::CancellationToken;
use tracing::error;
use uuid::Uuid;

use crate::db::{ActivityEventRow, EventSink, EventSinkHandle, WorkerEventRow};

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
        };
        recorder.spawn_flusher(flush_interval, shutdown);
        recorder
    }

    #[allow(clippy::too_many_arguments)]
    pub fn record_activity(
        &self,
        user_id: Option<i64>,
        user_email: Option<&str>,
        account_id: Option<i64>,
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
            account_id,
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
        model: Option<&str>,
        reason: Option<&str>,
        metadata_json: Option<&serde_json::Value>,
    ) {
        let row = WorkerEventRow {
            task_id,
            worker_id: worker_id.map(|v| v.to_string()),
            action: action.to_string(),
            task_kind: task_kind.to_string(),
            model: model.map(|v| v.to_string()),
            reason: reason.map(|v| v.to_string()),
            gateway_name: self.gateway_name.to_string(),
            metadata_json: metadata_json
                .cloned()
                .unwrap_or_else(|| serde_json::json!({})),
            created_at: Utc::now(),
        };
        self.enqueue(EventRow::Worker(row));
    }

    fn spawn_flusher(&self, flush_interval: Duration, shutdown: CancellationToken) {
        let sink = Arc::clone(&self.sink);
        let queue = Arc::clone(&self.queue);
        let capacity = self.capacity;
        let token = shutdown.child_token();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(flush_interval);
            loop {
                tokio::select! {
                    _ = token.cancelled() => {
                        while !queue.is_empty() {
                            let _ = Self::flush_once(
                                &sink,
                                &queue,
                                capacity,
                                flush_interval,
                                true,
                            ).await;
                            if queue.is_empty() {
                                break;
                            }
                            tokio::time::sleep(flush_interval).await;
                        }
                        break;
                    }
                    _ = interval.tick() => {
                        let _ = Self::flush_once(
                            &sink,
                            &queue,
                            capacity,
                            flush_interval,
                            false,
                        ).await;
                    }
                }
            }
        });
    }

    fn enqueue(&self, row: EventRow) {
        Self::enqueue_with_limit(&self.queue, self.capacity, QueuedEventRow::ready(row));
    }

    fn enqueue_with_limit(
        queue: &Arc<scc::Queue<QueuedEventRow>>,
        capacity: usize,
        row: QueuedEventRow,
    ) {
        let backlog = queue.len().saturating_add(1);
        let warn_multiple = capacity.saturating_mul(10).max(1);
        if backlog > capacity && (backlog == capacity + 1 || backlog.is_multiple_of(warn_multiple))
        {
            error!(
                queued = backlog,
                soft_capacity = capacity,
                "Event queue backlog exceeded configured capacity; preserving events until the sink recovers"
            );
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
        rows: Vec<QueuedEventRow>,
        flush_interval: Duration,
    ) {
        for mut queued in rows {
            queued.retry_attempts = queued.retry_attempts.saturating_add(1);
            queued.next_retry_at =
                Instant::now() + Self::retry_delay(flush_interval, queued.retry_attempts);
            Self::enqueue_with_limit(queue, capacity, queued);
        }
    }

    async fn flush_once(
        sink: &Arc<S>,
        queue: &Arc<scc::Queue<QueuedEventRow>>,
        capacity: usize,
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
            Self::enqueue_with_limit(queue, capacity, queued);
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
                Self::requeue_failed_rows(queue, capacity, activity_entries, flush_interval);
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
                Self::requeue_failed_rows(queue, capacity, worker_entries, flush_interval);
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
            Duration::from_millis(1),
            true,
        )
        .await;
    }
}

#[cfg(all(test, feature = "test-support"))]
mod tests {
    use super::*;
    use anyhow::{Result, anyhow};
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::Mutex;

    use crate::db::InMemoryEventSink;

    async fn stopped_recorder<S: EventSink + 'static>(
        sink: Arc<S>,
        capacity: usize,
    ) -> EventRecorder<S> {
        let shutdown = CancellationToken::new();
        shutdown.cancel();
        let recorder = EventRecorder::new(
            sink,
            Arc::<str>::from("gateway-test"),
            Duration::from_secs(60),
            capacity,
            shutdown,
        );
        tokio::task::yield_now().await;
        recorder
    }

    #[tokio::test]
    async fn preserves_events_beyond_soft_capacity() {
        let sink = Arc::new(InMemoryEventSink::default());
        let recorder = stopped_recorder(Arc::clone(&sink), 1).await;

        for index in 0..3 {
            let metadata = serde_json::json!({ "index": index });
            recorder.record_worker_event(
                None,
                Some("worker-1"),
                "task_assigned",
                "txt3d",
                Some("404-3dgs"),
                None,
                Some(&metadata),
            );
        }

        recorder.flush_once_for_test().await;

        let rows = sink.worker_rows().await;
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].model.as_deref(), Some("404-3dgs"));
        assert_eq!(rows[2].metadata_json["index"], serde_json::json!(2));
    }

    #[derive(Default)]
    struct EventuallySucceedsSink {
        attempts: AtomicUsize,
        fail_until: usize,
        worker: Mutex<Vec<WorkerEventRow>>,
    }

    impl EventuallySucceedsSink {
        async fn worker_rows(&self) -> Vec<WorkerEventRow> {
            self.worker.lock().await.clone()
        }
    }

    #[async_trait]
    impl EventSink for EventuallySucceedsSink {
        async fn record_activity_events_batch(&self, _rows: &[ActivityEventRow]) -> Result<()> {
            Ok(())
        }

        async fn record_worker_events_batch(&self, rows: &[WorkerEventRow]) -> Result<()> {
            let attempt = self.attempts.fetch_add(1, Ordering::Relaxed) + 1;
            if attempt <= self.fail_until {
                return Err(anyhow!("simulated worker sink failure"));
            }
            self.worker.lock().await.extend(rows.iter().cloned());
            Ok(())
        }
    }

    #[tokio::test]
    async fn retries_failed_batches_without_dropping_events() {
        let sink = Arc::new(EventuallySucceedsSink {
            fail_until: 12,
            ..Default::default()
        });
        let recorder = stopped_recorder(Arc::clone(&sink), 1).await;
        let metadata = serde_json::json!({ "worker_hotkey": "hotkey-1" });

        recorder.record_worker_event(
            None,
            Some("worker-1"),
            "result_success",
            "txt3d",
            Some("404-3dgs"),
            None,
            Some(&metadata),
        );

        for _ in 0..=sink.fail_until {
            recorder.flush_once_for_test().await;
        }

        let rows = sink.worker_rows().await;
        assert_eq!(rows.len(), 1);
        assert!(sink.attempts.load(Ordering::Relaxed) >= sink.fail_until + 1);
        assert_eq!(
            rows[0].metadata_json["worker_hotkey"].as_str(),
            Some("hotkey-1")
        );
    }
}
