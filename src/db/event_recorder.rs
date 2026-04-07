use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use chrono::Utc;
use tokio_util::sync::CancellationToken;
use tracing::error;
use uuid::Uuid;

use crate::db::{
    ActivityEventRow, EventSink, EventSinkHandle, RateLimitViolationBatchRow, WorkerEventRow,
};

#[derive(Clone)]
enum EventRow {
    Activity(ActivityEventRow),
    Worker(WorkerEventRow),
    RateLimitViolation(RateLimitViolationRow),
}

#[derive(Clone)]
struct RateLimitViolationRow {
    gateway_name: String,
    client_id: String,
    created_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Clone)]
pub struct EventRecorder<S: EventSink = EventSinkHandle> {
    sink: Arc<S>,
    gateway_name: Arc<str>,
    queue: Arc<scc::Queue<EventRow>>,
    capacity: usize,
    dropped: Arc<AtomicUsize>,
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
        };
        recorder.spawn_flusher(flush_interval, shutdown);
        recorder
    }

    #[allow(clippy::too_many_arguments)]
    pub fn record_activity(
        &self,
        user_id: Option<Uuid>,
        user_email: Option<&str>,
        company_id: Option<Uuid>,
        company_name: Option<&str>,
        action: &str,
        tool: &str,
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
            tool: tool.to_string(),
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

    /// Records a single rate-limit denial event for the provided client key.
    ///
    /// The recorder stores these events in memory and periodically flushes them as
    /// aggregated batch rows so we avoid writing one database row per denial.
    pub fn record_rate_limit_violation(&self, client_id: &str) {
        let row = RateLimitViolationRow {
            gateway_name: self.gateway_name.to_string(),
            client_id: client_id.to_string(),
            created_at: Utc::now(),
        };
        self.enqueue(EventRow::RateLimitViolation(row));
    }

    fn spawn_flusher(&self, flush_interval: Duration, shutdown: CancellationToken) {
        let sink = Arc::clone(&self.sink);
        let queue = Arc::clone(&self.queue);
        let capacity = self.capacity;
        let dropped = Arc::clone(&self.dropped);
        let token = shutdown.child_token();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(flush_interval);
            loop {
                tokio::select! {
                    _ = token.cancelled() => {
                        let _ = Self::flush_once(&sink, &queue, capacity, &dropped).await;
                        break;
                    }
                    _ = interval.tick() => {
                        let _ = Self::flush_once(&sink, &queue, capacity, &dropped).await;
                    }
                }
            }
        });
    }

    fn enqueue(&self, row: EventRow) {
        Self::enqueue_with_limit(&self.queue, self.capacity, &self.dropped, row);
    }

    fn enqueue_with_limit(
        queue: &Arc<scc::Queue<EventRow>>,
        capacity: usize,
        dropped: &Arc<AtomicUsize>,
        row: EventRow,
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

    async fn flush_once(
        sink: &Arc<S>,
        queue: &Arc<scc::Queue<EventRow>>,
        capacity: usize,
        dropped: &Arc<AtomicUsize>,
    ) -> anyhow::Result<()> {
        let mut activity_rows: Vec<ActivityEventRow> = Vec::new();
        let mut worker_rows: Vec<WorkerEventRow> = Vec::new();
        let mut rate_limit_rows: Vec<RateLimitViolationRow> = Vec::new();
        while let Some(entry) = queue.pop() {
            match &**entry {
                EventRow::Activity(row) => activity_rows.push(row.clone()),
                EventRow::Worker(row) => worker_rows.push(row.clone()),
                EventRow::RateLimitViolation(row) => rate_limit_rows.push(row.clone()),
            }
        }

        if !activity_rows.is_empty()
            && let Err(e) = sink.record_activity_events_batch(&activity_rows).await
        {
            error!(error = ?e, "Failed to flush activity events");
            for row in activity_rows {
                Self::enqueue_with_limit(queue, capacity, dropped, EventRow::Activity(row));
            }
        }
        if !worker_rows.is_empty()
            && let Err(e) = sink.record_worker_events_batch(&worker_rows).await
        {
            error!(error = ?e, "Failed to flush worker events");
            for row in worker_rows {
                Self::enqueue_with_limit(queue, capacity, dropped, EventRow::Worker(row));
            }
        }

        if !rate_limit_rows.is_empty() {
            let mut details = BTreeMap::<String, i64>::new();
            let mut window_start = rate_limit_rows[0].created_at;
            let mut window_end = rate_limit_rows[0].created_at;
            for row in &rate_limit_rows {
                *details.entry(row.client_id.clone()).or_insert(0) += 1;
                if row.created_at < window_start {
                    window_start = row.created_at;
                }
                if row.created_at > window_end {
                    window_end = row.created_at;
                }
            }

            let mut details_pairs: Vec<(String, i64)> = details.into_iter().collect();
            details_pairs.sort_by(|a, b| a.0.cmp(&b.0));
            let details_map: serde_json::Map<String, serde_json::Value> = details_pairs
                .into_iter()
                .map(|(k, v)| (k, serde_json::Value::from(v)))
                .collect();
            let batch_row = RateLimitViolationBatchRow {
                gateway_name: rate_limit_rows[0].gateway_name.clone(),
                window_start,
                window_end,
                total_count: rate_limit_rows.len() as i64,
                details: serde_json::Value::Object(details_map),
                created_at: Utc::now(),
            };
            let batch_rows = vec![batch_row];
            if let Err(e) = sink.record_rate_limit_violation_batches(&batch_rows).await {
                error!(error = ?e, "Failed to flush rate-limit violation aggregates");
                for row in rate_limit_rows {
                    Self::enqueue_with_limit(
                        queue,
                        capacity,
                        dropped,
                        EventRow::RateLimitViolation(row),
                    );
                }
            }
        }
        Ok(())
    }

    #[cfg(feature = "test-support")]
    #[allow(dead_code)]
    pub async fn flush_once_for_test(&self) {
        let _ = Self::flush_once(&self.sink, &self.queue, self.capacity, &self.dropped).await;
    }
}

#[cfg(all(test, feature = "test-support"))]
mod tests {
    use super::EventRecorder;
    use crate::db::{EventSinkHandle, InMemoryEventSink};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio_util::sync::CancellationToken;

    #[tokio::test]
    async fn rate_limit_violations_are_aggregated_per_client() {
        let sink = InMemoryEventSink::default();
        let shutdown = CancellationToken::new();
        let recorder = EventRecorder::new(
            Arc::new(EventSinkHandle::InMemory(sink.clone())),
            Arc::from("gw-test"),
            Duration::from_secs(60),
            1024,
            shutdown.clone(),
        );

        recorder.record_rate_limit_violation("user:alice");
        recorder.record_rate_limit_violation("user:alice");
        recorder.record_rate_limit_violation("ip:10");
        recorder.flush_once_for_test().await;

        let rows = sink.rate_limit_rows().await;
        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert_eq!(row.gateway_name, "gw-test");
        assert_eq!(row.total_count, 3);
        assert_eq!(
            row.details,
            serde_json::json!({
                "ip:10": 1,
                "user:alice": 2
            })
        );
    }
}
