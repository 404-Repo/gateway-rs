use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

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
        seed: u32
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
            seed
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
        while let Some(entry) = queue.pop() {
            match &**entry {
                EventRow::Activity(row) => activity_rows.push(row.clone()),
                EventRow::Worker(row) => worker_rows.push(row.clone()),
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
        Ok(())
    }

    #[cfg(feature = "test-support")]
    #[allow(dead_code)]
    pub async fn flush_once_for_test(&self) {
        let _ = Self::flush_once(&self.sink, &self.queue, self.capacity, &self.dropped).await;
    }
}
