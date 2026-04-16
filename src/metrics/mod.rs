use foldhash::fast::RandomState;
use prometheus::{
    Counter, CounterVec, Gauge, GaugeVec, IntGauge, IntGaugeVec, Opts, Registry, opts,
};
use scc::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const WORKER_METRICS_IDLE_TTL: Duration = Duration::from_secs(60 * 60);
const WORKER_METRICS_CLEANUP_INTERVAL: Duration = Duration::from_secs(60);

/// Per-worker metrics
pub struct MetricsEntry {
    pub completion_time_avg: Gauge,
    pub completion_time_max: Gauge,
    pub completed_tasks: Counter,
    pub failed_tasks: Counter,
    pub timeout_failed_tasks: Counter,
    pub tasks_received: Counter,
    pub best_results_total: Gauge,
    pub tasks_in_progress: IntGauge,
    last_touched_ms: AtomicU64,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum TaskKind {
    TextTo3D,
    ImageTo3D,
}

impl TaskKind {
    #[inline]
    pub(crate) fn label(self) -> &'static str {
        match self {
            TaskKind::TextTo3D => "txt3d",
            TaskKind::ImageTo3D => "img3d",
        }
    }
}

#[derive(Clone)]
pub struct Metrics {
    inner: Arc<MetricsInner>,
}

struct MetricsInner {
    alpha: f64,

    registry: Registry,

    queue_len: Gauge,
    queue_time_avg: Gauge,
    queue_time_max: Gauge,

    completed_tasks_by_kind: CounterVec,
    // Count requests by origin/type label (e.g. blender, unity, discord, api)
    requests_by_origin: CounterVec,

    completion_time_avg: GaugeVec,
    completion_time_max: GaugeVec,
    completed_tasks: CounterVec,
    failed_tasks: CounterVec,
    timeout_failed_tasks: CounterVec,
    tasks_received: CounterVec,
    best_results_total: GaugeVec,
    tasks_in_progress: IntGaugeVec,

    map: HashMap<String, Arc<MetricsEntry>, RandomState>,
    last_cleanup_ms: AtomicU64,
}

impl Metrics {
    pub fn new(alpha: f64) -> Result<Self, prometheus::Error> {
        let registry = Registry::new();

        let queue_len = Gauge::with_opts(opts!(
            "queue_len",
            "Number of tasks currently waiting in the queue"
        ))?;
        let queue_time_avg = Gauge::with_opts(opts!(
            "queue_time_avg",
            "EWMA of seconds tasks spend waiting in the queue"
        ))?;
        let queue_time_max = Gauge::with_opts(opts!(
            "queue_time_max",
            "Longest observed queue wait time in seconds"
        ))?;
        registry.register(Box::new(queue_len.clone()))?;
        registry.register(Box::new(queue_time_avg.clone()))?;
        registry.register(Box::new(queue_time_max.clone()))?;

        // Per-worker metric vectors
        let completion_time_avg = GaugeVec::new(
            Opts::new(
                "completion_time_avg",
                "Per-worker EWMA of seconds between assignment and successful completion",
            ),
            &["worker"],
        )?;
        let completed_tasks_by_kind = CounterVec::new(
            Opts::new(
                "tasks_completed_by_kind_total",
                "Total successful task submissions grouped by task kind",
            ),
            &["kind"],
        )?;
        let requests_by_origin = CounterVec::new(
            Opts::new(
                "requests_by_origin_total",
                "Total API requests grouped by origin",
            ),
            &["origin"],
        )?;
        let completion_time_max = GaugeVec::new(
            Opts::new(
                "completion_time_max",
                "Per-worker longest observed completion time in seconds",
            ),
            &["worker"],
        )?;
        let completed_tasks = CounterVec::new(
            Opts::new(
                "completed_tasks_total",
                "Per-worker count of successful task submissions accepted by the gateway",
            ),
            &["worker"],
        )?;
        let failed_tasks = CounterVec::new(
            Opts::new(
                "failed_tasks_total",
                "Per-worker count of submissions marked as failures",
            ),
            &["worker"],
        )?;
        let timeout_failed_tasks = CounterVec::new(
            Opts::new(
                "timeout_failures_total",
                "Per-worker count of assigned tasks that timed out without a result",
            ),
            &["worker"],
        )?;
        let tasks_received = CounterVec::new(
            Opts::new(
                "tasks_received_total",
                "Per-worker count of tasks assigned when responding to get_tasks",
            ),
            &["worker"],
        )?;
        let best_completed_tasks = GaugeVec::new(
            Opts::new(
                "best_completed_tasks",
                "Per-worker count of wins where this worker's result was selected as best",
            ),
            &["worker"],
        )?;
        let tasks_in_progress = IntGaugeVec::new(
            Opts::new(
                "tasks_in_progress",
                "Per-worker count of assigned tasks awaiting completion, failure, or timeout",
            ),
            &["worker"],
        )?;

        registry.register(Box::new(completion_time_avg.clone()))?;
        registry.register(Box::new(completion_time_max.clone()))?;
        registry.register(Box::new(completed_tasks.clone()))?;
        registry.register(Box::new(completed_tasks_by_kind.clone()))?;
        registry.register(Box::new(requests_by_origin.clone()))?;
        registry.register(Box::new(failed_tasks.clone()))?;
        registry.register(Box::new(timeout_failed_tasks.clone()))?;
        registry.register(Box::new(tasks_received.clone()))?;
        registry.register(Box::new(best_completed_tasks.clone()))?;
        registry.register(Box::new(tasks_in_progress.clone()))?;

        let inner = MetricsInner {
            alpha,
            registry,
            queue_len,
            queue_time_avg,
            queue_time_max,
            completed_tasks_by_kind,
            completion_time_avg,
            completion_time_max,
            completed_tasks,
            failed_tasks,
            timeout_failed_tasks,
            tasks_received,
            best_results_total: best_completed_tasks,
            tasks_in_progress,
            requests_by_origin,
            map: HashMap::with_capacity_and_hasher(16, RandomState::default()),
            last_cleanup_ms: AtomicU64::new(0),
        };

        Ok(Metrics {
            inner: Arc::new(inner),
        })
    }

    pub fn inc_request_origin(&self, origin: &str) {
        self.inner
            .requests_by_origin
            .with_label_values(&[origin])
            .inc();
    }

    pub fn registry(&self) -> &Registry {
        &self.inner.registry
    }

    pub fn set_queue_len(&self, len: usize) {
        self.inner.queue_len.set(len as f64);
    }

    pub fn inc_task_completed_kind(&self, kind: TaskKind) {
        self.inner
            .completed_tasks_by_kind
            .with_label_values(&[kind.label()])
            .inc();
    }

    async fn get_entry(&self, key: &str) -> Arc<MetricsEntry> {
        let now_ms = current_time_millis();
        if let Some(e) = self.inner.map.read_async(key, |_, v| v.clone()).await {
            e.touch(now_ms);
            e
        } else {
            let e = Arc::new(MetricsEntry {
                completion_time_avg: self.inner.completion_time_avg.with_label_values(&[key]),
                completion_time_max: self.inner.completion_time_max.with_label_values(&[key]),
                completed_tasks: self.inner.completed_tasks.with_label_values(&[key]),
                failed_tasks: self.inner.failed_tasks.with_label_values(&[key]),
                timeout_failed_tasks: self.inner.timeout_failed_tasks.with_label_values(&[key]),
                tasks_received: self.inner.tasks_received.with_label_values(&[key]),
                best_results_total: self.inner.best_results_total.with_label_values(&[key]),
                tasks_in_progress: self.inner.tasks_in_progress.with_label_values(&[key]),
                last_touched_ms: AtomicU64::new(now_ms),
            });
            let _ = self
                .inner
                .map
                .insert_async(key.to_string(), e.clone())
                .await;
            e
        }
    }

    pub async fn maybe_evict_stale_workers(&self) {
        let now_ms = current_time_millis();
        let last_cleanup_ms = self.inner.last_cleanup_ms.load(Ordering::Acquire);
        let min_next_cleanup_ms =
            last_cleanup_ms.saturating_add(duration_to_millis(WORKER_METRICS_CLEANUP_INTERVAL));
        if now_ms < min_next_cleanup_ms {
            return;
        }
        if self
            .inner
            .last_cleanup_ms
            .compare_exchange(last_cleanup_ms, now_ms, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        self.evict_stale_workers_inner(now_ms, WORKER_METRICS_IDLE_TTL)
            .await;
    }

    async fn evict_stale_workers_inner(&self, now_ms: u64, max_idle: Duration) {
        let stale_before_ms = now_ms.saturating_sub(duration_to_millis(max_idle));
        let completion_time_avg = self.inner.completion_time_avg.clone();
        let completion_time_max = self.inner.completion_time_max.clone();
        let completed_tasks = self.inner.completed_tasks.clone();
        let failed_tasks = self.inner.failed_tasks.clone();
        let timeout_failed_tasks = self.inner.timeout_failed_tasks.clone();
        let tasks_received = self.inner.tasks_received.clone();
        let best_results_total = self.inner.best_results_total.clone();
        let tasks_in_progress = self.inner.tasks_in_progress.clone();

        self.inner
            .map
            .retain_async(|key, entry| {
                if entry.tasks_in_progress.get() > 0
                    || entry.last_touched_ms.load(Ordering::Acquire) > stale_before_ms
                {
                    return true;
                }

                let labels = [key.as_str()];
                let _ = completion_time_avg.remove_label_values(&labels);
                let _ = completion_time_max.remove_label_values(&labels);
                let _ = completed_tasks.remove_label_values(&labels);
                let _ = failed_tasks.remove_label_values(&labels);
                let _ = timeout_failed_tasks.remove_label_values(&labels);
                let _ = tasks_received.remove_label_values(&labels);
                let _ = best_results_total.remove_label_values(&labels);
                let _ = tasks_in_progress.remove_label_values(&labels);
                false
            })
            .await;
    }

    pub fn record_queue_time(&self, v: f64) {
        let old = self.inner.queue_time_avg.get();
        self.inner
            .queue_time_avg
            .set(self.inner.alpha * v + (1.0 - self.inner.alpha) * old);
        let m = self.inner.queue_time_max.get().max(v);
        self.inner.queue_time_max.set(m);
    }

    pub async fn record_completion_time(&self, key: &str, v: f64) {
        let entry = self.get_entry(key).await;
        let old = entry.completion_time_avg.get();
        entry
            .completion_time_avg
            .set(self.inner.alpha * v + (1.0 - self.inner.alpha) * old);
        let m = entry.completion_time_max.get().max(v);
        entry.completion_time_max.set(m);
    }

    pub async fn inc_tasks_received(&self, key: &str, tasks: usize) {
        let entry = self.get_entry(key).await;
        entry.tasks_received.inc_by(tasks as f64);
    }

    pub async fn inc_task_completed(&self, key: &str) {
        let entry = self.get_entry(key).await;
        entry.completed_tasks.inc();
    }

    pub async fn inc_task_failed(&self, key: &str) {
        let entry = self.get_entry(key).await;
        entry.failed_tasks.inc();
    }

    pub async fn inc_timeout_failed(&self, key: &str) {
        let entry = self.get_entry(key).await;
        entry.timeout_failed_tasks.inc();
    }

    pub async fn inc_best_task(&self, key: &str) {
        self.get_entry(key).await.best_results_total.inc();
    }

    pub async fn start_task(&self, key: &str) -> TaskInProgressGuard {
        let entry = self.get_entry(key).await;
        TaskInProgressGuard::new(entry.tasks_in_progress.clone())
    }

    #[cfg(test)]
    pub async fn evict_stale_workers_for_test(&self, max_idle: Duration) {
        self.evict_stale_workers_inner(current_time_millis(), max_idle)
            .await;
    }

    #[cfg(test)]
    pub fn worker_entry_count(&self) -> usize {
        self.inner.map.len()
    }
}

pub struct TaskInProgressGuard {
    gauge: IntGauge,
}

impl TaskInProgressGuard {
    fn new(gauge: IntGauge) -> Self {
        gauge.inc();
        Self { gauge }
    }
}

impl Drop for TaskInProgressGuard {
    fn drop(&mut self) {
        self.gauge.dec();
    }
}

impl MetricsEntry {
    fn touch(&self, now_ms: u64) {
        self.last_touched_ms.store(now_ms, Ordering::Release);
    }
}

fn current_time_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

fn duration_to_millis(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::Metrics;

    fn worker_metric_present(metrics: &Metrics, metric_name: &str, worker: &str) -> bool {
        metrics.registry().gather().into_iter().any(|family| {
            family.name() == metric_name
                && family.metric.iter().any(|metric| {
                    metric
                        .label
                        .iter()
                        .any(|label| label.name() == "worker" && label.value() == worker)
                })
        })
    }

    #[tokio::test]
    async fn evicts_stale_worker_metrics_from_registry() {
        let metrics = Metrics::new(0.05).unwrap();

        metrics.inc_tasks_received("worker-a", 1).await;
        assert_eq!(metrics.worker_entry_count(), 1);
        assert!(worker_metric_present(
            &metrics,
            "tasks_received_total",
            "worker-a"
        ));

        metrics.evict_stale_workers_for_test(Duration::ZERO).await;

        assert_eq!(metrics.worker_entry_count(), 0);
        assert!(!worker_metric_present(
            &metrics,
            "tasks_received_total",
            "worker-a"
        ));
    }

    #[tokio::test]
    async fn keeps_active_workers_while_tasks_are_in_progress() {
        let metrics = Metrics::new(0.05).unwrap();

        let guard = metrics.start_task("worker-b").await;
        metrics.evict_stale_workers_for_test(Duration::ZERO).await;
        assert_eq!(metrics.worker_entry_count(), 1);

        drop(guard);
        metrics.evict_stale_workers_for_test(Duration::ZERO).await;
        assert_eq!(metrics.worker_entry_count(), 0);
    }
}
