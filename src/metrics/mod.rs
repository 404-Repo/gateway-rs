use foldhash::fast::RandomState;
use prometheus::{
    opts, Counter, CounterVec, Gauge, GaugeVec, IntGauge, IntGaugeVec, Opts, Registry,
};
use scc::HashMap;
use std::sync::Arc;

/// Per-validator metrics
pub struct MetricsEntry {
    pub completion_time_avg: Gauge,
    pub completion_time_max: Gauge,
    pub completed_tasks: Counter,
    pub failed_tasks: Counter,
    pub timeout_failed_tasks: Counter,
    pub tasks_received: Counter,
    pub best_results_total: Gauge,
    pub tasks_in_progress: IntGauge,
}

#[derive(Copy, Clone, Debug)]
pub enum TaskKind {
    TextTo3D,
    ImageTo3D,
}

impl TaskKind {
    #[inline]
    fn label(self) -> &'static str {
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
    // Count requests by origin/type label (e.g. blender, unity, discord, unknown)
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

        // Per-validator metric vectors
        let completion_time_avg = GaugeVec::new(
            Opts::new(
                "completion_time_avg",
                "Per-validator EWMA of seconds between assignment and successful completion",
            ),
            &["validator"],
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
                "Per-validator longest observed completion time in seconds",
            ),
            &["validator"],
        )?;
        let completed_tasks = CounterVec::new(
            Opts::new(
                "completed_tasks_total",
                "Per-validator count of successful task submissions accepted by the gateway",
            ),
            &["validator"],
        )?;
        let failed_tasks = CounterVec::new(
            Opts::new(
                "failed_tasks_total",
                "Per-validator count of submissions marked as failures",
            ),
            &["validator"],
        )?;
        let timeout_failed_tasks = CounterVec::new(
            Opts::new(
                "timeout_failures_total",
                "Per-validator count of assigned tasks that timed out without a result",
            ),
            &["validator"],
        )?;
        let tasks_received = CounterVec::new(
            Opts::new(
                "tasks_received_total",
                "Per-validator count of tasks assigned when responding to get_tasks",
            ),
            &["validator"],
        )?;
        let best_completed_tasks = GaugeVec::new(
            Opts::new(
                "best_completed_tasks",
                "Per-validator count of wins where this validator's result was selected as best",
            ),
            &["validator"],
        )?;
        let tasks_in_progress = IntGaugeVec::new(
            Opts::new(
                "tasks_in_progress",
                "Per-validator count of assigned tasks awaiting completion, failure, or timeout",
            ),
            &["validator"],
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
        if let Some(e) = self.inner.map.read_async(key, |_, v| v.clone()).await {
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
            });
            let _ = self
                .inner
                .map
                .insert_async(key.to_string(), e.clone())
                .await;
            e
        }
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
