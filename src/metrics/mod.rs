use foldhash::fast::RandomState;
use prometheus::{opts, Counter, CounterVec, Gauge, GaugeVec, Opts, Registry};
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

    map: HashMap<String, Arc<MetricsEntry>, RandomState>,
}

impl Metrics {
    pub fn new(alpha: f64) -> Result<Self, prometheus::Error> {
        let registry = Registry::new();

        let queue_len = Gauge::with_opts(opts!("queue_len", "Current queue length"))?;
        let queue_time_avg = Gauge::with_opts(opts!("queue_time_avg", "EWMA of queue time"))?;
        let queue_time_max = Gauge::with_opts(opts!("queue_time_max", "Max seen queue time"))?;
        registry.register(Box::new(queue_len.clone()))?;
        registry.register(Box::new(queue_time_avg.clone()))?;
        registry.register(Box::new(queue_time_max.clone()))?;

        // Per-validator metric vectors
        let completion_time_avg = GaugeVec::new(
            Opts::new("completion_time_avg", "EWMA of completion time for task"),
            &["validator"],
        )?;
        let completed_tasks_by_kind = CounterVec::new(
            Opts::new(
                "tasks_completed_by_kind_total",
                "Total completed tasks partitioned by kind",
            ),
            &["kind"],
        )?;
        let requests_by_origin = CounterVec::new(
            Opts::new(
                "requests_by_origin_total",
                "Total requests partitioned by origin/type",
            ),
            &["origin"],
        )?;
        let completion_time_max = GaugeVec::new(
            Opts::new("completion_time_max", "Max completion time for task"),
            &["validator"],
        )?;
        let completed_tasks = CounterVec::new(
            Opts::new("completed_tasks_total", "Total completed tasks"),
            &["validator"],
        )?;
        let failed_tasks = CounterVec::new(
            Opts::new("failed_tasks_total", "Total failed tasks"),
            &["validator"],
        )?;
        let timeout_failed_tasks = CounterVec::new(
            Opts::new(
                "timeout_failures_total",
                "Total task timeouts counted as failures",
            ),
            &["validator"],
        )?;
        let tasks_received = CounterVec::new(
            Opts::new("tasks_received_total", "Total tasks received"),
            &["validator"],
        )?;
        let best_completed_tasks = GaugeVec::new(
            Opts::new(
                "best_completed_tasks",
                "How many times specific validator won",
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
        self.get_entry(key)
            .await
            .tasks_received
            .inc_by(tasks as f64);
    }

    pub async fn inc_task_completed(&self, key: &str) {
        self.get_entry(key).await.completed_tasks.inc();
    }

    pub async fn inc_task_failed(&self, key: &str) {
        self.get_entry(key).await.failed_tasks.inc();
    }

    pub async fn inc_timeout_failed(&self, key: &str) {
        self.get_entry(key).await.timeout_failed_tasks.inc();
    }

    pub async fn inc_best_task(&self, key: &str) {
        self.get_entry(key).await.best_results_total.inc();
    }
}
