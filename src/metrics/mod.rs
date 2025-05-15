use foldhash::fast::RandomState;
use prometheus::{opts, Counter, CounterVec, Gauge, GaugeVec, Opts, Registry};
use scc::HashMap;
use std::sync::Arc;

/// Per-validator metrics
pub struct MetricsEntry {
    pub completion_time_avg: Gauge,
    pub completion_time_max: Gauge,
    pub completed_tasks: Counter,
    // TODO: Implement failed_tasks
    pub _failed_tasks: Counter,
    pub tasks_received: Counter,
    pub best_results_total: Gauge,
}

#[derive(Clone)]
pub struct Metrics {
    inner: Arc<MetricsInner>,
}

struct MetricsInner {
    alpha: f64,

    _registry: Registry,

    queue_time_avg: Gauge,
    queue_time_max: Gauge,

    completion_time_avg: GaugeVec,
    completion_time_max: GaugeVec,
    completed_tasks: CounterVec,
    failed_tasks: CounterVec,
    tasks_received: CounterVec,
    best_results_total: GaugeVec,

    map: HashMap<String, Arc<MetricsEntry>, RandomState>,
}

impl Metrics {
    pub fn new(alpha: f64) -> Result<Self, prometheus::Error> {
        let registry = Registry::new();

        // Global queue‐time metrics
        let queue_time_avg = Gauge::with_opts(opts!("queue_time_avg", "EWMA of queue time"))?;
        let queue_time_max = Gauge::with_opts(opts!("queue_time_max", "Max seen queue time"))?;
        registry.register(Box::new(queue_time_avg.clone()))?;
        registry.register(Box::new(queue_time_max.clone()))?;

        // Per-validator metric vectors
        let completion_time_avg = GaugeVec::new(
            Opts::new("completion_time_avg", "EWMA of completion time for task"),
            &["validator"],
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
        registry.register(Box::new(failed_tasks.clone()))?;
        registry.register(Box::new(tasks_received.clone()))?;
        registry.register(Box::new(best_completed_tasks.clone()))?;

        let inner = MetricsInner {
            alpha,
            _registry: registry,
            queue_time_avg,
            queue_time_max,
            completion_time_avg,
            completion_time_max,
            completed_tasks,
            failed_tasks,
            tasks_received,
            best_results_total: best_completed_tasks,
            map: HashMap::with_capacity_and_hasher(16, RandomState::default()),
        };

        Ok(Metrics {
            inner: Arc::new(inner),
        })
    }

    fn get_entry(&self, key: &str) -> Arc<MetricsEntry> {
        if let Some(e) = self.inner.map.read(key, |_, v| v.clone()) {
            e
        } else {
            let e = Arc::new(MetricsEntry {
                completion_time_avg: self.inner.completion_time_avg.with_label_values(&[key]),
                completion_time_max: self.inner.completion_time_max.with_label_values(&[key]),
                completed_tasks: self.inner.completed_tasks.with_label_values(&[key]),
                _failed_tasks: self.inner.failed_tasks.with_label_values(&[key]),
                tasks_received: self.inner.tasks_received.with_label_values(&[key]),
                best_results_total: self.inner.best_results_total.with_label_values(&[key]),
            });
            let _ = self.inner.map.insert(key.to_string(), e.clone());
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

    pub fn record_completion_time(&self, key: &str, v: f64) {
        let entry = self.get_entry(key);
        let old = entry.completion_time_avg.get();
        entry
            .completion_time_avg
            .set(self.inner.alpha * v + (1.0 - self.inner.alpha) * old);
        let m = entry.completion_time_max.get().max(v);
        entry.completion_time_max.set(m);
    }

    pub fn inc_tasks_received(&self, key: &str) {
        self.get_entry(key).tasks_received.inc();
    }

    pub fn inc_task_completed(&self, key: &str) {
        self.get_entry(key).completed_tasks.inc();
    }

    pub fn _inc_task_failed(&self, key: &str) {
        self.get_entry(key)._failed_tasks.inc();
    }

    pub fn inc_best_task(&self, key: &str) {
        self.get_entry(key).best_results_total.inc();
    }
}
