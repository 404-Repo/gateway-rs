use super::{
    DEFAULT_RESERVATION_SCAN_CAP, ExhaustedKey, ModelBucket, QueueEntry, TaskQueue, TaskRouting,
    WorkerRouting,
};
use crate::api::Task;
use crate::crypto::hotkey::Hotkey;
use prometheus::{IntGauge, opts};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier as ThreadBarrier, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use tokio::sync::Barrier;
use tokio::task;
use uuid::Uuid;

fn build_queue() -> TaskQueue {
    build_queue_with_config(2, 300, 1)
}

fn build_queue_with_config(dup: usize, ttl: u64, cleanup_interval: u64) -> TaskQueue {
    TaskQueue::builder()
        .dup(dup)
        .ttl(ttl)
        .cleanup_interval(cleanup_interval)
        .default_model("404-3dgs")
        .models(["404-3dgs", "404-mesh"])
        .build()
}

fn build_queue_with_len_gauge() -> (TaskQueue, IntGauge) {
    let gauge = IntGauge::with_opts(opts!("test_queue_len", "Observed test queue length"))
        .expect("queue len gauge");
    let queue = TaskQueue::builder()
        .dup(1)
        .ttl(300)
        .cleanup_interval(1)
        .default_model("404-3dgs")
        .models(["404-3dgs"])
        .queue_len_gauge(gauge.clone())
        .build();
    (queue, gauge)
}

fn create_task(prompt: &str, model: Option<&str>) -> Task {
    Task {
        id: Uuid::new_v4(),
        prompt: Some(Arc::new(prompt.to_string())),
        image: None,
        model: model.map(ToOwned::to_owned),
        seed: 0,
        model_params: None,
    }
}

fn push_task_with_required_tags(queue: &TaskQueue, task: Task, tags: &[&str]) {
    queue
        .try_reserve(usize::MAX)
        .expect("reserve queue slot")
        .push_with_routing(
            task,
            TaskRouting::with_required_worker_tags(
                tags.iter().map(|tag| tag.to_string()).collect(),
            ),
        );
}

fn bucket_task_ids(queue: &TaskQueue, model: &str) -> Vec<Uuid> {
    queue
        .inner
        .bucket_for_model(model)
        .snapshot_entries(usize::MAX)
        .into_iter()
        .map(|(_, entry)| entry.item.id)
        .collect()
}

#[derive(Default)]
struct MtDeliveryState {
    produced: HashMap<Uuid, Option<&'static str>>,
    delivered_pairs: HashSet<(Uuid, usize)>,
    delivered_counts: HashMap<Uuid, usize>,
}

struct MtOrderingState {
    produced_sequences: HashMap<Uuid, usize>,
    delivered_counts: HashMap<Uuid, usize>,
    last_sequence_by_worker: Vec<Option<usize>>,
}

fn mt_required_tag(sequence: usize) -> Option<&'static str> {
    match sequence % 3 {
        0 => Some("acme"),
        1 => Some("enterprise"),
        _ => None,
    }
}

fn mt_worker_tag(worker_idx: usize) -> &'static str {
    if worker_idx.is_multiple_of(2) {
        "acme"
    } else {
        "enterprise"
    }
}

fn mt_worker_tags(worker_idx: usize) -> Vec<String> {
    vec![mt_worker_tag(worker_idx).to_string()]
}

fn exhausted_key(hotkey: &Hotkey, tags: &[String]) -> ExhaustedKey {
    ExhaustedKey::new(hotkey, WorkerRouting::from_tags(tags).cache_key())
}

fn record_mt_delivery(state: &Mutex<MtDeliveryState>, worker_idx: usize, task: &Task, dup: usize) {
    let worker_tag = mt_worker_tag(worker_idx);
    let mut state = state.lock().expect("mt delivery state lock");
    let required_tag = *state
        .produced
        .get(&task.id)
        .expect("delivered task must have been produced by the stress test");

    if let Some(required_tag) = required_tag {
        assert_eq!(
            worker_tag, required_tag,
            "worker received a task with a non-matching tag"
        );
    }

    assert!(
        state.delivered_pairs.insert((task.id, worker_idx)),
        "task was delivered more than once to the same worker"
    );

    let delivered_count = state.delivered_counts.entry(task.id).or_default();
    *delivered_count += 1;
    assert!(
        *delivered_count <= dup,
        "task was delivered more times than the configured dup count"
    );
}

fn record_mt_ordered_delivery(
    state: &Mutex<MtOrderingState>,
    worker_idx: usize,
    task: &Task,
    dup: usize,
) {
    let mut state = state.lock().expect("mt ordering state lock");
    let sequence = *state
        .produced_sequences
        .get(&task.id)
        .expect("delivered task must have been produced by the ordering stress test");

    if let Some(required_tag) = mt_required_tag(sequence) {
        assert_eq!(
            mt_worker_tag(worker_idx),
            required_tag,
            "worker received a task with a non-matching tag"
        );
    }

    if let Some(previous_sequence) = state.last_sequence_by_worker[worker_idx] {
        assert!(
            previous_sequence < sequence,
            "worker observed non-increasing task order: previous={previous_sequence}, current={sequence}"
        );
    }
    state.last_sequence_by_worker[worker_idx] = Some(sequence);

    let delivered_count = state.delivered_counts.entry(task.id).or_default();
    *delivered_count += 1;
    assert!(
        *delivered_count <= dup,
        "task was delivered more times than the configured dup count"
    );
}

fn raw_entry(task: Task, available: usize, leased: usize, generation: usize) -> Arc<QueueEntry> {
    Arc::new(QueueEntry {
        item: Arc::new(task),
        routing: TaskRouting::default(),
        state: AtomicU64::new(QueueEntry::pack_state(available, leased, false)),
        timestamp: Instant::now(),
        generation,
    })
}

mod capacity;
mod concurrency;
mod expiration;
mod lifecycle;
mod routing;
