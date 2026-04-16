use super::{ModelBucket, QueueEntry, TaskQueue};
use crate::api::Task;
use crate::crypto::hotkey::Hotkey;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc;
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

fn raw_entry(
    task: Task,
    available: usize,
    leased: usize,
    queued: bool,
    generation: usize,
) -> Arc<QueueEntry> {
    Arc::new(QueueEntry {
        item: Arc::new(task),
        state: AtomicU64::new(QueueEntry::pack_state(available, leased, queued, false)),
        timestamp: Instant::now(),
        generation,
    })
}

#[tokio::test]
async fn normalizes_default_model_on_enqueue() {
    let queue = build_queue();
    let hotkey = Hotkey::from_bytes(&[1u8; 32]);
    queue.push(create_task("robot", None));

    let items = queue.pop_for_models(1, &hotkey, &["404-3dgs"]);
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].0.model.as_deref(), Some("404-3dgs"));
}

#[tokio::test]
async fn pop_for_models_only_scans_requested_buckets() {
    let queue = build_queue();
    let hotkey = Hotkey::from_bytes(&[2u8; 32]);
    queue.push(create_task("robot", Some("404-3dgs")));
    queue.push(create_task("car", Some("404-mesh")));

    let items = queue.pop_for_models(2, &hotkey, &["404-3dgs"]);
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].0.model.as_deref(), Some("404-3dgs"));
    assert!(queue.pop_for_models(1, &hotkey, &["404-3dgs"]).is_empty());
    let other_hotkey = Hotkey::from_bytes(&[9u8; 32]);
    let other_items = queue.pop_for_models(1, &other_hotkey, &["404-mesh"]);
    assert_eq!(other_items.len(), 1);
    assert_eq!(other_items[0].0.model.as_deref(), Some("404-mesh"));
}

#[tokio::test]
async fn pop_for_models_round_robins_across_requested_buckets() {
    let queue = build_queue();
    let hotkey = Hotkey::from_bytes(&[3u8; 32]);
    queue.push(create_task("robot", Some("404-3dgs")));
    queue.push(create_task("car", Some("404-mesh")));

    let items = queue.pop_for_models(2, &hotkey, &["404-3dgs", "404-mesh"]);
    assert_eq!(items.len(), 2);
    let models: HashSet<_> = items
        .into_iter()
        .filter_map(|(task, _)| task.model)
        .collect();
    assert_eq!(models.len(), 2);
    assert!(models.contains("404-3dgs"));
    assert!(models.contains("404-mesh"));
}

#[tokio::test]
async fn single_push_pop_honors_dup_across_hotkeys() {
    let queue = Arc::new(build_queue_with_config(3, 300, 1));
    let task = create_task("Task 1", None);
    queue.push(task.clone());

    let hk_a: Hotkey = Hotkey::from_bytes(&[11u8; 32]);
    let res_a = queue.pop(3, &hk_a);
    assert_eq!(res_a.len(), 1);
    let (item_a, dur_a) = &res_a[0];
    assert_eq!(item_a, &task);
    assert!(dur_a.is_none());
    assert!(queue.pop(3, &hk_a).is_empty());

    let hk_b: Hotkey = Hotkey::from_bytes(&[12u8; 32]);
    let res_b = queue.pop(3, &hk_b);
    assert_eq!(res_b.len(), 1);
    assert_eq!(res_b[0].0, task);
    assert!(res_b[0].1.is_none());

    let hk_c: Hotkey = Hotkey::from_bytes(&[13u8; 32]);
    let res_c = queue.pop(3, &hk_c);
    assert_eq!(res_c.len(), 1);
    assert_eq!(res_c[0].0, task);
    assert!(
        res_c[0].1.is_some(),
        "expected a duration on final delivery"
    );

    let hk_d: Hotkey = Hotkey::from_bytes(&[14u8; 32]);
    assert!(queue.pop(3, &hk_d).is_empty());
}

#[tokio::test]
async fn multiple_pushes_do_not_double_deliver_same_task_to_hotkey() {
    let queue = Arc::new(build_queue_with_config(2, 300, 1));
    let task1 = create_task("Task 1", None);
    let task2 = create_task("Task 2", None);
    queue.push(task1.clone());
    queue.push(task2.clone());
    queue.push(task1.clone());

    let hk: Hotkey = Hotkey::from_bytes(&[15u8; 32]);
    let res = queue.pop(2, &hk);
    let mut res_tasks: Vec<Task> = res.iter().map(|(task, _)| task.clone()).collect();
    res_tasks.sort_by(|a, b| a.prompt.cmp(&b.prompt));
    let mut expected = vec![task1, task2];
    expected.sort_by(|a, b| a.prompt.cmp(&b.prompt));
    assert_eq!(res_tasks, expected);
    for (_, duration) in res {
        assert!(duration.is_none());
    }
}

#[tokio::test]
async fn duplicate_uuid_entries_stay_deduped_until_last_copy_finishes() {
    let queue = build_queue_with_config(1, 300, 1);
    let task = create_task("duplicate", None);
    queue.push(task.clone());
    queue.push(task.clone());

    let hk_a = Hotkey::from_bytes(&[31u8; 32]);
    let first = queue.pop(1, &hk_a);
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].0, task);
    assert!(
        queue.pop(1, &hk_a).is_empty(),
        "same hotkey should remain deduped while another copy is still live"
    );

    let hk_b = Hotkey::from_bytes(&[32u8; 32]);
    let second = queue.pop(1, &hk_b);
    assert_eq!(second.len(), 1);
    assert_eq!(second[0].0, task);
}

#[tokio::test]
async fn completed_task_clears_dedupe_state_for_reused_uuid() {
    let queue = build_queue_with_config(1, 300, 1);
    let first = create_task("first", None);
    let mut second = create_task("second", None);
    second.id = first.id;

    let hk = Hotkey::from_bytes(&[33u8; 32]);
    queue.push(first);

    let first_pop = queue.pop(1, &hk);
    assert_eq!(first_pop.len(), 1);

    queue.push(second.clone());

    let second_pop = queue.pop(1, &hk);
    assert_eq!(second_pop.len(), 1);
    assert_eq!(second_pop[0].0.id, second.id);
    assert_eq!(
        second_pop[0].0.prompt.as_deref().map(String::as_str),
        Some("second")
    );
}

#[tokio::test]
async fn final_commit_reports_duration_at_commit_time() {
    let queue = build_queue_with_config(1, 300, 1);
    let task = create_task("delayed-final", None);
    queue.push(task.clone());

    let hotkey = Hotkey::from_bytes(&[38u8; 32]);
    let mut deliveries = queue.reserve(1, &hotkey);
    let delivery = deliveries.pop().expect("expected reserved delivery");
    let reserved_duration = delivery
        .duration()
        .expect("single-dup reservation should know it is final");

    tokio::time::sleep(Duration::from_millis(25)).await;

    let (committed, committed_duration) = delivery.commit();
    assert_eq!(committed, task);
    let committed_duration =
        committed_duration.expect("final commit should report a completion duration");
    assert!(
        committed_duration >= reserved_duration + Duration::from_millis(10),
        "commit duration should reflect time elapsed after reservation"
    );
}

#[tokio::test]
async fn completed_tasks_clear_internal_dedupe_bookkeeping() {
    let queue = build_queue_with_config(1, 300, 1);
    let hotkey = Hotkey::from_bytes(&[39u8; 32]);
    let mut completed = Vec::new();

    for idx in 0..4 {
        let task = create_task(&format!("done-{idx}"), None);
        queue.push(task.clone());

        let generation = queue
            .inner
            .active_ids
            .read_sync(&task.id, |_, state| state.generation)
            .expect("task should have active bookkeeping after push");

        let popped = queue.pop(1, &hotkey);
        assert_eq!(popped.len(), 1);
        assert_eq!(popped[0].0, task);
        completed.push((task.id, generation));
    }

    assert_eq!(queue.len(), 0);

    for (task_id, generation) in completed {
        assert!(
            queue
                .inner
                .active_ids
                .read_sync(&task_id, |_, _| ())
                .is_none(),
            "active bookkeeping should be removed for completed tasks"
        );
        assert!(
            queue
                .inner
                .sent
                .read_sync(&(task_id, generation), |_, _| ())
                .is_none(),
            "sent bookkeeping should be removed for completed tasks"
        );
    }
}

#[tokio::test]
async fn unknown_model_uses_default_bucket() {
    let queue = build_queue();
    let hotkey = Hotkey::from_bytes(&[40u8; 32]);
    queue.push(create_task("robot", Some("missing-model")));

    let items = queue.pop_for_models(1, &hotkey, &["404-3dgs"]);
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].0.model.as_deref(), Some("missing-model"));
}

#[tokio::test]
async fn model_filtered_pop_and_reserve_ignore_empty_or_unknown_models() {
    let queue = build_queue();
    let task = create_task("robot", Some("404-3dgs"));
    queue.push(task.clone());

    let hotkey = Hotkey::from_bytes(&[41u8; 32]);
    let empty: [&str; 0] = [];
    assert!(queue.reserve_for_models(1, &hotkey, &empty).is_empty());
    assert!(
        queue
            .pop_for_models(1, &hotkey, &["missing-model"])
            .is_empty()
    );
    assert_eq!(queue.len(), 1);

    let items = queue.pop_for_models(1, &hotkey, &["404-3dgs"]);
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].0, task);
}

#[tokio::test]
async fn dropped_delivery_restores_hotkey_and_dup_budget() {
    let queue = build_queue_with_config(2, 300, 1);
    let task = create_task("rollback", Some("404-3dgs"));
    queue.push(task.clone());

    let hk_a = Hotkey::from_bytes(&[34u8; 32]);
    let mut deliveries = queue.reserve_for_models(1, &hk_a, &["404-3dgs"]);
    let delivery = deliveries.pop().expect("expected reserved delivery");
    drop(delivery);

    let retry_a = queue.pop(1, &hk_a);
    assert_eq!(retry_a.len(), 1);
    assert_eq!(retry_a[0].0, task);

    let hk_b = Hotkey::from_bytes(&[35u8; 32]);
    let retry_b = queue.pop(1, &hk_b);
    assert_eq!(retry_b.len(), 1);
    assert_eq!(retry_b[0].0, task);

    let hk_c = Hotkey::from_bytes(&[36u8; 32]);
    assert!(queue.pop(1, &hk_c).is_empty());
}

#[tokio::test]
async fn duplicate_reservations_can_be_held_concurrently() {
    let queue = build_queue_with_config(2, 300, 60);
    let task = create_task("parallel-dup", Some("404-3dgs"));
    queue.push(task.clone());

    let hk_a = Hotkey::from_bytes(&[60u8; 32]);
    let hk_b = Hotkey::from_bytes(&[61u8; 32]);

    let mut first = queue.reserve_for_models(1, &hk_a, &["404-3dgs"]);
    let mut second = queue.reserve_for_models(1, &hk_b, &["404-3dgs"]);

    assert_eq!(first.len(), 1);
    assert_eq!(second.len(), 1);
    assert_eq!(first[0].task(), &task);
    assert_eq!(second[0].task(), &task);
    assert!(queue.pop_for_models(1, &hk_a, &["404-3dgs"]).is_empty());
    assert!(queue.pop_for_models(1, &hk_b, &["404-3dgs"]).is_empty());

    drop(first.pop().expect("first delivery"));
    drop(second.pop().expect("second delivery"));
}

#[tokio::test]
async fn expired_reserved_delivery_does_not_requeue_on_drop() {
    let queue = build_queue_with_config(1, 1, 60);
    let task = create_task("expired-rollback", Some("404-3dgs"));
    queue.push(task);

    let hk = Hotkey::from_bytes(&[62u8; 32]);
    let mut deliveries = queue.reserve_for_models(1, &hk, &["404-3dgs"]);
    let delivery = deliveries.pop().expect("expected reserved task");

    tokio::time::sleep(Duration::from_secs(2)).await;
    drop(delivery);

    assert!(queue.pop_for_models(1, &hk, &["404-3dgs"]).is_empty());
    assert_eq!(queue.len(), 0);
}

#[tokio::test]
async fn final_commit_retires_task_after_expired_duplicate_is_observed() {
    let queue = build_queue_with_config(2, 1, 60);
    let task = create_task("expired-commit", Some("404-3dgs"));
    queue.push(task.clone());

    let hk_a = Hotkey::from_bytes(&[64u8; 32]);
    let hk_b = Hotkey::from_bytes(&[65u8; 32]);
    let mut deliveries = queue.reserve_for_models(1, &hk_a, &["404-3dgs"]);
    let delivery = deliveries.pop().expect("expected reserved task");
    let generation = delivery
        .entry
        .as_ref()
        .map(|entry| entry.generation)
        .expect("delivery should hold generation");

    tokio::time::sleep(Duration::from_secs(2)).await;

    assert!(
        queue.pop_for_models(1, &hk_b, &["404-3dgs"]).is_empty(),
        "expired duplicate should be reaped instead of delivered"
    );
    assert_eq!(
        queue.len(),
        1,
        "outstanding lease should keep bookkeeping alive until final commit"
    );

    let (committed, duration) = delivery.commit();
    assert_eq!(committed, task);
    assert!(
        duration.is_some(),
        "final commit should report queue duration once it retires the task"
    );
    assert_eq!(queue.len(), 0);
    assert!(
        queue
            .inner
            .active_ids
            .read_sync(&task.id, |_, _| ())
            .is_none()
    );
    assert!(
        queue
            .inner
            .sent
            .read_sync(&(task.id, generation), |_, _| ())
            .is_none()
    );
}

#[tokio::test]
async fn final_expired_rollback_retires_task_after_duplicate_is_observed() {
    let queue = build_queue_with_config(2, 1, 60);
    let task = create_task("expired-final-rollback", Some("404-3dgs"));
    queue.push(task.clone());

    let hk_a = Hotkey::from_bytes(&[66u8; 32]);
    let hk_b = Hotkey::from_bytes(&[67u8; 32]);
    let mut deliveries = queue.reserve_for_models(1, &hk_a, &["404-3dgs"]);
    let delivery = deliveries.pop().expect("expected reserved task");
    let generation = delivery
        .entry
        .as_ref()
        .map(|entry| entry.generation)
        .expect("delivery should hold generation");

    tokio::time::sleep(Duration::from_secs(2)).await;

    assert!(
        queue.pop_for_models(1, &hk_b, &["404-3dgs"]).is_empty(),
        "expired duplicate should be reaped instead of delivered"
    );
    assert_eq!(
        queue.len(),
        1,
        "outstanding expired lease should keep bookkeeping alive until rollback"
    );

    delivery.rollback();

    assert_eq!(queue.len(), 0);
    assert!(
        queue
            .inner
            .active_ids
            .read_sync(&task.id, |_, _| ())
            .is_none()
    );
    assert!(
        queue
            .inner
            .sent
            .read_sync(&(task.id, generation), |_, _| ())
            .is_none()
    );
    let hk_c = Hotkey::from_bytes(&[68u8; 32]);
    assert!(queue.pop_for_models(1, &hk_c, &["404-3dgs"]).is_empty());
}

#[tokio::test]
async fn zero_remaining_entry_is_reaped_if_encountered() {
    let queue = build_queue_with_config(1, 300, 1);
    let task = create_task("stale", Some("404-3dgs"));
    let generation = queue.inner.acquire_task_generation(task.id);
    let entry = raw_entry(task.clone(), 0, 0, true, generation);
    queue.inner.len.fetch_add(1, Ordering::Relaxed);
    let bucket = queue.inner.bucket_for_model("404-3dgs");
    bucket.q.push(entry);
    bucket.live.fetch_add(1, Ordering::Relaxed);
    bucket.activity.fetch_add(1, Ordering::AcqRel);
    bucket.generation.fetch_add(1, Ordering::AcqRel);

    let hotkey = Hotkey::from_bytes(&[42u8; 32]);
    assert!(queue.pop_for_models(1, &hotkey, &["404-3dgs"]).is_empty());
    assert_eq!(queue.len(), 0);
    assert!(
        queue
            .inner
            .active_ids
            .read_sync(&task.id, |_, _| ())
            .is_none()
    );
}

#[tokio::test]
async fn pop_more_than_exists_returns_all_available_tasks() {
    for &dup in &[1usize, 4usize] {
        let queue = Arc::new(build_queue_with_config(dup, 300, 1));
        let task1 = create_task("Task A", None);
        let task2 = create_task("Task B", None);
        queue.push(task1.clone());
        queue.push(task2.clone());

        let hk_alpha: Hotkey = Hotkey::from_bytes(&[16u8; 32]);
        let res = queue.pop(50, &hk_alpha);
        let mut res_tasks: Vec<Task> = res.iter().map(|(task, _)| task.clone()).collect();
        res_tasks.sort_by(|a, b| a.prompt.cmp(&b.prompt));
        let mut expected = vec![task1.clone(), task2.clone()];
        expected.sort_by(|a, b| a.prompt.cmp(&b.prompt));
        assert_eq!(res_tasks, expected);

        for (_, duration) in &res {
            if dup == 1 {
                assert!(duration.is_some());
            } else {
                assert!(duration.is_none());
            }
        }

        if dup == 1 {
            let hk_beta: Hotkey = Hotkey::from_bytes(&[17u8; 32]);
            assert!(queue.pop(3, &hk_beta).is_empty());
        }

        assert!(queue.pop(50, &hk_alpha).is_empty());
    }
}

#[tokio::test]
async fn requeue_duplicates_allow_new_hotkeys_to_receive_tasks() {
    let queue = Arc::new(build_queue_with_config(3, 300, 1));
    let task1 = create_task("hello", None);
    let task2 = create_task("world", None);
    queue.push(task1.clone());
    queue.push(task2.clone());

    let hk_1: Hotkey = Hotkey::from_bytes(&[18u8; 32]);
    let res1 = queue.pop(2, &hk_1);
    let mut res1_tasks: Vec<Task> = res1.iter().map(|(task, _)| task.clone()).collect();
    res1_tasks.sort_by(|a, b| a.prompt.cmp(&b.prompt));
    let mut expected = vec![task1.clone(), task2.clone()];
    expected.sort_by(|a, b| a.prompt.cmp(&b.prompt));
    assert_eq!(res1_tasks, expected);
    for (_, duration) in res1 {
        assert!(duration.is_none());
    }

    assert!(queue.pop(2, &hk_1).is_empty());

    let hk_2: Hotkey = Hotkey::from_bytes(&[19u8; 32]);
    let res2 = queue.pop(2, &hk_2);
    let mut res2_tasks: Vec<Task> = res2.iter().map(|(task, _)| task.clone()).collect();
    res2_tasks.sort_by(|a, b| a.prompt.cmp(&b.prompt));
    assert_eq!(res2_tasks, expected);
    for (_, duration) in res2 {
        assert!(duration.is_none());
    }
}

#[tokio::test]
async fn exhausted_hotkey_receives_newly_enqueued_task() {
    let queue = build_queue_with_config(2, 300, 1);
    let hk: Hotkey = Hotkey::from_bytes(&[25u8; 32]);
    let first = create_task("first", Some("404-3dgs"));
    let second = create_task("second", Some("404-3dgs"));

    queue.push(first.clone());

    let first_pop = queue.pop_for_models(1, &hk, &["404-3dgs"]);
    assert_eq!(first_pop.len(), 1);
    assert_eq!(first_pop[0].0, first);
    assert!(first_pop[0].1.is_none());

    assert!(
        queue.pop_for_models(4, &hk, &["404-3dgs"]).is_empty(),
        "hotkey should be exhausted until new work arrives"
    );

    queue.push(second.clone());

    let second_pop = queue.pop_for_models(4, &hk, &["404-3dgs"]);
    assert_eq!(second_pop.len(), 1);
    assert_eq!(second_pop[0].0, second);
    assert!(second_pop[0].1.is_none());
}

#[tokio::test]
async fn rollback_clears_exhausted_cache_for_same_hotkey() {
    let queue = build_queue_with_config(2, 300, 60);
    let task = create_task("rollback-exhausted", Some("404-3dgs"));
    queue.push(task.clone());

    let hk = Hotkey::from_bytes(&[69u8; 32]);
    let bucket = queue.inner.bucket_for_model("404-3dgs");
    let mut deliveries = queue.reserve_for_models(1, &hk, &["404-3dgs"]);
    let delivery = deliveries.pop().expect("expected reserved delivery");

    assert!(
        queue.pop_for_models(1, &hk, &["404-3dgs"]).is_empty(),
        "same hotkey should be marked exhausted after scanning only already-sent work"
    );
    let generation = bucket.generation.load(Ordering::Acquire);
    assert_eq!(
        bucket.exhausted_generation(&hk),
        Some(generation),
        "empty scan should cache the hotkey as exhausted for the current generation"
    );

    delivery.rollback();

    assert!(
        bucket.exhausted_generation(&hk).is_none(),
        "rollback should invalidate exhausted-cache state for the same hotkey"
    );
    let retry = queue.pop_for_models(1, &hk, &["404-3dgs"]);
    assert_eq!(retry.len(), 1);
    assert_eq!(retry[0].0, task);
}

#[tokio::test]
async fn requeue_for_other_hotkeys_keeps_same_hotkey_blocked() {
    let queue = build_queue_with_config(2, 300, 60);
    let task = create_task("same-worker-requeue", Some("404-3dgs"));
    queue.push(task.clone());

    let hk_a = Hotkey::from_bytes(&[70u8; 32]);
    let mut deliveries = queue.reserve_for_models(1, &hk_a, &["404-3dgs"]);
    let delivery = deliveries.pop().expect("expected reserved delivery");
    delivery.requeue_for_other_hotkeys();

    assert!(
        queue.pop_for_models(1, &hk_a, &["404-3dgs"]).is_empty(),
        "same hotkey should not receive a task that was requeued for other workers"
    );

    let hk_b = Hotkey::from_bytes(&[71u8; 32]);
    let retry = queue.pop_for_models(1, &hk_b, &["404-3dgs"]);
    assert_eq!(retry.len(), 1);
    assert_eq!(retry[0].0, task);
}

#[tokio::test]
async fn in_flight_pop_does_not_mark_bucket_exhausted() {
    let bucket = Arc::new(ModelBucket::default());
    let entry = raw_entry(create_task("robot", Some("404-3dgs")), 2, 0, false, 7);
    bucket.enqueue_new(entry);

    let hotkey = Hotkey::from_bytes(&[26u8; 32]);
    let generation = bucket.generation.load(Ordering::Acquire);
    let start_activity = bucket.activity.load(Ordering::Acquire);
    let start_in_flight = bucket.in_flight.load(Ordering::Acquire);
    assert_eq!(start_in_flight, 0);

    let (ready_tx, ready_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let worker_bucket = Arc::clone(&bucket);
    let handle = std::thread::spawn(move || {
        worker_bucket
            .pop_visible_entry_with_hook(|| {
                ready_tx.send(()).expect("signal pop readiness");
                release_rx.recv().expect("wait for release");
            })
            .expect("expected queued entry")
    });

    ready_rx.recv().expect("wait for worker pop");
    assert_eq!(bucket.in_flight.load(Ordering::Acquire), 1);
    assert!(
        bucket.q.pop().is_none(),
        "worker should hold the only entry"
    );

    bucket.mark_exhausted_if_stable(&hotkey, generation, start_activity, 0, start_in_flight);
    assert!(
        bucket.exhausted_generation(&hotkey).is_none(),
        "in-flight work must block exhausted-cache writes"
    );

    release_tx.send(()).expect("resume worker");
    let popped = handle.join().expect("worker should join");
    bucket.finish_entry(popped, true);
}

#[tokio::test]
async fn concurrent_push_pop_returns_unique_tasks_per_hotkey() {
    let queue = Arc::new(build_queue_with_config(3, 300, 1));
    let q_producer = Arc::clone(&queue);
    let producer = task::spawn(async move {
        for i in 0..10 {
            q_producer.push(create_task(&format!("Task {}", i), None));
        }
    });
    producer.await.expect("producer task should complete");

    let mut handles = Vec::new();
    for idx in 0..3 {
        let q = Arc::clone(&queue);
        let hotkey: Hotkey = match idx {
            0 => Hotkey::from_bytes(&[20u8; 32]),
            1 => Hotkey::from_bytes(&[21u8; 32]),
            _ => Hotkey::from_bytes(&[22u8; 32]),
        };
        handles.push(task::spawn(async move {
            let res = q.pop(5, &hotkey);
            let set: HashSet<_> = res.iter().map(|(task, _)| task.id).collect();
            assert_eq!(set.len(), res.len());
            for (_, duration) in &res {
                assert!(duration.is_none());
            }
            res
        }));
    }

    for handle in handles {
        handle.await.expect("consumer task should complete");
    }
}

#[tokio::test]
async fn concurrent_polls_from_same_hotkey_do_not_duplicate_task() {
    let queue = Arc::new(build_queue_with_config(1, 300, 1));
    let task = create_task("single", None);
    queue.push(task.clone());

    let hotkey = Hotkey::from_bytes(&[23u8; 32]);
    let barrier = Arc::new(Barrier::new(5));
    let mut handles = Vec::new();
    for _ in 0..5 {
        let q = Arc::clone(&queue);
        let barrier = Arc::clone(&barrier);
        let hotkey = hotkey.clone();
        handles.push(task::spawn(async move {
            barrier.wait().await;
            q.pop(1, &hotkey)
        }));
    }

    let mut delivered = Vec::new();
    for handle in handles {
        delivered.extend(handle.await.expect("same-hotkey consumer should complete"));
    }

    assert_eq!(delivered.len(), 1);
    assert_eq!(delivered[0].0, task);
}

#[tokio::test]
async fn lazy_expiration_reaps_before_background_cleanup_runs() {
    let queue = build_queue_with_config(1, 1, 60);
    queue.push(create_task("lazy-expired", None));
    tokio::time::sleep(Duration::from_secs(2)).await;

    let hk = Hotkey::from_bytes(&[63u8; 32]);
    assert!(queue.pop(1, &hk).is_empty());
    assert_eq!(queue.len(), 0);
}

#[tokio::test]
async fn cleanup_removes_old_entries() {
    let queue = build_queue_with_config(1, 1, 1);
    queue.push(create_task("Old Task", None));
    tokio::time::sleep(Duration::from_secs(2)).await;

    let hk: Hotkey = Hotkey::from_bytes(&[23u8; 32]);
    assert!(queue.pop(1, &hk).is_empty());
}

#[tokio::test]
async fn dropping_queue_clone_does_not_stop_cleanup() {
    let queue = build_queue_with_config(1, 1, 1);
    let clone = queue.clone();
    drop(clone);

    queue.push(create_task("Old Task", None));
    tokio::time::sleep(Duration::from_secs(2)).await;

    let hk: Hotkey = Hotkey::from_bytes(&[37u8; 32]);
    assert!(queue.pop(1, &hk).is_empty());
}

#[tokio::test]
async fn cleanup_removes_expired_entries_behind_fresh_front_entry() {
    let queue = build_queue_with_config(2, 3, 1);
    let old_task = create_task("Old Task", None);
    queue.push(old_task.clone());

    tokio::time::sleep(Duration::from_millis(2100)).await;

    let fresh_task = create_task("Fresh Task", None);
    queue.push(fresh_task.clone());

    let hk_a: Hotkey = Hotkey::from_bytes(&[26u8; 32]);
    let first_pop = queue.pop(1, &hk_a);
    assert_eq!(first_pop.len(), 1);
    assert_eq!(first_pop[0].0, old_task);
    assert!(first_pop[0].1.is_none());

    tokio::time::sleep(Duration::from_millis(1200)).await;

    assert_eq!(
        queue.len(),
        1,
        "cleanup should remove the expired requeued task while preserving fresh work"
    );

    let hk_b: Hotkey = Hotkey::from_bytes(&[27u8; 32]);
    let second_pop = queue.pop(2, &hk_b);
    assert_eq!(second_pop.len(), 1);
    assert_eq!(second_pop[0].0, fresh_task);
    assert!(second_pop[0].1.is_none());
}

#[tokio::test]
async fn try_reserve_respects_capacity() {
    let queue = build_queue_with_config(1, 300, 1);
    let task = create_task("Task A", None);

    let first_slot = queue.try_reserve(1).expect("first reservation should fit");
    assert_eq!(queue.len(), 1);
    assert!(
        queue.try_reserve(1).is_none(),
        "second reservation should exceed the bound"
    );

    first_slot.push(task.clone());
    assert_eq!(queue.len(), 1);

    let hk: Hotkey = Hotkey::from_bytes(&[24u8; 32]);
    let popped = queue.pop(1, &hk);
    assert_eq!(popped.len(), 1);
    assert_eq!(popped[0].0, task);
}

#[tokio::test]
async fn concurrent_try_reserve_respects_capacity() {
    let queue = Arc::new(build_queue_with_config(1, 300, 1));
    let max_len = 4;
    let contenders = 16;
    let barrier = Arc::new(Barrier::new(contenders));
    let mut handles = Vec::with_capacity(contenders);

    for _ in 0..contenders {
        let q = Arc::clone(&queue);
        let barrier = Arc::clone(&barrier);
        handles.push(task::spawn(async move {
            barrier.wait().await;
            q.try_reserve(max_len)
        }));
    }

    let mut reservations = Vec::new();
    for handle in handles {
        if let Some(reservation) = handle.await.expect("reservation racer should complete") {
            reservations.push(reservation);
        }
    }

    assert_eq!(reservations.len(), max_len);
    assert_eq!(queue.len(), max_len);

    drop(reservations);
    assert_eq!(queue.len(), 0);
}

#[tokio::test]
async fn moderate_contention_drains_all_tasks() {
    let queue = build_queue_with_config(1, 10, 1);
    let producers = 2;
    let tasks_per_producer = 500;
    let consumers = 4;
    let barrier = Arc::new(Barrier::new(producers + consumers));

    for producer_idx in 0..producers {
        let q = queue.clone();
        let barrier = Arc::clone(&barrier);
        task::spawn(async move {
            barrier.wait().await;
            for i in 0..tasks_per_producer {
                q.push(create_task(&format!("p{producer_idx}-{i}"), None));
            }
        });
    }

    let counter = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::with_capacity(consumers);
    for consumer_idx in 0..consumers {
        let q = queue.clone();
        let barrier = Arc::clone(&barrier);
        let counter = Arc::clone(&counter);
        handles.push(task::spawn(async move {
            let hotkey = Hotkey::from_bytes(&[30u8 + consumer_idx as u8; 32]);
            barrier.wait().await;
            let mut idle_start: Option<Instant> = None;
            loop {
                if counter.load(Ordering::Relaxed) == producers * tasks_per_producer {
                    break;
                }

                let items = q.pop(7, &hotkey);
                if !items.is_empty() {
                    counter.fetch_add(items.len(), Ordering::Relaxed);
                    idle_start = None;
                } else {
                    idle_start.get_or_insert_with(Instant::now);
                    if idle_start.expect("idle start should be set").elapsed()
                        >= Duration::from_secs(5)
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        }));
    }

    for handle in handles {
        handle.await.expect("contention consumer should complete");
    }

    assert_eq!(queue.len(), 0);
    assert_eq!(
        counter.load(Ordering::Relaxed),
        producers * tasks_per_producer
    );
}

#[tokio::test]
async fn overlapping_uuid_reuse_keeps_same_hotkey_deduped() {
    let queue = build_queue_with_config(1, 300, 1);
    let fixed_id = Uuid::new_v4();
    let mut first = create_task("first", None);
    first.id = fixed_id;
    let mut second = create_task("second", None);
    second.id = fixed_id;

    queue.push(first.clone());

    let hk_a = Hotkey::from_bytes(&[50u8; 32]);
    let hk_b = Hotkey::from_bytes(&[51u8; 32]);
    let mut deliveries = queue.reserve(1, &hk_a);
    let delivery = deliveries.pop().expect("expected reserved first copy");

    let generation = delivery
        .entry
        .as_ref()
        .map(|entry| entry.generation)
        .expect("reserved delivery should hold the generation");

    assert_eq!(
        queue
            .inner
            .active_ids
            .read_sync(&fixed_id, |_, state| (state.count, state.generation)),
        Some((1, generation))
    );

    queue.push(second.clone());

    assert_eq!(
        queue
            .inner
            .active_ids
            .read_sync(&fixed_id, |_, state| (state.count, state.generation)),
        Some((2, generation)),
        "overlapping UUID reuse should share the active generation"
    );

    assert!(
        queue.pop(1, &hk_a).is_empty(),
        "same hotkey should stay deduped while another copy is still active"
    );

    let other = queue.pop(1, &hk_b);
    assert_eq!(other.len(), 1);
    assert_eq!(other[0].0.id, fixed_id);
    assert_eq!(
        other[0].0.prompt.as_deref().map(String::as_str),
        Some("second")
    );

    let (committed, duration) = delivery.commit();
    assert_eq!(committed.id, fixed_id);
    assert_eq!(
        committed.prompt.as_deref().map(String::as_str),
        Some("first")
    );
    assert!(duration.is_some());

    assert!(
        queue
            .inner
            .active_ids
            .read_sync(&fixed_id, |_, _| ())
            .is_none(),
        "finishing the final overlapping copy should drop active bookkeeping"
    );
}
