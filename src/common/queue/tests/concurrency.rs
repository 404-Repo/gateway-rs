use super::*;

#[tokio::test]
async fn bucket_activity_without_generation_change_does_not_mark_bucket_exhausted() {
    let bucket = Arc::new(ModelBucket::default());
    let entry = raw_entry(create_task("robot", Some("404-3dgs")), 2, 0, 7);
    bucket.enqueue_new(entry);

    let hotkey = Hotkey::from_bytes(&[26u8; 32]);
    let generation = bucket.generation.load(Ordering::Acquire);
    let start_activity = bucket.activity.load(Ordering::Acquire);

    bucket.activity.fetch_add(1, Ordering::AcqRel);
    let key = exhausted_key(&hotkey, &[]);
    bucket.mark_exhausted_if_stable(&key, generation, start_activity, 0);
    assert!(
        bucket.exhausted_generation(&key).is_none(),
        "bucket activity must block exhausted-cache writes even when generation is unchanged"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_push_pop_returns_unique_tasks_per_hotkey() {
    const HOTKEYS: usize = 3;
    const TASKS: usize = 64;

    let queue = Arc::new(build_queue_with_config(HOTKEYS, 300, 1));
    let start = Arc::new(ThreadBarrier::new(HOTKEYS + 1));
    let delivered_count = Arc::new(AtomicUsize::new(0));
    let delivered_by_task = Arc::new(Mutex::new(HashMap::<Uuid, HashSet<usize>>::new()));

    let producer = {
        let queue = Arc::clone(&queue);
        let start = Arc::clone(&start);
        thread::spawn(move || {
            start.wait();
            for i in 0..TASKS {
                queue.push(create_task(&format!("Task {}", i), None));
            }
        })
    };

    let mut handles = Vec::new();
    for worker_idx in 0..HOTKEYS {
        let queue = Arc::clone(&queue);
        let start = Arc::clone(&start);
        let delivered_count = Arc::clone(&delivered_count);
        let delivered_by_task = Arc::clone(&delivered_by_task);
        handles.push(thread::spawn(move || {
            let hotkey = Hotkey::from_bytes(&[20u8 + worker_idx as u8; 32]);
            let deadline = Instant::now() + Duration::from_secs(10);
            start.wait();

            while delivered_count.load(Ordering::Acquire) < TASKS * HOTKEYS {
                let res = queue.pop(5, &hotkey);
                if res.is_empty() {
                    assert!(
                        Instant::now() < deadline,
                        "concurrent push/pop test stalled with {} deliveries",
                        delivered_count.load(Ordering::Relaxed)
                    );
                    thread::yield_now();
                    continue;
                }

                let set: HashSet<_> = res.iter().map(|(task, _)| task.id).collect();
                assert_eq!(set.len(), res.len());
                for (task, _) in res {
                    {
                        let mut delivered_by_task =
                            delivered_by_task.lock().expect("delivered map lock");
                        let workers = delivered_by_task.entry(task.id).or_default();
                        assert!(
                            workers.insert(worker_idx),
                            "worker received the same task twice"
                        );
                        assert!(
                            workers.len() <= HOTKEYS,
                            "task was delivered more times than expected"
                        );
                    }
                    delivered_count.fetch_add(1, Ordering::Release);
                }
            }
        }));
    }

    producer.join().expect("producer thread should not panic");
    for handle in handles {
        handle.join().expect("consumer thread should not panic");
    }

    assert_eq!(queue.len(), 0);
    assert_eq!(delivered_count.load(Ordering::Relaxed), TASKS * HOTKEYS);
    let delivered_by_task = delivered_by_task.lock().expect("delivered map lock");
    assert_eq!(delivered_by_task.len(), TASKS);
    for workers in delivered_by_task.values() {
        assert_eq!(workers.len(), HOTKEYS);
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_threaded_scan_pick_stress_preserves_correctness() {
    const PRODUCERS: usize = 2;
    const CONSUMERS: usize = 8;
    const DUP: usize = 2;
    const RUN_FOR: Duration = Duration::from_secs(3);

    let queue = Arc::new(build_queue_with_config(DUP, 60, 1));
    let state = Arc::new(Mutex::new(MtDeliveryState::default()));
    let stop = Arc::new(AtomicBool::new(false));
    let produced_count = Arc::new(AtomicUsize::new(0));
    let committed_count = Arc::new(AtomicUsize::new(0));
    let start = Arc::new(ThreadBarrier::new(PRODUCERS + CONSUMERS + 1));
    let mut handles = Vec::with_capacity(PRODUCERS + CONSUMERS);

    for producer_idx in 0..PRODUCERS {
        let queue = Arc::clone(&queue);
        let state = Arc::clone(&state);
        let stop = Arc::clone(&stop);
        let produced_count = Arc::clone(&produced_count);
        let start = Arc::clone(&start);
        handles.push(thread::spawn(move || {
            start.wait();
            let mut local_sequence = 0usize;

            while !stop.load(Ordering::Acquire) {
                let sequence = producer_idx * 1_000_000 + local_sequence;
                let required_tag = mt_required_tag(sequence);
                let task = create_task(
                    &format!(
                        "mt-producer-{producer_idx}-task-{local_sequence}-tag-{}",
                        required_tag.unwrap_or("public")
                    ),
                    Some("404-3dgs"),
                );

                {
                    let mut state = state.lock().expect("mt delivery state lock");
                    state.produced.insert(task.id, required_tag);
                }

                match required_tag {
                    Some(tag) => push_task_with_required_tags(&queue, task, &[tag]),
                    None => queue.push(task),
                }
                produced_count.fetch_add(1, Ordering::Relaxed);
                local_sequence += 1;
                thread::sleep(Duration::from_millis(1));
            }
        }));
    }

    for worker_idx in 0..CONSUMERS {
        let queue = Arc::clone(&queue);
        let state = Arc::clone(&state);
        let stop = Arc::clone(&stop);
        let committed_count = Arc::clone(&committed_count);
        let start = Arc::clone(&start);
        handles.push(thread::spawn(move || {
            let hotkey = Hotkey::from_bytes(&[worker_idx as u8 + 80; 32]);
            let worker_tags = mt_worker_tags(worker_idx);
            start.wait();

            while !stop.load(Ordering::Acquire) {
                let deliveries = queue.reserve_for_models_with_routing(
                    8,
                    &hotkey,
                    &["404-3dgs"],
                    WorkerRouting::from_tags(worker_tags.as_slice()),
                );
                if deliveries.is_empty() {
                    thread::yield_now();
                    continue;
                }

                for delivery in deliveries {
                    let (task, _) = delivery.commit();
                    record_mt_delivery(&state, worker_idx, &task, DUP);
                    committed_count.fetch_add(1, Ordering::Relaxed);
                }
            }
        }));
    }

    start.wait();
    thread::sleep(RUN_FOR);
    stop.store(true, Ordering::Release);

    for handle in handles {
        handle.join().expect("mt stress thread should not panic");
    }

    let drain_deadline = Instant::now() + Duration::from_secs(10);
    while !queue.is_empty() {
        let mut progressed = false;
        for worker_idx in 0..CONSUMERS {
            let hotkey = Hotkey::from_bytes(&[worker_idx as u8 + 80; 32]);
            let worker_tags = mt_worker_tags(worker_idx);
            let deliveries = queue.reserve_for_models_with_routing(
                16,
                &hotkey,
                &["404-3dgs"],
                WorkerRouting::from_tags(worker_tags.as_slice()),
            );

            if deliveries.is_empty() {
                continue;
            }
            progressed = true;

            for delivery in deliveries {
                let (task, _) = delivery.commit();
                record_mt_delivery(&state, worker_idx, &task, DUP);
                committed_count.fetch_add(1, Ordering::Relaxed);
            }
        }

        if !progressed {
            assert!(
                Instant::now() < drain_deadline,
                "mt stress drain stalled with {} queued tasks",
                queue.len()
            );
            thread::yield_now();
        }
    }

    let produced_count = produced_count.load(Ordering::Relaxed);
    assert!(
        produced_count >= 200,
        "mt stress should produce enough tasks to exercise concurrent queue paths"
    );

    let state = state.lock().expect("mt delivery state lock");
    assert_eq!(state.produced.len(), produced_count);
    assert_eq!(
        committed_count.load(Ordering::Relaxed),
        produced_count * DUP
    );

    for task_id in state.produced.keys() {
        assert_eq!(
            state.delivered_counts.get(task_id).copied(),
            Some(DUP),
            "produced task was not committed exactly dup times"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_threaded_commit_only_scan_pick_preserves_per_worker_ordering() {
    const CONSUMERS: usize = 16;
    const DUP: usize = 2;
    const RUN_FOR: Duration = Duration::from_secs(3);

    let queue = Arc::new(build_queue_with_config(DUP, 60, 60));
    let state = Arc::new(Mutex::new(MtOrderingState {
        produced_sequences: HashMap::new(),
        delivered_counts: HashMap::new(),
        last_sequence_by_worker: vec![None; CONSUMERS],
    }));
    let stop = Arc::new(AtomicBool::new(false));
    let produced_count = Arc::new(AtomicUsize::new(0));
    let committed_count = Arc::new(AtomicUsize::new(0));
    let start = Arc::new(ThreadBarrier::new(CONSUMERS + 2));
    let mut handles = Vec::with_capacity(CONSUMERS + 1);

    {
        let queue = Arc::clone(&queue);
        let state = Arc::clone(&state);
        let stop = Arc::clone(&stop);
        let produced_count = Arc::clone(&produced_count);
        let start = Arc::clone(&start);
        handles.push(thread::spawn(move || {
            start.wait();
            let mut sequence = 0usize;

            while !stop.load(Ordering::Acquire) {
                let required_tag = mt_required_tag(sequence);
                let task = create_task(
                    &format!(
                        "mt-order-task-{sequence}-tag-{}",
                        required_tag.unwrap_or("public")
                    ),
                    Some("404-3dgs"),
                );

                {
                    let mut state = state.lock().expect("mt ordering state lock");
                    state.produced_sequences.insert(task.id, sequence);
                }

                match required_tag {
                    Some(tag) => push_task_with_required_tags(&queue, task, &[tag]),
                    None => queue.push(task),
                }
                produced_count.fetch_add(1, Ordering::Relaxed);
                sequence += 1;
                thread::sleep(Duration::from_millis(1));
            }
        }));
    }

    for worker_idx in 0..CONSUMERS {
        let queue = Arc::clone(&queue);
        let state = Arc::clone(&state);
        let stop = Arc::clone(&stop);
        let committed_count = Arc::clone(&committed_count);
        let start = Arc::clone(&start);
        handles.push(thread::spawn(move || {
            let hotkey = Hotkey::from_bytes(&[worker_idx as u8 + 100; 32]);
            let worker_tags = mt_worker_tags(worker_idx);
            start.wait();

            while !stop.load(Ordering::Acquire) {
                let deliveries = queue.reserve_for_models_with_routing(
                    8,
                    &hotkey,
                    &["404-3dgs"],
                    WorkerRouting::from_tags(worker_tags.as_slice()),
                );
                if deliveries.is_empty() {
                    thread::yield_now();
                    continue;
                }

                for delivery in deliveries {
                    let (task, _) = delivery.commit();
                    record_mt_ordered_delivery(&state, worker_idx, &task, DUP);
                    committed_count.fetch_add(1, Ordering::Relaxed);
                }
            }
        }));
    }

    start.wait();
    thread::sleep(RUN_FOR);
    stop.store(true, Ordering::Release);

    for handle in handles {
        handle
            .join()
            .expect("mt ordering stress thread should not panic");
    }

    let drain_deadline = Instant::now() + Duration::from_secs(30);
    while !queue.is_empty() {
        let mut progressed = false;
        for worker_idx in 0..CONSUMERS {
            let hotkey = Hotkey::from_bytes(&[worker_idx as u8 + 100; 32]);
            let worker_tags = mt_worker_tags(worker_idx);
            let deliveries = queue.reserve_for_models_with_routing(
                16,
                &hotkey,
                &["404-3dgs"],
                WorkerRouting::from_tags(worker_tags.as_slice()),
            );

            if deliveries.is_empty() {
                continue;
            }
            progressed = true;

            for delivery in deliveries {
                let (task, _) = delivery.commit();
                record_mt_ordered_delivery(&state, worker_idx, &task, DUP);
                committed_count.fetch_add(1, Ordering::Relaxed);
            }
        }

        if !progressed {
            assert!(
                Instant::now() < drain_deadline,
                "mt ordering stress drain stalled with {} queued tasks",
                queue.len()
            );
            thread::yield_now();
        }
    }

    let produced_count = produced_count.load(Ordering::Relaxed);
    assert!(
        produced_count >= 1_000,
        "mt ordering stress should produce enough ordered tasks to prove no reshuffling"
    );

    let state = state.lock().expect("mt ordering state lock");
    assert_eq!(state.produced_sequences.len(), produced_count);
    assert_eq!(
        committed_count.load(Ordering::Relaxed),
        produced_count * DUP
    );

    for task_id in state.produced_sequences.keys() {
        assert_eq!(
            state.delivered_counts.get(task_id).copied(),
            Some(DUP),
            "ordered stress task was not committed exactly dup times"
        );
    }
}

#[derive(Copy, Clone)]
enum MixedDeliveryAction {
    Commit,
    Rollback,
    Requeue,
}

#[derive(Default)]
struct MixedMtState {
    produced: HashMap<Uuid, Option<&'static str>>,
    attempts: HashMap<(Uuid, usize), usize>,
    committed_pairs: HashSet<(Uuid, usize)>,
    committed_counts: HashMap<Uuid, usize>,
}

fn next_mixed_delivery_action(
    state: &Mutex<MixedMtState>,
    worker_idx: usize,
    task: &Task,
) -> MixedDeliveryAction {
    let worker_tag = mt_worker_tag(worker_idx);
    let mut state = state.lock().expect("mixed mt state lock");
    let required_tag = *state
        .produced
        .get(&task.id)
        .expect("delivered task must have been produced by the mixed stress test");

    assert_eq!(
        worker_tag, required_tag,
        "worker received a task outside its requested tag routing"
    );

    let attempts = state.attempts.entry((task.id, worker_idx)).or_default();
    *attempts += 1;

    if *attempts == 1 && worker_idx == 0 {
        MixedDeliveryAction::Requeue
    } else if *attempts == 1 && worker_idx == 1 {
        MixedDeliveryAction::Rollback
    } else {
        MixedDeliveryAction::Commit
    }
}

fn record_mixed_commit(state: &Mutex<MixedMtState>, worker_idx: usize, task: &Task, dup: usize) {
    let worker_tag = mt_worker_tag(worker_idx);
    let mut state = state.lock().expect("mixed mt state lock");
    let required_tag = *state
        .produced
        .get(&task.id)
        .expect("committed task must have been produced by the mixed stress test");

    assert_eq!(
        worker_tag, required_tag,
        "worker committed a task outside its requested tag routing"
    );

    assert!(
        state.committed_pairs.insert((task.id, worker_idx)),
        "task was committed more than once by the same worker"
    );

    let committed_count = state.committed_counts.entry(task.id).or_default();
    *committed_count += 1;
    assert!(
        *committed_count <= dup,
        "task was committed more times than the configured dup count"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_threaded_mixed_rollback_requeue_stress_commits_every_slot_exactly_once() {
    const CONSUMERS: usize = 8;
    const DUP: usize = 1;
    const TASKS: usize = 6_000;

    let queue = Arc::new(build_queue_with_config(DUP, 60, 60));
    let state = Arc::new(Mutex::new(MixedMtState::default()));
    let committed_count = Arc::new(AtomicUsize::new(0));
    let rollback_count = Arc::new(AtomicUsize::new(0));
    let requeue_count = Arc::new(AtomicUsize::new(0));
    let start = Arc::new(ThreadBarrier::new(CONSUMERS + 1));
    let mut handles = Vec::with_capacity(CONSUMERS);

    for sequence in 0..TASKS {
        let required_tag = mt_required_tag(sequence);
        let task = create_task(
            &format!(
                "mt-mixed-preloaded-task-{sequence}-tag-{}",
                required_tag.unwrap_or("public")
            ),
            Some("404-3dgs"),
        );

        {
            let mut state = state.lock().expect("mixed mt state lock");
            state.produced.insert(task.id, required_tag);
        }

        match required_tag {
            Some(tag) => push_task_with_required_tags(&queue, task, &[tag]),
            None => queue.push(task),
        }
    }

    for worker_idx in 0..CONSUMERS {
        let queue = Arc::clone(&queue);
        let state = Arc::clone(&state);
        let committed_count = Arc::clone(&committed_count);
        let rollback_count = Arc::clone(&rollback_count);
        let requeue_count = Arc::clone(&requeue_count);
        let start = Arc::clone(&start);
        handles.push(thread::spawn(move || {
            let hotkey = Hotkey::from_bytes(&[worker_idx as u8 + 120; 32]);
            let worker_tags = mt_worker_tags(worker_idx);
            start.wait();
            let deadline = Instant::now() + Duration::from_secs(30);

            while committed_count.load(Ordering::Acquire) < TASKS * DUP && Instant::now() < deadline
            {
                let deliveries = queue.reserve_for_models_with_routing(
                    8,
                    &hotkey,
                    &["404-3dgs"],
                    WorkerRouting::from_tags(worker_tags.as_slice()),
                );
                if deliveries.is_empty() {
                    if queue.is_empty() {
                        break;
                    }
                    thread::yield_now();
                    continue;
                }

                for delivery in deliveries {
                    match next_mixed_delivery_action(&state, worker_idx, delivery.task()) {
                        MixedDeliveryAction::Commit => {
                            let (task, _) = delivery.commit();
                            record_mixed_commit(&state, worker_idx, &task, DUP);
                            committed_count.fetch_add(1, Ordering::Relaxed);
                        }
                        MixedDeliveryAction::Rollback => {
                            rollback_count.fetch_add(1, Ordering::Relaxed);
                            delivery.rollback();
                        }
                        MixedDeliveryAction::Requeue => {
                            requeue_count.fetch_add(1, Ordering::Relaxed);
                            delivery.requeue_for_other_hotkeys();
                        }
                    }
                }
            }
        }));
    }

    start.wait();

    for handle in handles {
        handle
            .join()
            .expect("mixed mt stress thread should not panic");
    }

    assert_eq!(
        TASKS,
        state.lock().expect("mixed mt state lock").produced.len()
    );
    assert!(
        rollback_count.load(Ordering::Relaxed) > 0,
        "mixed mt stress should exercise rollback"
    );
    assert!(
        requeue_count.load(Ordering::Relaxed) > 0,
        "mixed mt stress should exercise requeue_for_other_hotkeys"
    );

    let state = state.lock().expect("mixed mt state lock");
    assert_eq!(state.produced.len(), TASKS);
    assert!(
        queue
            .inner
            .bucket_for_model("404-3dgs")
            .snapshot_entries(usize::MAX)
            .is_empty(),
        "mixed mt stress should leave no bucket entries after draining"
    );
    let recorded_commits = state.committed_counts.values().copied().sum::<usize>();
    assert_eq!(
        committed_count.load(Ordering::Relaxed),
        recorded_commits,
        "atomic commit counter should match recorded per-task commits"
    );
    assert_eq!(
        recorded_commits,
        TASKS * DUP,
        "every enqueued slot must be committed exactly once: a shortfall means a \
         requeued/rolled-back slot was stranded or dropped without delivery"
    );
    assert!(
        recorded_commits <= TASKS * DUP,
        "mixed stress must not commit more slots than were enqueued"
    );
    assert_eq!(
        state.committed_pairs.len(),
        recorded_commits,
        "mixed stress must not commit the same task more than once per worker"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_polls_from_same_hotkey_do_not_duplicate_task() {
    let queue = Arc::new(build_queue_with_config(1, 300, 1));
    let task = create_task("single", None);
    queue.push(task.clone());

    let hotkey = Hotkey::from_bytes(&[23u8; 32]);
    let barrier = Arc::new(ThreadBarrier::new(5));
    let mut handles = Vec::new();
    for _ in 0..5 {
        let q = Arc::clone(&queue);
        let barrier = Arc::clone(&barrier);
        let hotkey = hotkey.clone();
        handles.push(thread::spawn(move || {
            barrier.wait();
            q.pop(1, &hotkey)
        }));
    }

    let mut delivered = Vec::new();
    for handle in handles {
        delivered.extend(handle.join().expect("same-hotkey consumer should complete"));
    }

    assert_eq!(delivered.len(), 1);
    assert_eq!(delivered[0].0, task);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_polls_racing_final_slot_leave_no_loser_sent_state() {
    let queue = Arc::new(build_queue_with_config(1, 300, 1));
    let task = create_task("final-slot", Some("404-3dgs"));
    queue.push(task.clone());

    let racers = 64;
    let barrier = Arc::new(ThreadBarrier::new(racers));
    let mut handles = Vec::with_capacity(racers);
    for racer_idx in 0..racers {
        let q = Arc::clone(&queue);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            let hotkey = Hotkey::from_bytes(&[90u8 + racer_idx as u8; 32]);
            barrier.wait();
            q.reserve_for_models(1, &hotkey, &["404-3dgs"])
        }));
    }

    let mut winners = Vec::new();
    for handle in handles {
        winners.extend(handle.join().expect("final-slot racer should complete"));
    }

    assert_eq!(
        winners.len(),
        1,
        "exactly one worker should acquire the final available slot"
    );
    assert_eq!(winners[0].task(), &task);
    let generation = winners[0]
        .entry
        .as_ref()
        .map(|entry| entry.generation)
        .expect("winner should hold the queue entry generation");
    let key = (task.id, generation);
    assert_eq!(
        queue.inner.sent.read_sync(&key, |_, hotkeys| hotkeys.len()),
        Some(1),
        "losing racers must not leave stale sent-hotkey state"
    );

    let (committed, _) = winners.pop().expect("winner").commit();
    assert_eq!(committed, task);
    assert_eq!(queue.len(), 0);
    assert!(
        queue.inner.sent.read_sync(&key, |_, _| ()).is_none(),
        "committing the final slot should clear sent bookkeeping"
    );
    assert!(
        queue
            .inner
            .active_ids
            .read_sync(&task.id, |_, _| ())
            .is_none(),
        "committing the final slot should clear active-id bookkeeping"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn moderate_contention_drains_all_tasks() {
    let queue = Arc::new(build_queue_with_config(1, 10, 1));
    let producers = 2;
    let tasks_per_producer = 500;
    let consumers = 4;
    let total_tasks = producers * tasks_per_producer;
    let barrier = Arc::new(ThreadBarrier::new(producers + consumers));

    let mut producer_handles = Vec::with_capacity(producers);
    for producer_idx in 0..producers {
        let q = Arc::clone(&queue);
        let barrier = Arc::clone(&barrier);
        producer_handles.push(thread::spawn(move || {
            barrier.wait();
            for i in 0..tasks_per_producer {
                q.push(create_task(&format!("p{producer_idx}-{i}"), None));
            }
        }));
    }

    let counter = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::with_capacity(consumers);
    for consumer_idx in 0..consumers {
        let q = Arc::clone(&queue);
        let barrier = Arc::clone(&barrier);
        let counter = Arc::clone(&counter);
        handles.push(thread::spawn(move || {
            let hotkey = Hotkey::from_bytes(&[30u8 + consumer_idx as u8; 32]);
            let deadline = Instant::now() + Duration::from_secs(10);
            barrier.wait();

            while counter.load(Ordering::Acquire) < total_tasks {
                let items = q.pop(7, &hotkey);
                if !items.is_empty() {
                    counter.fetch_add(items.len(), Ordering::Release);
                    continue;
                }

                assert!(
                    Instant::now() < deadline,
                    "contention consumer stalled with {} delivered tasks",
                    counter.load(Ordering::Relaxed)
                );
                thread::yield_now();
            }
        }));
    }

    for handle in producer_handles {
        handle.join().expect("contention producer should complete");
    }
    for handle in handles {
        handle.join().expect("contention consumer should complete");
    }

    assert_eq!(queue.len(), 0);
    assert_eq!(counter.load(Ordering::Relaxed), total_tasks);
}
