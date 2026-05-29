use super::*;

#[tokio::test]
async fn concurrent_bucket_activity_does_not_mark_bucket_exhausted() {
    let bucket = Arc::new(ModelBucket::default());
    let entry = raw_entry(create_task("robot", Some("404-3dgs")), 2, 0, 7);
    bucket.enqueue_new(entry);

    let hotkey = Hotkey::from_bytes(&[26u8; 32]);
    let generation = bucket.generation.load(Ordering::Acquire);
    let start_activity = bucket.activity.load(Ordering::Acquire);

    bucket.enqueue_new(raw_entry(
        create_task("new robot", Some("404-3dgs")),
        1,
        0,
        8,
    ));
    let key = exhausted_key(&hotkey, &[]);
    bucket.mark_exhausted_if_stable(&key, generation, start_activity, 0);
    assert!(
        bucket.exhausted_generation(&key).is_none(),
        "concurrent bucket activity must block exhausted-cache writes"
    );
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
            res
        }));
    }

    for handle in handles {
        handle.await.expect("consumer task should complete");
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

    let queue = Arc::new(build_queue_with_config(DUP, 60, 1));
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
