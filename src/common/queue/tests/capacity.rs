use super::*;

#[test]
#[should_panic(expected = "task queue requires a non-empty default_model")]
fn build_requires_non_empty_default_model() {
    let _ = TaskQueue::builder().default_model(" ").build();
}

#[test]
#[should_panic(expected = "task queue dup exceeds supported internal slot count")]
fn build_rejects_unsupported_dup_count() {
    let _ = TaskQueue::builder()
        .dup(usize::MAX)
        .default_model("404-3dgs")
        .build();
}

#[tokio::test]
async fn queue_len_gauge_tracks_atomic_len_changes() {
    let (queue, gauge) = build_queue_with_len_gauge();
    assert_eq!(gauge.get(), 0);

    let uncommitted_slot = queue.try_reserve(usize::MAX).expect("reserve queue slot");
    assert_eq!(queue.len(), 1);
    assert_eq!(gauge.get(), 1);
    drop(uncommitted_slot);
    assert_eq!(queue.len(), 0);
    assert_eq!(gauge.get(), 0);

    let task = create_task("observed", Some("404-3dgs"));
    queue.push(task.clone());
    assert_eq!(queue.len(), 1);
    assert_eq!(gauge.get(), 1);

    let deliveries = queue.reserve_for_models(1, &Hotkey::from_bytes(&[26u8; 32]), &["404-3dgs"]);
    assert_eq!(deliveries.len(), 1);
    let (delivered, _) = deliveries.into_iter().next().expect("delivery").commit();
    assert_eq!(delivered, task);
    assert_eq!(queue.len(), 0);
    assert_eq!(gauge.get(), 0);
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_try_reserve_respects_capacity() {
    let queue = Arc::new(build_queue_with_config(1, 300, 1));
    let max_len = 4;
    let contenders = 16;
    let barrier = Arc::new(ThreadBarrier::new(contenders));
    let mut handles = Vec::with_capacity(contenders);

    for _ in 0..contenders {
        let q = Arc::clone(&queue);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            barrier.wait();
            q.try_reserve(max_len)
        }));
    }

    let mut reservations = Vec::new();
    for handle in handles {
        if let Some(reservation) = handle.join().expect("reservation racer should complete") {
            reservations.push(reservation);
        }
    }

    assert_eq!(reservations.len(), max_len);
    assert_eq!(queue.len(), max_len);

    drop(reservations);
    assert_eq!(queue.len(), 0);
}
