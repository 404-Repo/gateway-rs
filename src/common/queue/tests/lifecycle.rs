use super::*;

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
async fn final_dup_slot_with_outstanding_lease_reports_duration_only_on_commit() {
    let queue = build_queue_with_config(2, 300, 1);
    let task = create_task("dup-final-duration", None);
    queue.push(task.clone());

    let hk_a = Hotkey::from_bytes(&[72u8; 32]);
    let hk_b = Hotkey::from_bytes(&[73u8; 32]);
    let first = queue
        .reserve(1, &hk_a)
        .pop()
        .expect("expected first duplicate lease");
    let second = queue
        .reserve(1, &hk_b)
        .pop()
        .expect("expected second duplicate lease");

    assert!(
        first.duration().is_none(),
        "first dup lease is not the final candidate"
    );
    assert!(
        second.duration().is_none(),
        "last slot is not final while another lease is still outstanding"
    );

    let (first_task, first_duration) = first.commit();
    assert_eq!(first_task, task);
    assert!(
        first_duration.is_none(),
        "non-retiring commit should not report queue duration"
    );

    let (second_task, second_duration) = second.commit();
    assert_eq!(second_task, task);
    assert!(
        second_duration.is_some(),
        "final retiring commit should report queue duration"
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
async fn retire_single_delivery_removes_task_and_bookkeeping() {
    let queue = build_queue_with_config(1, 300, 60);
    let task = create_task("retire-single", Some("404-3dgs"));
    queue.push(task.clone());

    let hotkey = Hotkey::from_bytes(&[75u8; 32]);
    let mut deliveries = queue.reserve_for_models(1, &hotkey, &["404-3dgs"]);
    let delivery = deliveries.pop().expect("expected delivery to retire");
    let generation = delivery
        .entry
        .as_ref()
        .map(|entry| entry.generation)
        .expect("delivery should hold generation");

    delivery.retire();

    assert_eq!(queue.len(), 0);
    assert!(queue.pop_for_models(1, &hotkey, &["404-3dgs"]).is_empty());
    assert!(
        queue
            .inner
            .active_ids
            .read_sync(&task.id, |_, _| ())
            .is_none(),
        "retired task should clear active bookkeeping"
    );
    assert!(
        queue
            .inner
            .sent
            .read_sync(&(task.id, generation), |_, _| ())
            .is_none(),
        "retired task should clear sent bookkeeping"
    );
}

#[tokio::test]
async fn retire_with_sibling_lease_cannot_be_resurrected_by_rollback() {
    let queue = build_queue_with_config(2, 300, 60);
    let task = create_task("retire-dup", Some("404-3dgs"));
    queue.push(task.clone());

    let hk_a = Hotkey::from_bytes(&[76u8; 32]);
    let hk_b = Hotkey::from_bytes(&[77u8; 32]);
    let first = queue
        .reserve_for_models(1, &hk_a, &["404-3dgs"])
        .pop()
        .expect("expected first lease");
    let second = queue
        .reserve_for_models(1, &hk_b, &["404-3dgs"])
        .pop()
        .expect("expected sibling lease");

    first.retire();
    assert_eq!(
        queue.len(),
        1,
        "outstanding sibling lease should keep bookkeeping until it closes"
    );

    second.rollback();

    assert_eq!(queue.len(), 0);
    let hk_c = Hotkey::from_bytes(&[78u8; 32]);
    assert!(
        queue.pop_for_models(1, &hk_c, &["404-3dgs"]).is_empty(),
        "rollback of a sibling lease must not resurrect a retired task"
    );
    assert!(
        queue
            .inner
            .active_ids
            .read_sync(&task.id, |_, _| ())
            .is_none()
    );
}

#[tokio::test]
async fn rollback_rewinds_cursor_for_same_routing_key() {
    let queue = build_queue_with_config(1, 300, 60);
    let first = create_task("first", Some("404-3dgs"));
    let second = create_task("second", Some("404-3dgs"));
    queue.push(first.clone());
    queue.push(second);

    let hotkey = Hotkey::from_bytes(&[74u8; 32]);
    let delivery = queue
        .reserve_for_models(1, &hotkey, &["404-3dgs"])
        .pop()
        .expect("expected first delivery");
    assert_eq!(delivery.task(), &first);

    delivery.rollback();

    let retry = queue.reserve_for_models(1, &hotkey, &["404-3dgs"]);
    assert_eq!(retry.len(), 1);
    assert_eq!(
        retry[0].task(),
        &first,
        "rollback should make the restored lower sequence visible before later work"
    );
}

#[tokio::test]
async fn rollback_from_other_hotkey_can_reoffer_earlier_sequence_after_later_delivery() {
    let queue = build_queue_with_config(1, 300, 60);
    let first = create_task("first", Some("404-3dgs"));
    let second = create_task("second", Some("404-3dgs"));
    queue.push(first.clone());
    queue.push(second.clone());

    let holder_hotkey = Hotkey::from_bytes(&[79u8; 32]);
    let worker_hotkey = Hotkey::from_bytes(&[80u8; 32]);
    let held_first = queue
        .reserve_for_models(1, &holder_hotkey, &["404-3dgs"])
        .pop()
        .expect("expected first task to be held by another hotkey");

    let later = queue.reserve_for_models(1, &worker_hotkey, &["404-3dgs"]);
    assert_eq!(later.len(), 1);
    assert_eq!(later[0].task(), &second);
    later.into_iter().next().expect("later delivery").commit();

    held_first.rollback();

    let reoffered = queue.reserve_for_models(1, &worker_hotkey, &["404-3dgs"]);
    assert_eq!(reoffered.len(), 1);
    assert_eq!(
        reoffered[0].task(),
        &first,
        "rollback/requeue paths are best-effort ordered, not strictly ordered"
    );
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
async fn zero_remaining_entry_is_reaped_if_encountered() {
    let queue = build_queue_with_config(1, 300, 1);
    let task = create_task("stale", Some("404-3dgs"));
    let generation = queue.inner.acquire_task_generation(task.id);
    let entry = raw_entry(task.clone(), 0, 0, generation);
    queue.inner.increment_len();
    let bucket = queue.inner.bucket_for_model("404-3dgs");
    bucket.enqueue_new(entry);

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
async fn retire_if_idle_marks_leased_entry_retired_until_lease_closes() {
    let task = create_task("leased", Some("404-3dgs"));
    let entry = raw_entry(task, 1, 0, 1);

    assert!(
        entry.try_acquire_lease().is_some(),
        "test setup should acquire the only available lease"
    );
    assert!(
        !entry.retire_if_idle(),
        "entry with an outstanding lease cannot be removed immediately"
    );
    assert!(
        QueueEntry::is_retired(entry.load_state()),
        "retired marker should prevent sibling rollback from restoring capacity"
    );
    assert!(
        entry.finish_rollback(false).retire_now,
        "closing the last lease should remove a previously retired entry"
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
    let key = exhausted_key(&hk, &[]);
    assert_eq!(
        bucket.exhausted_generation(&key),
        Some(generation),
        "empty scan should cache the hotkey as exhausted for the current generation"
    );

    delivery.rollback();

    assert!(
        bucket.exhausted_generation(&key).is_none(),
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
async fn requeue_with_remaining_dup_capacity_keeps_already_exhausted_hotkey_blocked() {
    let queue = build_queue_with_config(3, 300, 60);
    let task = create_task("dup-requeue-exhausted", Some("404-3dgs"));
    queue.push(task.clone());

    let exhausted_hotkey = Hotkey::from_bytes(&[81u8; 32]);
    let mut exhausted_deliveries = queue.reserve_for_models(1, &exhausted_hotkey, &["404-3dgs"]);
    let exhausted_delivery = exhausted_deliveries
        .pop()
        .expect("expected first lease for hotkey that will become exhausted");

    assert!(
        queue
            .reserve_for_models(1, &exhausted_hotkey, &["404-3dgs"])
            .is_empty(),
        "same hotkey should exhaust after scanning only already-sent work"
    );

    let bucket = queue.inner.bucket_for_model("404-3dgs");
    let exhausted_key = exhausted_key(&exhausted_hotkey, &[]);
    let generation_before_requeue = bucket.generation.load(Ordering::Acquire);
    assert_eq!(
        bucket.exhausted_generation(&exhausted_key),
        Some(generation_before_requeue),
        "test setup should cache the already-sent hotkey as exhausted"
    );

    let requeue_hotkey = Hotkey::from_bytes(&[82u8; 32]);
    let requeued_delivery = queue
        .reserve_for_models(1, &requeue_hotkey, &["404-3dgs"])
        .pop()
        .expect("expected sibling lease to requeue");
    requeued_delivery.requeue_for_other_hotkeys();

    assert_eq!(
        bucket.generation.load(Ordering::Acquire),
        generation_before_requeue,
        "requeue with remaining available dup capacity should not need a generation bump"
    );
    assert!(
        queue
            .reserve_for_models(1, &exhausted_hotkey, &["404-3dgs"])
            .is_empty(),
        "requeue_for_other_hotkeys must not unblock a hotkey that already received this task"
    );

    let fresh_hotkey = Hotkey::from_bytes(&[83u8; 32]);
    let fresh_delivery = queue.reserve_for_models(1, &fresh_hotkey, &["404-3dgs"]);
    assert_eq!(fresh_delivery.len(), 1);
    assert_eq!(
        fresh_delivery[0].task(),
        &task,
        "a fresh hotkey should still see the requeued available slot"
    );

    drop(exhausted_delivery);
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
