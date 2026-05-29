use super::*;

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
    let task = create_task("Old Task", None);
    queue.push(task.clone());
    tokio::time::sleep(Duration::from_millis(2500)).await;

    assert_eq!(queue.len(), 0);
    assert!(
        queue
            .inner
            .active_ids
            .read_sync(&task.id, |_, _| ())
            .is_none(),
        "background cleanup should clear active bookkeeping for expired entries"
    );
}

#[tokio::test]
async fn cleanup_removes_idle_zero_capacity_entries_before_ttl() {
    let queue = build_queue_with_config(1, 300, 1);
    let task = create_task("idle-zero", Some("404-3dgs"));
    let generation = queue.inner.acquire_task_generation(task.id);
    let entry = raw_entry(task.clone(), 0, 0, generation);
    queue.inner.increment_len();
    queue.inner.bucket_for_model("404-3dgs").enqueue_new(entry);

    tokio::time::sleep(Duration::from_millis(1200)).await;

    assert_eq!(queue.len(), 0);
    assert!(
        queue
            .inner
            .active_ids
            .read_sync(&task.id, |_, _| ())
            .is_none(),
        "cleanup should clear bookkeeping for idle zero-capacity entries"
    );
}

#[tokio::test]
async fn dropping_queue_clone_does_not_stop_cleanup() {
    let queue = build_queue_with_config(1, 1, 1);
    let clone = queue.clone();
    drop(clone);

    let task = create_task("Old Task", None);
    queue.push(task.clone());
    tokio::time::sleep(Duration::from_millis(2500)).await;

    assert_eq!(queue.len(), 0);
    assert!(
        queue
            .inner
            .active_ids
            .read_sync(&task.id, |_, _| ())
            .is_none(),
        "background cleanup should continue after dropping a queue clone"
    );
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
        "cleanup should remove the expired remaining duplicate while preserving fresh work"
    );

    let hk_b: Hotkey = Hotkey::from_bytes(&[27u8; 32]);
    let second_pop = queue.pop(2, &hk_b);
    assert_eq!(second_pop.len(), 1);
    assert_eq!(second_pop[0].0, fresh_task);
    assert!(second_pop[0].1.is_none());
}
