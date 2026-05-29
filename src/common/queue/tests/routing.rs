use super::*;

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
async fn reserve_for_models_returns_tagged_task_for_matching_worker_tags() {
    let queue = build_queue();
    let task = create_task("matching", Some("404-3dgs"));
    push_task_with_required_tags(&queue, task.clone(), &["acme"]);

    let worker_tags = vec!["acme".to_string()];
    let items = queue.reserve_for_models_with_routing(
        1,
        &Hotkey::from_bytes(&[20u8; 32]),
        &["404-3dgs"],
        WorkerRouting::from_tags(worker_tags.as_slice()),
    );

    assert_eq!(items.len(), 1);
    assert_eq!(items[0].task(), &task);
}

#[tokio::test]
async fn worker_tag_filter_keeps_tagged_task_for_later_matching_worker() {
    let queue = build_queue();
    let task = create_task("restricted", Some("404-3dgs"));
    push_task_with_required_tags(&queue, task.clone(), &["acme"]);

    let non_matching_tags = vec!["other".to_string()];
    let non_matching_items = queue.reserve_for_models_with_routing(
        1,
        &Hotkey::from_bytes(&[24u8; 32]),
        &["404-3dgs"],
        WorkerRouting::from_tags(non_matching_tags.as_slice()),
    );
    assert!(
        non_matching_items.is_empty(),
        "non-matching worker must not receive tagged task"
    );

    let matching_tags = vec!["acme".to_string()];
    let matching_items = queue.reserve_for_models_with_routing(
        1,
        &Hotkey::from_bytes(&[25u8; 32]),
        &["404-3dgs"],
        WorkerRouting::from_tags(matching_tags.as_slice()),
    );
    assert_eq!(matching_items.len(), 1);
    assert_eq!(matching_items[0].task(), &task);
}

#[tokio::test]
async fn worker_tag_filter_keeps_public_task_for_untagged_worker() {
    let queue = build_queue();
    let task = create_task("public", Some("404-3dgs"));
    queue.push(task.clone());

    let tagged_worker_tags = vec!["acme".to_string()];
    let tagged_items = queue.reserve_for_models_with_routing(
        1,
        &Hotkey::from_bytes(&[26u8; 32]),
        &["404-3dgs"],
        WorkerRouting::from_tags(tagged_worker_tags.as_slice()),
    );
    assert!(
        tagged_items.is_empty(),
        "worker requesting tagged work must not receive public task"
    );

    let untagged_items =
        queue.reserve_for_models(1, &Hotkey::from_bytes(&[37u8; 32]), &["404-3dgs"]);
    assert_eq!(untagged_items.len(), 1);
    assert_eq!(untagged_items[0].task(), &task);
}

#[tokio::test]
async fn worker_tag_filter_does_not_reshuffle_skipped_tasks() {
    let queue = build_queue();
    let first = create_task("first restricted", Some("404-3dgs"));
    let second = create_task("second restricted", Some("404-3dgs"));
    push_task_with_required_tags(&queue, first.clone(), &["acme"]);
    push_task_with_required_tags(&queue, second.clone(), &["enterprise"]);

    assert_eq!(
        bucket_task_ids(&queue, "404-3dgs"),
        vec![first.id, second.id]
    );

    let non_matching_tags = vec!["other".to_string()];
    let non_matching_items = queue.reserve_for_models_with_routing(
        1,
        &Hotkey::from_bytes(&[27u8; 32]),
        &["404-3dgs"],
        WorkerRouting::from_tags(non_matching_tags.as_slice()),
    );
    assert!(non_matching_items.is_empty());

    assert_eq!(
        bucket_task_ids(&queue, "404-3dgs"),
        vec![first.id, second.id]
    );
}

#[tokio::test]
async fn exhausted_cache_is_worker_routing_aware() {
    let queue = build_queue();
    let hotkey = Hotkey::from_bytes(&[28u8; 32]);
    let task = create_task("restricted", Some("404-3dgs"));
    push_task_with_required_tags(&queue, task.clone(), &["acme"]);

    let non_matching_tags = vec!["other".to_string()];
    let non_matching_items = queue.reserve_for_models_with_routing(
        1,
        &hotkey,
        &["404-3dgs"],
        WorkerRouting::from_tags(non_matching_tags.as_slice()),
    );
    assert!(non_matching_items.is_empty());

    let bucket = queue.inner.bucket_for_model("404-3dgs");
    let non_matching_key = exhausted_key(&hotkey, non_matching_tags.as_slice());
    assert_eq!(
        bucket.exhausted_generation(&non_matching_key),
        Some(bucket.generation.load(Ordering::Acquire)),
        "non-matching routing should cache its own exhausted scan"
    );

    let matching_tags = vec!["acme".to_string()];
    let matching_items = queue.reserve_for_models_with_routing(
        1,
        &hotkey,
        &["404-3dgs"],
        WorkerRouting::from_tags(matching_tags.as_slice()),
    );
    assert_eq!(matching_items.len(), 1);
    assert_eq!(matching_items[0].task(), &task);
}

#[tokio::test]
async fn bounded_scan_cursor_reaches_work_after_ineligible_prefix() {
    let queue = build_queue();
    let hotkey = Hotkey::from_bytes(&[29u8; 32]);
    let worker_tags = Vec::<String>::new();

    for idx in 0..=DEFAULT_RESERVATION_SCAN_CAP {
        push_task_with_required_tags(
            &queue,
            create_task(&format!("restricted-{idx}"), Some("404-3dgs")),
            &["acme"],
        );
    }
    let public = create_task("public-after-prefix", Some("404-3dgs"));
    queue.push(public.clone());

    let first = queue.reserve_for_models_with_routing(
        1,
        &hotkey,
        &["404-3dgs"],
        WorkerRouting::from_tags(worker_tags.as_slice()),
    );
    assert!(
        first.is_empty(),
        "first bounded scan should stop before reaching the public task"
    );

    let second = queue.reserve_for_models_with_routing(
        1,
        &hotkey,
        &["404-3dgs"],
        WorkerRouting::from_tags(worker_tags.as_slice()),
    );
    assert_eq!(second.len(), 1);
    assert_eq!(second[0].task(), &public);
}

#[tokio::test]
async fn reservation_scan_cap_can_track_queue_limit_above_default() {
    let queue = TaskQueue::builder()
        .dup(2)
        .ttl(300)
        .cleanup_interval(1)
        .default_model("404-3dgs")
        .models(["404-3dgs"])
        .reservation_scan_cap(DEFAULT_RESERVATION_SCAN_CAP + 64)
        .build();
    let hotkey = Hotkey::from_bytes(&[32u8; 32]);
    let worker_tags = Vec::<String>::new();

    for idx in 0..=DEFAULT_RESERVATION_SCAN_CAP {
        push_task_with_required_tags(
            &queue,
            create_task(&format!("restricted-{idx}"), Some("404-3dgs")),
            &["acme"],
        );
    }
    let public = create_task("public-above-default-cap", Some("404-3dgs"));
    queue.push(public.clone());

    let items = queue.reserve_for_models_with_routing(
        1,
        &hotkey,
        &["404-3dgs"],
        WorkerRouting::from_tags(worker_tags.as_slice()),
    );
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].task(), &public);
}

#[tokio::test]
async fn reservation_scan_cap_can_be_built_below_default() {
    let queue = TaskQueue::builder()
        .dup(2)
        .ttl(300)
        .cleanup_interval(1)
        .default_model("404-3dgs")
        .models(["404-3dgs"])
        .reservation_scan_cap(2)
        .build();
    let hotkey = Hotkey::from_bytes(&[35u8; 32]);
    let worker_tags = Vec::<String>::new();

    for idx in 0..2 {
        push_task_with_required_tags(
            &queue,
            create_task(&format!("restricted-below-default-{idx}"), Some("404-3dgs")),
            &["acme"],
        );
    }
    let public = create_task("public-after-below-default-cap", Some("404-3dgs"));
    queue.push(public.clone());

    let first = queue.reserve_for_models_with_routing(
        1,
        &hotkey,
        &["404-3dgs"],
        WorkerRouting::from_tags(worker_tags.as_slice()),
    );
    assert!(
        first.is_empty(),
        "custom scan cap below default should stop before later eligible work"
    );

    let second = queue.reserve_for_models_with_routing(
        1,
        &hotkey,
        &["404-3dgs"],
        WorkerRouting::from_tags(worker_tags.as_slice()),
    );
    assert_eq!(second.len(), 1);
    assert_eq!(second[0].task(), &public);
}

#[tokio::test]
async fn reservation_scan_cap_can_be_raised_after_build() {
    let queue = build_queue();
    let hotkey = Hotkey::from_bytes(&[33u8; 32]);
    let worker_tags = Vec::<String>::new();

    for idx in 0..=DEFAULT_RESERVATION_SCAN_CAP {
        push_task_with_required_tags(
            &queue,
            create_task(&format!("restricted-{idx}"), Some("404-3dgs")),
            &["acme"],
        );
    }
    let public = create_task("public-after-scan-cap-update", Some("404-3dgs"));
    queue.push(public.clone());

    let first = queue.reserve_for_models_with_routing(
        1,
        &hotkey,
        &["404-3dgs"],
        WorkerRouting::from_tags(worker_tags.as_slice()),
    );
    assert!(first.is_empty());

    let other_hotkey = Hotkey::from_bytes(&[34u8; 32]);
    queue.set_reservation_scan_cap(DEFAULT_RESERVATION_SCAN_CAP + 64);

    let second = queue.reserve_for_models_with_routing(
        1,
        &other_hotkey,
        &["404-3dgs"],
        WorkerRouting::from_tags(worker_tags.as_slice()),
    );
    assert_eq!(second.len(), 1);
    assert_eq!(second[0].task(), &public);
}

#[tokio::test]
async fn reservation_scan_cap_can_be_lowered_after_build() {
    let queue = build_queue();
    let hotkey = Hotkey::from_bytes(&[36u8; 32]);
    let worker_tags = Vec::<String>::new();

    for idx in 0..2 {
        push_task_with_required_tags(
            &queue,
            create_task(&format!("restricted-after-lower-{idx}"), Some("404-3dgs")),
            &["acme"],
        );
    }
    let public = create_task("public-after-runtime-lower", Some("404-3dgs"));
    queue.push(public.clone());

    queue.set_reservation_scan_cap(2);
    let first = queue.reserve_for_models_with_routing(
        1,
        &hotkey,
        &["404-3dgs"],
        WorkerRouting::from_tags(worker_tags.as_slice()),
    );
    assert!(
        first.is_empty(),
        "runtime scan cap below default should stop before later eligible work"
    );

    let second = queue.reserve_for_models_with_routing(
        1,
        &hotkey,
        &["404-3dgs"],
        WorkerRouting::from_tags(worker_tags.as_slice()),
    );
    assert_eq!(second.len(), 1);
    assert_eq!(second[0].task(), &public);
}

#[tokio::test]
async fn scan_cursor_wraps_when_start_seq_is_past_tail() {
    let queue = build_queue();
    let hotkey = Hotkey::from_bytes(&[30u8; 32]);
    let task = create_task("wrap-target", Some("404-3dgs"));
    queue.push(task.clone());

    let bucket = queue.inner.bucket_for_model("404-3dgs");
    let key = exhausted_key(&hotkey, &[]);
    bucket.update_scan_cursor(&key, u64::MAX);

    let items = queue.reserve_for_models(1, &hotkey, &["404-3dgs"]);
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].task(), &task);
}

#[tokio::test]
async fn scan_cursor_isolated_by_worker_routing_for_same_hotkey() {
    let queue = build_queue();
    let hotkey = Hotkey::from_bytes(&[31u8; 32]);
    let b_task = create_task("b-target", Some("404-3dgs"));
    let a_task = create_task("a-target", Some("404-3dgs"));
    push_task_with_required_tags(&queue, b_task.clone(), &["b"]);
    push_task_with_required_tags(&queue, a_task.clone(), &["a"]);
    for idx in 0..DEFAULT_RESERVATION_SCAN_CAP {
        queue.push(create_task(&format!("public-{idx}"), Some("404-3dgs")));
    }

    let a_tags = vec!["a".to_string()];
    let a_items = queue.reserve_for_models_with_routing(
        1,
        &hotkey,
        &["404-3dgs"],
        WorkerRouting::from_tags(a_tags.as_slice()),
    );
    assert_eq!(a_items.len(), 1);
    assert_eq!(a_items[0].task(), &a_task);

    let b_tags = vec!["b".to_string()];
    let b_items = queue.reserve_for_models_with_routing(
        1,
        &hotkey,
        &["404-3dgs"],
        WorkerRouting::from_tags(b_tags.as_slice()),
    );
    assert_eq!(b_items.len(), 1);
    assert_eq!(b_items[0].task(), &b_task);
}

#[tokio::test]
async fn empty_default_and_explicit_empty_worker_routing_share_cache_key() {
    let explicit: Vec<String> = Vec::new();
    assert!(
        WorkerRouting::default().cache_key() == WorkerRouting::from_tags(&explicit).cache_key()
    );
}

#[tokio::test]
async fn reserve_for_models_filters_by_worker_tags() {
    let queue = build_queue();
    let hotkey = Hotkey::from_bytes(&[21u8; 32]);
    let restricted = create_task("restricted", Some("404-3dgs"));
    let public = create_task("public", Some("404-3dgs"));
    push_task_with_required_tags(&queue, restricted.clone(), &["acme"]);
    queue.push(public.clone());

    let no_tag_items = queue.pop_for_models(2, &hotkey, &["404-3dgs"]);
    assert_eq!(no_tag_items.len(), 1);
    assert_eq!(no_tag_items[0].0, public);

    let tagged_hotkey = Hotkey::from_bytes(&[22u8; 32]);
    let worker_tags = vec!["acme".to_string()];
    let tagged_items = queue.reserve_for_models_with_routing(
        1,
        &tagged_hotkey,
        &["404-3dgs"],
        WorkerRouting::from_tags(worker_tags.as_slice()),
    );
    assert_eq!(tagged_items.len(), 1);
    assert_eq!(tagged_items[0].task(), &restricted);
}

#[tokio::test]
async fn worker_tag_filter_uses_any_matching_tag() {
    let queue = build_queue();
    let task = create_task("enterprise", Some("404-3dgs"));
    push_task_with_required_tags(&queue, task.clone(), &["acme", "premium"]);

    let worker_tags = vec!["premium".to_string()];
    let items = queue.reserve_for_models_with_routing(
        1,
        &Hotkey::from_bytes(&[23u8; 32]),
        &["404-3dgs"],
        WorkerRouting::from_tags(worker_tags.as_slice()),
    );
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].task(), &task);
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
