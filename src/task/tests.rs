use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use rand::RngExt;
use scc::Queue;
use uuid::Uuid;

use super::{TaskManager, TaskManagerInit, TaskStatus};
use crate::api::Task;
use crate::api::request::AddTaskResultRequest;
use crate::crypto::hotkey::Hotkey;
use crate::http3::rate_limits::RateLimitReservation;
use crate::metrics::Metrics;
use crate::raft::rate_limit::{ClientKey, DistributedRateLimiter};
use crate::raft::store::{RateLimitDelta, RateLimitMutation, Subject};

fn tasks_in_progress_total(metrics: &Metrics) -> f64 {
    metrics
        .registry()
        .gather()
        .into_iter()
        .find(|mf| mf.name() == "tasks_in_progress")
        .map(|mf| {
            mf.metric
                .into_iter()
                .filter_map(|m| m.get_gauge().as_ref().map(|g| g.value()))
                .sum()
        })
        .unwrap_or(0.0)
}

fn make_result(worker: &str, worker_id: &str, instant: Instant) -> AddTaskResultRequest {
    AddTaskResultRequest {
        worker_hotkey: worker.parse().unwrap(),
        worker_id: worker_id.to_string().into(),
        asset: Some(vec![]),
        reason: None,
        instant,
    }
}

#[tokio::test]
async fn test_cleanup() {
    const CLEANUP_INTERVAL: Duration = Duration::from_millis(200);
    const RESULT_LIFETIME: Duration = Duration::from_millis(400);
    const EXPECTED_RESULTS: usize = 2;
    const WAIT_DURATION: Duration = Duration::from_millis(600);

    let task_manager = TaskManager::new(
        10,
        EXPECTED_RESULTS,
        CLEANUP_INTERVAL,
        RESULT_LIFETIME,
        Metrics::new(0.05).unwrap(),
        None,
    )
    .await;
    let now = Instant::now();
    let worker1: Hotkey = Hotkey::from_bytes(&[1u8; 32]);
    let worker2: Hotkey = Hotkey::from_bytes(&[2u8; 32]);
    let worker1_str = worker1.to_string();
    let worker2_str = worker2.to_string();

    let task_id1 = Uuid::new_v4();
    task_manager
        .record_assignment(task_id1, worker1.clone(), worker1.to_string().into())
        .await;
    task_manager
        .record_assignment(task_id1, worker2.clone(), worker2.to_string().into())
        .await;
    task_manager
        .add_result(
            task_id1,
            make_result(
                worker1_str.as_str(),
                worker1_str.as_str(),
                now - Duration::from_millis(600),
            ),
        )
        .await
        .unwrap();

    let task_id2 = Uuid::new_v4();
    task_manager
        .record_assignment(task_id2, worker1.clone(), worker1.to_string().into())
        .await;
    task_manager
        .record_assignment(task_id2, worker2.clone(), worker2.to_string().into())
        .await;
    task_manager
        .add_result(
            task_id2,
            make_result(
                worker1_str.as_str(),
                worker1_str.as_str(),
                now - Duration::from_millis(600),
            ),
        )
        .await
        .unwrap();
    task_manager
        .add_result(
            task_id2,
            make_result(
                worker2_str.as_str(),
                worker2_str.as_str(),
                now - Duration::from_millis(200),
            ),
        )
        .await
        .unwrap();

    let task_id3 = Uuid::new_v4();
    task_manager
        .record_assignment(task_id3, worker1.clone(), worker1.to_string().into())
        .await;
    task_manager
        .record_assignment(task_id3, worker2.clone(), worker2.to_string().into())
        .await;
    task_manager
        .add_result(
            task_id3,
            make_result(
                worker1_str.as_str(),
                worker1_str.as_str(),
                now - Duration::from_millis(100),
            ),
        )
        .await
        .unwrap();
    task_manager
        .add_result(
            task_id3,
            make_result(
                worker2_str.as_str(),
                worker2_str.as_str(),
                now - Duration::from_millis(100),
            ),
        )
        .await
        .unwrap();

    assert_eq!(
        task_manager.get_status(task_id1).await,
        TaskStatus::PartialResult(1)
    );
    assert_ne!(
        task_manager.get_status(task_id2).await,
        TaskStatus::PartialResult(1)
    );
    assert_ne!(
        task_manager.get_status(task_id3).await,
        TaskStatus::PartialResult(1)
    );

    tokio::time::sleep(WAIT_DURATION).await;

    assert_eq!(
        task_manager.get_status(task_id1).await,
        TaskStatus::NoResult
    );
    assert_eq!(
        task_manager.get_status(task_id2).await,
        TaskStatus::NoResult
    );
    assert_eq!(
        task_manager.get_status(task_id3).await,
        TaskStatus::NoResult
    );
}

#[tokio::test]
async fn image_task_persists_until_all_assignments_complete() {
    const CLEANUP_INTERVAL: Duration = Duration::from_millis(50);
    const RESULT_LIFETIME: Duration = Duration::from_millis(500);

    let metrics = Metrics::new(0.05).unwrap();
    let task_manager =
        TaskManager::new(4, 2, CLEANUP_INTERVAL, RESULT_LIFETIME, metrics, None).await;

    let task_id = Uuid::new_v4();
    let image = Bytes::from_static(b"image-bytes");
    let task = Task {
        id: task_id,
        prompt: None,
        image: Some(image.clone()),
        model: Some("404-mesh".to_string()),
        seed: 0,
        model_params: None,
    };
    task_manager.add_task(task).await;

    let first_worker: Hotkey = Hotkey::from_bytes(&[1u8; 32]);
    let second_worker: Hotkey = Hotkey::from_bytes(&[2u8; 32]);

    task_manager
        .record_assignment(
            task_id,
            first_worker.clone(),
            first_worker.to_string().into(),
        )
        .await;
    task_manager
        .record_assignment(
            task_id,
            second_worker.clone(),
            second_worker.to_string().into(),
        )
        .await;

    let first_result = AddTaskResultRequest {
        worker_hotkey: first_worker.clone(),
        worker_id: first_worker.to_string().into(),
        asset: Some(vec![]),
        reason: None,
        instant: Instant::now(),
    };

    let outcome_after_first = task_manager
        .add_result(task_id, first_result)
        .await
        .unwrap();
    assert!(
        !outcome_after_first.completed,
        "task should wait for all assignments before cleanup"
    );
    assert!(
        task_manager.get_image(task_id).await.is_some(),
        "image payload was cleaned up before all workers responded"
    );

    let second_result = AddTaskResultRequest {
        worker_hotkey: second_worker.clone(),
        worker_id: second_worker.to_string().into(),
        asset: Some(vec![]),
        reason: None,
        instant: Instant::now(),
    };

    let outcome_after_second = task_manager
        .add_result(task_id, second_result)
        .await
        .unwrap();
    assert!(
        outcome_after_second.completed,
        "last worker should allow cleanup to proceed"
    );
    task_manager.finalize_task(task_id).await;

    assert!(
        task_manager.get_image(task_id).await.is_none(),
        "payload should be cleaned up after final result"
    );
}

#[tokio::test]
async fn model_persists_until_result_retrieval() {
    const CLEANUP_INTERVAL: Duration = Duration::from_millis(50);
    const RESULT_LIFETIME: Duration = Duration::from_millis(500);

    let metrics = Metrics::new(0.05).unwrap();
    let task_manager =
        TaskManager::new(2, 1, CLEANUP_INTERVAL, RESULT_LIFETIME, metrics, None).await;

    let task_id = Uuid::new_v4();
    let task = Task {
        id: task_id,
        prompt: Some(Arc::new("model check".to_string())),
        image: None,
        model: Some("404-3dgs".to_string()),
        seed: 0,
        model_params: None,
    };
    task_manager.add_task(task).await;

    let worker: Hotkey = Hotkey::from_bytes(&[3u8; 32]);
    task_manager
        .record_assignment(task_id, worker.clone(), worker.to_string().into())
        .await;

    let outcome = task_manager
        .add_result(
            task_id,
            AddTaskResultRequest {
                worker_hotkey: worker.clone(),
                worker_id: worker.to_string().into(),
                asset: Some(vec![]),
                reason: None,
                instant: Instant::now(),
            },
        )
        .await
        .unwrap();
    assert!(outcome.completed, "single result should complete the task");
    task_manager.finalize_task(task_id).await;

    assert_eq!(
        task_manager.get_model(task_id).await.as_deref(),
        Some("404-3dgs")
    );

    let results_bundle = task_manager
        .get_result(task_id)
        .await
        .expect("result bundle should exist");
    assert_eq!(results_bundle.model.as_deref(), Some("404-3dgs"));
    assert_eq!(results_bundle.results.len(), 1);
    assert!(task_manager.get_model(task_id).await.is_none());
}

#[tokio::test]
async fn tasks_in_progress_gauge_tracks_assignments() {
    const CLEANUP_INTERVAL: Duration = Duration::from_millis(50);
    const RESULT_LIFETIME: Duration = Duration::from_millis(500);

    let metrics = Metrics::new(0.05).unwrap();
    let task_manager = TaskManager::new(
        2,
        1,
        CLEANUP_INTERVAL,
        RESULT_LIFETIME,
        metrics.clone(),
        None,
    )
    .await;

    let task_id = Uuid::new_v4();
    let worker: Hotkey = Hotkey::from_bytes(&[7u8; 32]);
    let task = Task {
        id: task_id,
        prompt: Some(Arc::new("hello".to_string())),
        image: None,
        model: None,
        seed: 0,
        model_params: None,
    };
    task_manager.add_task(task).await;
    task_manager
        .record_assignment(task_id, worker.clone(), worker.to_string().into())
        .await;

    let mut in_progress = None;
    for mf in metrics.registry().gather() {
        if mf.name() == "tasks_in_progress" {
            for m in &mf.metric {
                let matches_worker = m
                    .label
                    .iter()
                    .any(|lp| lp.name() == "worker" && lp.value() == worker.to_string());
                if matches_worker {
                    in_progress = m.get_gauge().as_ref().map(|g| g.value());
                }
            }
        }
    }
    assert_eq!(
        Some(1.0),
        in_progress,
        "tasks_in_progress should reflect active assignment"
    );

    let _complete = task_manager
        .add_result(
            task_id,
            AddTaskResultRequest {
                worker_hotkey: worker.clone(),
                worker_id: worker.to_string().into(),
                asset: Some(vec![]),
                reason: None,
                instant: Instant::now(),
            },
        )
        .await
        .unwrap();
    task_manager.finalize_task(task_id).await;

    let mut in_progress_after = None;
    for mf in metrics.registry().gather() {
        if mf.name() == "tasks_in_progress" {
            for m in &mf.metric {
                let matches_worker = m
                    .label
                    .iter()
                    .any(|lp| lp.name() == "worker" && lp.value() == worker.to_string());
                if matches_worker {
                    in_progress_after = m.get_gauge().as_ref().map(|g| g.value());
                }
            }
        }
    }
    assert_eq!(
        Some(0.0),
        in_progress_after,
        "tasks_in_progress should decrement after completion"
    );
}

#[tokio::test]
async fn tasks_in_progress_handles_multiple_random_assignments() {
    const CLEANUP_INTERVAL: Duration = Duration::from_millis(50);
    const RESULT_LIFETIME: Duration = Duration::from_millis(500);

    let mut rng = rand::rng();
    let assignment_count = rng.random_range(2..=5);

    let metrics = Metrics::new(0.05).unwrap();
    let task_manager = TaskManager::new(
        assignment_count * 2,
        assignment_count,
        CLEANUP_INTERVAL,
        RESULT_LIFETIME,
        metrics.clone(),
        None,
    )
    .await;

    let task_id = Uuid::new_v4();
    let task = Task {
        id: task_id,
        prompt: Some(Arc::new("batch".to_string())),
        image: None,
        model: None,
        seed: 0,
        model_params: None,
    };
    task_manager.add_task(task).await;

    let mut workers = Vec::with_capacity(assignment_count);
    for idx in 0..assignment_count {
        let v: Hotkey = Hotkey::from_bytes(&[(idx as u8) + 1; 32]);
        task_manager
            .record_assignment(task_id, v.clone(), v.to_string().into())
            .await;
        workers.push(v);
    }

    assert_eq!(
        tasks_in_progress_total(&metrics),
        assignment_count as f64,
        "gauge should equal number of assignments"
    );

    let mut completed = false;
    for v in workers.iter() {
        let outcome = task_manager
            .add_result(
                task_id,
                AddTaskResultRequest {
                    worker_hotkey: v.clone(),
                    worker_id: v.to_string().into(),
                    asset: Some(vec![]),
                    reason: None,
                    instant: Instant::now(),
                },
            )
            .await
            .unwrap();
        completed = outcome.completed;
    }
    assert!(
        completed,
        "last result should mark all assignments complete"
    );
    task_manager.finalize_task(task_id).await;

    assert_eq!(
        tasks_in_progress_total(&metrics),
        0.0,
        "gauge should be cleared after all completions"
    );
}

#[tokio::test]
async fn test_timeout_increments_metric() {
    const CLEANUP_INTERVAL: Duration = Duration::from_millis(50);
    const RESULT_LIFETIME: Duration = Duration::from_millis(120);

    let metrics = Metrics::new(0.05).unwrap();
    let task_manager = TaskManager::new(
        8,
        1,
        CLEANUP_INTERVAL,
        RESULT_LIFETIME,
        metrics.clone(),
        None,
    )
    .await;

    let task_id = Uuid::new_v4();
    let task = Task {
        id: task_id,
        prompt: None,
        image: Some(Bytes::from_static(b"")),
        model: None,
        seed: 0,
        model_params: None,
    };
    task_manager.add_task(task).await;

    let worker: Hotkey = Hotkey::from_bytes(&[42u8; 32]);
    task_manager
        .record_assignment(task_id, worker.clone(), worker.to_string().into())
        .await;

    tokio::time::sleep(Duration::from_millis(
        RESULT_LIFETIME.as_millis() as u64 + 200,
    ))
    .await;

    let families = metrics.registry().gather();
    let mut found = false;
    for mf in families {
        if mf.name() == "timeout_failures_total" {
            for m in &mf.metric {
                let matched = m
                    .label
                    .iter()
                    .any(|lp| lp.name() == "worker" && lp.value() == worker.to_string());
                if matched && m.get_counter().as_ref().is_some_and(|c| c.value() >= 1.0) {
                    found = true;
                    break;
                }
            }
        }
        if found {
            break;
        }
    }
    assert!(found, "timeout_failures_total for worker was not found");
}

#[tokio::test]
async fn emits_refund_when_task_completes_with_only_failures() {
    const CLEANUP_INTERVAL: Duration = Duration::from_millis(50);
    const RESULT_LIFETIME: Duration = Duration::from_millis(500);

    let metrics = Metrics::new(0.05).unwrap();
    let mutation_queue = Arc::new(Queue::<RateLimitMutation>::default());
    let limiter = DistributedRateLimiter::new(64);
    let task_manager = TaskManager::new_with_rate_limit_mutation_queue(TaskManagerInit {
        initial_capacity: 4,
        expected_results: 2,
        cleanup_interval: CLEANUP_INTERVAL,
        result_lifetime: RESULT_LIFETIME,
        rate_limit_mutation_queue: mutation_queue.clone(),
        metrics,
        worker_event_recorder: None,
    })
    .await;

    let task_id = Uuid::new_v4();
    let task = Task {
        id: task_id,
        prompt: Some(Arc::new("refund".to_string())),
        image: None,
        model: None,
        seed: 0,
        model_params: None,
    };
    task_manager
        .add_task_with_rate_limit_reservation(
            task,
            Some(RateLimitReservation::new(
                limiter.clone(),
                vec![RateLimitDelta {
                    subject: Subject::User,
                    id: 777u128,
                    hour_epoch: 900,
                    day_epoch: 90,
                    add_hour: 1,
                    add_day: 0,
                }],
            )),
        )
        .await;

    let worker1: Hotkey = Hotkey::from_bytes(&[11u8; 32]);
    let worker2: Hotkey = Hotkey::from_bytes(&[12u8; 32]);
    task_manager
        .record_assignment(task_id, worker1.clone(), worker1.to_string().into())
        .await;
    task_manager
        .record_assignment(task_id, worker2.clone(), worker2.to_string().into())
        .await;

    let _ = task_manager
        .add_result(
            task_id,
            AddTaskResultRequest {
                worker_hotkey: worker1.clone(),
                worker_id: worker1.to_string().into(),
                asset: None,
                reason: Some("worker failed".into()),
                instant: Instant::now(),
            },
        )
        .await
        .unwrap();
    let completed = task_manager
        .add_result(
            task_id,
            AddTaskResultRequest {
                worker_hotkey: worker2.clone(),
                worker_id: worker2.to_string().into(),
                asset: None,
                reason: Some("worker failed".into()),
                instant: Instant::now(),
            },
        )
        .await
        .unwrap();
    assert!(completed.completed);

    let refund = mutation_queue
        .pop()
        .expect("a refund should be emitted for all-failure completion");
    assert_eq!(refund.subject, Subject::User);
    assert_eq!(refund.id, 777u128);
    assert_eq!(refund.hour_epoch, 900);
    assert_eq!(refund.day_epoch, 90);
    assert_eq!(refund.hour_delta, -1);
    assert_eq!(refund.day_delta, 0);
}

#[tokio::test]
async fn waits_for_expected_results_before_emitting_refund() {
    const CLEANUP_INTERVAL: Duration = Duration::from_millis(50);
    const RESULT_LIFETIME: Duration = Duration::from_millis(500);

    let metrics = Metrics::new(0.05).unwrap();
    let mutation_queue = Arc::new(Queue::<RateLimitMutation>::default());
    let limiter = DistributedRateLimiter::new(64);
    let task_manager = TaskManager::new_with_rate_limit_mutation_queue(TaskManagerInit {
        initial_capacity: 4,
        expected_results: 2,
        cleanup_interval: CLEANUP_INTERVAL,
        result_lifetime: RESULT_LIFETIME,
        rate_limit_mutation_queue: mutation_queue.clone(),
        metrics,
        worker_event_recorder: None,
    })
    .await;

    let task_id = Uuid::new_v4();
    task_manager
        .add_task_with_rate_limit_reservation(
            Task {
                id: task_id,
                prompt: Some(Arc::new("deferred refund".to_string())),
                image: None,
                model: None,
                seed: 0,
                model_params: None,
            },
            Some(RateLimitReservation::new(
                limiter,
                vec![RateLimitDelta {
                    subject: Subject::User,
                    id: 901u128,
                    hour_epoch: 910,
                    day_epoch: 91,
                    add_hour: 1,
                    add_day: 0,
                }],
            )),
        )
        .await;

    let worker1: Hotkey = Hotkey::from_bytes(&[31u8; 32]);
    let worker2: Hotkey = Hotkey::from_bytes(&[32u8; 32]);

    task_manager
        .record_assignment(task_id, worker1.clone(), worker1.to_string().into())
        .await;

    let first = task_manager
        .add_result(
            task_id,
            AddTaskResultRequest {
                worker_hotkey: worker1.clone(),
                worker_id: worker1.to_string().into(),
                asset: None,
                reason: Some("worker failed".into()),
                instant: Instant::now(),
            },
        )
        .await
        .unwrap();
    assert!(
        !first.completed,
        "single failed attempt must not complete when more results are expected"
    );
    assert!(
        mutation_queue.pop().is_none(),
        "refund must wait until expected attempts are exhausted"
    );

    task_manager
        .record_assignment(task_id, worker2.clone(), worker2.to_string().into())
        .await;

    let second = task_manager
        .add_result(
            task_id,
            AddTaskResultRequest {
                worker_hotkey: worker2.clone(),
                worker_id: worker2.to_string().into(),
                asset: None,
                reason: Some("worker failed".into()),
                instant: Instant::now(),
            },
        )
        .await
        .unwrap();
    assert!(second.completed);
    assert!(
        mutation_queue.pop().is_some(),
        "refund should be emitted after final expected failure"
    );
}

#[tokio::test]
async fn does_not_emit_refund_when_timeout_occurs_after_partial_success() {
    const CLEANUP_INTERVAL: Duration = Duration::from_millis(40);
    const RESULT_LIFETIME: Duration = Duration::from_millis(120);

    let metrics = Metrics::new(0.05).unwrap();
    let mutation_queue = Arc::new(Queue::<RateLimitMutation>::default());
    let limiter = DistributedRateLimiter::new(64);
    let task_manager = TaskManager::new_with_rate_limit_mutation_queue(TaskManagerInit {
        initial_capacity: 4,
        expected_results: 2,
        cleanup_interval: CLEANUP_INTERVAL,
        result_lifetime: RESULT_LIFETIME,
        rate_limit_mutation_queue: mutation_queue.clone(),
        metrics,
        worker_event_recorder: None,
    })
    .await;

    let task_id = Uuid::new_v4();
    let task = Task {
        id: task_id,
        prompt: Some(Arc::new("partial".to_string())),
        image: None,
        model: None,
        seed: 0,
        model_params: None,
    };
    task_manager
        .add_task_with_rate_limit_reservation(
            task,
            Some(RateLimitReservation::new(
                limiter.clone(),
                vec![RateLimitDelta {
                    subject: Subject::User,
                    id: 888u128,
                    hour_epoch: 901,
                    day_epoch: 90,
                    add_hour: 1,
                    add_day: 0,
                }],
            )),
        )
        .await;

    let worker1: Hotkey = Hotkey::from_bytes(&[21u8; 32]);
    let worker2: Hotkey = Hotkey::from_bytes(&[22u8; 32]);
    task_manager
        .record_assignment(task_id, worker1.clone(), worker1.to_string().into())
        .await;
    task_manager
        .record_assignment(task_id, worker2.clone(), worker2.to_string().into())
        .await;

    let _ = task_manager
        .add_result(
            task_id,
            AddTaskResultRequest {
                worker_hotkey: worker1.clone(),
                worker_id: worker1.to_string().into(),
                asset: Some(vec![1, 2, 3]),
                reason: None,
                instant: Instant::now(),
            },
        )
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(
        RESULT_LIFETIME.as_millis() as u64 + 200,
    ))
    .await;

    assert!(
        mutation_queue.pop().is_none(),
        "partial success must not trigger refund on timeout"
    );
}

#[tokio::test]
async fn refund_rolls_back_local_pending_capacity() {
    const CLEANUP_INTERVAL: Duration = Duration::from_millis(50);
    const RESULT_LIFETIME: Duration = Duration::from_millis(500);

    let metrics = Metrics::new(0.05).unwrap();
    let mutation_queue = Arc::new(Queue::<RateLimitMutation>::default());
    let limiter = DistributedRateLimiter::new(64);
    let key = ClientKey {
        subject: Subject::User,
        id: 999u128,
    };
    let epochs = (901u64, 90u64);

    assert!(limiter.check_and_incr(key, 1, 0, 0, 0, epochs).await);
    assert!(!limiter.check_and_incr(key, 1, 0, 0, 0, epochs).await);

    let task_manager = TaskManager::new_with_rate_limit_mutation_queue(TaskManagerInit {
        initial_capacity: 2,
        expected_results: 1,
        cleanup_interval: CLEANUP_INTERVAL,
        result_lifetime: RESULT_LIFETIME,
        rate_limit_mutation_queue: mutation_queue.clone(),
        metrics,
        worker_event_recorder: None,
    })
    .await;

    let task_id = Uuid::new_v4();
    task_manager
        .add_task_with_rate_limit_reservation(
            Task {
                id: task_id,
                prompt: Some(Arc::new("rollback".to_string())),
                image: None,
                model: None,
                seed: 0,
                model_params: None,
            },
            Some(RateLimitReservation::new(
                limiter.clone(),
                vec![RateLimitDelta {
                    subject: key.subject,
                    id: key.id,
                    hour_epoch: epochs.0,
                    day_epoch: epochs.1,
                    add_hour: 1,
                    add_day: 0,
                }],
            )),
        )
        .await;

    let worker: Hotkey = Hotkey::from_bytes(&[42u8; 32]);
    task_manager
        .record_assignment(task_id, worker.clone(), worker.to_string().into())
        .await;
    let completed = task_manager
        .add_result(
            task_id,
            AddTaskResultRequest {
                worker_hotkey: worker.clone(),
                worker_id: worker.to_string().into(),
                asset: None,
                reason: Some("worker failed".into()),
                instant: Instant::now(),
            },
        )
        .await
        .unwrap();
    assert!(completed.completed);

    let refund = mutation_queue
        .pop()
        .expect("a refund mutation should be emitted");
    assert_eq!(refund.subject, Subject::User);
    assert_eq!(refund.id, 999u128);
    assert_eq!(refund.hour_delta, -1);

    assert!(
        limiter.check_and_incr(key, 1, 0, 0, 0, epochs).await,
        "refund must restore local pending capacity immediately"
    );
}
