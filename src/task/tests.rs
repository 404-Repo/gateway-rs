use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use rand::RngExt;
use uuid::Uuid;

use super::{AddResultError, TaskManager, TaskManagerInit, TaskStatus};
use crate::api::Task;
use crate::api::request::AddTaskResultRequest;
use crate::common::rate_limit_buffer::RateLimitMutationBuffer;
use crate::crypto::hotkey::Hotkey;
use crate::http3::rate_limits::RateLimitReservation;
use crate::metrics::Metrics;
use crate::raft::rate_limit::{CheckAndIncrParams, ClientKey, DistributedRateLimiter};
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
        assignment_token: Uuid::nil(),
        asset: Some(Bytes::new()),
        reason: None,
        instant,
    }
}

fn make_failed_result(worker: &str, worker_id: &str, instant: Instant) -> AddTaskResultRequest {
    AddTaskResultRequest {
        worker_hotkey: worker.parse().unwrap(),
        worker_id: worker_id.to_string().into(),
        assignment_token: Uuid::nil(),
        asset: None,
        reason: Some("worker failed".into()),
        instant,
    }
}

fn sample_task(task_id: Uuid) -> Task {
    Task {
        id: task_id,
        prompt: Some(Arc::new("task".to_string())),
        image: None,
        model: None,
        seed: 0,
        model_params: None,
    }
}

async fn pop_only_mutation(queue: &RateLimitMutationBuffer) -> RateLimitMutation {
    let batch = queue
        .drain_batch(1)
        .await
        .expect("a rate-limit batch should be emitted");
    assert_eq!(
        batch.mutations.len(),
        1,
        "tests expect a single mutation per emitted batch"
    );
    batch.mutations.into_iter().next().expect("single mutation")
}

#[tokio::test]
async fn single_success_stays_partial_until_expected_results_are_exhausted() {
    let task_manager = TaskManager::new(
        4,
        2,
        Duration::from_millis(50),
        Duration::from_millis(500),
        Metrics::new(0.05).unwrap(),
        None,
    )
    .await;
    let worker: Hotkey = Hotkey::from_bytes(&[51u8; 32]);
    let worker_str = worker.to_string();
    let task_id = Uuid::new_v4();

    task_manager.add_task(sample_task(task_id)).await;
    task_manager
        .record_assignment(task_id, worker.clone(), worker_str.clone().into())
        .await;
    task_manager
        .add_result(
            task_id,
            make_result(worker_str.as_str(), worker_str.as_str(), Instant::now()),
        )
        .await
        .unwrap();

    assert_eq!(
        task_manager.get_status(task_id).await,
        TaskStatus::PartialResult(1)
    );
}

#[tokio::test]
async fn single_failure_stays_partial_until_expected_results_are_exhausted() {
    let task_manager = TaskManager::new(
        4,
        2,
        Duration::from_millis(50),
        Duration::from_millis(500),
        Metrics::new(0.05).unwrap(),
        None,
    )
    .await;
    let worker: Hotkey = Hotkey::from_bytes(&[52u8; 32]);
    let worker_str = worker.to_string();
    let task_id = Uuid::new_v4();

    task_manager.add_task(sample_task(task_id)).await;
    task_manager
        .record_assignment(task_id, worker.clone(), worker_str.clone().into())
        .await;
    task_manager
        .add_result(
            task_id,
            make_failed_result(worker_str.as_str(), worker_str.as_str(), Instant::now()),
        )
        .await
        .unwrap();

    assert_eq!(
        task_manager.get_status(task_id).await,
        TaskStatus::PartialResult(0)
    );
}

#[tokio::test]
async fn staged_result_is_invisible_until_committed() {
    let task_manager = TaskManager::new(
        4,
        1,
        Duration::from_millis(50),
        Duration::from_millis(500),
        Metrics::new(0.05).unwrap(),
        None,
    )
    .await;
    let worker: Hotkey = Hotkey::from_bytes(&[58u8; 32]);
    let worker_str = worker.to_string();
    let task_id = Uuid::new_v4();

    task_manager.add_task(sample_task(task_id)).await;
    task_manager
        .record_assignment(task_id, worker.clone(), worker_str.clone().into())
        .await;

    task_manager
        .stage_result(
            task_id,
            make_result(worker_str.as_str(), worker_str.as_str(), Instant::now()),
        )
        .await
        .unwrap();

    assert_eq!(task_manager.get_status(task_id).await, TaskStatus::NoResult);
    assert!(
        task_manager.get_result(task_id).await.is_none(),
        "staged results must stay invisible until durable finalize succeeds"
    );

    let outcome = task_manager
        .commit_staged_result(task_id, &worker)
        .await
        .unwrap();
    assert!(outcome.completed);
    assert_eq!(
        task_manager.get_status(task_id).await,
        TaskStatus::Success {
            worker_id: worker_str.clone().into(),
        }
    );
    assert_eq!(
        task_manager
            .get_result(task_id)
            .await
            .expect("committed result should be readable")
            .results
            .len(),
        1
    );
}

#[tokio::test]
async fn rollback_staged_result_allows_retry() {
    let task_manager = TaskManager::new(
        4,
        1,
        Duration::from_millis(50),
        Duration::from_millis(500),
        Metrics::new(0.05).unwrap(),
        None,
    )
    .await;
    let worker: Hotkey = Hotkey::from_bytes(&[59u8; 32]);
    let worker_str = worker.to_string();
    let task_id = Uuid::new_v4();

    task_manager.add_task(sample_task(task_id)).await;
    task_manager
        .record_assignment(task_id, worker.clone(), worker_str.clone().into())
        .await;

    task_manager
        .stage_result(
            task_id,
            make_result(worker_str.as_str(), worker_str.as_str(), Instant::now()),
        )
        .await
        .unwrap();
    task_manager.rollback_staged_result(task_id, &worker).await;

    assert!(task_manager.is_assigned(task_id, &worker).await);
    assert_eq!(task_manager.get_status(task_id).await, TaskStatus::NoResult);
    assert!(task_manager.get_result(task_id).await.is_none());

    let outcome = task_manager
        .add_result(
            task_id,
            make_result(worker_str.as_str(), worker_str.as_str(), Instant::now()),
        )
        .await
        .unwrap();
    assert!(
        outcome.completed,
        "retry after rollback should still succeed"
    );
}

#[tokio::test]
async fn stage_result_rejects_double_stage_for_same_worker() {
    let task_manager = TaskManager::new(
        4,
        1,
        Duration::from_millis(50),
        Duration::from_millis(500),
        Metrics::new(0.05).unwrap(),
        None,
    )
    .await;
    let worker: Hotkey = Hotkey::from_bytes(&[60u8; 32]);
    let worker_str = worker.to_string();
    let task_id = Uuid::new_v4();

    task_manager.add_task(sample_task(task_id)).await;
    task_manager
        .record_assignment(task_id, worker.clone(), worker_str.clone().into())
        .await;

    task_manager
        .stage_result(
            task_id,
            make_result(worker_str.as_str(), worker_str.as_str(), Instant::now()),
        )
        .await
        .unwrap();

    let second_stage = task_manager
        .stage_result(
            task_id,
            make_result(worker_str.as_str(), worker_str.as_str(), Instant::now()),
        )
        .await;
    assert!(matches!(second_stage, Err(AddResultError::AlreadyStaged)));
}

#[tokio::test]
async fn staged_result_defers_timeout_until_commit() {
    const CLEANUP_INTERVAL: Duration = Duration::from_millis(40);
    const RESULT_LIFETIME: Duration = Duration::from_millis(120);

    let metrics = Metrics::new(0.05).unwrap();
    let mutation_queue = RateLimitMutationBuffer::default();
    let limiter = DistributedRateLimiter::new(64);
    let task_manager = TaskManager::new_with_rate_limit_mutation_queue(TaskManagerInit {
        initial_capacity: 4,
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
            &sample_task(task_id),
            Some(RateLimitReservation::new(
                limiter,
                vec![RateLimitDelta {
                    subject: Subject::User,
                    id: 4_242u128,
                    day_epoch: 99,
                    add_active: 1,
                    add_day: 0,
                }],
            )),
        )
        .await;

    let worker: Hotkey = Hotkey::from_bytes(&[61u8; 32]);
    let worker_str = worker.to_string();
    task_manager
        .record_assignment(task_id, worker.clone(), worker_str.clone().into())
        .await;
    task_manager
        .stage_result(
            task_id,
            make_result(worker_str.as_str(), worker_str.as_str(), Instant::now()),
        )
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(
        RESULT_LIFETIME.as_millis() as u64 + 200,
    ))
    .await;

    assert!(
        task_manager.is_assigned(task_id, &worker).await,
        "timeout cleanup must defer while a staged result is waiting to commit"
    );
    assert_eq!(task_manager.get_status(task_id).await, TaskStatus::NoResult);
    assert!(
        task_manager.get_result(task_id).await.is_none(),
        "staged result must remain hidden before commit"
    );
    assert!(
        mutation_queue.drain_batch(1).await.is_none(),
        "timeout cleanup must not emit completion side effects while a staged result exists"
    );

    let outcome = task_manager
        .commit_staged_result(task_id, &worker)
        .await
        .unwrap();
    assert!(outcome.completed);
    assert!(
        mutation_queue.drain_batch(1).await.is_some(),
        "completion side effects should emit after the staged result commits"
    );
}

#[tokio::test]
async fn mixed_outcome_reports_success_when_expected_results_are_exhausted() {
    let task_manager = TaskManager::new(
        4,
        2,
        Duration::from_millis(50),
        Duration::from_millis(500),
        Metrics::new(0.05).unwrap(),
        None,
    )
    .await;
    let worker1: Hotkey = Hotkey::from_bytes(&[53u8; 32]);
    let worker2: Hotkey = Hotkey::from_bytes(&[54u8; 32]);
    let worker1_str = worker1.to_string();
    let worker2_str = worker2.to_string();
    let task_id = Uuid::new_v4();

    task_manager.add_task(sample_task(task_id)).await;
    task_manager
        .record_assignment(task_id, worker1.clone(), worker1_str.clone().into())
        .await;
    task_manager
        .record_assignment(task_id, worker2.clone(), worker2_str.clone().into())
        .await;
    task_manager
        .add_result(
            task_id,
            make_failed_result(worker1_str.as_str(), worker1_str.as_str(), Instant::now()),
        )
        .await
        .unwrap();
    task_manager
        .add_result(
            task_id,
            make_result(worker2_str.as_str(), worker2_str.as_str(), Instant::now()),
        )
        .await
        .unwrap();

    assert_eq!(
        task_manager.get_status(task_id).await,
        TaskStatus::Success {
            worker_id: worker2_str.into(),
        }
    );
}

#[tokio::test]
async fn one_of_one_success_reports_success() {
    let task_manager = TaskManager::new(
        4,
        1,
        Duration::from_millis(50),
        Duration::from_millis(500),
        Metrics::new(0.05).unwrap(),
        None,
    )
    .await;
    let worker: Hotkey = Hotkey::from_bytes(&[55u8; 32]);
    let worker_str = worker.to_string();
    let task_id = Uuid::new_v4();

    task_manager.add_task(sample_task(task_id)).await;
    task_manager
        .record_assignment(task_id, worker.clone(), worker_str.clone().into())
        .await;
    task_manager
        .add_result(
            task_id,
            make_result(worker_str.as_str(), worker_str.as_str(), Instant::now()),
        )
        .await
        .unwrap();

    assert_eq!(
        task_manager.get_status(task_id).await,
        TaskStatus::Success {
            worker_id: worker_str.into(),
        }
    );
}

#[tokio::test]
async fn partial_success_becomes_success_after_task_timeout() {
    const CLEANUP_INTERVAL: Duration = Duration::from_millis(40);
    const RESULT_LIFETIME: Duration = Duration::from_millis(120);

    let task_manager = TaskManager::new(
        4,
        2,
        CLEANUP_INTERVAL,
        RESULT_LIFETIME,
        Metrics::new(0.05).unwrap(),
        None,
    )
    .await;
    let worker1: Hotkey = Hotkey::from_bytes(&[56u8; 32]);
    let worker2: Hotkey = Hotkey::from_bytes(&[57u8; 32]);
    let worker1_str = worker1.to_string();
    let worker2_str = worker2.to_string();
    let task_id = Uuid::new_v4();

    task_manager.add_task(sample_task(task_id)).await;
    task_manager
        .record_assignment(task_id, worker1.clone(), worker1_str.clone().into())
        .await;
    task_manager
        .record_assignment(task_id, worker2.clone(), worker2_str.clone().into())
        .await;
    tokio::time::sleep(Duration::from_millis(40)).await;
    task_manager
        .add_result(
            task_id,
            make_result(worker1_str.as_str(), worker1_str.as_str(), Instant::now()),
        )
        .await
        .unwrap();

    assert_eq!(
        task_manager.get_status(task_id).await,
        TaskStatus::PartialResult(1)
    );

    tokio::time::sleep(Duration::from_millis(90)).await;

    assert_eq!(
        task_manager.get_status(task_id).await,
        TaskStatus::Success {
            worker_id: worker1_str.into(),
        }
    );
}

#[tokio::test]
async fn timeout_without_results_becomes_failure() {
    const CLEANUP_INTERVAL: Duration = Duration::from_millis(40);
    const RESULT_LIFETIME: Duration = Duration::from_millis(120);

    let task_manager = TaskManager::new(
        4,
        2,
        CLEANUP_INTERVAL,
        RESULT_LIFETIME,
        Metrics::new(0.05).unwrap(),
        None,
    )
    .await;
    let worker1: Hotkey = Hotkey::from_bytes(&[58u8; 32]);
    let worker2: Hotkey = Hotkey::from_bytes(&[59u8; 32]);
    let task_id = Uuid::new_v4();

    task_manager.add_task(sample_task(task_id)).await;
    task_manager
        .record_assignment(task_id, worker1.clone(), worker1.to_string().into())
        .await;
    task_manager
        .record_assignment(task_id, worker2.clone(), worker2.to_string().into())
        .await;

    tokio::time::sleep(Duration::from_millis(
        RESULT_LIFETIME.as_millis() as u64 + 120,
    ))
    .await;

    assert_eq!(
        task_manager.get_status(task_id).await,
        TaskStatus::Failure {
            reason: Arc::<str>::from("Task timed out"),
        }
    );
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
    task_manager.add_task(sample_task(task_id1)).await;
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
    task_manager.add_task(sample_task(task_id2)).await;
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
    task_manager.finalize_task(task_id2).await;

    let task_id3 = Uuid::new_v4();
    task_manager.add_task(sample_task(task_id3)).await;
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
    task_manager.finalize_task(task_id3).await;

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

    // task_id1 has an unfinished worker (worker2 never responded) so it times out as Failure
    assert_eq!(
        task_manager.get_status(task_id1).await,
        TaskStatus::Failure {
            reason: Arc::<str>::from("Task timed out"),
        }
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
        assignment_token: Uuid::nil(),
        asset: Some(Bytes::new()),
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
        assignment_token: Uuid::nil(),
        asset: Some(Bytes::new()),
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
                assignment_token: Uuid::nil(),
                asset: Some(Bytes::new()),
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

    // get_result is now idempotent — model and results remain until TTL expiry
    let results_bundle2 = task_manager
        .get_result(task_id)
        .await
        .expect("second get_result should still return results");
    assert_eq!(results_bundle2.model.as_deref(), Some("404-3dgs"));
    assert_eq!(results_bundle2.results.len(), 1);
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
                assignment_token: Uuid::nil(),
                asset: Some(Bytes::new()),
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
                    assignment_token: Uuid::nil(),
                    asset: Some(Bytes::new()),
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
    let mutation_queue = RateLimitMutationBuffer::default();
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
            &task,
            Some(RateLimitReservation::new(
                limiter.clone(),
                vec![RateLimitDelta {
                    subject: Subject::User,
                    id: 777u128,
                    day_epoch: 90,
                    add_active: 1,
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
                assignment_token: Uuid::nil(),
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
                assignment_token: Uuid::nil(),
                asset: None,
                reason: Some("worker failed".into()),
                instant: Instant::now(),
            },
        )
        .await
        .unwrap();
    assert!(completed.completed);

    let refund = pop_only_mutation(&mutation_queue).await;
    assert_eq!(refund.subject, Subject::User);
    assert_eq!(refund.id, 777u128);
    assert_eq!(refund.day_epoch, 90);
    assert_eq!(refund.active_delta, -1);
    assert_eq!(refund.day_delta, 0);
}

#[tokio::test]
async fn waits_for_expected_results_before_emitting_refund() {
    const CLEANUP_INTERVAL: Duration = Duration::from_millis(50);
    const RESULT_LIFETIME: Duration = Duration::from_millis(500);

    let metrics = Metrics::new(0.05).unwrap();
    let mutation_queue = RateLimitMutationBuffer::default();
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
            &Task {
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
                    day_epoch: 91,
                    add_active: 1,
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
                assignment_token: Uuid::nil(),
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
        mutation_queue.drain_batch(1).await.is_none(),
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
                assignment_token: Uuid::nil(),
                asset: None,
                reason: Some("worker failed".into()),
                instant: Instant::now(),
            },
        )
        .await
        .unwrap();
    assert!(second.completed);
    assert!(
        mutation_queue.drain_batch(1).await.is_some(),
        "refund should be emitted after final expected failure"
    );
}

#[tokio::test]
async fn does_not_emit_refund_when_timeout_occurs_after_partial_success() {
    const CLEANUP_INTERVAL: Duration = Duration::from_millis(40);
    const RESULT_LIFETIME: Duration = Duration::from_millis(120);

    let metrics = Metrics::new(0.05).unwrap();
    let mutation_queue = RateLimitMutationBuffer::default();
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
            &task,
            Some(RateLimitReservation::new(
                limiter.clone(),
                vec![RateLimitDelta {
                    subject: Subject::User,
                    id: 888u128,
                    day_epoch: 90,
                    add_active: 1,
                    add_day: 1,
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
                assignment_token: Uuid::nil(),
                asset: Some(Bytes::from_static(&[1, 2, 3])),
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

    // Partial success triggers the success path which releases the active slot
    // but must NOT refund day charges.
    let mutation = pop_only_mutation(&mutation_queue).await;
    assert_eq!(mutation.active_delta, -1, "active slot should be released");
    assert_eq!(mutation.day_delta, 0, "day charge must not be refunded");
    assert!(
        mutation_queue.drain_batch(1).await.is_none(),
        "only one mutation (active release) should be emitted"
    );
}

#[tokio::test]
async fn refund_rolls_back_local_pending_capacity() {
    const CLEANUP_INTERVAL: Duration = Duration::from_millis(50);
    const RESULT_LIFETIME: Duration = Duration::from_millis(500);

    let metrics = Metrics::new(0.05).unwrap();
    let mutation_queue = RateLimitMutationBuffer::default();
    let limiter = DistributedRateLimiter::new(64);
    let key = ClientKey {
        subject: Subject::User,
        id: 999u128,
    };
    let day_epoch = 90u64;

    assert!(
        limiter
            .check_and_incr(CheckAndIncrParams {
                key,
                active_limit: 1,
                daily_limit: 0,
                cluster_active: 0,
                cluster_day: 0,
                day_epoch,
            })
            .await
            .is_ok()
    );
    assert!(
        limiter
            .check_and_incr(CheckAndIncrParams {
                key,
                active_limit: 1,
                daily_limit: 0,
                cluster_active: 0,
                cluster_day: 0,
                day_epoch,
            })
            .await
            .is_err()
    );

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
            &Task {
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
                    day_epoch,
                    add_active: 1,
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
                assignment_token: Uuid::nil(),
                asset: None,
                reason: Some("worker failed".into()),
                instant: Instant::now(),
            },
        )
        .await
        .unwrap();
    assert!(completed.completed);

    let refund = pop_only_mutation(&mutation_queue).await;
    assert_eq!(refund.subject, Subject::User);
    assert_eq!(refund.id, 999u128);
    assert_eq!(refund.active_delta, -1);
    assert_eq!(refund.day_delta, 0);

    assert!(
        limiter
            .check_and_incr(CheckAndIncrParams {
                key,
                active_limit: 1,
                daily_limit: 0,
                cluster_active: 0,
                cluster_day: 0,
                day_epoch,
            })
            .await
            .is_ok(),
        "refund must restore local pending capacity immediately"
    );
}

#[tokio::test]
async fn stale_assignment_token_is_rejected_after_reassignment() {
    let task_manager = TaskManager::new(
        4,
        2,
        Duration::from_millis(50),
        Duration::from_secs(5),
        Metrics::new(0.05).unwrap(),
        None,
    )
    .await;

    let task_id = Uuid::new_v4();
    task_manager.add_task(sample_task(task_id)).await;

    let worker: Hotkey = Hotkey::from_bytes(&[55u8; 32]);
    let worker_id: Arc<str> = worker.to_string().into();
    let first_token = Uuid::new_v4();
    let second_token = Uuid::new_v4();

    task_manager
        .record_assignment_with_token(task_id, worker.clone(), worker_id.clone(), first_token)
        .await;
    let first_outcome = task_manager
        .add_result(
            task_id,
            AddTaskResultRequest {
                worker_hotkey: worker.clone(),
                worker_id: worker_id.clone(),
                assignment_token: first_token,
                asset: None,
                reason: Some("first attempt failed".into()),
                instant: Instant::now(),
            },
        )
        .await
        .expect("first result should be accepted");
    assert!(
        !first_outcome.completed,
        "task should remain open after the first failed attempt"
    );

    task_manager
        .record_assignment_with_token(task_id, worker.clone(), worker_id.clone(), second_token)
        .await;

    assert!(
        task_manager
            .is_assigned_with_token(task_id, &worker, second_token)
            .await
    );
    assert!(
        !task_manager
            .is_assigned_with_token(task_id, &worker, first_token)
            .await
    );

    let stale_stage = task_manager
        .stage_result(
            task_id,
            AddTaskResultRequest {
                worker_hotkey: worker.clone(),
                worker_id: worker_id.clone(),
                assignment_token: first_token,
                asset: Some(Bytes::new()),
                reason: None,
                instant: Instant::now(),
            },
        )
        .await;
    assert!(matches!(stale_stage, Err(AddResultError::NotAssigned)));

    task_manager
        .stage_result(
            task_id,
            AddTaskResultRequest {
                worker_hotkey: worker,
                worker_id,
                assignment_token: second_token,
                asset: Some(Bytes::new()),
                reason: None,
                instant: Instant::now(),
            },
        )
        .await
        .expect("current assignment token should still be accepted");
}
