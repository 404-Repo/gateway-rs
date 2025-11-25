use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use rand::Rng;
use uuid::Uuid;

use super::{TaskManager, TaskStatus};
use crate::api::request::AddTaskResultRequest;
use crate::api::Task;
use crate::bittensor::hotkey::Hotkey;
use crate::metrics::Metrics;

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

fn make_result(validator: &str, miner: &str, score: f32, instant: Instant) -> AddTaskResultRequest {
    AddTaskResultRequest {
        validator_hotkey: validator.parse().unwrap(),
        miner_hotkey: Some(miner.parse().unwrap()),
        miner_uid: None,
        miner_rating: None,
        asset: Some(vec![]),
        score: Some(score),
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
    )
    .await;
    let now = Instant::now();

    let task_id1 = Uuid::new_v4();
    task_manager
        .add_result(
            task_id1,
            make_result(
                "5GTmkzxbXSFh8ApLU24fzWUu2asZs89V5eJnN3ufubTg9Pj7",
                "5GTmkzxbXSFh8ApLU24fzWUu2asZs89V5eJnN3ufubTg9Pj7",
                0.5,
                now - Duration::from_millis(600),
            ),
        )
        .await;

    let task_id2 = Uuid::new_v4();
    task_manager
        .add_result(
            task_id2,
            make_result(
                "5GTmkzxbXSFh8ApLU24fzWUu2asZs89V5eJnN3ufubTg9Pj7",
                "5GTmkzxbXSFh8ApLU24fzWUu2asZs89V5eJnN3ufubTg9Pj7",
                0.6,
                now - Duration::from_millis(600),
            ),
        )
        .await;
    task_manager
        .add_result(
            task_id2,
            make_result(
                "5GTmkzxbXSFh8ApLU24fzWUu2asZs89V5eJnN3ufubTg9Pj7",
                "5GTmkzxbXSFh8ApLU24fzWUu2asZs89V5eJnN3ufubTg9Pj7",
                0.7,
                now - Duration::from_millis(200),
            ),
        )
        .await;

    let task_id3 = Uuid::new_v4();
    task_manager
        .add_result(
            task_id3,
            make_result(
                "5GTmkzxbXSFh8ApLU24fzWUu2asZs89V5eJnN3ufubTg9Pj7",
                "5GTmkzxbXSFh8ApLU24fzWUu2asZs89V5eJnN3ufubTg9Pj7",
                0.8,
                now - Duration::from_millis(100),
            ),
        )
        .await;
    task_manager
        .add_result(
            task_id3,
            make_result(
                "5GTmkzxbXSFh8ApLU24fzWUu2asZs89V5eJnN3ufubTg9Pj7",
                "5GTmkzxbXSFh8ApLU24fzWUu2asZs89V5eJnN3ufubTg9Pj7",
                0.9,
                now - Duration::from_millis(100),
            ),
        )
        .await;

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
    let task_manager = TaskManager::new(4, 2, CLEANUP_INTERVAL, RESULT_LIFETIME, metrics).await;

    let task_id = Uuid::new_v4();
    let image = Bytes::from_static(b"image-bytes");
    let task = Task {
        id: task_id,
        prompt: None,
        image: Some(image.clone()),
    };
    task_manager.add_task(task).await;

    let first_validator: Hotkey = Hotkey::from_bytes(&[1u8; 32]);
    let second_validator: Hotkey = Hotkey::from_bytes(&[2u8; 32]);

    task_manager
        .record_assignment(task_id, first_validator.clone())
        .await;
    task_manager
        .record_assignment(task_id, second_validator.clone())
        .await;

    let first_result = AddTaskResultRequest {
        validator_hotkey: first_validator.clone(),
        miner_hotkey: Some(first_validator.clone()),
        miner_uid: None,
        miner_rating: None,
        asset: Some(vec![]),
        score: Some(0.4),
        reason: None,
        instant: Instant::now(),
    };

    let complete_after_first = task_manager.add_result(task_id, first_result).await;
    assert!(
        !complete_after_first,
        "task should wait for all assignments before cleanup"
    );
    assert!(
        task_manager.get_image(task_id).await.is_some(),
        "image payload was cleaned up before all validators responded"
    );

    let second_result = AddTaskResultRequest {
        validator_hotkey: second_validator.clone(),
        miner_hotkey: Some(second_validator),
        miner_uid: None,
        miner_rating: None,
        asset: Some(vec![]),
        score: Some(0.5),
        reason: None,
        instant: Instant::now(),
    };

    let complete_after_second = task_manager.add_result(task_id, second_result).await;
    assert!(
        complete_after_second,
        "last validator should allow cleanup to proceed"
    );
    task_manager.finalize_task(task_id).await;

    assert!(
        task_manager.get_image(task_id).await.is_none(),
        "payload should be cleaned up after final result"
    );
}

#[tokio::test]
async fn tasks_in_progress_gauge_tracks_assignments() {
    const CLEANUP_INTERVAL: Duration = Duration::from_millis(50);
    const RESULT_LIFETIME: Duration = Duration::from_millis(500);

    let metrics = Metrics::new(0.05).unwrap();
    let task_manager =
        TaskManager::new(2, 1, CLEANUP_INTERVAL, RESULT_LIFETIME, metrics.clone()).await;

    let task_id = Uuid::new_v4();
    let validator: Hotkey = Hotkey::from_bytes(&[7u8; 32]);
    let task = Task {
        id: task_id,
        prompt: Some(Arc::new("hello".to_string())),
        image: None,
    };
    task_manager.add_task(task).await;
    task_manager
        .record_assignment(task_id, validator.clone())
        .await;

    let mut in_progress = None;
    for mf in metrics.registry().gather() {
        if mf.name() == "tasks_in_progress" {
            for m in &mf.metric {
                let matches_validator = m
                    .label
                    .iter()
                    .any(|lp| lp.name() == "validator" && lp.value() == validator.to_string());
                if matches_validator {
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
                validator_hotkey: validator.clone(),
                miner_hotkey: Some(validator.clone()),
                miner_uid: None,
                miner_rating: None,
                asset: Some(vec![]),
                score: Some(0.2),
                reason: None,
                instant: Instant::now(),
            },
        )
        .await;
    task_manager.finalize_task(task_id).await;

    let mut in_progress_after = None;
    for mf in metrics.registry().gather() {
        if mf.name() == "tasks_in_progress" {
            for m in &mf.metric {
                let matches_validator = m
                    .label
                    .iter()
                    .any(|lp| lp.name() == "validator" && lp.value() == validator.to_string());
                if matches_validator {
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
    )
    .await;

    let task_id = Uuid::new_v4();
    let task = Task {
        id: task_id,
        prompt: Some(Arc::new("batch".to_string())),
        image: None,
    };
    task_manager.add_task(task).await;

    let mut validators = Vec::with_capacity(assignment_count);
    for idx in 0..assignment_count {
        let v: Hotkey = Hotkey::from_bytes(&[(idx as u8) + 1; 32]);
        task_manager.record_assignment(task_id, v.clone()).await;
        validators.push(v);
    }

    assert_eq!(
        tasks_in_progress_total(&metrics),
        assignment_count as f64,
        "gauge should equal number of assignments"
    );

    let mut completed = false;
    for (i, v) in validators.iter().enumerate() {
        completed = task_manager
            .add_result(
                task_id,
                AddTaskResultRequest {
                    validator_hotkey: v.clone(),
                    miner_hotkey: Some(v.clone()),
                    miner_uid: None,
                    miner_rating: None,
                    asset: Some(vec![]),
                    score: Some(i as f32 + 0.1),
                    reason: None,
                    instant: Instant::now(),
                },
            )
            .await;
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
    let task_manager =
        TaskManager::new(8, 1, CLEANUP_INTERVAL, RESULT_LIFETIME, metrics.clone()).await;

    let task_id = Uuid::new_v4();
    let task = Task {
        id: task_id,
        prompt: None,
        image: Some(Bytes::from_static(b"")),
    };
    task_manager.add_task(task).await;

    let validator: Hotkey = Hotkey::from_bytes(&[42u8; 32]);
    task_manager
        .record_assignment(task_id, validator.clone())
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
                    .any(|lp| lp.name() == "validator" && lp.value() == validator.to_string());
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
    assert!(found, "timeout_failures_total for validator was not found");
}
