use scc::Queue;

use crate::raft::Gateway;
use crate::raft::store::{RateLimitMutation, Subject};

fn mutation(subject: Subject, id: u128, hour_delta: i64, day_delta: i64) -> RateLimitMutation {
    RateLimitMutation {
        subject,
        id,
        hour_epoch: 10,
        day_epoch: 20,
        hour_delta,
        day_delta,
    }
}

#[test]
fn does_not_drain_new_mutations_while_retry_batch_is_pending() {
    let queue = Queue::<RateLimitMutation>::default();
    queue.push(mutation(Subject::User, 200, 1, 0));

    let mut pending = vec![mutation(Subject::User, 100, 1, 0)];
    let mut pending_request_id = Some(123u128);

    Gateway::collect_pending_rate_limit_mutations(
        &queue,
        64,
        &mut pending,
        &mut pending_request_id,
    );

    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].id, 100u128);
    assert_eq!(pending_request_id, Some(123u128));

    let queued = queue
        .pop()
        .expect("new mutation must remain queued while retrying");
    assert_eq!(queued.id, 200u128);
}

#[test]
fn coalesces_signed_mutations_per_subject_key_epoch() {
    let queue = Queue::<RateLimitMutation>::default();
    queue.push(mutation(Subject::User, 500, 3, 1));
    queue.push(mutation(Subject::User, 500, -2, 0));
    queue.push(mutation(Subject::User, 500, -1, -1));

    let mut pending = Vec::<RateLimitMutation>::new();
    let mut pending_request_id = None;

    Gateway::collect_pending_rate_limit_mutations(
        &queue,
        64,
        &mut pending,
        &mut pending_request_id,
    );

    // Net-zero entries are dropped.
    assert!(pending.is_empty());
    assert!(pending_request_id.is_none());

    queue.push(mutation(Subject::User, 500, 5, 0));
    queue.push(mutation(Subject::User, 500, -2, 0));
    Gateway::collect_pending_rate_limit_mutations(
        &queue,
        64,
        &mut pending,
        &mut pending_request_id,
    );
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].hour_delta, 3);
    assert_eq!(pending[0].day_delta, 0);
}

#[test]
fn respects_batch_limit_when_collecting_mutations() {
    let queue = Queue::<RateLimitMutation>::default();
    queue.push(mutation(Subject::User, 1, 1, 0));
    queue.push(mutation(Subject::User, 2, 1, 0));
    queue.push(mutation(Subject::User, 3, 1, 0));

    let mut pending = Vec::<RateLimitMutation>::new();
    let mut pending_request_id = None;

    Gateway::collect_pending_rate_limit_mutations(&queue, 2, &mut pending, &mut pending_request_id);

    assert_eq!(pending.len(), 2);
    assert!(pending_request_id.is_some());
    assert_eq!(queue.len(), 1);
}
