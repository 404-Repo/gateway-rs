use crate::common::rate_limit_buffer::RateLimitMutationBuffer;
use crate::protocol::RaftMessageType;
use crate::raft::Gateway;
use crate::raft::TypeConfig;
use crate::raft::store::{RateLimitMutation, RateLimitMutationBatch, Request, Subject};
use openraft::raft::AppendEntriesRequest;
use openraft::{Entry, EntryPayload, LeaderId, LogId, Vote};

const TEST_MAX_MESSAGE_SIZE: usize = 256 * 1024;
const TEST_MAX_PAYLOAD_ENTRIES: u64 = 100;

fn mutation(subject: Subject, id: u128, day_delta: i64) -> RateLimitMutation {
    RateLimitMutation {
        subject,
        id,
        day_epoch: 20,
        active_delta: 0,
        day_delta,
    }
}

fn batch(request_id: u128, mutations: Vec<RateLimitMutation>) -> RateLimitMutationBatch {
    RateLimitMutationBatch::new(request_id, mutations)
}

#[tokio::test]
async fn does_not_drain_new_mutations_while_retry_batch_is_pending() {
    let queue = RateLimitMutationBuffer::default();
    queue
        .push(batch(200, vec![mutation(Subject::User, 200, 1)]))
        .await;

    let mut pending = Some(batch(123, vec![mutation(Subject::User, 100, 1)]));

    Gateway::collect_pending_rate_limit_mutations(
        &queue,
        64,
        TEST_MAX_MESSAGE_SIZE,
        TEST_MAX_PAYLOAD_ENTRIES,
        &mut pending,
    )
    .await
    .expect("collect pending mutations");

    let pending = pending.expect("retry batch should remain intact");
    assert_eq!(pending.mutations.len(), 1);
    assert_eq!(pending.mutations[0].id, 100u128);
    assert_eq!(pending.request_id, 123u128);

    let queued = queue
        .drain_batch(64)
        .await
        .expect("new mutation must remain queued while retrying");
    assert_eq!(queued.mutations[0].id, 200u128);
}

#[tokio::test]
async fn coalesces_signed_mutations_per_subject_key_epoch() {
    let queue = RateLimitMutationBuffer::default();
    queue
        .push(batch(1, vec![mutation(Subject::User, 500, 3)]))
        .await;
    queue
        .push(batch(2, vec![mutation(Subject::User, 500, -2)]))
        .await;
    queue
        .push(batch(3, vec![mutation(Subject::User, 500, -1)]))
        .await;

    let mut pending = None;

    Gateway::collect_pending_rate_limit_mutations(
        &queue,
        64,
        TEST_MAX_MESSAGE_SIZE,
        TEST_MAX_PAYLOAD_ENTRIES,
        &mut pending,
    )
    .await
    .expect("collect pending mutations");

    // Net-zero entries are dropped.
    assert!(pending.is_none());

    queue
        .push(batch(4, vec![mutation(Subject::User, 500, 5)]))
        .await;
    queue
        .push(batch(5, vec![mutation(Subject::User, 500, -2)]))
        .await;
    Gateway::collect_pending_rate_limit_mutations(
        &queue,
        64,
        TEST_MAX_MESSAGE_SIZE,
        TEST_MAX_PAYLOAD_ENTRIES,
        &mut pending,
    )
    .await
    .expect("collect pending mutations");
    let pending = pending.expect("coalesced batch");
    assert_eq!(pending.mutations.len(), 1);
    assert_eq!(pending.mutations[0].day_delta, 3);
}

#[tokio::test]
async fn respects_batch_limit_when_collecting_mutations() {
    let queue = RateLimitMutationBuffer::default();
    queue
        .push(batch(1, vec![mutation(Subject::User, 1, 1)]))
        .await;
    queue
        .push(batch(2, vec![mutation(Subject::User, 2, 1)]))
        .await;
    queue
        .push(batch(3, vec![mutation(Subject::User, 3, 1)]))
        .await;

    let mut pending = None;

    Gateway::collect_pending_rate_limit_mutations(
        &queue,
        2,
        TEST_MAX_MESSAGE_SIZE,
        TEST_MAX_PAYLOAD_ENTRIES,
        &mut pending,
    )
    .await
    .expect("collect pending mutations");

    let pending = pending.expect("pending batch should be created");
    assert_eq!(pending.mutations.len(), 2);
    assert_eq!(queue.len(), 1);
}

#[tokio::test]
async fn byte_bounds_a_full_raft_entry_batch_to_the_transport_limit() {
    const CONFIGURED_BATCH_SIZE: usize = 8_192;

    let queue = RateLimitMutationBuffer::default();
    let mutations = (0..CONFIGURED_BATCH_SIZE)
        .map(|index| RateLimitMutation {
            subject: Subject::Company,
            id: u128::MAX - index as u128,
            day_epoch: u64::MAX,
            active_delta: i64::MAX,
            day_delta: i64::MAX,
        })
        .collect();
    queue.push(batch(1, mutations)).await;

    let mut pending = None;
    Gateway::collect_pending_rate_limit_mutations(
        &queue,
        CONFIGURED_BATCH_SIZE,
        TEST_MAX_MESSAGE_SIZE,
        TEST_MAX_PAYLOAD_ENTRIES,
        &mut pending,
    )
    .await
    .expect("collect byte-bounded mutations");

    let pending = pending.expect("pending batch should be created");
    assert!(pending.mutations.len() < CONFIGURED_BATCH_SIZE);
    assert_eq!(pending.mutations.len() + queue.len(), CONFIGURED_BATCH_SIZE);

    let entries = (0..TEST_MAX_PAYLOAD_ENTRIES)
        .map(|index| Entry::<TypeConfig> {
            log_id: LogId::new(LeaderId::new(u64::MAX, u64::MAX), index),
            payload: EntryPayload::Normal(Request::RateLimitMutations {
                request_id: pending.request_id,
                mutations: pending.mutations.clone(),
            }),
        })
        .collect();
    let message = RaftMessageType::AppendEntriesRequest(AppendEntriesRequest {
        vote: Vote::new_committed(u64::MAX, u64::MAX),
        prev_log_id: Some(LogId::new(LeaderId::new(u64::MAX, u64::MAX), u64::MAX)),
        entries,
        leader_commit: Some(LogId::new(LeaderId::new(u64::MAX, u64::MAX), u64::MAX)),
    });
    let encoded = rmp_serde::to_vec_named(&message).expect("serialize AppendEntries request");
    assert!(
        encoded.len() <= TEST_MAX_MESSAGE_SIZE,
        "encoded AppendEntries request is {} bytes",
        encoded.len()
    );
}
