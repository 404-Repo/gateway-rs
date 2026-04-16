use foldhash::fast::RandomState;
use scc::HashMap;
use scc::hash_map::Entry;
use std::sync::Arc;

use crate::raft::store::{RateLimitMutation, RateLimitMutationBatch, Subject};

type AggregatedMutationKey = (Subject, u128, u64);
type AggregatedMutationDelta = (i64, i64);
/// Aggregates pending rate-limit mutations so retries do not create
/// an unbounded backlog of raw batches in memory.
#[derive(Clone)]
pub struct RateLimitMutationBuffer {
    inner: Arc<HashMap<AggregatedMutationKey, AggregatedMutationDelta, RandomState>>,
}

impl Default for RateLimitMutationBuffer {
    fn default() -> Self {
        Self {
            inner: Arc::new(HashMap::with_capacity_and_hasher(
                64,
                RandomState::default(),
            )),
        }
    }
}

impl RateLimitMutationBuffer {
    pub async fn push(&self, batch: RateLimitMutationBatch) {
        if batch.is_empty() {
            return;
        }

        for mutation in batch.mutations {
            let key = (mutation.subject, mutation.id, mutation.day_epoch);
            match self.inner.entry_async(key).await {
                Entry::Occupied(mut entry) => {
                    let accumulated = entry.get_mut();
                    accumulated.0 = accumulated.0.saturating_add(mutation.active_delta);
                    accumulated.1 = accumulated.1.saturating_add(mutation.day_delta);
                    if accumulated.0 == 0 && accumulated.1 == 0 {
                        let _ = entry.remove_entry();
                    }
                }
                Entry::Vacant(entry) => {
                    if mutation.active_delta != 0 || mutation.day_delta != 0 {
                        entry.insert_entry((mutation.active_delta, mutation.day_delta));
                    }
                }
            }
        }
    }

    pub async fn drain_batch(&self, max_mutations: usize) -> Option<RateLimitMutationBatch> {
        let limit = max_mutations.max(1);
        let mut mutations = Vec::with_capacity(limit);

        let _ = self
            .inner
            .iter_mut_async(|entry| {
                if mutations.len() >= limit {
                    return false;
                }

                let ((subject, id, day_epoch), (active_delta, day_delta)) = entry.consume();
                if active_delta != 0 || day_delta != 0 {
                    mutations.push(RateLimitMutation {
                        subject,
                        id,
                        day_epoch,
                        active_delta,
                        day_delta,
                    });
                }
                true
            })
            .await;

        RateLimitMutationBatch::with_generated_request_id(mutations)
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
