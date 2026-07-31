use anyhow::{Result, anyhow};
use foldhash::fast::RandomState;
use scc::HashMap;
use scc::hash_map::Entry;
use serde::Serialize;
use std::io::{self, Write};
use std::sync::Arc;

use crate::raft::store::{RateLimitMutation, RateLimitMutationBatch, Subject};

type AggregatedMutationKey = (Subject, u128, u64);
type AggregatedMutationDelta = (i64, i64);

#[derive(Serialize)]
enum RateLimitRequestRef<'a> {
    RateLimitMutations {
        request_id: u128,
        mutations: &'a [RateLimitMutation],
    },
}

#[derive(Default)]
struct SerializedSize(usize);

impl Write for SerializedSize {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0 = self.0.saturating_add(buf.len());
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn serialized_request_size(
    request_id: u128,
    mutations: &[RateLimitMutation],
) -> Result<usize, rmp_serde::encode::Error> {
    let mut size = SerializedSize::default();
    rmp_serde::encode::write_named(
        &mut size,
        &RateLimitRequestRef::RateLimitMutations {
            request_id,
            mutations,
        },
    )?;
    Ok(size.0)
}

fn fitting_mutation_prefix(
    batch: &RateLimitMutationBatch,
    max_serialized_bytes: usize,
) -> Result<usize, rmp_serde::encode::Error> {
    if serialized_request_size(batch.request_id, &batch.mutations)? <= max_serialized_bytes {
        return Ok(batch.mutations.len());
    }

    let mut fitting = 0;
    let mut too_large = batch.mutations.len();
    while fitting + 1 < too_large {
        let candidate = fitting + (too_large - fitting) / 2;
        if serialized_request_size(batch.request_id, &batch.mutations[..candidate])?
            <= max_serialized_bytes
        {
            fitting = candidate;
        } else {
            too_large = candidate;
        }
    }
    Ok(fitting)
}

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

    pub async fn drain_batch_with_max_bytes(
        &self,
        max_mutations: usize,
        max_serialized_bytes: usize,
    ) -> Result<Option<RateLimitMutationBatch>> {
        let Some(mut batch) = self.drain_batch(max_mutations).await else {
            return Ok(None);
        };

        let fitting = match fitting_mutation_prefix(&batch, max_serialized_bytes) {
            Ok(fitting) => fitting,
            Err(error) => {
                self.push(batch).await;
                return Err(error.into());
            }
        };

        if fitting == 0 {
            let minimum_size =
                match serialized_request_size(batch.request_id, &batch.mutations[..1]) {
                    Ok(size) => size,
                    Err(error) => {
                        self.push(batch).await;
                        return Err(error.into());
                    }
                };
            self.push(batch).await;
            return Err(anyhow!(
                "rate-limit Raft request byte limit {max_serialized_bytes} is smaller than one mutation ({minimum_size} bytes)"
            ));
        }

        if fitting < batch.mutations.len() {
            let overflow = batch.mutations.split_off(fitting);
            // The aggregation buffer does not retain request IDs; the next drain
            // assigns a new ID to the overflow batch.
            self.push(RateLimitMutationBatch::new(0, overflow)).await;
        }

        Ok(Some(batch))
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
