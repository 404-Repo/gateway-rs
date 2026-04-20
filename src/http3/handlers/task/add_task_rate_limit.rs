use tokio::time::{Duration, sleep};
use tracing::error;

use crate::http3::error::ServerError;
use crate::http3::rate_limits::RateLimitReservation;
use crate::raft::gateway_state::GatewayState;
use crate::raft::store::RateLimitMutationBatch;

const RECONCILIATION_RETRY_INITIAL_DELAY: Duration = Duration::from_millis(250);
const RECONCILIATION_RETRY_MAX_DELAY: Duration = Duration::from_secs(5);
const RECONCILIATION_RETRY_MAX_ATTEMPTS: u32 = 12;

pub(super) struct PendingRateLimitRollbackGuard {
    reservation: Option<RateLimitReservation>,
    published_charge_rollback: Option<(GatewayState, RateLimitMutationBatch)>,
}

impl PendingRateLimitRollbackGuard {
    pub(super) fn new(reservation: Option<RateLimitReservation>) -> Self {
        Self {
            reservation,
            published_charge_rollback: None,
        }
    }

    pub(super) fn arm(&mut self, reservation: Option<RateLimitReservation>) {
        self.reservation = reservation;
    }

    pub(super) fn arm_published_charge_rollback(
        &mut self,
        gateway_state: GatewayState,
        batch: RateLimitMutationBatch,
    ) {
        if batch.is_empty() {
            self.published_charge_rollback = None;
        } else {
            self.published_charge_rollback = Some((gateway_state, batch));
        }
    }

    pub(super) fn disarm(&mut self) {
        self.reservation = None;
        self.published_charge_rollback = None;
    }

    pub(super) fn has_pending_work(&self) -> bool {
        self.reservation.is_some() || self.published_charge_rollback.is_some()
    }

    pub(super) async fn rollback_now(&mut self) {
        if let Some(reservation) = self.reservation.as_ref() {
            reservation.rollback_pending().await;
        }
        self.reservation = None;
        if let Some((gateway_state, batch)) = self.published_charge_rollback.take() {
            rollback_published_charge(gateway_state, batch).await;
        }
    }
}

impl Drop for PendingRateLimitRollbackGuard {
    fn drop(&mut self) {
        let reservation = self.reservation.take();
        let published_charge_rollback = self.published_charge_rollback.take();

        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                if let Some(reservation) = reservation {
                    reservation.rollback_pending().await;
                }
                if let Some((gateway_state, batch)) = published_charge_rollback {
                    rollback_published_charge(gateway_state, batch).await;
                }
            });
        }
    }
}

pub(super) async fn publish_accepted_reservation(
    gateway_state: &GatewayState,
    reservation: &RateLimitReservation,
) -> Result<Option<RateLimitMutationBatch>, ServerError> {
    let Some(charge_batch) = reservation.accepted_charge_batch() else {
        return Ok(None);
    };
    let rollback_batch = reservation.refund_batch();

    gateway_state
        .submit_rate_limit_mutation_batch(&charge_batch, None)
        .await
        .map_err(|err| {
            if let Some(rollback_batch) = rollback_batch.clone() {
                reconcile_failed_accepted_publish(
                    gateway_state.clone(),
                    charge_batch.clone(),
                    rollback_batch,
                );
            }
            error!(
                error = ?err,
                request_id = charge_batch.request_id,
                retry_context = "publish accepted task reservation",
                "Failed to publish accepted task reservation"
            );
            ServerError::ServiceUnavailable("Task limiter is unavailable".to_string())
        })?;

    Ok(rollback_batch)
}

async fn retry_rate_limit_batch(
    gateway_state: &GatewayState,
    batch: &RateLimitMutationBatch,
    retry_context: &'static str,
) -> bool {
    let mut delay = RECONCILIATION_RETRY_INITIAL_DELAY;
    for attempt in 1..=RECONCILIATION_RETRY_MAX_ATTEMPTS {
        match gateway_state
            .submit_rate_limit_mutation_batch(batch, None)
            .await
        {
            Ok(_) => return true,
            Err(err) => {
                if attempt == RECONCILIATION_RETRY_MAX_ATTEMPTS {
                    error!(
                        error = ?err,
                        attempt,
                        max_attempts = RECONCILIATION_RETRY_MAX_ATTEMPTS,
                        request_id = batch.request_id,
                        retry_context,
                        "Rate limit reconciliation exhausted"
                    );
                    return false;
                }

                error!(
                    error = ?err,
                    attempt,
                    max_attempts = RECONCILIATION_RETRY_MAX_ATTEMPTS,
                    request_id = batch.request_id,
                    retry_context,
                    "Retrying rate limit reconciliation"
                );
                sleep(delay).await;
                delay = (delay * 2).min(RECONCILIATION_RETRY_MAX_DELAY);
            }
        }
    }
    false
}

fn reconcile_failed_accepted_publish(
    gateway_state: GatewayState,
    charge_batch: RateLimitMutationBatch,
    rollback_batch: RateLimitMutationBatch,
) {
    if charge_batch.is_empty() {
        return;
    }

    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        handle.spawn(async move {
            if retry_rate_limit_batch(
                &gateway_state,
                &charge_batch,
                "publish accepted task reservation before rollback",
            )
            .await
            {
                rollback_published_charge(gateway_state, rollback_batch).await;
            }
        });
    }
}

async fn rollback_published_charge(gateway_state: GatewayState, batch: RateLimitMutationBatch) {
    if batch.is_empty() {
        return;
    }

    if let Err(err) = gateway_state
        .submit_rate_limit_mutation_batch(&batch, None)
        .await
    {
        error!(
            error = ?err,
            request_id = batch.request_id,
            retry_context = "rollback accepted task charge",
            "Failed to rollback published accepted task charge"
        );
        reconcile_failed_charge_rollback(gateway_state, batch);
    }
}

fn reconcile_failed_charge_rollback(gateway_state: GatewayState, batch: RateLimitMutationBatch) {
    if batch.is_empty() {
        return;
    }

    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        handle.spawn(async move {
            let _ = retry_rate_limit_batch(&gateway_state, &batch, "rollback accepted task charge")
                .await;
        });
    }
}

#[cfg(test)]
mod tests {
    use super::{PendingRateLimitRollbackGuard, RateLimitReservation};
    use crate::raft::rate_limit::{CheckAndIncrParams, ClientKey, DistributedRateLimiter};
    use crate::raft::store::{RateLimitDelta, Subject};

    #[tokio::test]
    async fn rollback_guard_restores_local_pending_capacity_on_drop() {
        let limiter = DistributedRateLimiter::new(64);
        let key = ClientKey {
            subject: Subject::User,
            id: 42u128,
        };
        let day_epoch = 50u64;

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

        {
            let reservation = RateLimitReservation::new(
                limiter.clone(),
                vec![RateLimitDelta {
                    subject: key.subject,
                    id: key.id,
                    day_epoch,
                    add_active: 1,
                    add_day: 0,
                }],
            );
            let mut guard = PendingRateLimitRollbackGuard::new(None);
            guard.arm(Some(reservation));
        }

        tokio::task::yield_now().await;

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
            "rollback guard should release pending local capacity"
        );
    }

    #[tokio::test]
    async fn rollback_guard_restores_local_pending_capacity_on_explicit_rollback() {
        let limiter = DistributedRateLimiter::new(64);
        let key = ClientKey {
            subject: Subject::User,
            id: 99u128,
        };
        let day_epoch = 80u64;

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

        let reservation = RateLimitReservation::new(
            limiter.clone(),
            vec![RateLimitDelta {
                subject: key.subject,
                id: key.id,
                day_epoch,
                add_active: 1,
                add_day: 0,
            }],
        );
        let mut guard = PendingRateLimitRollbackGuard::new(Some(reservation));
        guard.rollback_now().await;

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
            "explicit rollback should release pending local capacity"
        );
    }
}
