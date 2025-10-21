use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use foldhash::fast::RandomState;
use salvo::{handler, Depot, Request};
use scc::HashCache;

use crate::config::HTTPConfig;
use crate::http3::error::ServerError;
use crate::http3::rate_limits::RateLimitContext;
use crate::raft::gateway_state::GatewayState;
use crate::raft::store::{RateLimitDelta, Subject};

#[derive(Copy, Clone, Eq, PartialEq, Hash)]
pub struct ClientKey {
    pub subject: Subject,
    pub id: u128,
}

#[derive(Copy, Clone, Default)]
pub struct Window {
    pub hour_epoch: u64,
    pub day_epoch: u64,
    // Local increments waiting to be reconciled with the cluster snapshot
    pub hour: u32,
    pub day: u32,
    // Cluster totals last observed, used to drop already-applied pending counts
    pub cluster_hour_seen: u64,
    pub cluster_day_seen: u64,
}

#[derive(Clone)]
pub struct DistributedRateLimiter {
    inner: Arc<HashCache<ClientKey, Window, RandomState>>,
}

impl DistributedRateLimiter {
    pub fn new(max_capacity: usize) -> Self {
        let max_capacity = max_capacity.max(1);
        let min_capacity = max_capacity.min(8192);
        Self {
            inner: Arc::new(HashCache::with_capacity_and_hasher(
                min_capacity,
                max_capacity,
                RandomState::default(),
            )),
        }
    }

    pub(crate) fn current_epochs() -> (u64, u64) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        (now / 3600, now / 86400)
    }

    pub async fn check_and_incr(
        &self,
        key: ClientKey,
        hourly_limit: u64,
        daily_limit: u64,
        cluster_hour: u64,
        cluster_day: u64,
        epochs: (u64, u64),
    ) -> bool {
        if hourly_limit == 0 && daily_limit == 0 {
            return true;
        }

        let (hour_epoch, day_epoch) = epochs;
        let mut allowed = true;
        self.inner
            .entry_async(key)
            .await
            .and_modify(|w| {
                // On hour rollover, reset local tally against the fresh snapshot
                if w.hour_epoch != hour_epoch {
                    w.hour = 0;
                    w.hour_epoch = hour_epoch;
                    w.cluster_hour_seen = cluster_hour;
                }
                // Same for the day window
                if w.day_epoch != day_epoch {
                    w.day = 0;
                    w.day_epoch = day_epoch;
                    w.cluster_day_seen = cluster_day;
                }

                // Drop local pending counts already reflected in the cluster snapshot
                if let Some(delta) = cluster_hour.checked_sub(w.cluster_hour_seen) {
                    if delta > 0 {
                        let to_sub = delta.min(w.hour as u64) as u32;
                        w.hour = w.hour.saturating_sub(to_sub);
                        w.cluster_hour_seen = cluster_hour;
                    }
                }
                if let Some(delta) = cluster_day.checked_sub(w.cluster_day_seen) {
                    if delta > 0 {
                        let to_sub = delta.min(w.day as u64) as u32;
                        w.day = w.day.saturating_sub(to_sub);
                        w.cluster_day_seen = cluster_day;
                    }
                }

                // Combine cluster totals with remaining local increments
                let eff_hour_total = cluster_hour + w.hour as u64;
                let eff_day_total = cluster_day + w.day as u64;

                if (hourly_limit > 0 && eff_hour_total >= hourly_limit)
                    || (daily_limit > 0 && eff_day_total >= daily_limit)
                {
                    allowed = false;
                    return;
                }

                w.hour = w.hour.saturating_add(1);
                w.day = w.day.saturating_add(1);
            })
            .or_put_with(|| {
                let allow_initial = !((hourly_limit > 0 && cluster_hour >= hourly_limit)
                    || (daily_limit > 0 && cluster_day >= daily_limit));

                if !allow_initial {
                    allowed = false;
                }

                Window {
                    hour_epoch,
                    day_epoch,
                    hour: if allow_initial { 1 } else { 0 },
                    day: if allow_initial { 1 } else { 0 },
                    cluster_hour_seen: cluster_hour,
                    cluster_day_seen: cluster_day,
                }
            });

        allowed
    }
}

async fn cluster_usage(
    gs: &GatewayState,
    subject: Subject,
    id: u128,
    hour_epoch: u64,
    day_epoch: u64,
    require_day_match: bool,
) -> (u64, u64) {
    match gs.get_cluster_rate_window(subject, id).await {
        Some(w)
            if w.hour_epoch == hour_epoch && (!require_day_match || w.day_epoch == day_epoch) =>
        {
            let day = if require_day_match { w.day } else { 0 };
            (w.hour, day)
        }
        _ => (0, 0),
    }
}

struct SubjectParams<'a> {
    subject: Subject,
    id: u128,
    hourly_limit: u64,
    daily_limit: u64,
    add_day: u16,
    error_msg: &'a str,
    require_day_match: bool,
}

async fn enforce_subject(
    limiter: &DistributedRateLimiter,
    gs: &GatewayState,
    epochs: (u64, u64),
    params: SubjectParams<'_>,
) -> Result<(), ServerError> {
    let SubjectParams {
        subject,
        id,
        hourly_limit,
        daily_limit,
        add_day,
        error_msg,
        require_day_match,
    } = params;

    let (hour_epoch, day_epoch) = epochs;
    let has_limits = hourly_limit > 0 || daily_limit > 0;
    let (cluster_hour, cluster_day) = if has_limits {
        cluster_usage(gs, subject, id, hour_epoch, day_epoch, require_day_match).await
    } else {
        (0, 0)
    };

    if !limiter
        .check_and_incr(
            ClientKey { subject, id },
            hourly_limit,
            daily_limit,
            cluster_hour,
            cluster_day,
            (hour_epoch, day_epoch),
        )
        .await
    {
        return Err(ServerError::TooManyRequests(error_msg.to_string()));
    }

    if has_limits {
        let delta = RateLimitDelta {
            subject,
            id,
            hour_epoch,
            day_epoch,
            add_hour: 1,
            add_day,
        };
        gs.enqueue_rate_limit_delta(delta);
    }

    Ok(())
}

#[handler]
pub async fn enforce_rate_limit(depot: &mut Depot, req: &mut Request) -> Result<(), ServerError> {
    let ctx = depot
        .obtain::<RateLimitContext>()
        .map_err(|e| ServerError::Internal(format!("RateLimitContext missing: {:?}", e)))?;

    if ctx.is_whitelisted_ip {
        return Ok(());
    }

    let limiter = depot
        .obtain::<DistributedRateLimiter>()
        .map_err(|e| ServerError::Internal(format!("RateLimiter missing: {:?}", e)))?;
    let gs = depot
        .obtain::<GatewayState>()
        .map_err(|e| ServerError::Internal(format!("Failed to obtain GatewayState: {:?}", e)))?;
    let http_cfg = depot
        .obtain::<HTTPConfig>()
        .map_err(|e| ServerError::Internal(format!("Failed to obtain HTTPConfig: {:?}", e)))?;

    let epochs = DistributedRateLimiter::current_epochs();

    // Company keys first, enforce their limits if the header matches
    if let Some(key_str) = req.headers().get("x-api-key").and_then(|v| v.to_str().ok()) {
        if let Some((cid, (_name, hourly_limit, daily_limit))) =
            gs.get_company_info_from_key(key_str).await
        {
            enforce_subject(
                limiter,
                gs,
                epochs,
                SubjectParams {
                    subject: Subject::Company,
                    id: cid.as_u128(),
                    hourly_limit,
                    daily_limit,
                    add_day: 1,
                    error_msg: "Company rate limit exceeded",
                    require_day_match: true,
                },
            )
            .await?;
            return Ok(());
        }
    }

    // Otherwise fall back to per-user quota when we have a user id
    if let Some(user_id) = ctx.user_id {
        let hourly = http_cfg.add_task_user_id_per_user_hourly_rate_limit as u64;
        enforce_subject(
            limiter,
            gs,
            epochs,
            SubjectParams {
                subject: Subject::User,
                id: user_id.as_u128(),
                hourly_limit: hourly,
                daily_limit: 0,
                add_day: 0,
                error_msg: "User rate limit exceeded",
                require_day_match: false,
            },
        )
        .await?;
    }

    Ok(())
}
