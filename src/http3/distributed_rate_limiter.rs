use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use foldhash::fast::RandomState;
use scc::HashCache;
use scc::hash_cache::Entry as CacheEntry;
use tokio::time::interval;

use crate::raft::gateway_state::GatewayState;
use crate::raft::store::Subject;

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

struct EpochCache {
    hour_epoch: AtomicU64,
    day_epoch: AtomicU64,
}

#[derive(Copy, Clone)]
struct ClusterCacheEntry {
    hour_epoch: u64,
    day_epoch: u64,
    hour: u64,
    day: u64,
    updated_at: Instant,
}

pub(crate) struct ClusterUsageParams {
    pub subject: Subject,
    pub id: u128,
    pub hourly_limit: u64,
    pub daily_limit: u64,
    pub require_day_match: bool,
    pub epochs: (u64, u64),
}

const EPOCH_REFRESH_INTERVAL: Duration = Duration::from_secs(1);
const CLUSTER_CACHE_TTL: Duration = Duration::from_secs(1);
const CLUSTER_CACHE_REFRESH_MARGIN: u64 = 2;

#[derive(Clone)]
pub struct DistributedRateLimiter {
    inner: Arc<HashCache<ClientKey, Window, RandomState>>,
    epoch_cache: Arc<EpochCache>,
    cluster_cache: Arc<HashCache<ClientKey, ClusterCacheEntry, RandomState>>,
    refresh_enabled: bool,
}

impl DistributedRateLimiter {
    pub fn new(max_capacity: usize) -> Self {
        let max_capacity = max_capacity.max(1);
        let min_capacity = max_capacity.min(8192);
        let (hour_epoch, day_epoch) = Self::current_epochs();
        let epoch_cache = Arc::new(EpochCache {
            hour_epoch: AtomicU64::new(hour_epoch),
            day_epoch: AtomicU64::new(day_epoch),
        });
        let refresh_enabled = match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                let cache = Arc::clone(&epoch_cache);
                handle.spawn(async move {
                    let mut tick = interval(EPOCH_REFRESH_INTERVAL);
                    loop {
                        tick.tick().await;
                        let (hour_epoch, day_epoch) = DistributedRateLimiter::current_epochs();
                        cache.hour_epoch.store(hour_epoch, Ordering::Relaxed);
                        cache.day_epoch.store(day_epoch, Ordering::Relaxed);
                    }
                });
                true
            }
            Err(_) => false,
        };
        let cluster_cache = Arc::new(HashCache::with_capacity_and_hasher(
            min_capacity,
            max_capacity,
            RandomState::default(),
        ));
        Self {
            inner: Arc::new(HashCache::with_capacity_and_hasher(
                min_capacity,
                max_capacity,
                RandomState::default(),
            )),
            epoch_cache,
            cluster_cache,
            refresh_enabled,
        }
    }

    pub(crate) fn current_epochs() -> (u64, u64) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        (now / 3600, now / 86400)
    }

    pub fn epochs(&self) -> (u64, u64) {
        if !self.refresh_enabled {
            return Self::current_epochs();
        }
        (
            self.epoch_cache.hour_epoch.load(Ordering::Relaxed),
            self.epoch_cache.day_epoch.load(Ordering::Relaxed),
        )
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

                if hourly_limit == 0 {
                    w.hour = 0;
                    w.cluster_hour_seen = cluster_hour;
                } else {
                    // Drop local pending counts already reflected in the cluster snapshot
                    if let Some(delta) = cluster_hour.checked_sub(w.cluster_hour_seen)
                        && delta > 0
                    {
                        let to_sub = delta.min(w.hour as u64) as u32;
                        w.hour = w.hour.saturating_sub(to_sub);
                        w.cluster_hour_seen = cluster_hour;
                    }
                }

                if daily_limit == 0 {
                    w.day = 0;
                    w.cluster_day_seen = cluster_day;
                } else if let Some(delta) = cluster_day.checked_sub(w.cluster_day_seen)
                    && delta > 0
                {
                    let to_sub = delta.min(w.day as u64) as u32;
                    w.day = w.day.saturating_sub(to_sub);
                    w.cluster_day_seen = cluster_day;
                }

                // Combine cluster totals with remaining local increments
                let eff_hour_total = if hourly_limit > 0 {
                    cluster_hour + w.hour as u64
                } else {
                    0
                };
                let eff_day_total = if daily_limit > 0 {
                    cluster_day + w.day as u64
                } else {
                    0
                };

                if (hourly_limit > 0 && eff_hour_total >= hourly_limit)
                    || (daily_limit > 0 && eff_day_total >= daily_limit)
                {
                    allowed = false;
                    return;
                }

                if hourly_limit > 0 {
                    w.hour = w.hour.saturating_add(1);
                }
                if daily_limit > 0 {
                    w.day = w.day.saturating_add(1);
                }
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
                    hour: if allow_initial && hourly_limit > 0 {
                        1
                    } else {
                        0
                    },
                    day: if allow_initial && daily_limit > 0 {
                        1
                    } else {
                        0
                    },
                    cluster_hour_seen: cluster_hour,
                    cluster_day_seen: cluster_day,
                }
            });

        allowed
    }
}

impl DistributedRateLimiter {
    pub(crate) async fn cluster_usage(
        &self,
        gs: &GatewayState,
        params: ClusterUsageParams,
    ) -> (u64, u64) {
        let ClusterUsageParams {
            subject,
            id,
            hourly_limit,
            daily_limit,
            require_day_match,
            epochs,
        } = params;
        let now = Instant::now();
        let key = ClientKey { subject, id };
        let (hour_epoch, day_epoch) = epochs;

        match self.cluster_cache.entry_async(key).await {
            CacheEntry::Occupied(mut entry) => {
                let cached = *entry.get();
                let is_fresh = now.duration_since(cached.updated_at) <= CLUSTER_CACHE_TTL;
                let epoch_match = cached.hour_epoch == hour_epoch
                    && (!require_day_match || cached.day_epoch == day_epoch);
                let near_hour_limit = hourly_limit > 0
                    && cached.hour.saturating_add(CLUSTER_CACHE_REFRESH_MARGIN) >= hourly_limit;
                let near_day_limit = require_day_match
                    && daily_limit > 0
                    && cached.day.saturating_add(CLUSTER_CACHE_REFRESH_MARGIN) >= daily_limit;

                if is_fresh && epoch_match && !near_hour_limit && !near_day_limit {
                    let day = if require_day_match { cached.day } else { 0 };
                    return (cached.hour, day);
                }

                let (hour, day) = gs
                    .get_cluster_rate_usage(subject, id, hour_epoch, day_epoch)
                    .await;
                *entry.get_mut() = ClusterCacheEntry {
                    hour_epoch,
                    day_epoch,
                    hour,
                    day,
                    updated_at: now,
                };
                let day = if require_day_match { day } else { 0 };
                (hour, day)
            }
            CacheEntry::Vacant(entry) => {
                let (hour, day) = gs
                    .get_cluster_rate_usage(subject, id, hour_epoch, day_epoch)
                    .await;
                entry.put_entry(ClusterCacheEntry {
                    hour_epoch,
                    day_epoch,
                    hour,
                    day,
                    updated_at: now,
                });
                let day = if require_day_match { day } else { 0 };
                (hour, day)
            }
        }
    }
}
