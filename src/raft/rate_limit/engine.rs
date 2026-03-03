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

pub struct ClusterUsageParams {
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
                    } else if cluster_hour < w.cluster_hour_seen {
                        // Cluster counters can decrease when refunded; reset baseline.
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
                } else if cluster_day < w.cluster_day_seen {
                    // Cluster counters can decrease when refunded; reset baseline.
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

    pub async fn rollback_pending(
        &self,
        key: ClientKey,
        hour_decrement: u32,
        day_decrement: u32,
        epochs: (u64, u64),
    ) {
        if hour_decrement == 0 && day_decrement == 0 {
            return;
        }

        let (hour_epoch, day_epoch) = epochs;
        self.inner
            .entry_async(key)
            .await
            .and_modify(|w| {
                if hour_decrement > 0 && w.hour_epoch == hour_epoch {
                    w.hour = w.hour.saturating_sub(hour_decrement);
                }
                if day_decrement > 0 && w.day_epoch == day_epoch {
                    w.day = w.day.saturating_sub(day_decrement);
                }
            })
            .or_put_with(|| Window {
                hour_epoch,
                day_epoch,
                ..Window::default()
            });
    }

    pub async fn cluster_usage(&self, gs: &GatewayState, params: ClusterUsageParams) -> (u64, u64) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::NodeConfig;
    use crate::config_runtime::RuntimeConfigStore;
    use crate::crypto::crypto_provider::init_crypto_provider;
    use crate::db::{ApiKeyValidator, Database, EventRecorder, EventSinkHandle};
    use crate::metrics::Metrics;
    use crate::raft::gateway_state::{GatewayState, GatewayStateInit};
    use crate::raft::network::Network;
    use crate::raft::store::{RateLimitDelta, RateLimitMutation};
    use crate::raft::{LogStore, StateMachineStore};
    use crate::task::TaskManager;
    use openraft::BasicNode;
    use scc::Queue;
    use std::collections::BTreeMap;
    use std::path::PathBuf;
    use std::sync::atomic::AtomicU64;
    use std::sync::{Arc, Once};
    use tokio_util::sync::CancellationToken;
    use uuid::Uuid;

    const GENERIC_GLOBAL_SUBJECT_ID: u128 = 0;
    const USER_SUBJECT_ID: u128 = 101;
    const COMPANY_SUBJECT_ID: u128 = 202;
    const GENERIC_IP_SUBJECT_ID: u128 = 303;

    static TEST_CRYPTO_INIT: Once = Once::new();

    fn load_single_node_config() -> (NodeConfig, PathBuf) {
        let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push("dev-env/config/config-single.toml");
        let contents = std::fs::read_to_string(&path).expect("read config-single.toml");
        let config: NodeConfig = toml::from_str(&contents).expect("parse config-single.toml");
        (config, path)
    }

    async fn build_gateway_state() -> GatewayState {
        TEST_CRYPTO_INIT.call_once(|| {
            init_crypto_provider().expect("crypto provider init must succeed");
        });

        let (config, config_path) = load_single_node_config();
        let config = Arc::new(config);
        let runtime_config = Arc::new(
            RuntimeConfigStore::new(config_path, config.as_ref().clone())
                .await
                .expect("runtime config store"),
        );

        let db = Arc::new(Database::new_mock());
        let key_validator = Arc::new(
            ApiKeyValidator::new(
                Arc::clone(&db),
                Duration::from_secs(config.db.api_keys_update_interval),
                config.db.keys_cache_ttl_sec,
                config.db.keys_cache_initial_capacity,
                config.db.keys_cache_max_capacity,
                &config.http.api_key_secret,
                config.db.deleted_keys_ttl_minutes,
            )
            .expect("api key validator"),
        );

        let state_machine_store = Arc::new(StateMachineStore::default());
        let node_clients = scc::HashMap::with_capacity_and_hasher(1, RandomState::default());
        let network = Network::new(Arc::new(node_clients));
        let raft_config = Arc::new(
            openraft::Config {
                cluster_name: config.raft.cluster_name.clone(),
                heartbeat_interval: 100,
                election_timeout_min: 300,
                election_timeout_max: 600,
                ..Default::default()
            }
            .validate()
            .expect("raft config"),
        );
        let raft = crate::raft::Raft::new(
            config.network.node_id,
            Arc::clone(&raft_config),
            network,
            LogStore::default(),
            Arc::clone(&state_machine_store),
        )
        .await
        .expect("raft");

        let mut members = BTreeMap::new();
        members.insert(
            config.network.node_id,
            BasicNode {
                addr: format!(
                    "{}:{}",
                    config.network.external_ip, config.network.server_port
                ),
            },
        );
        let _ = raft.initialize(members).await;

        let task_manager = TaskManager::new(
            config.basic.taskmanager_initial_capacity,
            config.basic.unique_workers_per_task,
            Duration::from_secs(config.basic.taskmanager_cleanup_interval),
            Duration::from_secs(config.basic.taskmanager_result_lifetime),
            Metrics::new(0.05).expect("metrics"),
            None,
        )
        .await;

        let shutdown = CancellationToken::new();
        let event_recorder = EventRecorder::new(
            Arc::new(EventSinkHandle::Noop),
            Arc::from(config.network.name.as_str()),
            Duration::from_secs(60),
            1024,
            shutdown,
        );
        let rate_limit_queue = Arc::new(Queue::<RateLimitMutation>::default());
        GatewayState::new(GatewayStateInit {
            state: state_machine_store,
            raft,
            last_task_acquisition: Arc::new(AtomicU64::new(0)),
            key_validator_updater: key_validator,
            task_manager,
            config: runtime_config,
            rate_limit_queue,
            event_recorder,
        })
    }

    async fn consume_once_and_sync_cluster(
        limiter: &DistributedRateLimiter,
        gateway_state: &GatewayState,
        subject: Subject,
        id: u128,
        hourly_limit: u64,
        daily_limit: u64,
        require_day_match: bool,
        add_day: u32,
    ) -> bool {
        let epochs = limiter.epochs();
        let (cluster_hour, cluster_day) = limiter
            .cluster_usage(
                gateway_state,
                ClusterUsageParams {
                    subject,
                    id,
                    hourly_limit,
                    daily_limit,
                    require_day_match,
                    epochs,
                },
            )
            .await;

        let allowed = limiter
            .check_and_incr(
                ClientKey { subject, id },
                hourly_limit,
                daily_limit,
                cluster_hour,
                cluster_day,
                epochs,
            )
            .await;

        if !allowed {
            return false;
        }

        gateway_state
            .apply_rate_limit_deltas(
                Uuid::new_v4().as_u128(),
                vec![RateLimitDelta {
                    subject,
                    id,
                    hour_epoch: epochs.0,
                    day_epoch: epochs.1,
                    add_hour: 1,
                    add_day,
                }],
            )
            .await
            .expect("apply synced rate-limit delta");

        true
    }

    #[tokio::test]
    async fn user_subject_limits_synchronize_across_gateway_instances() {
        let gateway_state = build_gateway_state().await;
        let gateway_a = DistributedRateLimiter::new(256);
        let gateway_b = DistributedRateLimiter::new(256);

        assert!(
            consume_once_and_sync_cluster(
                &gateway_a,
                &gateway_state,
                Subject::User,
                USER_SUBJECT_ID,
                1,
                0,
                false,
                0,
            )
            .await
        );
        assert!(
            !consume_once_and_sync_cluster(
                &gateway_b,
                &gateway_state,
                Subject::User,
                USER_SUBJECT_ID,
                1,
                0,
                false,
                0,
            )
            .await
        );
    }

    #[tokio::test]
    async fn company_subject_limits_synchronize_across_gateway_instances() {
        let gateway_state = build_gateway_state().await;
        let gateway_a = DistributedRateLimiter::new(256);
        let gateway_b = DistributedRateLimiter::new(256);

        assert!(
            consume_once_and_sync_cluster(
                &gateway_a,
                &gateway_state,
                Subject::Company,
                COMPANY_SUBJECT_ID,
                3,
                1,
                true,
                1,
            )
            .await
        );
        assert!(
            !consume_once_and_sync_cluster(
                &gateway_b,
                &gateway_state,
                Subject::Company,
                COMPANY_SUBJECT_ID,
                3,
                1,
                true,
                1,
            )
            .await
        );
    }

    #[tokio::test]
    async fn generic_ip_subject_limits_synchronize_across_gateway_instances() {
        let gateway_state = build_gateway_state().await;
        let gateway_a = DistributedRateLimiter::new(256);
        let gateway_b = DistributedRateLimiter::new(256);

        assert!(
            consume_once_and_sync_cluster(
                &gateway_a,
                &gateway_state,
                Subject::GenericIp,
                GENERIC_IP_SUBJECT_ID,
                1,
                0,
                false,
                0,
            )
            .await
        );
        assert!(
            !consume_once_and_sync_cluster(
                &gateway_b,
                &gateway_state,
                Subject::GenericIp,
                GENERIC_IP_SUBJECT_ID,
                1,
                0,
                false,
                0,
            )
            .await
        );
    }

    #[tokio::test]
    async fn generic_global_pool_limits_synchronize_across_gateway_instances() {
        let gateway_state = build_gateway_state().await;
        let gateway_a = DistributedRateLimiter::new(256);
        let gateway_b = DistributedRateLimiter::new(256);

        assert!(
            consume_once_and_sync_cluster(
                &gateway_a,
                &gateway_state,
                Subject::GenericGlobal,
                GENERIC_GLOBAL_SUBJECT_ID,
                1,
                0,
                false,
                0,
            )
            .await
        );
        assert!(
            !consume_once_and_sync_cluster(
                &gateway_b,
                &gateway_state,
                Subject::GenericGlobal,
                GENERIC_GLOBAL_SUBJECT_ID,
                1,
                0,
                false,
                0,
            )
            .await
        );
    }

    #[tokio::test]
    async fn local_windows_reset_on_hour_and_day_rollover() {
        let limiter = DistributedRateLimiter::new(64);

        let user_key = ClientKey {
            subject: Subject::User,
            id: 808u128,
        };
        assert!(limiter.check_and_incr(user_key, 1, 0, 0, 0, (10, 1)).await);
        assert!(!limiter.check_and_incr(user_key, 1, 0, 0, 0, (10, 1)).await);
        assert!(limiter.check_and_incr(user_key, 1, 0, 0, 0, (11, 1)).await);

        let company_key = ClientKey {
            subject: Subject::Company,
            id: 909u128,
        };
        assert!(
            limiter
                .check_and_incr(company_key, 0, 1, 0, 0, (11, 1))
                .await
        );
        assert!(
            !limiter
                .check_and_incr(company_key, 0, 1, 0, 0, (11, 1))
                .await
        );
        assert!(
            limiter
                .check_and_incr(company_key, 0, 1, 0, 0, (11, 2))
                .await
        );
    }

    #[tokio::test]
    async fn cluster_usage_refreshes_when_near_limit() {
        let gateway_state = build_gateway_state().await;
        let limiter = DistributedRateLimiter::new(64);
        let subject = Subject::GenericGlobal;
        let id = GENERIC_GLOBAL_SUBJECT_ID;
        let epochs = limiter.epochs();

        let initial = limiter
            .cluster_usage(
                &gateway_state,
                ClusterUsageParams {
                    subject,
                    id,
                    hourly_limit: 2,
                    daily_limit: 0,
                    require_day_match: false,
                    epochs,
                },
            )
            .await;
        assert_eq!(initial, (0, 0));

        gateway_state
            .apply_rate_limit_deltas(
                Uuid::new_v4().as_u128(),
                vec![RateLimitDelta {
                    subject,
                    id,
                    hour_epoch: epochs.0,
                    day_epoch: epochs.1,
                    add_hour: 1,
                    add_day: 0,
                }],
            )
            .await
            .expect("apply rate-limit delta");

        let refreshed = limiter
            .cluster_usage(
                &gateway_state,
                ClusterUsageParams {
                    subject,
                    id,
                    hourly_limit: 2,
                    daily_limit: 0,
                    require_day_match: false,
                    epochs,
                },
            )
            .await;
        assert_eq!(refreshed, (1, 0));
    }

    #[tokio::test]
    async fn rollback_pending_restores_capacity_after_partial_success() {
        let limiter = DistributedRateLimiter::new(64);
        let epochs = (42, 7);
        let global_key = ClientKey {
            subject: Subject::GenericGlobal,
            id: GENERIC_GLOBAL_SUBJECT_ID,
        };
        let ip_key = ClientKey {
            subject: Subject::GenericIp,
            id: GENERIC_IP_SUBJECT_ID,
        };

        assert!(limiter.check_and_incr(global_key, 1, 0, 0, 0, epochs).await);
        assert!(!limiter.check_and_incr(ip_key, 1, 0, 1, 0, epochs).await);

        limiter.rollback_pending(global_key, 1, 0, epochs).await;

        assert!(limiter.check_and_incr(global_key, 1, 0, 0, 0, epochs).await);
    }
}
