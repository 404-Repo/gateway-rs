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
    pub day_epoch: u64,
    pub active: u32,
    /// Local increments waiting to be reconciled with the cluster snapshot
    pub day: u32,
    pub cluster_active_seen: u64,
    /// Cluster totals last observed, used to drop already-applied pending counts
    pub cluster_day_seen: u64,
}

struct EpochCache {
    day_epoch: AtomicU64,
}

#[derive(Copy, Clone)]
struct ClusterCacheEntry {
    active: u64,
    day_epoch: u64,
    day: u64,
    updated_at: Instant,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum RateLimitRejection {
    Active,
    Daily,
}

pub struct ClusterUsageParams {
    pub subject: Subject,
    pub id: u128,
    pub active_limit: u64,
    pub daily_limit: u64,
    pub require_day_match: bool,
    pub day_epoch: u64,
}

#[derive(Copy, Clone)]
pub struct CheckAndIncrParams {
    pub key: ClientKey,
    pub active_limit: u64,
    pub daily_limit: u64,
    pub cluster_active: u64,
    pub cluster_day: u64,
    pub day_epoch: u64,
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
        let day_epoch = Self::current_day_epoch();
        let epoch_cache = Arc::new(EpochCache {
            day_epoch: AtomicU64::new(day_epoch),
        });
        let refresh_enabled = match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                let cache = Arc::downgrade(&epoch_cache);
                handle.spawn(async move {
                    let mut tick = interval(EPOCH_REFRESH_INTERVAL);
                    loop {
                        tick.tick().await;
                        let Some(cache) = cache.upgrade() else {
                            break;
                        };
                        let day_epoch = DistributedRateLimiter::current_day_epoch();
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

    pub(crate) fn current_day_epoch() -> u64 {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        now / 86400
    }

    pub fn day_epoch(&self) -> u64 {
        if !self.refresh_enabled {
            return Self::current_day_epoch();
        }
        self.epoch_cache.day_epoch.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    fn epoch_cache_weak(&self) -> std::sync::Weak<EpochCache> {
        Arc::downgrade(&self.epoch_cache)
    }

    pub async fn check_and_incr(
        &self,
        params: CheckAndIncrParams,
    ) -> Result<(), RateLimitRejection> {
        let CheckAndIncrParams {
            key,
            active_limit,
            daily_limit,
            cluster_active,
            cluster_day,
            day_epoch,
        } = params;
        if active_limit == 0 && daily_limit == 0 {
            return Ok(());
        }

        let mut rejection = None;
        self.inner
            .entry_async(key)
            .await
            .and_modify(|w| {
                // On day rollover, reset local tally against the fresh snapshot
                if w.day_epoch != day_epoch {
                    w.day = 0;
                    w.day_epoch = day_epoch;
                    w.cluster_day_seen = cluster_day;
                }

                if active_limit == 0 {
                    w.active = 0;
                    w.cluster_active_seen = cluster_active;
                } else if let Some(delta) = cluster_active.checked_sub(w.cluster_active_seen)
                    && delta > 0
                {
                    let to_sub = delta.min(w.active as u64) as u32;
                    w.active = w.active.saturating_sub(to_sub);
                    w.cluster_active_seen = cluster_active;
                } else if cluster_active < w.cluster_active_seen {
                    w.cluster_active_seen = cluster_active;
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
                let eff_active_total = if active_limit > 0 {
                    cluster_active + w.active as u64
                } else {
                    0
                };
                let eff_day_total = if daily_limit > 0 {
                    cluster_day + w.day as u64
                } else {
                    0
                };

                if (active_limit > 0 && eff_active_total >= active_limit)
                    || (daily_limit > 0 && eff_day_total >= daily_limit)
                {
                    rejection = Some(if active_limit > 0 && eff_active_total >= active_limit {
                        RateLimitRejection::Active
                    } else {
                        RateLimitRejection::Daily
                    });
                    return;
                }

                if active_limit > 0 {
                    w.active = w.active.saturating_add(1);
                }
                if daily_limit > 0 {
                    w.day = w.day.saturating_add(1);
                }
            })
            .or_put_with(|| {
                let allow_initial = !((active_limit > 0 && cluster_active >= active_limit)
                    || (daily_limit > 0 && cluster_day >= daily_limit));

                if !allow_initial {
                    rejection = Some(if active_limit > 0 && cluster_active >= active_limit {
                        RateLimitRejection::Active
                    } else {
                        RateLimitRejection::Daily
                    });
                }

                Window {
                    day_epoch,
                    active: if allow_initial && active_limit > 0 {
                        1
                    } else {
                        0
                    },
                    day: if allow_initial && daily_limit > 0 {
                        1
                    } else {
                        0
                    },
                    cluster_active_seen: cluster_active,
                    cluster_day_seen: cluster_day,
                }
            });

        rejection.map_or(Ok(()), Err)
    }

    pub async fn rollback_pending(
        &self,
        key: ClientKey,
        active_decrement: u32,
        day_decrement: u32,
        day_epoch: u64,
    ) {
        if active_decrement == 0 && day_decrement == 0 {
            return;
        }

        self.inner
            .entry_async(key)
            .await
            .and_modify(|w| {
                if active_decrement > 0 {
                    w.active = w.active.saturating_sub(active_decrement);
                }
                if day_decrement > 0 && w.day_epoch == day_epoch {
                    w.day = w.day.saturating_sub(day_decrement);
                }
            })
            .or_put_with(|| Window {
                day_epoch,
                ..Window::default()
            });
    }

    pub async fn cluster_usage(&self, gs: &GatewayState, params: ClusterUsageParams) -> (u64, u64) {
        let ClusterUsageParams {
            subject,
            id,
            active_limit,
            daily_limit,
            require_day_match,
            day_epoch,
        } = params;
        let now = Instant::now();
        let key = ClientKey { subject, id };

        if let Some(cached) = self.cluster_cache.read_async(&key, |_, entry| *entry).await {
            let is_fresh = now.duration_since(cached.updated_at) <= CLUSTER_CACHE_TTL;
            let epoch_match = !require_day_match || cached.day_epoch == day_epoch;
            let near_active_limit = active_limit > 0
                && cached.active.saturating_add(CLUSTER_CACHE_REFRESH_MARGIN) >= active_limit;
            let near_day_limit = require_day_match
                && daily_limit > 0
                && cached.day.saturating_add(CLUSTER_CACHE_REFRESH_MARGIN) >= daily_limit;

            if is_fresh && epoch_match && !near_active_limit && !near_day_limit {
                let day = if require_day_match { cached.day } else { 0 };
                return (cached.active, day);
            }
        }

        let (active, day) = gs.get_cluster_rate_usage(subject, id, day_epoch).await;
        match self.cluster_cache.entry_async(key).await {
            CacheEntry::Occupied(mut entry) => {
                *entry.get_mut() = ClusterCacheEntry {
                    active,
                    day_epoch,
                    day,
                    updated_at: now,
                };
            }
            CacheEntry::Vacant(entry) => {
                entry.put_entry(ClusterCacheEntry {
                    active,
                    day_epoch,
                    day,
                    updated_at: now,
                });
            }
        }
        let day = if require_day_match { day } else { 0 };
        (active, day)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::NodeConfig;
    use crate::config_runtime::RuntimeConfigStore;
    use crate::db::{
        ApiKeyValidator, ApiKeyValidatorConfig, Database, EventRecorder, EventSinkHandle,
        TaskLifecycleStoreHandle, api_key_sync_interval,
    };
    use crate::metrics::Metrics;
    use crate::raft::gateway_state::{GatewayState, GatewayStateInit};
    use crate::raft::network::Network;
    use crate::raft::store::{RateLimitDelta, RateLimitMutation};
    use crate::raft::test_utils::ensure_crypto_provider_for_tests;
    use crate::raft::{LogStore, StateMachineStore};
    use crate::task::TaskManager;
    use openraft::BasicNode;
    use std::collections::BTreeMap;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::atomic::AtomicU64;
    use tokio_util::sync::CancellationToken;
    use uuid::Uuid;

    const GENERIC_GLOBAL_SUBJECT_ID: u128 = 0;
    const USER_SUBJECT_ID: u128 = 101;
    const COMPANY_SUBJECT_ID: u128 = 202;
    const GENERIC_IP_SUBJECT_ID: u128 = 303;

    fn load_single_node_config() -> (NodeConfig, PathBuf) {
        let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push("dev-env/config/config-single.toml");
        let contents = std::fs::read_to_string(&path).expect("read config-single.toml");
        let config: NodeConfig = toml::from_str(&contents).expect("parse config-single.toml");
        (config, path)
    }

    async fn build_gateway_state() -> GatewayState {
        ensure_crypto_provider_for_tests();

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
                ApiKeyValidatorConfig {
                    update_interval: api_key_sync_interval(config.db.api_keys_update_interval),
                    cache_ttl_sec: config.db.keys_cache_ttl_sec,
                    cache_initial_capacity: config.db.keys_cache_initial_capacity,
                    cache_max_capacity: config.db.keys_cache_max_capacity,
                    api_key_secret: &config.http.api_key_secret,
                    negative_cache_ttl_sec: config.http.invalid_api_key_negative_cache_ttl_sec,
                    unknown_key_ip_miss_ttl_sec: config.http.invalid_api_key_ip_miss_ttl_sec,
                    unknown_key_ip_cooldown_ttl_sec: config
                        .http
                        .invalid_api_key_ip_cooldown_ttl_sec,
                    unknown_key_ip_cache_capacity: config.http.invalid_api_key_ip_cache_capacity,
                    unknown_key_ip_miss_limit: config.http.invalid_api_key_ip_miss_limit,
                    deleted_keys_ttl_minutes: config.db.deleted_keys_ttl_minutes,
                    fallback_generic_key: config.http.generic_key,
                },
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
        GatewayState::new(GatewayStateInit {
            state: state_machine_store,
            raft,
            last_task_acquisition: Arc::new(AtomicU64::new(0)),
            task_lifecycle_store: TaskLifecycleStoreHandle::Noop(Default::default()),
            api_key_validator: key_validator,
            task_manager,
            config: runtime_config,
            event_recorder,
        })
    }

    async fn consume_once_and_sync_cluster(
        limiter: &DistributedRateLimiter,
        gateway_state: &GatewayState,
        params: ClusterUsageParams,
        add_day: u32,
    ) -> bool {
        let day_epoch = limiter.day_epoch();
        let (cluster_active, cluster_day) = limiter
            .cluster_usage(
                gateway_state,
                ClusterUsageParams {
                    subject: params.subject,
                    id: params.id,
                    active_limit: params.active_limit,
                    daily_limit: params.daily_limit,
                    require_day_match: params.require_day_match,
                    day_epoch,
                },
            )
            .await;

        let allowed = limiter
            .check_and_incr(CheckAndIncrParams {
                key: ClientKey {
                    subject: params.subject,
                    id: params.id,
                },
                active_limit: params.active_limit,
                daily_limit: params.daily_limit,
                cluster_active,
                cluster_day,
                day_epoch,
            })
            .await
            .is_ok();

        if !allowed {
            return false;
        }

        gateway_state
            .apply_rate_limit_mutations(
                Uuid::new_v4().as_u128(),
                vec![RateLimitMutation::from_delta(RateLimitDelta {
                    subject: params.subject,
                    id: params.id,
                    day_epoch,
                    add_active: u32::from(params.active_limit > 0),
                    add_day,
                })],
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
                ClusterUsageParams {
                    subject: Subject::User,
                    id: USER_SUBJECT_ID,
                    active_limit: 1,
                    daily_limit: 0,
                    require_day_match: false,
                    day_epoch: 0,
                },
                0,
            )
            .await
        );
        assert!(
            !consume_once_and_sync_cluster(
                &gateway_b,
                &gateway_state,
                ClusterUsageParams {
                    subject: Subject::User,
                    id: USER_SUBJECT_ID,
                    active_limit: 1,
                    daily_limit: 0,
                    require_day_match: false,
                    day_epoch: 0,
                },
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
                ClusterUsageParams {
                    subject: Subject::Company,
                    id: COMPANY_SUBJECT_ID,
                    active_limit: 1,
                    daily_limit: 1,
                    require_day_match: true,
                    day_epoch: 0,
                },
                1,
            )
            .await
        );
        assert!(
            !consume_once_and_sync_cluster(
                &gateway_b,
                &gateway_state,
                ClusterUsageParams {
                    subject: Subject::Company,
                    id: COMPANY_SUBJECT_ID,
                    active_limit: 1,
                    daily_limit: 1,
                    require_day_match: true,
                    day_epoch: 0,
                },
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
                ClusterUsageParams {
                    subject: Subject::GenericIp,
                    id: GENERIC_IP_SUBJECT_ID,
                    active_limit: 0,
                    daily_limit: 1,
                    require_day_match: true,
                    day_epoch: 0,
                },
                1,
            )
            .await
        );
        assert!(
            !consume_once_and_sync_cluster(
                &gateway_b,
                &gateway_state,
                ClusterUsageParams {
                    subject: Subject::GenericIp,
                    id: GENERIC_IP_SUBJECT_ID,
                    active_limit: 0,
                    daily_limit: 1,
                    require_day_match: true,
                    day_epoch: 0,
                },
                1,
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
                ClusterUsageParams {
                    subject: Subject::GenericGlobal,
                    id: GENERIC_GLOBAL_SUBJECT_ID,
                    active_limit: 0,
                    daily_limit: 1,
                    require_day_match: true,
                    day_epoch: 0,
                },
                1,
            )
            .await
        );
        assert!(
            !consume_once_and_sync_cluster(
                &gateway_b,
                &gateway_state,
                ClusterUsageParams {
                    subject: Subject::GenericGlobal,
                    id: GENERIC_GLOBAL_SUBJECT_ID,
                    active_limit: 0,
                    daily_limit: 1,
                    require_day_match: true,
                    day_epoch: 0,
                },
                1,
            )
            .await
        );
    }

    #[tokio::test]
    async fn local_windows_reset_on_day_rollover() {
        let limiter = DistributedRateLimiter::new(64);

        let generic_key = ClientKey {
            subject: Subject::GenericGlobal,
            id: 808u128,
        };
        assert!(
            limiter
                .check_and_incr(CheckAndIncrParams {
                    key: generic_key,
                    active_limit: 0,
                    daily_limit: 1,
                    cluster_active: 0,
                    cluster_day: 0,
                    day_epoch: 1,
                })
                .await
                .is_ok()
        );
        assert!(
            limiter
                .check_and_incr(CheckAndIncrParams {
                    key: generic_key,
                    active_limit: 0,
                    daily_limit: 1,
                    cluster_active: 0,
                    cluster_day: 0,
                    day_epoch: 1,
                })
                .await
                .is_err()
        );
        // Day rollover resets window
        assert!(
            limiter
                .check_and_incr(CheckAndIncrParams {
                    key: generic_key,
                    active_limit: 0,
                    daily_limit: 1,
                    cluster_active: 0,
                    cluster_day: 0,
                    day_epoch: 2,
                })
                .await
                .is_ok()
        );

        let company_key = ClientKey {
            subject: Subject::Company,
            id: 909u128,
        };
        assert!(
            limiter
                .check_and_incr(CheckAndIncrParams {
                    key: company_key,
                    active_limit: 0,
                    daily_limit: 1,
                    cluster_active: 0,
                    cluster_day: 0,
                    day_epoch: 1,
                })
                .await
                .is_ok()
        );
        assert!(
            limiter
                .check_and_incr(CheckAndIncrParams {
                    key: company_key,
                    active_limit: 0,
                    daily_limit: 1,
                    cluster_active: 0,
                    cluster_day: 0,
                    day_epoch: 1,
                })
                .await
                .is_err()
        );
        assert!(
            limiter
                .check_and_incr(CheckAndIncrParams {
                    key: company_key,
                    active_limit: 0,
                    daily_limit: 1,
                    cluster_active: 0,
                    cluster_day: 0,
                    day_epoch: 2,
                })
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn cluster_usage_refreshes_when_near_limit() {
        let gateway_state = build_gateway_state().await;
        let limiter = DistributedRateLimiter::new(64);
        let subject = Subject::GenericGlobal;
        let id = GENERIC_GLOBAL_SUBJECT_ID;
        let day_epoch = limiter.day_epoch();

        let initial = limiter
            .cluster_usage(
                &gateway_state,
                ClusterUsageParams {
                    subject,
                    id,
                    active_limit: 0,
                    daily_limit: 2,
                    require_day_match: true,
                    day_epoch,
                },
            )
            .await;
        assert_eq!(initial, (0, 0));

        gateway_state
            .apply_rate_limit_mutations(
                Uuid::new_v4().as_u128(),
                vec![RateLimitMutation::from_delta(RateLimitDelta {
                    subject,
                    id,
                    day_epoch,
                    add_active: 0,
                    add_day: 1,
                })],
            )
            .await
            .expect("apply rate-limit delta");

        let refreshed = limiter
            .cluster_usage(
                &gateway_state,
                ClusterUsageParams {
                    subject,
                    id,
                    active_limit: 0,
                    daily_limit: 2,
                    require_day_match: true,
                    day_epoch,
                },
            )
            .await;
        assert_eq!(refreshed, (0, 1));
    }

    #[tokio::test]
    async fn rollback_pending_restores_capacity_after_partial_success() {
        let limiter = DistributedRateLimiter::new(64);
        let day_epoch = 7u64;
        let global_key = ClientKey {
            subject: Subject::GenericGlobal,
            id: GENERIC_GLOBAL_SUBJECT_ID,
        };
        let ip_key = ClientKey {
            subject: Subject::GenericIp,
            id: GENERIC_IP_SUBJECT_ID,
        };

        assert!(
            limiter
                .check_and_incr(CheckAndIncrParams {
                    key: global_key,
                    active_limit: 0,
                    daily_limit: 1,
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
                    key: ip_key,
                    active_limit: 0,
                    daily_limit: 1,
                    cluster_active: 0,
                    cluster_day: 1,
                    day_epoch,
                })
                .await
                .is_err()
        );

        limiter.rollback_pending(global_key, 0, 1, day_epoch).await;

        assert!(
            limiter
                .check_and_incr(CheckAndIncrParams {
                    key: global_key,
                    active_limit: 0,
                    daily_limit: 1,
                    cluster_active: 0,
                    cluster_day: 0,
                    day_epoch,
                })
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn refresher_task_does_not_keep_epoch_cache_alive() {
        let weak_cache = {
            let limiter = DistributedRateLimiter::new(64);
            limiter.epoch_cache_weak()
        };

        tokio::task::yield_now().await;
        assert!(
            weak_cache.upgrade().is_none(),
            "background refresher must not keep a dropped limiter alive"
        );
    }
}
