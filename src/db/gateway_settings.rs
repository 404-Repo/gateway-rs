use anyhow::Result;
use foldhash::HashSet as FoldHashSet;
use portable_atomic::AtomicU128;
use sdd::{AtomicOwned, Guard, Owned, Tag};
use std::collections::HashSet;
use std::net::IpAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use uuid::Uuid;

use super::Database;
use super::data_access::GatewaySettingsRow;
use super::model_catalog::ModelCatalog;
use crate::http3::whitelist::resolve_rate_limit_whitelist_with_status;
use crate::task::TaskManager;

const DEFAULT_REGISTERED_GENERATION_LIMIT: u64 = 0;
const DEFAULT_REGISTERED_WINDOW_MS: u64 = 24 * 60 * 60 * 1000;
const DEFAULT_GUEST_GENERATION_LIMIT: u64 = 1;
const DEFAULT_GUEST_WINDOW_MS: u64 = 24 * 60 * 60 * 1000;
const DEFAULT_TASKMANAGER_TASK_LIFETIME_SEC: u64 = 600;
const DEFAULT_TASKMANAGER_RESULT_LIFETIME_SEC: u64 = 300;

pub fn gateway_settings_sync_interval(update_interval_secs: u64) -> Duration {
    Duration::from_secs(update_interval_secs.max(1))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CachedGatewaySettings {
    generic_key: Option<Uuid>,
    generic_global_daily_limit: u64,
    generic_per_ip_daily_limit: u64,
    generic_window_ms: u64,
    add_task_unauthorized_per_ip_daily_rate_limit: u64,
    max_task_queue_len: u64,
    request_file_size_limit: u64,
    taskmanager_task_lifetime_sec: u64,
    taskmanager_result_lifetime_sec: u64,
    guest_generation_limit: u64,
    guest_window_ms: u64,
    registered_generation_limit: u64,
    registered_window_ms: u64,
}

pub struct GatewayRuntimeSettingsConfig {
    pub update_interval: Duration,
    pub fallback_generic_key: Option<Uuid>,
}

pub struct GatewayRuntimeSettingsStore {
    db: Arc<Database>,
    update_interval: Duration,
    last_gateway_settings_sync_ms: AtomicU64,
    has_database_gateway_settings: AtomicBool,
    generic_key: AtomicU128,
    generic_global_daily_limit: AtomicU64,
    generic_per_ip_daily_limit: AtomicU64,
    generic_window_ms: AtomicU64,
    add_task_unauthorized_per_ip_daily_rate_limit: AtomicU64,
    max_task_queue_len: AtomicU64,
    request_file_size_limit: AtomicU64,
    taskmanager_task_lifetime_sec: AtomicU64,
    taskmanager_result_lifetime_sec: AtomicU64,
    guest_generation_limit: AtomicU64,
    guest_window_ms: AtomicU64,
    registered_generation_limit: AtomicU64,
    registered_window_ms: AtomicU64,
    rate_limit_whitelist: AtomicOwned<Arc<HashSet<IpAddr>>>,
    model_catalog: AtomicOwned<Arc<ModelCatalog>>,
}

impl GatewayRuntimeSettingsStore {
    pub fn new(db: Arc<Database>, config: GatewayRuntimeSettingsConfig) -> Self {
        Self {
            db,
            update_interval: config.update_interval,
            last_gateway_settings_sync_ms: AtomicU64::new(0),
            has_database_gateway_settings: AtomicBool::new(false),
            generic_key: AtomicU128::new(
                config
                    .fallback_generic_key
                    .map(|key| key.as_u128())
                    .unwrap_or(0),
            ),
            generic_global_daily_limit: AtomicU64::new(0),
            generic_per_ip_daily_limit: AtomicU64::new(0),
            generic_window_ms: AtomicU64::new(DEFAULT_GUEST_WINDOW_MS),
            add_task_unauthorized_per_ip_daily_rate_limit: AtomicU64::new(0),
            max_task_queue_len: AtomicU64::new(0),
            request_file_size_limit: AtomicU64::new(0),
            taskmanager_task_lifetime_sec: AtomicU64::new(DEFAULT_TASKMANAGER_TASK_LIFETIME_SEC),
            taskmanager_result_lifetime_sec: AtomicU64::new(
                DEFAULT_TASKMANAGER_RESULT_LIFETIME_SEC,
            ),
            guest_generation_limit: AtomicU64::new(DEFAULT_GUEST_GENERATION_LIMIT),
            guest_window_ms: AtomicU64::new(DEFAULT_GUEST_WINDOW_MS),
            registered_generation_limit: AtomicU64::new(DEFAULT_REGISTERED_GENERATION_LIMIT),
            registered_window_ms: AtomicU64::new(DEFAULT_REGISTERED_WINDOW_MS),
            rate_limit_whitelist: AtomicOwned::new(Arc::new(HashSet::new())),
            model_catalog: AtomicOwned::new(Arc::new(ModelCatalog::default())),
        }
    }

    pub async fn run(
        self: Arc<Self>,
        task_manager: Option<TaskManager>,
        shutdown: CancellationToken,
    ) {
        let mut interval = tokio::time::interval(self.update_interval);
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = interval.tick() => {
                    let (settings_result, catalog_result) = tokio::join!(
                        self.refresh_gateway_settings_once(),
                        self.update_model_catalog(),
                    );
                    self.handle_refresh_results(
                        settings_result,
                        catalog_result,
                        task_manager.as_ref(),
                    );
                }
            }
        }
    }

    #[cfg(feature = "test-support")]
    pub async fn sync_gateway_settings_for_test(&self) -> Result<()> {
        self.refresh_once().await
    }

    fn encode_generic_key(key: Option<Uuid>) -> u128 {
        key.map(|value| value.as_u128()).unwrap_or(0)
    }

    fn decode_generic_key(raw: u128) -> Option<Uuid> {
        (raw != 0).then_some(Uuid::from_u128(raw))
    }

    fn key_prefix(key: &Uuid) -> String {
        key.to_string().chars().take(6).collect()
    }

    fn cached_gateway_settings(&self) -> CachedGatewaySettings {
        CachedGatewaySettings {
            generic_key: Self::decode_generic_key(self.generic_key.load(Ordering::Acquire)),
            generic_global_daily_limit: self.generic_global_daily_limit.load(Ordering::Acquire),
            generic_per_ip_daily_limit: self.generic_per_ip_daily_limit.load(Ordering::Acquire),
            generic_window_ms: self.generic_window_ms.load(Ordering::Acquire),
            add_task_unauthorized_per_ip_daily_rate_limit: self
                .add_task_unauthorized_per_ip_daily_rate_limit
                .load(Ordering::Acquire),
            max_task_queue_len: self.max_task_queue_len.load(Ordering::Acquire),
            request_file_size_limit: self.request_file_size_limit.load(Ordering::Acquire),
            taskmanager_task_lifetime_sec: self
                .taskmanager_task_lifetime_sec
                .load(Ordering::Acquire),
            taskmanager_result_lifetime_sec: self
                .taskmanager_result_lifetime_sec
                .load(Ordering::Acquire),
            guest_generation_limit: self.guest_generation_limit.load(Ordering::Acquire),
            guest_window_ms: self.guest_window_ms.load(Ordering::Acquire),
            registered_generation_limit: self.registered_generation_limit.load(Ordering::Acquire),
            registered_window_ms: self.registered_window_ms.load(Ordering::Acquire),
        }
    }

    fn cached_rate_limit_whitelist(&self) -> Arc<HashSet<IpAddr>> {
        let guard = Guard::new();
        self.rate_limit_whitelist
            .load(Ordering::Acquire, &guard)
            .as_ref()
            .cloned()
            .unwrap_or_else(|| Arc::new(HashSet::new()))
    }

    fn store_gateway_settings(&self, settings: CachedGatewaySettings) {
        self.generic_key.store(
            Self::encode_generic_key(settings.generic_key),
            Ordering::Release,
        );
        self.generic_global_daily_limit
            .store(settings.generic_global_daily_limit, Ordering::Release);
        self.generic_per_ip_daily_limit
            .store(settings.generic_per_ip_daily_limit, Ordering::Release);
        self.generic_window_ms
            .store(settings.generic_window_ms, Ordering::Release);
        self.add_task_unauthorized_per_ip_daily_rate_limit.store(
            settings.add_task_unauthorized_per_ip_daily_rate_limit,
            Ordering::Release,
        );
        self.max_task_queue_len
            .store(settings.max_task_queue_len, Ordering::Release);
        self.request_file_size_limit
            .store(settings.request_file_size_limit, Ordering::Release);
        self.taskmanager_task_lifetime_sec
            .store(settings.taskmanager_task_lifetime_sec, Ordering::Release);
        self.taskmanager_result_lifetime_sec
            .store(settings.taskmanager_result_lifetime_sec, Ordering::Release);
        self.guest_generation_limit
            .store(settings.guest_generation_limit, Ordering::Release);
        self.guest_window_ms
            .store(settings.guest_window_ms, Ordering::Release);
        self.registered_generation_limit
            .store(settings.registered_generation_limit, Ordering::Release);
        self.registered_window_ms
            .store(settings.registered_window_ms, Ordering::Release);
    }

    fn store_rate_limit_whitelist(&self, whitelist: Arc<HashSet<IpAddr>>) {
        let previous = self
            .rate_limit_whitelist
            .swap((Some(Owned::new(whitelist)), Tag::None), Ordering::AcqRel);
        drop(previous);
    }

    pub(crate) async fn refresh_once(&self) -> Result<()> {
        let (settings_result, models_result) = tokio::join!(
            self.refresh_gateway_settings_once(),
            self.update_model_catalog(),
        );
        settings_result?;
        models_result?;
        Ok(())
    }

    async fn refresh_gateway_settings_once(&self) -> Result<()> {
        let now = self.db.server_time_ms().await?;
        self.update_gateway_settings(now).await
    }

    fn handle_refresh_results(
        &self,
        settings_result: Result<()>,
        catalog_result: Result<()>,
        task_manager: Option<&TaskManager>,
    ) {
        match settings_result {
            Ok(()) => {
                if self.has_database_gateway_settings()
                    && let Some(task_manager) = task_manager
                {
                    task_manager.set_lifetimes(
                        Duration::from_secs(self.taskmanager_task_lifetime_sec().max(1)),
                        Duration::from_secs(self.taskmanager_result_lifetime_sec().max(1)),
                    );
                }
            }
            Err(err) => {
                error!("Error updating gateway settings: {:?}", err);
            }
        }
        if let Err(err) = catalog_result {
            error!("Error updating model catalog: {:?}", err);
        }
    }

    async fn update_model_catalog(&self) -> Result<()> {
        let rows = self.db.fetch_active_model_catalog().await?;
        let next = Arc::new(ModelCatalog::from_rows(rows));
        let previous = self.model_catalog();
        if *next != *previous {
            info!(
                active_model_task_kind_pairs = next.model_count(),
                "Refreshed active model catalog from database"
            );
        }
        let previous = self
            .model_catalog
            .swap((Some(Owned::new(next)), Tag::None), Ordering::AcqRel);
        drop(previous);
        Ok(())
    }

    pub fn model_catalog(&self) -> Arc<ModelCatalog> {
        let guard = Guard::new();
        self.model_catalog
            .load(Ordering::Acquire, &guard)
            .as_ref()
            .cloned()
            .unwrap_or_else(|| Arc::new(ModelCatalog::default()))
    }

    async fn update_gateway_settings(&self, now: i64) -> Result<()> {
        let previous = self.cached_gateway_settings();
        let previous_rate_limit_whitelist = self.cached_rate_limit_whitelist();
        let had_database_gateway_settings = self.has_database_gateway_settings();
        match self.db.fetch_gateway_settings().await? {
            Some(GatewaySettingsRow {
                generic_key,
                generic_global_daily_limit,
                generic_per_ip_daily_limit,
                generic_window_ms,
                add_task_unauthorized_per_ip_daily_rate_limit,
                rate_limit_whitelist,
                max_task_queue_len,
                request_file_size_limit,
                taskmanager_task_lifetime_sec,
                taskmanager_result_lifetime_sec,
                guest_generation_limit,
                guest_window_ms,
                registered_generation_limit,
                registered_window_ms,
            }) => {
                let normalized_rate_limit_whitelist: FoldHashSet<String> = rate_limit_whitelist
                    .into_iter()
                    .map(|entry| entry.trim().to_lowercase())
                    .filter(|entry| !entry.is_empty())
                    .collect();
                let resolved_rate_limit_whitelist =
                    resolve_rate_limit_whitelist_with_status(&normalized_rate_limit_whitelist)
                        .await;
                let next = CachedGatewaySettings {
                    generic_key: Some(generic_key),
                    generic_global_daily_limit,
                    generic_per_ip_daily_limit,
                    generic_window_ms,
                    add_task_unauthorized_per_ip_daily_rate_limit,
                    max_task_queue_len,
                    request_file_size_limit,
                    taskmanager_task_lifetime_sec,
                    taskmanager_result_lifetime_sec,
                    guest_generation_limit,
                    guest_window_ms,
                    registered_generation_limit,
                    registered_window_ms,
                };
                let next_rate_limit_whitelist = if resolved_rate_limit_whitelist
                    .had_resolution_failures
                    && had_database_gateway_settings
                {
                    warn!(
                        gateway_generic_key_prefix = %Self::key_prefix(&generic_key),
                        whitelist_entries = normalized_rate_limit_whitelist.len(),
                        "Gateway rate-limit whitelist resolution was partial; keeping the last good resolved set"
                    );
                    Arc::clone(&previous_rate_limit_whitelist)
                } else {
                    Arc::new(resolved_rate_limit_whitelist.ips)
                };
                let whitelist_changed =
                    *next_rate_limit_whitelist != *previous_rate_limit_whitelist;
                if next != previous || whitelist_changed {
                    info!(
                        generic_key_prefix = %Self::key_prefix(&generic_key),
                        generic_global_daily_limit,
                        generic_per_ip_daily_limit,
                        generic_window_ms,
                        add_task_unauthorized_per_ip_daily_rate_limit,
                        max_task_queue_len,
                        request_file_size_limit,
                        taskmanager_task_lifetime_sec,
                        taskmanager_result_lifetime_sec,
                        guest_generation_limit,
                        guest_window_ms,
                        registered_generation_limit,
                        registered_window_ms,
                        rate_limit_whitelist_entries = normalized_rate_limit_whitelist.len(),
                        resolved_rate_limit_whitelist_ips = next_rate_limit_whitelist.len(),
                        "Refreshed gateway settings from database"
                    );
                }
                self.store_gateway_settings(next);
                self.store_rate_limit_whitelist(next_rate_limit_whitelist);
                self.has_database_gateway_settings
                    .store(true, Ordering::Release);
            }
            None => {
                warn!("app_settings row 1 not found; keeping cached gateway settings");
            }
        }
        self.last_gateway_settings_sync_ms
            .store(now as u64, Ordering::Release);
        Ok(())
    }

    pub fn generic_key(&self) -> Option<Uuid> {
        self.cached_gateway_settings().generic_key
    }

    pub fn generic_global_daily_limit(&self) -> u64 {
        self.cached_gateway_settings().generic_global_daily_limit
    }

    pub fn generic_per_ip_daily_limit(&self) -> u64 {
        self.cached_gateway_settings().generic_per_ip_daily_limit
    }

    pub fn generic_window_ms(&self) -> u64 {
        self.cached_gateway_settings().generic_window_ms
    }

    pub fn is_generic_key(&self, api_key: &Uuid) -> bool {
        self.generic_key()
            .is_some_and(|generic_key| generic_key == *api_key)
    }

    pub fn has_database_gateway_settings(&self) -> bool {
        self.has_database_gateway_settings.load(Ordering::Acquire)
    }

    pub fn add_task_unauthorized_per_ip_daily_rate_limit(&self) -> u64 {
        self.cached_gateway_settings()
            .add_task_unauthorized_per_ip_daily_rate_limit
    }

    pub fn max_task_queue_len(&self) -> u64 {
        self.cached_gateway_settings().max_task_queue_len
    }

    pub fn request_file_size_limit(&self) -> u64 {
        self.cached_gateway_settings().request_file_size_limit
    }

    pub fn taskmanager_task_lifetime_sec(&self) -> u64 {
        self.cached_gateway_settings().taskmanager_task_lifetime_sec
    }

    pub fn taskmanager_result_lifetime_sec(&self) -> u64 {
        self.cached_gateway_settings()
            .taskmanager_result_lifetime_sec
    }

    pub fn guest_generation_limit(&self) -> u64 {
        self.cached_gateway_settings().guest_generation_limit
    }

    pub fn guest_window_ms(&self) -> u64 {
        self.cached_gateway_settings().guest_window_ms
    }

    pub fn registered_generation_limit(&self) -> u64 {
        self.cached_gateway_settings().registered_generation_limit
    }

    pub fn registered_window_ms(&self) -> u64 {
        self.cached_gateway_settings().registered_window_ms
    }

    pub fn is_rate_limit_whitelisted_ip(&self, ip: &IpAddr) -> bool {
        self.cached_rate_limit_whitelist().contains(ip)
    }

    #[cfg(any(test, feature = "test-support"))]
    #[allow(dead_code)]
    pub fn seed_model_catalog(&self, rows: Vec<(String, String)>) {
        let catalog = Arc::new(ModelCatalog::from_rows(rows));
        let previous = self
            .model_catalog
            .swap((Some(Owned::new(catalog)), Tag::None), Ordering::AcqRel);
        drop(previous);
    }

    #[cfg(any(test, feature = "test-support"))]
    #[allow(dead_code)]
    pub fn seed_gateway_settings(&self, generic_key: Option<Uuid>) {
        let current = self.cached_gateway_settings();
        self.store_gateway_settings(CachedGatewaySettings {
            generic_key,
            generic_global_daily_limit: current.generic_global_daily_limit,
            generic_per_ip_daily_limit: current.generic_per_ip_daily_limit,
            generic_window_ms: current.generic_window_ms,
            add_task_unauthorized_per_ip_daily_rate_limit: current
                .add_task_unauthorized_per_ip_daily_rate_limit,
            max_task_queue_len: current.max_task_queue_len,
            request_file_size_limit: current.request_file_size_limit,
            taskmanager_task_lifetime_sec: current.taskmanager_task_lifetime_sec,
            taskmanager_result_lifetime_sec: current.taskmanager_result_lifetime_sec,
            guest_generation_limit: current.guest_generation_limit,
            guest_window_ms: current.guest_window_ms,
            registered_generation_limit: current.registered_generation_limit,
            registered_window_ms: current.registered_window_ms,
        });
    }

    #[cfg(any(test, feature = "test-support"))]
    #[allow(dead_code)]
    pub fn seed_generic_key_limits(
        &self,
        generic_key: Option<Uuid>,
        generic_global_daily_limit: u64,
        generic_per_ip_daily_limit: u64,
        generic_window_ms: u64,
    ) {
        let current = self.cached_gateway_settings();
        self.store_gateway_settings(CachedGatewaySettings {
            generic_key,
            generic_global_daily_limit,
            generic_per_ip_daily_limit,
            generic_window_ms,
            add_task_unauthorized_per_ip_daily_rate_limit: current
                .add_task_unauthorized_per_ip_daily_rate_limit,
            max_task_queue_len: current.max_task_queue_len,
            request_file_size_limit: current.request_file_size_limit,
            taskmanager_task_lifetime_sec: current.taskmanager_task_lifetime_sec,
            taskmanager_result_lifetime_sec: current.taskmanager_result_lifetime_sec,
            guest_generation_limit: current.guest_generation_limit,
            guest_window_ms: current.guest_window_ms,
            registered_generation_limit: current.registered_generation_limit,
            registered_window_ms: current.registered_window_ms,
        });
    }
}

#[cfg(test)]
mod tests {
    use anyhow::anyhow;

    use super::*;
    use crate::metrics::Metrics;

    #[tokio::test]
    async fn catalog_refresh_failure_does_not_block_task_lifetime_update() {
        let store = GatewayRuntimeSettingsStore::new(
            Arc::new(Database::new_mock()),
            GatewayRuntimeSettingsConfig {
                update_interval: Duration::from_secs(1),
                fallback_generic_key: None,
            },
        );
        let current = store.cached_gateway_settings();
        store.store_gateway_settings(CachedGatewaySettings {
            taskmanager_task_lifetime_sec: 7,
            taskmanager_result_lifetime_sec: 11,
            ..current
        });
        store
            .has_database_gateway_settings
            .store(true, Ordering::Release);
        let task_manager = TaskManager::new_with_task_lifetime(
            1,
            1,
            Duration::from_secs(1),
            Duration::from_secs(60),
            Duration::from_secs(60),
            Metrics::new(0.05).expect("metrics"),
            None,
        )
        .await;

        store.handle_refresh_results(
            Ok(()),
            Err(anyhow!("model catalog unavailable")),
            Some(&task_manager),
        );

        assert_eq!(
            task_manager.lifetimes_for_test(),
            (Duration::from_secs(7), Duration::from_secs(11))
        );
    }
}
