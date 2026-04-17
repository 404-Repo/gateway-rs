use anyhow::Result;
use foldhash::fast::RandomState;
use foldhash::{HashMap as FoldHashMap, HashSet as FoldHashSet};
use moka::future::Cache;
use std::fmt::Display;
use std::hash::Hash;
use std::net::IpAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use uuid::Uuid;

use super::Database;
use super::data_access::ApiKeyBillingOwnerRow;
use super::gateway_settings::{GatewayRuntimeSettingsConfig, GatewayRuntimeSettingsStore};
use super::task_lifecycle::GenerationBillingOwner;
use crate::crypto::crypto_provider::ApiKeyHasher;

const DELTA_SYNC_OVERLAP_MS: i64 = 5_000;

pub fn api_key_sync_interval(update_interval_secs: u64) -> Duration {
    Duration::from_secs(update_interval_secs.max(1))
}

#[derive(Debug, Clone)]
struct CachedApiKeyRecord {
    user_id: Option<i64>,
    company_id: Option<Uuid>,
    billing_owner: GenerationBillingOwner,
}

#[derive(Debug, Clone)]
struct CachedUserMeta {
    email: Option<Arc<str>>,
    concurrent_limit: u64,
    daily_limit: u64,
}

#[derive(Clone)]
struct UnknownKeyIpGuard {
    misses: Cache<Arc<str>, Arc<AtomicU64>, RandomState>,
    cooldowns: Cache<Arc<str>, (), RandomState>,
    miss_limit: u64,
}

impl UnknownKeyIpGuard {
    fn new(miss_ttl: Duration, cooldown_ttl: Duration, capacity: u64, miss_limit: u64) -> Self {
        Self {
            misses: Cache::builder()
                .max_capacity(capacity.max(1))
                .time_to_live(miss_ttl)
                .build_with_hasher(RandomState::default()),
            cooldowns: Cache::builder()
                .max_capacity(capacity.max(1))
                .time_to_live(cooldown_ttl)
                .build_with_hasher(RandomState::default()),
            miss_limit: miss_limit.max(1),
        }
    }

    async fn is_blocked(&self, source_key: &Arc<str>) -> bool {
        self.cooldowns.get(source_key).await.is_some()
    }

    async fn record_miss(&self, source_key: Arc<str>) -> bool {
        let misses = self
            .misses
            .get_with(source_key.clone(), async { Arc::new(AtomicU64::new(0)) })
            .await;
        let attempts = misses.fetch_add(1, Ordering::AcqRel) + 1;
        if attempts >= self.miss_limit {
            self.cooldowns.insert(source_key, ()).await;
            true
        } else {
            false
        }
    }
}

enum CachedLookupRecord {
    Found(CachedApiKeyRecord),
    NegativeCached,
    Unknown,
}

pub struct ApiKeyValidator {
    db: Arc<Database>,
    // mapping from user_id -> list of api_key_hashes
    users: scc::HashMap<i64, Vec<[u8; 32]>, RandomState>,
    // mapping from user_id -> cached metadata used by the gateway auth/rate-limit path
    users_meta: scc::HashMap<i64, CachedUserMeta, RandomState>,
    // mapping from company_id -> cached metadata used by the gateway auth/rate-limit path
    companies: scc::HashMap<Uuid, (Arc<str>, u64, u64), RandomState>,
    // forward mapping: company_id -> list of api_key_hashes
    company_keys: scc::HashMap<Uuid, Vec<[u8; 32]>, RandomState>,
    // reverse mapping: api_key_hash -> key record for auth and billing
    api_keys_by_hash: scc::HashMap<[u8; 32], CachedApiKeyRecord, RandomState>,
    // Keyed BLAKE3 hasher for API key verification
    hasher: ApiKeyHasher,
    // Cache of validated API key hashes -> key record
    validated_key_cache: Cache<[u8; 32], CachedApiKeyRecord, RandomState>,
    negative_key_cache: Cache<[u8; 32], (), RandomState>,
    unknown_key_ip_guard: UnknownKeyIpGuard,
    update_interval: Duration,
    // synchronization timestamps (milliseconds since epoch; 0 means "never")
    last_users_sync_ms: AtomicU64,
    last_companies_meta_sync_ms: AtomicU64,
    last_company_keys_sync_ms: AtomicU64,
    last_billing_owners_sync_ms: AtomicU64,
    gateway_settings: Arc<GatewayRuntimeSettingsStore>,
}

#[derive(Debug, Clone, Default)]
pub struct ApiKeyLookup {
    pub user_id: Option<i64>,
    pub user_email: Option<Arc<str>>,
    pub user_limits: Option<(u64, u64)>,
    pub company_id: Option<Uuid>,
    pub company_info: Option<(Arc<str>, u64, u64)>,
    pub billing_owner: Option<GenerationBillingOwner>,
    pub auth_lookup_blocked: bool,
}

pub struct ApiKeyValidatorConfig<'a> {
    pub update_interval: Duration,
    pub cache_ttl_sec: u64,
    pub cache_initial_capacity: usize,
    pub cache_max_capacity: u64,
    pub api_key_secret: &'a str,
    pub negative_cache_ttl_sec: u64,
    pub unknown_key_ip_miss_ttl_sec: u64,
    pub unknown_key_ip_cooldown_ttl_sec: u64,
    pub unknown_key_ip_cache_capacity: u64,
    pub unknown_key_ip_miss_limit: u64,
    pub deleted_keys_ttl_minutes: u64,
    pub fallback_generic_key: Option<Uuid>,
}

impl ApiKeyValidator {
    pub fn new(db: Arc<Database>, config: ApiKeyValidatorConfig<'_>) -> Result<Self> {
        let ApiKeyValidatorConfig {
            update_interval,
            cache_ttl_sec,
            cache_initial_capacity,
            cache_max_capacity,
            api_key_secret,
            negative_cache_ttl_sec,
            unknown_key_ip_miss_ttl_sec,
            unknown_key_ip_cooldown_ttl_sec,
            unknown_key_ip_cache_capacity,
            unknown_key_ip_miss_limit,
            deleted_keys_ttl_minutes: _,
            fallback_generic_key,
        } = config;
        let gateway_settings = Arc::new(GatewayRuntimeSettingsStore::new(
            Arc::clone(&db),
            GatewayRuntimeSettingsConfig {
                update_interval,
                fallback_generic_key,
            },
        ));
        let companies = scc::HashMap::with_capacity_and_hasher(4096, RandomState::default());
        let company_keys = scc::HashMap::with_capacity_and_hasher(4096, RandomState::default());
        Ok(Self {
            db,
            users: scc::HashMap::with_capacity_and_hasher(4096, RandomState::default()),
            users_meta: scc::HashMap::with_capacity_and_hasher(4096, RandomState::default()),
            companies,
            company_keys,
            api_keys_by_hash: scc::HashMap::with_capacity_and_hasher(4096, RandomState::default()),
            hasher: ApiKeyHasher::new(api_key_secret)?,
            validated_key_cache: Cache::builder()
                .initial_capacity(cache_initial_capacity)
                .max_capacity(cache_max_capacity)
                .time_to_live(Duration::from_secs(cache_ttl_sec))
                .build_with_hasher(RandomState::default()),
            negative_key_cache: Cache::builder()
                .initial_capacity(cache_initial_capacity)
                .max_capacity(cache_max_capacity)
                .time_to_live(Duration::from_secs(negative_cache_ttl_sec.max(1)))
                .build_with_hasher(RandomState::default()),
            unknown_key_ip_guard: UnknownKeyIpGuard::new(
                Duration::from_secs(unknown_key_ip_miss_ttl_sec.max(1)),
                Duration::from_secs(unknown_key_ip_cooldown_ttl_sec.max(1)),
                unknown_key_ip_cache_capacity.max(1),
                unknown_key_ip_miss_limit.max(1),
            ),
            update_interval,
            last_users_sync_ms: AtomicU64::new(0),
            last_companies_meta_sync_ms: AtomicU64::new(0),
            last_company_keys_sync_ms: AtomicU64::new(0),
            last_billing_owners_sync_ms: AtomicU64::new(0),
            gateway_settings,
        })
    }

    pub async fn run(self: Arc<Self>, shutdown: CancellationToken) {
        let mut interval = tokio::time::interval(self.update_interval);
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = interval.tick() => {
                    if let Err(e) = self.update_hashes().await {
                        error!("Error updating keys: {:?}", e);
                    }
                }
            }
        }
    }

    #[cfg(feature = "test-support")]
    pub async fn sync_db_caches_for_test(&self) -> Result<()> {
        self.gateway_settings
            .sync_gateway_settings_for_test()
            .await?;
        self.update_hashes().await
    }

    pub fn generic_key(&self) -> Option<Uuid> {
        self.gateway_settings.generic_key()
    }

    pub fn is_generic_key(&self, api_key: &Uuid) -> bool {
        self.gateway_settings.is_generic_key(api_key)
    }

    pub fn has_database_gateway_settings(&self) -> bool {
        self.gateway_settings.has_database_gateway_settings()
    }

    pub fn add_task_unauthorized_per_ip_daily_rate_limit(&self) -> u64 {
        self.gateway_settings
            .add_task_unauthorized_per_ip_daily_rate_limit()
    }

    pub fn max_task_queue_len(&self) -> u64 {
        self.gateway_settings.max_task_queue_len()
    }

    pub fn request_file_size_limit(&self) -> u64 {
        self.gateway_settings.request_file_size_limit()
    }

    pub fn guest_generation_limit(&self) -> u64 {
        self.gateway_settings.guest_generation_limit()
    }

    pub fn guest_window_ms(&self) -> u64 {
        self.gateway_settings.guest_window_ms()
    }

    pub fn generic_global_daily_limit(&self) -> u64 {
        self.gateway_settings.generic_global_daily_limit()
    }

    pub fn generic_per_ip_daily_limit(&self) -> u64 {
        self.gateway_settings.generic_per_ip_daily_limit()
    }

    pub fn generic_window_ms(&self) -> u64 {
        self.gateway_settings.generic_window_ms()
    }

    pub fn registered_generation_limit(&self) -> u64 {
        self.gateway_settings.registered_generation_limit()
    }

    pub fn registered_window_ms(&self) -> u64 {
        self.gateway_settings.registered_window_ms()
    }

    pub fn is_rate_limit_whitelisted_ip(&self, ip: &IpAddr) -> bool {
        self.gateway_settings.is_rate_limit_whitelisted_ip(ip)
    }

    pub fn gateway_settings(&self) -> Arc<GatewayRuntimeSettingsStore> {
        Arc::clone(&self.gateway_settings)
    }

    fn convert_hash(bytes: &[u8]) -> Option<[u8; 32]> {
        (bytes.len() == 32).then(|| {
            let mut array = [0u8; 32];
            array.copy_from_slice(bytes);
            array
        })
    }

    fn convert_hashes<I, T>(hashes: I) -> Vec<[u8; 32]>
    where
        I: IntoIterator<Item = T>,
        T: AsRef<[u8]>,
    {
        hashes
            .into_iter()
            .filter_map(|bytes| Self::convert_hash(bytes.as_ref()))
            .collect()
    }

    async fn update_entity_hashes<K>(
        &self,
        forward_map: &scc::HashMap<K, Vec<[u8; 32]>, RandomState>,
        id: K,
        new_hashes: Vec<[u8; 32]>,
    ) where
        K: Copy + Eq + Hash + Display + Send + Sync + 'static,
    {
        if let Some(entry) = forward_map.get_async(&id).await {
            let prev = entry.get();
            let new_set: FoldHashSet<[u8; 32]> = new_hashes.iter().copied().collect();
            let prev_set: FoldHashSet<[u8; 32]> = prev.iter().copied().collect();

            let removed: Vec<[u8; 32]> = prev_set.difference(&new_set).copied().collect();
            let added: Vec<[u8; 32]> = new_set.difference(&prev_set).copied().collect();

            if !removed.is_empty() || !added.is_empty() {
                info!(
                    "Entity {}: {} API key hashes removed, {} added",
                    id,
                    removed.len(),
                    added.len()
                );
            }
        } else if !new_hashes.is_empty() {
            info!(
                "Entity {}: {} API key hashes removed, {} added",
                id,
                0,
                new_hashes.len()
            );
        }

        match forward_map.entry_async(id).await {
            scc::hash_map::Entry::Occupied(mut entry) => {
                *entry.get_mut() = new_hashes;
            }
            scc::hash_map::Entry::Vacant(entry) => {
                entry.insert_entry(new_hashes);
            }
        }
    }

    async fn apply_user_hashes(&self, user_id: i64, new_hashes: Vec<[u8; 32]>) {
        self.update_entity_hashes(&self.users, user_id, new_hashes)
            .await;
    }

    async fn apply_company_hashes(&self, cid: Uuid, new_hashes: Vec<[u8; 32]>) {
        self.update_entity_hashes(&self.company_keys, cid, new_hashes)
            .await;
    }

    async fn upsert_billing_owner(&self, key_hash: [u8; 32], owner: GenerationBillingOwner) {
        let record = CachedApiKeyRecord {
            user_id: owner.user_id,
            company_id: owner.company_id,
            billing_owner: owner,
        };
        self.negative_key_cache.invalidate(&key_hash).await;
        let _ = self
            .api_keys_by_hash
            .insert_async(key_hash, record.clone())
            .await;
        self.validated_key_cache.insert(key_hash, record).await;
    }

    async fn remove_billing_owner(&self, key_hash: [u8; 32]) {
        let _ = self.api_keys_by_hash.remove_async(&key_hash).await;
        self.validated_key_cache.invalidate(&key_hash).await;
        self.negative_key_cache.insert(key_hash, ()).await;
    }

    async fn update_billing_owners(&self, now: i64) -> Result<()> {
        let last_sync_ms = self.last_billing_owners_sync_ms.load(Ordering::Acquire);

        if let Some(since) = Self::delta_since(last_sync_ms, now, "billing_owners") {
            let deltas = self
                .db
                .fetch_delta_api_key_billing_owners(since, now)
                .await?;
            for ApiKeyBillingOwnerRow {
                api_key_hash,
                owner,
                revoked_at,
            } in deltas
            {
                let Some(hash) = Self::convert_hash(&api_key_hash) else {
                    continue;
                };
                if revoked_at.is_some() {
                    self.remove_billing_owner(hash).await;
                } else {
                    self.upsert_billing_owner(hash, owner).await;
                }
            }
        } else {
            let all = self.db.fetch_all_api_key_billing_owners().await?;
            self.validated_key_cache.invalidate_all();
            self.api_keys_by_hash.clear_async().await;
            for ApiKeyBillingOwnerRow {
                api_key_hash,
                owner,
                ..
            } in all
            {
                let Some(hash) = Self::convert_hash(&api_key_hash) else {
                    continue;
                };
                self.upsert_billing_owner(hash, owner).await;
            }
        }

        self.last_billing_owners_sync_ms
            .store(now as u64, Ordering::Release);
        Ok(())
    }

    async fn update_hashes(&self) -> Result<()> {
        let now = self.db.server_time_ms().await?;
        let initial_sync_pending = self.last_users_sync_ms.load(Ordering::Acquire) == 0
            || self.last_companies_meta_sync_ms.load(Ordering::Acquire) == 0
            || self.last_company_keys_sync_ms.load(Ordering::Acquire) == 0
            || self.last_billing_owners_sync_ms.load(Ordering::Acquire) == 0;

        if initial_sync_pending {
            self.update_users_data(now).await?;
            self.update_companies_meta(now).await?;
            self.update_company_keys(now).await?;
            self.update_billing_owners(now).await?;
        } else {
            tokio::try_join!(
                self.update_users_data(now),
                self.update_companies_meta(now),
                self.update_company_keys(now),
                self.update_billing_owners(now),
            )?;
        }

        Ok(())
    }

    async fn update_users_data(&self, now: i64) -> Result<()> {
        let last_sync_ms = self.last_users_sync_ms.load(Ordering::Acquire);

        if let Some(since) = Self::delta_since(last_sync_ms, now, "users") {
            let deltas = self.db.fetch_delta_user_key_hashes(since, now).await?;
            let total_new = deltas.iter().map(|(_, _, _, _, v)| v.len()).sum::<usize>();
            info!(
                "Fetched {} user API key hashes for {} users (delta)",
                total_new,
                deltas.len()
            );
            for (user_id, email, concurrent, daily, new_hashes) in deltas {
                let converted = Self::convert_hashes(new_hashes);
                let normalized_email = email.trim();
                let value = CachedUserMeta {
                    email: (!normalized_email.is_empty())
                        .then(|| Arc::<str>::from(normalized_email)),
                    concurrent_limit: concurrent,
                    daily_limit: daily,
                };
                match self.users_meta.entry_async(user_id).await {
                    scc::hash_map::Entry::Occupied(mut entry) => {
                        *entry.get_mut() = value;
                    }
                    scc::hash_map::Entry::Vacant(entry) => {
                        entry.insert_entry(value);
                    }
                }
                self.apply_user_hashes(user_id, converted).await;
            }
        } else {
            // Full fetch on first run
            let all = self.db.fetch_all_user_key_hashes().await?;
            info!(
                "Retrieved {} API key hashes for {} users from the database",
                all.iter().map(|(_, _, _, _, v)| v.len()).sum::<usize>(),
                all.len()
            );

            // Rebuild maps
            self.users.clear_async().await;
            self.users_meta.clear_async().await;
            for (user_id, email, concurrent, daily, hashes) in all {
                let list = Self::convert_hashes(hashes);
                let _ = self.users.insert_async(user_id, list).await;
                let normalized_email = email.trim();
                let _ = self
                    .users_meta
                    .insert_async(
                        user_id,
                        CachedUserMeta {
                            email: (!normalized_email.is_empty())
                                .then(|| Arc::<str>::from(normalized_email)),
                            concurrent_limit: concurrent,
                            daily_limit: daily,
                        },
                    )
                    .await;
            }
        }

        self.last_users_sync_ms.store(now as u64, Ordering::Release);
        Ok(())
    }

    async fn update_companies_meta(&self, now: i64) -> Result<()> {
        let last_sync_ms = self.last_companies_meta_sync_ms.load(Ordering::Acquire);

        if let Some(since) = Self::delta_since(last_sync_ms, now, "companies_meta") {
            let deltas = self.db.fetch_delta_companies_meta(since, now).await?;
            for (cid, limits) in deltas {
                let (name, concurrent, daily) = limits;
                let value = (Arc::<str>::from(name), concurrent, daily);
                match self.companies.entry_async(cid).await {
                    scc::hash_map::Entry::Occupied(mut entry) => {
                        *entry.get_mut() = value;
                    }
                    scc::hash_map::Entry::Vacant(entry) => {
                        entry.insert_entry(value);
                    }
                }
            }
        } else {
            let all = self.db.fetch_full_companies_meta().await?;
            self.companies.clear_async().await;
            for (cid, limits) in all {
                let (name, concurrent, daily) = limits;
                let _ = self
                    .companies
                    .insert_async(cid, (Arc::<str>::from(name), concurrent, daily))
                    .await;
            }
        }

        self.last_companies_meta_sync_ms
            .store(now as u64, Ordering::Release);
        Ok(())
    }

    async fn update_company_keys(&self, now: i64) -> Result<()> {
        let last_sync_ms = self.last_company_keys_sync_ms.load(Ordering::Acquire);

        if let Some(since) = Self::delta_since(last_sync_ms, now, "company_keys") {
            let deltas = self.db.fetch_delta_company_keys(since, now).await?;
            let companies = deltas.len();
            let total_new = deltas.iter().map(|(_, v)| v.len()).sum::<usize>();
            info!(
                "Fetched {} company API key hashes for {} companies (delta)",
                total_new, companies
            );
            for (cid, hashes) in deltas {
                let list = Self::convert_hashes(hashes);
                self.apply_company_hashes(cid, list).await;
            }
        } else {
            let all_keys = self.db.fetch_full_company_keys().await?;
            let total_hashes = all_keys.len();
            self.company_keys.clear_async().await;

            let mut grouped: FoldHashMap<Uuid, Vec<[u8; 32]>> = FoldHashMap::default();
            for (cid, hash) in all_keys {
                if let Some(array) = Self::convert_hash(&hash) {
                    grouped.entry(cid).or_default().push(array);
                }
            }
            let companies = grouped.len();
            info!(
                "Retrieved {} company API key hashes for {} companies from the database",
                total_hashes, companies
            );
            for (cid, list) in grouped {
                let _ = self.company_keys.insert_async(cid, list).await;
            }
        }

        self.last_company_keys_sync_ms
            .store(now as u64, Ordering::Release);
        Ok(())
    }

    async fn lookup_cached_record(&self, key_hash: [u8; 32]) -> CachedLookupRecord {
        if let Some(record) = self.validated_key_cache.get(&key_hash).await {
            return CachedLookupRecord::Found(record);
        }
        if let Some(entry) = self.api_keys_by_hash.get_async(&key_hash).await {
            let record = entry.get().clone();
            self.validated_key_cache
                .insert(key_hash, record.clone())
                .await;
            return CachedLookupRecord::Found(record);
        }
        if self.negative_key_cache.get(&key_hash).await.is_some() {
            return CachedLookupRecord::NegativeCached;
        }

        CachedLookupRecord::Unknown
    }

    async fn lookup_record_with_db_fallback(
        &self,
        key_hash: [u8; 32],
    ) -> Result<Option<CachedApiKeyRecord>> {
        let owner = self.db.fetch_api_key_billing_owner(&key_hash).await?;
        if let Some(cached_owner) = owner {
            self.upsert_billing_owner(key_hash, cached_owner.clone())
                .await;
            return Ok(Some(CachedApiKeyRecord {
                user_id: cached_owner.user_id,
                company_id: cached_owner.company_id,
                billing_owner: cached_owner,
            }));
        }
        self.negative_key_cache.insert(key_hash, ()).await;
        Ok(None)
    }

    async fn build_lookup(&self, record: Option<CachedApiKeyRecord>) -> ApiKeyLookup {
        let user_id = record.as_ref().and_then(|value| value.user_id);
        let company_id = record.as_ref().and_then(|value| value.company_id);

        let company_info = if let Some(cid) = company_id {
            if let Some(entry) = self.companies.get_async(&cid).await {
                Some(entry.get().clone())
            } else {
                match self.db.fetch_company_meta(cid).await {
                    Ok(Some((name, concurrent, daily))) => {
                        let company_info = (Arc::<str>::from(name), concurrent, daily);
                        let _ = self.companies.insert_async(cid, company_info.clone()).await;
                        Some(company_info)
                    }
                    Ok(None) => None,
                    Err(err) => {
                        warn!("Failed to fetch company metadata for {}: {:?}", cid, err);
                        None
                    }
                }
            }
        } else {
            None
        };

        let (user_email, user_limits) = if let Some(uid) = user_id {
            if let Some(entry) = self.users_meta.get_async(&uid).await {
                let meta = entry.get().clone();
                (meta.email, Some((meta.concurrent_limit, meta.daily_limit)))
            } else {
                match self.db.fetch_user_meta(uid).await {
                    Ok(Some((email, concurrent, daily))) => {
                        let normalized_email = email.trim();
                        let email = (!normalized_email.is_empty())
                            .then(|| Arc::<str>::from(normalized_email));
                        let _ = self
                            .users_meta
                            .insert_async(
                                uid,
                                CachedUserMeta {
                                    email: email.clone(),
                                    concurrent_limit: concurrent,
                                    daily_limit: daily,
                                },
                            )
                            .await;
                        (email, Some((concurrent, daily)))
                    }
                    Ok(None) => (None, None),
                    Err(err) => {
                        warn!("Failed to fetch user metadata for {}: {:?}", uid, err);
                        (None, None)
                    }
                }
            }
        } else {
            (None, None)
        };

        ApiKeyLookup {
            user_id,
            user_email,
            user_limits,
            company_id,
            company_info,
            billing_owner: record.map(|value| value.billing_owner),
            auth_lookup_blocked: false,
        }
    }

    pub async fn lookup(&self, api_key: &str) -> ApiKeyLookup {
        self.lookup_with_unknown_key_guard(api_key, None).await
    }

    pub async fn lookup_with_unknown_key_guard(
        &self,
        api_key: &str,
        source_key: Option<Arc<str>>,
    ) -> ApiKeyLookup {
        let key_hash = self.hasher.compute_hash_array(api_key);

        match self.lookup_cached_record(key_hash).await {
            CachedLookupRecord::Found(record) => return self.build_lookup(Some(record)).await,
            CachedLookupRecord::NegativeCached => {
                let blocked = if let Some(source_key) = source_key {
                    if self.unknown_key_ip_guard.is_blocked(&source_key).await {
                        true
                    } else {
                        self.unknown_key_ip_guard.record_miss(source_key).await
                    }
                } else {
                    false
                };
                let mut lookup = self.build_lookup(None).await;
                lookup.auth_lookup_blocked = blocked;
                return lookup;
            }
            CachedLookupRecord::Unknown => {}
        }

        if let Some(source_key) = source_key.as_ref()
            && self.unknown_key_ip_guard.is_blocked(source_key).await
        {
            // Once an IP enters cooldown, skip DB fallback for unknown hashes to keep
            // repeated UUID probes from reaching Postgres. Known-good in-memory keys still pass.
            let mut lookup = self.build_lookup(None).await;
            lookup.auth_lookup_blocked = true;
            return lookup;
        }

        let (record, should_record_miss) = match self.lookup_record_with_db_fallback(key_hash).await
        {
            Ok(record) => {
                let should_record_miss = record.is_none();
                (record, should_record_miss)
            }
            Err(err) => {
                warn!("Failed to lookup API key record in database: {:?}", err);
                (None, false)
            }
        };

        let blocked = if should_record_miss {
            if let Some(source_key) = source_key {
                self.unknown_key_ip_guard.record_miss(source_key).await
            } else {
                false
            }
        } else {
            false
        };

        let mut lookup = self.build_lookup(record).await;
        lookup.auth_lookup_blocked = blocked;
        lookup
    }

    #[cfg(any(test, feature = "test-support"))]
    #[allow(dead_code)]
    pub fn seed_gateway_settings(&self, generic_key: Option<Uuid>) {
        self.gateway_settings.seed_gateway_settings(generic_key);
    }

    #[cfg(any(test, feature = "test-support"))]
    #[allow(dead_code)]
    pub async fn seed_user_key(&self, api_key: &str, user_id: i64, user_email: Option<&str>) {
        let key_hash = self.hasher.compute_hash_array(api_key);
        let _ = self.users.insert_async(user_id, vec![key_hash]).await;
        let _ = self
            .users_meta
            .insert_async(
                user_id,
                CachedUserMeta {
                    email: user_email
                        .map(str::trim)
                        .filter(|email| !email.is_empty())
                        .map(Arc::<str>::from),
                    concurrent_limit: 1,
                    daily_limit: 10,
                },
            )
            .await;
        let owner = GenerationBillingOwner {
            api_key_id: 1,
            account_id: 1,
            user_id: Some(user_id),
            company_id: None,
        };
        self.upsert_billing_owner(key_hash, owner).await;
    }

    #[cfg(any(test, feature = "test-support"))]
    #[allow(dead_code)]
    pub async fn seed_company_key(
        &self,
        api_key: &str,
        company_id: Uuid,
        company_name: &str,
        concurrent_limit: u64,
        daily_limit: u64,
    ) {
        let key_hash = self.hasher.compute_hash_array(api_key);
        let _ = self
            .companies
            .insert_async(
                company_id,
                (
                    Arc::<str>::from(company_name),
                    concurrent_limit,
                    daily_limit,
                ),
            )
            .await;
        let _ = self
            .company_keys
            .insert_async(company_id, vec![key_hash])
            .await;
        let owner = GenerationBillingOwner {
            api_key_id: 1,
            account_id: 1,
            user_id: None,
            company_id: Some(company_id),
        };
        self.upsert_billing_owner(key_hash, owner).await;
    }

    fn delta_since(last_sync_ms: u64, now: i64, label: &'static str) -> Option<i64> {
        if last_sync_ms == 0 {
            return None;
        }
        let last_sync_ms = last_sync_ms as i64;
        if now <= last_sync_ms {
            warn!(
                stream = label,
                previous_ms = last_sync_ms,
                current_ms = now,
                "Database clock moved backwards; forcing a full resync"
            );
            return None;
        }
        Some(last_sync_ms.saturating_sub(DELTA_SYNC_OVERLAP_MS).max(0))
    }
}

#[cfg(test)]
mod tests {
    use super::{ApiKeyValidator, DELTA_SYNC_OVERLAP_MS, UnknownKeyIpGuard};
    use std::sync::Arc;
    use std::time::Duration;

    #[test]
    fn delta_since_uses_overlap_window_for_forward_progress() {
        assert_eq!(
            ApiKeyValidator::delta_since(12_000, 20_000, "users"),
            Some(12_000 - DELTA_SYNC_OVERLAP_MS)
        );
    }

    #[test]
    fn delta_since_clamps_to_zero_when_last_sync_is_inside_overlap_window() {
        assert_eq!(ApiKeyValidator::delta_since(2_000, 7_500, "users"), Some(0));
    }

    #[test]
    fn delta_since_returns_none_before_first_sync() {
        assert_eq!(ApiKeyValidator::delta_since(0, 10_000, "users"), None);
    }

    #[test]
    fn delta_since_returns_none_when_clock_stalls_or_moves_backwards() {
        assert_eq!(ApiKeyValidator::delta_since(10_000, 10_000, "users"), None);
        assert_eq!(ApiKeyValidator::delta_since(10_000, 9_999, "users"), None);
    }

    #[tokio::test]
    async fn unknown_key_ip_guard_blocks_after_threshold() {
        let miss_limit = 3;
        let guard = UnknownKeyIpGuard::new(
            Duration::from_secs(60),
            Duration::from_secs(60),
            32,
            miss_limit,
        );
        let source = Arc::<str>::from("127.0.0.1");

        for _ in 0..(miss_limit - 1) {
            assert!(!guard.record_miss(Arc::clone(&source)).await);
        }

        assert!(guard.record_miss(Arc::clone(&source)).await);
        assert!(guard.is_blocked(&source).await);
    }
}
