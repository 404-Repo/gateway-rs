use anyhow::Result;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use foldhash::fast::RandomState;
use foldhash::{HashMap as FoldHashMap, HashSet as FoldHashSet};
use moka::future::Cache;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use uuid::Uuid;

use super::Database;
use crate::common::crypto_provider::ApiKeyHasher;

pub struct ApiKeyValidator {
    db: Arc<Database>,
    // mapping from user_id -> list of api_key_hashes
    users: scc::HashMap<Uuid, Vec<[u8; 32]>, RandomState>,
    // reverse mapping: api_key_hash -> user_id
    api_key_hashes: scc::HashMap<[u8; 32], Uuid, RandomState>,
    // mapping from company_id -> (name, hourly, daily) rate limits
    companies: scc::HashMap<Uuid, (String, u64, u64), RandomState>,
    // forward mapping: company_id -> list of api_key_hashes
    company_keys: scc::HashMap<Uuid, Vec<[u8; 32]>, RandomState>,
    // reverse mapping: company_api_key_hash -> company_id
    company_api_key_hashes: scc::HashMap<[u8; 32], Uuid, RandomState>,
    // Keyed BLAKE3 hasher for API key verification
    hasher: ApiKeyHasher,
    // Cache of validated API key hashes -> user_id with TTL
    validated_api_key_cache: Cache<[u8; 32], Uuid, RandomState>,
    // Cache of validated company API key hashes -> company_id with TTL
    validated_company_key_cache: Cache<[u8; 32], Uuid, RandomState>,
    update_interval: Duration,
    // synchronization timestamps (seconds since epoch; 0 means "never")
    last_users_sync_sec: AtomicU64,
    last_companies_meta_sync_sec: AtomicU64,
    last_company_keys_sync_sec: AtomicU64,
    // TTL for physically removing soft-deleted keys
    deleted_keys_ttl_minutes: u64,
}

impl ApiKeyValidator {
    pub fn new(
        db: Arc<Database>,
        update_interval: Duration,
        cache_ttl_sec: u64,
        cache_initial_capacity: usize,
        cache_max_capacity: u64,
        api_key_secret: &str,
        deleted_keys_ttl_minutes: u64,
    ) -> Result<Self> {
        let validation_cache = Cache::builder()
            .initial_capacity(cache_initial_capacity)
            .max_capacity(cache_max_capacity)
            .time_to_live(Duration::from_secs(cache_ttl_sec))
            .build_with_hasher(RandomState::default());

        let company_validation_cache = Cache::builder()
            .initial_capacity(cache_initial_capacity)
            .max_capacity(cache_max_capacity)
            .time_to_live(Duration::from_secs(cache_ttl_sec))
            .build_with_hasher(RandomState::default());

        let companies = scc::HashMap::with_capacity_and_hasher(4096, RandomState::default());
        let company_api_key_hashes =
            scc::HashMap::with_capacity_and_hasher(4096, RandomState::default());
        let company_keys = scc::HashMap::with_capacity_and_hasher(4096, RandomState::default());
        Ok(Self {
            db,
            users: scc::HashMap::with_capacity_and_hasher(4096, RandomState::default()),
            api_key_hashes: scc::HashMap::with_capacity_and_hasher(4096, RandomState::default()),
            companies,
            company_keys,
            company_api_key_hashes,
            hasher: ApiKeyHasher::new(api_key_secret)?,
            validated_api_key_cache: validation_cache,
            validated_company_key_cache: company_validation_cache,
            update_interval,
            last_users_sync_sec: AtomicU64::new(0),
            last_companies_meta_sync_sec: AtomicU64::new(0),
            last_company_keys_sync_sec: AtomicU64::new(0),
            deleted_keys_ttl_minutes,
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

    async fn update_entity_hashes(
        &self,
        forward_map: &scc::HashMap<Uuid, Vec<[u8; 32]>, RandomState>,
        reverse_map: &scc::HashMap<[u8; 32], Uuid, RandomState>,
        cache: &Cache<[u8; 32], Uuid, RandomState>,
        id: Uuid,
        new_hashes: Vec<[u8; 32]>,
    ) {
        if let Some(entry) = forward_map.get_async(&id).await {
            let prev = entry.get();
            let new_set: FoldHashSet<[u8; 32]> = new_hashes.iter().copied().collect();
            let prev_set: FoldHashSet<[u8; 32]> = prev.iter().copied().collect();

            let removed: Vec<[u8; 32]> = prev_set.difference(&new_set).copied().collect();
            let added: Vec<[u8; 32]> = new_set.difference(&prev_set).copied().collect();

            for hash in &removed {
                let _ = reverse_map.remove_async(hash).await;
                cache.invalidate(hash).await;
            }

            for hash in &added {
                let _ = reverse_map.insert_async(*hash, id).await;
            }

            if !removed.is_empty() || !added.is_empty() {
                info!(
                    "Entity {}: {} API key hashes removed, {} added",
                    id,
                    removed.len(),
                    added.len()
                );
            }
        } else {
            for hash in &new_hashes {
                let _ = reverse_map.insert_async(*hash, id).await;
            }

            if !new_hashes.is_empty() {
                info!(
                    "Entity {}: {} API key hashes removed, {} added",
                    id,
                    0,
                    new_hashes.len()
                );
            }
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

    async fn apply_user_hashes(&self, user_id: Uuid, new_hashes: Vec<[u8; 32]>) {
        self.update_entity_hashes(
            &self.users,
            &self.api_key_hashes,
            &self.validated_api_key_cache,
            user_id,
            new_hashes,
        )
        .await;
    }

    async fn apply_company_hashes(&self, cid: Uuid, new_hashes: Vec<[u8; 32]>) {
        self.update_entity_hashes(
            &self.company_keys,
            &self.company_api_key_hashes,
            &self.validated_company_key_cache,
            cid,
            new_hashes,
        )
        .await;
    }

    async fn update_hashes(&self) -> Result<()> {
        let now = self.db.server_time_utc().await?;
        tokio::try_join!(
            self.update_users_data(now),
            self.update_companies_meta(now),
            self.update_company_keys(now),
        )?;

        // Cleanup soft-deleted keys beyond TTL using the same server time window
        let cutoff = now - ChronoDuration::minutes(self.deleted_keys_ttl_minutes as i64);
        let _ = self.db.cleanup_deleted_keys_before(cutoff).await;
        Ok(())
    }

    async fn update_users_data(&self, now: DateTime<Utc>) -> Result<()> {
        let last_sync_sec = self.last_users_sync_sec.load(Ordering::Acquire);

        if last_sync_sec != 0 {
            let since = DateTime::<Utc>::from_timestamp(last_sync_sec as i64, 0)
                .unwrap_or_else(|| now - ChronoDuration::seconds(1));
            let deltas = self.db.fetch_delta_users_keys(since, now).await?;
            let total_new = deltas.iter().map(|(_, v)| v.len()).sum::<usize>();
            info!(
                "Fetched {} user API key hashes for {} users (delta)",
                total_new,
                deltas.len()
            );
            for (user_id, new_hashes) in deltas {
                let mut converted: Vec<[u8; 32]> = Vec::with_capacity(new_hashes.len());
                for v in new_hashes {
                    if v.len() == 32 {
                        let mut a = [0u8; 32];
                        a.copy_from_slice(&v);
                        converted.push(a);
                    }
                }
                self.apply_user_hashes(user_id, converted).await;
            }
        } else {
            // Full fetch on first run
            let all = self.db.fetch_full_users_keys().await?;
            info!(
                "Retrieved {} API key hashes for {} users from the database",
                all.iter().map(|(_, v)| v.len()).sum::<usize>(),
                all.len()
            );

            // Rebuild maps
            self.validated_api_key_cache.invalidate_all();
            self.api_key_hashes.clear_async().await;
            self.users.clear_async().await;
            for (user_id, hashes) in all {
                let mut list: Vec<[u8; 32]> = Vec::with_capacity(hashes.len());
                for v in hashes {
                    if v.len() == 32 {
                        let mut a = [0u8; 32];
                        a.copy_from_slice(&v);
                        // update reverse map and push into forward list
                        let _ = self.api_key_hashes.insert_async(a, user_id).await;
                        list.push(a);
                    }
                }
                let _ = self.users.insert_async(user_id, list).await;
            }
        }

        self.last_users_sync_sec
            .store(now.timestamp() as u64, Ordering::Release);
        Ok(())
    }

    async fn update_companies_meta(&self, now: DateTime<Utc>) -> Result<()> {
        let last_sync_sec = self.last_companies_meta_sync_sec.load(Ordering::Acquire);

        if last_sync_sec != 0 {
            let since = DateTime::<Utc>::from_timestamp(last_sync_sec as i64, 0)
                .unwrap_or_else(|| now - ChronoDuration::seconds(1));
            let deltas = self.db.fetch_delta_companies_meta(since, now).await?;
            for (cid, limits) in deltas {
                let _ = self.companies.insert_async(cid, limits).await;
            }
        } else {
            let all = self.db.fetch_full_companies_meta().await?;
            self.companies.clear_async().await;
            for (cid, limits) in all {
                let _ = self.companies.insert_async(cid, limits).await;
            }
        }

        self.last_companies_meta_sync_sec
            .store(now.timestamp() as u64, Ordering::Release);
        Ok(())
    }

    async fn update_company_keys(&self, now: DateTime<Utc>) -> Result<()> {
        let last_sync_sec = self.last_company_keys_sync_sec.load(Ordering::Acquire);

        if last_sync_sec != 0 {
            let since = DateTime::<Utc>::from_timestamp(last_sync_sec as i64, 0)
                .unwrap_or_else(|| now - ChronoDuration::seconds(1));
            let deltas = self.db.fetch_delta_company_keys(since, now).await?;
            let companies = deltas.len();
            let total_new = deltas.iter().map(|(_, v)| v.len()).sum::<usize>();
            info!(
                "Fetched {} company API key hashes for {} companies (delta)",
                total_new, companies
            );
            for (cid, hashes) in deltas {
                let mut list: Vec<[u8; 32]> = Vec::with_capacity(hashes.len());
                for v in hashes {
                    if v.len() == 32 {
                        let mut a = [0u8; 32];
                        a.copy_from_slice(&v);
                        list.push(a);
                    }
                }
                self.apply_company_hashes(cid, list).await;
            }
        } else {
            let all_keys = self.db.fetch_full_company_keys().await?;
            let total_hashes = all_keys.len();
            self.validated_company_key_cache.invalidate_all();
            self.company_api_key_hashes.clear_async().await;
            self.company_keys.clear_async().await;

            let mut grouped: FoldHashMap<Uuid, Vec<[u8; 32]>> = FoldHashMap::default();
            for (cid, hash) in all_keys {
                if hash.len() == 32 {
                    let mut a = [0u8; 32];
                    a.copy_from_slice(&hash);
                    let _ = self.company_api_key_hashes.insert_async(a, cid).await;
                    grouped.entry(cid).or_default().push(a);
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

        self.last_company_keys_sync_sec
            .store(now.timestamp() as u64, Ordering::Release);
        Ok(())
    }

    #[inline]
    async fn find_id_for_api_key(
        &self,
        api_key: &str,
        cache: &Cache<[u8; 32], Uuid, RandomState>,
        hashes: &scc::HashMap<[u8; 32], Uuid, RandomState>,
    ) -> Option<Uuid> {
        let key_hash = self.hasher.compute_hash_array(api_key);
        if let Some(id) = cache.get(&key_hash).await {
            return Some(id);
        }

        if let Some(entry) = hashes.get_async(&key_hash).await {
            let id = *entry.get();
            cache.insert(key_hash, id).await;
            return Some(id);
        }

        None
    }

    async fn find_user_for_api_key(&self, api_key: &str) -> Option<Uuid> {
        self.find_id_for_api_key(api_key, &self.validated_api_key_cache, &self.api_key_hashes)
            .await
    }

    async fn find_company_for_api_key(&self, api_key: &str) -> Option<Uuid> {
        self.find_id_for_api_key(
            api_key,
            &self.validated_company_key_cache,
            &self.company_api_key_hashes,
        )
        .await
    }

    pub async fn get_user_id(&self, api_key: &str) -> Option<Uuid> {
        self.find_user_for_api_key(api_key).await
    }

    pub async fn is_valid_api_key(&self, api_key: &str) -> bool {
        self.find_user_for_api_key(api_key).await.is_some()
    }

    pub async fn is_company_key(&self, api_key: &str) -> bool {
        self.find_company_for_api_key(api_key).await.is_some()
    }

    pub async fn get_company_info_from_key(
        &self,
        api_key: &str,
    ) -> Option<(Uuid, (String, u64, u64))> {
        if let Some(cid) = self.find_company_for_api_key(api_key).await {
            self.companies
                .get_async(&cid)
                .await
                .map(|company_info| (cid, company_info.get().clone()))
        } else {
            None
        }
    }
}
