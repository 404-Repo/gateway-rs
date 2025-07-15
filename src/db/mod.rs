use anyhow::{anyhow, Result};
use foldhash::fast::RandomState;
use moka::sync::Cache;
use rustls::{ClientConfig, RootCertStore};
use rustls_pemfile::certs;
use sdd::{AtomicOwned, Guard, Owned, Tag};
use std::error::Error as _;
use std::io;
use std::io::BufReader;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tokio::fs;
use tokio_postgres::{Client, Config, Error, Row};
use tokio_postgres_rustls::MakeRustlsConnect;
use tracing::{error, info};
use uuid::Uuid;

use crate::common::cert::{load_certificates, load_private_key};
use crate::common::crypto_provider::ApiKeyHasher;

#[derive(Debug, Clone)]
pub struct PostgresConnectionConfig<'a> {
    pub host: &'a str,
    pub port: u16,
    pub user: &'a str,
    pub password: &'a str,
    pub dbname: &'a str,
    pub sslcert_path: &'a str,
    pub sslkey_path: &'a str,
    pub sslrootcert_path: &'a str,
}

#[derive(Debug, Clone)]
pub struct PostgresConnectionConfigOwned {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub dbname: String,
    pub sslcert_path: String,
    pub sslkey_path: String,
    pub sslrootcert_path: String,
}

impl PostgresConnectionConfigOwned {
    pub fn borrow(&self) -> PostgresConnectionConfig<'_> {
        PostgresConnectionConfig {
            host: &self.host,
            port: self.port,
            user: &self.user,
            password: &self.password,
            dbname: &self.dbname,
            sslcert_path: &self.sslcert_path,
            sslkey_path: &self.sslkey_path,
            sslrootcert_path: &self.sslrootcert_path,
        }
    }
}

pub struct Database {
    client: AtomicOwned<Arc<Client>>,
    config: PostgresConnectionConfigOwned,
}

pub struct DatabaseBuilder {
    sslcert_path: Option<String>,
    sslkey_path: Option<String>,
    sslrootcert_path: Option<String>,
    host: Option<String>,
    port: Option<u16>,
    user: Option<String>,
    password: Option<String>,
    dbname: Option<String>,
}

async fn connect_postgres(config: &PostgresConnectionConfig<'_>) -> Result<Arc<Client>> {
    let ca_cert_bytes = fs::read(config.sslrootcert_path).await.map_err(|e| {
        anyhow!(
            "Failed to read CA certificate at {}: {}",
            config.sslrootcert_path,
            e
        )
    })?;
    let mut reader = BufReader::new(&ca_cert_bytes[..]);
    let certs = certs(&mut reader)
        .collect::<std::io::Result<Vec<_>>>()
        .map_err(|e| anyhow!("Failed to parse CA certificate as PEM: {}", e))?;

    let mut root_store = RootCertStore::empty();
    for cert in certs {
        root_store
            .add(cert)
            .map_err(|e| anyhow!("Failed to add CA certificate to root store: {}", e))?;
    }

    let client_certs = load_certificates(config.sslcert_path).await.map_err(|e| {
        anyhow!(
            "Failed to read client certificate at {}: {}",
            config.sslcert_path,
            e
        )
    })?;

    let client_key = load_private_key(config.sslkey_path).await.map_err(|e| {
        anyhow!(
            "Failed to read client private key at {}: {}",
            config.sslkey_path,
            e
        )
    })?;

    let tls_config = ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_client_auth_cert(client_certs, client_key)?;

    let tls_connector = MakeRustlsConnect::new(tls_config);

    let mut pg_cfg = Config::new();
    pg_cfg.host(config.host);
    pg_cfg.port(config.port);
    pg_cfg.user(config.user);
    pg_cfg.password(config.password);
    pg_cfg.dbname(config.dbname);
    pg_cfg.ssl_mode(tokio_postgres::config::SslMode::Require);

    let (client, connection) = pg_cfg
        .connect(tls_connector)
        .await
        .map_err(|e| anyhow!("Failed to connect to PostgreSQL using rustls: {}", e))?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            error!("Database connection error: {}", e);
        }
    });

    Ok(Arc::new(client))
}

impl DatabaseBuilder {
    pub fn new() -> Self {
        Self {
            sslcert_path: None,
            sslkey_path: None,
            sslrootcert_path: None,
            host: None,
            port: None,
            user: None,
            password: None,
            dbname: None,
        }
    }

    pub fn sslcert_path(mut self, path: &str) -> Self {
        self.sslcert_path = Some(path.into());
        self
    }

    pub fn sslkey_path(mut self, path: &str) -> Self {
        self.sslkey_path = Some(path.into());
        self
    }

    pub fn sslrootcert_path(mut self, path: &str) -> Self {
        self.sslrootcert_path = Some(path.into());
        self
    }

    pub fn host(mut self, host: &str) -> Self {
        self.host = Some(host.into());
        self
    }

    pub fn port(mut self, port: u16) -> Self {
        self.port = Some(port);
        self
    }

    pub fn user(mut self, user: &str) -> Self {
        self.user = Some(user.into());
        self
    }

    pub fn password(mut self, password: &str) -> Self {
        self.password = Some(password.into());
        self
    }

    pub fn dbname(mut self, dbname: &str) -> Self {
        self.dbname = Some(dbname.into());
        self
    }

    pub async fn build(self) -> Result<Database> {
        let sslcert_path = self
            .sslcert_path
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| anyhow!("sslcert path is required and cannot be empty"))?;
        let sslkey_path = self
            .sslkey_path
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| anyhow!("sslkey path is required and cannot be empty"))?;
        let sslrootcert_path = self
            .sslrootcert_path
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| anyhow!("sslrootcert path is required and cannot be empty"))?;
        let host = self
            .host
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| anyhow!("Host is required and cannot be empty"))?;
        let port = self.port.ok_or_else(|| anyhow!("Port is required"))?;
        let user = self
            .user
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| anyhow!("User is required and cannot be empty"))?;
        let password = self
            .password
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| anyhow!("Password is required and cannot be empty"))?;
        let dbname = self
            .dbname
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| anyhow!("Database name is required and cannot be empty"))?;

        let client = connect_postgres(&PostgresConnectionConfig {
            host: &host,
            port,
            user: &user,
            password: &password,
            dbname: &dbname,
            sslcert_path: &sslcert_path,
            sslkey_path: &sslkey_path,
            sslrootcert_path: &sslrootcert_path,
        })
        .await?;

        Ok(Database {
            client: AtomicOwned::new(client),
            config: PostgresConnectionConfigOwned {
                host,
                port,
                user,
                password,
                dbname,
                sslcert_path,
                sslkey_path,
                sslrootcert_path,
            },
        })
    }
}

impl Database {
    fn should_reconnect(&self, e: &Error) -> bool {
        if e.is_closed() {
            return true;
        }

        if let Some(src) = e.source() {
            if let Some(io_err) = src.downcast_ref::<io::Error>() {
                return matches!(
                    io_err.kind(),
                    io::ErrorKind::TimedOut
                        | io::ErrorKind::ConnectionRefused
                        | io::ErrorKind::ConnectionReset
                        | io::ErrorKind::ConnectionAborted
                );
            }
        }

        false
    }

    pub async fn reconnect(&self) -> Result<()> {
        let new_client = connect_postgres(&self.config.borrow()).await?;

        let _old = self
            .client
            .swap((Some(Owned::new(new_client)), Tag::None), Ordering::AcqRel);

        Ok(())
    }

    pub async fn query(
        &self,
        sql: &str,
        params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
    ) -> Result<Vec<Row>> {
        let client_handle: Arc<Client> = {
            let guard = Guard::new();
            let shared = self.client.load(Ordering::Acquire, &guard);
            shared
                .as_ref()
                .cloned()
                .ok_or_else(|| anyhow!("Database client is not initialized"))?
        };

        match client_handle.query(sql, params).await {
            Ok(rows) => Ok(rows),
            Err(e) => {
                info!("Database query failed: {}. Trying to reconnect...", e);
                if self.should_reconnect(&e) {
                    self.reconnect().await?;
                }

                let retry_handle: Arc<Client> = {
                    let guard = Guard::new();
                    let shared = self.client.load(Ordering::Acquire, &guard);
                    shared.as_ref().cloned().ok_or_else(|| {
                        anyhow!("Database client is not initialized after reconnect")
                    })?
                };

                Ok(retry_handle.query(sql, params).await?)
            }
        }
    }
}

pub struct ApiKeyValidator {
    db: Arc<Database>,
    // mapping from user_id -> list of api_key_hashes
    users: scc::HashMap<Uuid, Vec<String>, RandomState>,
    // reverse mapping: api_key_hash -> user_id
    api_key_hashes: scc::HashMap<String, Uuid, RandomState>,
    // mapping from company_id -> (name, hourly, daily) rate limits
    companies: scc::HashMap<Uuid, (String, u64, u64), RandomState>,
    // reverse mapping: company_api_key_hash -> company_id
    company_api_key_hashes: scc::HashMap<String, Uuid, RandomState>,
    // Argon2 hasher for API key verification
    hasher: ApiKeyHasher,
    // For validated API keys (api_key -> user_id) with TTL
    validation_cache: Cache<String, Uuid, RandomState>,
    // For validated company API keys (api_key -> company_id) with TTL
    company_validation_cache: Cache<String, Uuid, RandomState>,
    update_interval: Duration,
}

impl ApiKeyValidator {
    pub fn new(
        db: Arc<Database>,
        update_interval: Duration,
        cache_ttl_sec: u64,
        cache_max_capacity: u64,
    ) -> Result<Self> {
        let validation_cache = Cache::builder()
            .max_capacity(cache_max_capacity)
            .time_to_live(Duration::from_secs(cache_ttl_sec))
            .build_with_hasher(RandomState::default());

        let company_validation_cache = Cache::builder()
            .max_capacity(cache_max_capacity)
            .time_to_live(Duration::from_secs(cache_ttl_sec))
            .build_with_hasher(RandomState::default());

        let companies = scc::HashMap::with_capacity_and_hasher(4096, RandomState::default());
        let company_api_key_hashes =
            scc::HashMap::with_capacity_and_hasher(4096, RandomState::default());
        Ok(Self {
            db,
            users: scc::HashMap::with_capacity_and_hasher(4096, RandomState::default()),
            api_key_hashes: scc::HashMap::with_capacity_and_hasher(4096, RandomState::default()),
            companies,
            company_api_key_hashes,
            hasher: ApiKeyHasher::new()?,
            validation_cache,
            company_validation_cache,
            update_interval,
        })
    }

    pub async fn run(self: Arc<Self>) {
        let mut interval = tokio::time::interval(self.update_interval);
        loop {
            if let Err(e) = self.update_hashes().await {
                error!("Error updating keys: {:?}", e);
            }
            interval.tick().await;
        }
    }

    async fn update_hashes(&self) -> Result<()> {
        self.update_users_data().await?;
        self.update_companies_data().await
    }

    async fn update_users_data(&self) -> Result<()> {
        let query = r#"
    SELECT
        u.id,
        COALESCE(array_agg(a.api_key_hash) FILTER (WHERE a.api_key_hash IS NOT NULL), '{}') AS api_key_hashes
    FROM users u
    LEFT JOIN api_keys a ON u.id = a.user_id
    GROUP BY u.id;
"#;
        let rows = self.db.query(query, &[]).await?;

        let new_keys: foldhash::HashMap<Uuid, Vec<String>> = rows
            .into_iter()
            .map(|row| {
                let user_id: Uuid = row.get("id");
                let api_key_hashes: Vec<String> = row.get("api_key_hashes");
                (user_id, api_key_hashes)
            })
            .collect();

        let total_api_keys: usize = new_keys.values().map(|v| v.len()).sum();
        info!(
            "Retrieved {} API key hashes for {} users from the database",
            total_api_keys,
            new_keys.len()
        );

        let new_user_ids: foldhash::HashSet<_> = new_keys.keys().copied().collect();
        self.users
            .retain_async(|k, _| new_user_ids.contains(k))
            .await;
        for (user_id, new_api_key_hashes) in &new_keys {
            self.users
                .entry_async(*user_id)
                .await
                .and_modify(|v| {
                    if *v != *new_api_key_hashes {
                        *v = new_api_key_hashes.clone();
                    }
                })
                .or_insert(new_api_key_hashes.clone());
        }

        let new_api_key_hash_set: foldhash::HashSet<String> =
            new_keys.values().flatten().cloned().collect();

        self.api_key_hashes
            .retain_async(|k, _| new_api_key_hash_set.contains(k))
            .await;

        for (user_id, api_key_hashes_vec) in &new_keys {
            for api_key_hash in api_key_hashes_vec {
                self.api_key_hashes
                    .entry_async(api_key_hash.clone())
                    .await
                    .and_modify(|existing_user| {
                        if *existing_user != *user_id {
                            *existing_user = *user_id;
                        }
                    })
                    .or_insert(*user_id);
            }
        }
        Ok(())
    }

    async fn update_companies_data(&self) -> Result<()> {
        let comp_rows = self
            .db
            .query(
                "SELECT id, name, rate_limit_hourly, rate_limit_daily FROM companies",
                &[],
            )
            .await?;

        let new_companies: foldhash::HashMap<Uuid, (String, u64, u64)> = comp_rows
            .into_iter()
            .map(|row| {
                let company_id: Uuid = row.get("id");
                let name: String = row.get("name");
                let hourly: i32 = row.get("rate_limit_hourly");
                let daily: i32 = row.get("rate_limit_daily");
                (company_id, (name, hourly as u64, daily as u64))
            })
            .collect();

        let company_keys: foldhash::HashSet<_> = new_companies.keys().copied().collect();
        self.companies
            .retain_async(|k, _| company_keys.contains(k))
            .await;
        for (cid, limits) in &new_companies {
            self.companies
                .entry_async(*cid)
                .await
                .and_modify(|v| {
                    if *v != *limits {
                        *v = limits.clone();
                    }
                })
                .or_insert(limits.clone());
        }

        let cak_rows = self
            .db
            .query("SELECT company_id, api_key_hash FROM company_api_keys", &[])
            .await?;

        let new_company_keys: foldhash::HashMap<String, Uuid> = cak_rows
            .into_iter()
            .map(|row| {
                let company_id: Uuid = row.get("company_id");
                let hash: String = row.get("api_key_hash");
                (hash, company_id)
            })
            .collect();

        info!(
            "Retrieved {} company API keys for {} companies from the database",
            new_company_keys.len(),
            new_companies.len()
        );

        let cak_hashes: foldhash::HashSet<_> = new_company_keys.keys().cloned().collect();
        self.company_api_key_hashes
            .retain_async(|k, _| cak_hashes.contains(k))
            .await;

        for (hash, cid) in new_company_keys {
            self.company_api_key_hashes
                .entry_async(hash)
                .await
                .and_modify(|v| {
                    if *v != cid {
                        *v = cid;
                    }
                })
                .or_insert(cid);
        }

        Ok(())
    }

    fn find_user_for_api_key(&self, api_key: &str) -> Option<Uuid> {
        if let Some(user_id) = self.validation_cache.get(api_key) {
            return Some(user_id);
        }

        // Scan through hashes to find a match
        let mut result = None;
        self.api_key_hashes.scan(|hash, user_id| {
            if let Ok(true) = self.hasher.verify_api_key(api_key, hash) {
                result = Some(*user_id);
                self.validation_cache.insert(api_key.to_string(), *user_id);
            }
        });
        result
    }

    fn find_company_for_api_key(&self, api_key: &str) -> Option<Uuid> {
        if let Some(company_id) = self.company_validation_cache.get(api_key) {
            return Some(company_id);
        }

        let mut company_id = None;
        self.company_api_key_hashes.scan(|hash, cid| {
            if self.hasher.verify_api_key(api_key, hash).unwrap_or(false) {
                company_id = Some(*cid);
            }
        });

        if let Some(cid) = company_id {
            self.company_validation_cache
                .insert(api_key.to_string(), cid);
        }
        company_id
    }

    pub fn get_user_id(&self, api_key: &str) -> Option<Uuid> {
        self.find_user_for_api_key(api_key)
    }

    pub fn is_valid_api_key(&self, api_key: &str) -> bool {
        self.find_user_for_api_key(api_key).is_some()
    }

    pub fn is_company_key(&self, api_key: &str) -> bool {
        self.find_company_for_api_key(api_key).is_some()
    }

    pub fn get_company_info_from_key(&self, api_key: &str) -> Option<(Uuid, (String, u64, u64))> {
        self.find_company_for_api_key(api_key).and_then(|cid| {
            self.companies
                .get(&cid)
                .map(|company_info| (cid, company_info.get().clone()))
        })
    }
}
