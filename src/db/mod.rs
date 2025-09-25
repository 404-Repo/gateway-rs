mod key_validator;

use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use foldhash::fast::RandomState;
use rustls::{ClientConfig, RootCertStore};
use rustls_pemfile::certs;
use scc::HashMap as SccHashMap;
use sdd::{AtomicOwned, Guard, Owned, Tag};
use std::error::Error as _;
use std::io;
use std::io::BufReader;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::fs;
use tokio_postgres::{types::ToSql, Client, Config, Error, Row, Statement};
use tokio_postgres_rustls::MakeRustlsConnect;
use tracing::{error, info};

use crate::common::cert::{load_certificates, load_private_key};
pub use key_validator::ApiKeyValidator;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum StmtKey {
    ServerTimeUtc,
    FullUsersKeys,
    DeltaUsersKeys,
    FullCompaniesMeta,
    DeltaCompaniesMeta,
    FullCompanyKeys,
    DeltaCompanyKeys,
    CompanyKeysForIds,
    CleanupDeletedKeysBoth,
}

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
    prepared: SccHashMap<StmtKey, Statement, RandomState>,
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

    fn require_nonempty(opt: Option<String>, name: &str) -> Result<String> {
        opt.filter(|s| !s.trim().is_empty())
            .ok_or_else(|| anyhow!("{name} is required and cannot be empty"))
    }

    pub async fn build(self) -> Result<Database> {
        let sslcert_path = Self::require_nonempty(self.sslcert_path, "sslcert path")?;
        let sslkey_path = Self::require_nonempty(self.sslkey_path, "sslkey path")?;
        let sslrootcert_path = Self::require_nonempty(self.sslrootcert_path, "sslrootcert path")?;
        let host = Self::require_nonempty(self.host, "Host")?;
        let user = Self::require_nonempty(self.user, "User")?;
        let password = Self::require_nonempty(self.password, "Password")?;
        let dbname = Self::require_nonempty(self.dbname, "Database name")?;
        let port = self.port.ok_or_else(|| anyhow!("Port is required"))?;

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

        let db = Database {
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
            prepared: SccHashMap::with_capacity_and_hasher(16, RandomState::default()),
        };

        db.prepare_all().await?;
        Ok(db)
    }
}

impl Database {
    async fn load_client(&self) -> Result<Arc<Client>> {
        let guard = Guard::new();
        let shared = self.client.load(Ordering::Acquire, &guard);
        shared
            .as_ref()
            .cloned()
            .ok_or_else(|| anyhow!("Database client is not initialized"))
    }

    async fn upsert_stmt(&self, key: StmtKey, stmt: Statement) {
        match self.prepared.insert_async(key, stmt.clone()).await {
            Ok(()) => {}
            Err((_k, _v)) => match self.prepared.entry_async(key).await {
                scc::hash_map::Entry::Occupied(mut entry) => {
                    *entry.get_mut() = stmt;
                }
                scc::hash_map::Entry::Vacant(entry) => {
                    entry.insert_entry(stmt);
                }
            },
        }
    }

    async fn prepare_all(&self) -> Result<()> {
        let client = self.load_client().await?;
        self.prepared.clear_async().await;
        self.upsert_stmt(
            StmtKey::ServerTimeUtc,
            client.prepare(Self::Q_SERVER_TIME_UTC).await?,
        )
        .await;
        self.upsert_stmt(
            StmtKey::FullUsersKeys,
            client.prepare(Self::Q_FULL_USERS_KEYS).await?,
        )
        .await;
        self.upsert_stmt(
            StmtKey::DeltaUsersKeys,
            client.prepare(Self::Q_DELTA_USERS_KEYS).await?,
        )
        .await;
        self.upsert_stmt(
            StmtKey::FullCompaniesMeta,
            client.prepare(Self::Q_FULL_COMPANIES_META).await?,
        )
        .await;
        self.upsert_stmt(
            StmtKey::DeltaCompaniesMeta,
            client.prepare(Self::Q_DELTA_COMPANIES_META).await?,
        )
        .await;
        self.upsert_stmt(
            StmtKey::FullCompanyKeys,
            client.prepare(Self::Q_FULL_COMPANY_KEYS).await?,
        )
        .await;
        self.upsert_stmt(
            StmtKey::DeltaCompanyKeys,
            client.prepare(Self::Q_DELTA_COMPANY_KEYS).await?,
        )
        .await;
        self.upsert_stmt(
            StmtKey::CompanyKeysForIds,
            client.prepare(Self::Q_COMPANY_KEYS_FOR_IDS).await?,
        )
        .await;
        self.upsert_stmt(
            StmtKey::CleanupDeletedKeysBoth,
            client.prepare(Self::Q_CLEANUP_DELETED_KEYS_BOTH).await?,
        )
        .await;
        Ok(())
    }

    async fn get_statement(&self, key: StmtKey) -> Result<Statement> {
        if let Some(entry) = self.prepared.get_async(&key).await {
            return Ok(entry.get().clone());
        }
        let client = self.load_client().await?;
        let sql = match key {
            StmtKey::ServerTimeUtc => Self::Q_SERVER_TIME_UTC,
            StmtKey::FullUsersKeys => Self::Q_FULL_USERS_KEYS,
            StmtKey::DeltaUsersKeys => Self::Q_DELTA_USERS_KEYS,
            StmtKey::FullCompaniesMeta => Self::Q_FULL_COMPANIES_META,
            StmtKey::DeltaCompaniesMeta => Self::Q_DELTA_COMPANIES_META,
            StmtKey::FullCompanyKeys => Self::Q_FULL_COMPANY_KEYS,
            StmtKey::DeltaCompanyKeys => Self::Q_DELTA_COMPANY_KEYS,
            StmtKey::CompanyKeysForIds => Self::Q_COMPANY_KEYS_FOR_IDS,
            StmtKey::CleanupDeletedKeysBoth => Self::Q_CLEANUP_DELETED_KEYS_BOTH,
        };
        let stmt = client.prepare(sql).await?;
        self.upsert_stmt(key, stmt.clone()).await;
        Ok(stmt)
    }

    const Q_SERVER_TIME_UTC: &'static str = r#"
SELECT NOW()::timestamptz AS server_time_utc;
"#;

    const Q_FULL_USERS_KEYS: &'static str = r#"
SELECT u.id,
       COALESCE(array_agg(a.api_key_hash) FILTER (WHERE a.deleted_at IS NULL), '{}') AS api_key_hashes
FROM users u
LEFT JOIN api_keys a ON u.id = a.user_id
GROUP BY u.id;
"#;

    const Q_DELTA_USERS_KEYS: &'static str = r#"
WITH changed AS (
  SELECT DISTINCT user_id
  FROM api_keys
    WHERE (created_at > $1 AND created_at <= $2)
         OR (updated_at > $1 AND updated_at <= $2)
         OR (deleted_at IS NOT NULL AND deleted_at > $1 AND deleted_at <= $2)
)
SELECT c.user_id AS id,
       COALESCE(array_agg(a.api_key_hash) FILTER (WHERE a.deleted_at IS NULL), '{}') AS api_key_hashes
FROM changed c
LEFT JOIN api_keys a ON a.user_id = c.user_id
GROUP BY c.user_id;
"#;

    const Q_FULL_COMPANIES_META: &'static str = r#"
SELECT id, name, rate_limit_hourly, rate_limit_daily FROM companies;
"#;

    const Q_DELTA_COMPANIES_META: &'static str = r#"
SELECT id, name, rate_limit_hourly, rate_limit_daily
FROM companies
WHERE (updated_at > $1 AND updated_at <= $2)
   OR (updated_at IS NULL AND created_at > $1 AND created_at <= $2);
"#;

    const Q_FULL_COMPANY_KEYS: &'static str = r#"
SELECT company_id, api_key_hash FROM company_api_keys WHERE deleted_at IS NULL;
"#;

    const Q_COMPANY_KEYS_FOR_IDS: &'static str = r#"
SELECT company_id, api_key_hash
FROM company_api_keys
WHERE deleted_at IS NULL AND company_id = ANY($1);
"#;

    const Q_DELTA_COMPANY_KEYS: &'static str = r#"
WITH changed AS (
  SELECT DISTINCT company_id
  FROM company_api_keys
  WHERE (created_at > $1 AND created_at <= $2)
     OR (updated_at > $1 AND updated_at <= $2)
     OR (deleted_at IS NOT NULL AND deleted_at > $1 AND deleted_at <= $2)
)
SELECT c.company_id,
       COALESCE(array_agg(k.api_key_hash) FILTER (WHERE k.deleted_at IS NULL), '{}') AS api_key_hashes
FROM changed c
LEFT JOIN company_api_keys k ON k.company_id = c.company_id
GROUP BY c.company_id;
"#;

    const Q_CLEANUP_DELETED_KEYS_BOTH: &'static str = r#"
WITH del_user AS (
  DELETE FROM api_keys
  WHERE deleted_at IS NOT NULL AND deleted_at < $1
  RETURNING 1
), del_company AS (
  DELETE FROM company_api_keys
  WHERE deleted_at IS NOT NULL AND deleted_at < $1
  RETURNING 1
)
SELECT
  (SELECT COUNT(*) FROM del_user) AS n1,
  (SELECT COUNT(*) FROM del_company) AS n2;
"#;

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
        // Re-prepare statements on the new connection
        self.prepare_all().await?;
        Ok(())
    }

    async fn query_prepared(
        &self,
        key: StmtKey,
        params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
    ) -> Result<Vec<Row>> {
        let stmt = self.get_statement(key).await?;
        let client = self.load_client().await?;
        match client.query(&stmt, params).await {
            Ok(rows) => Ok(rows),
            Err(e) => {
                info!("Database query failed: {}. Trying to reconnect...", e);
                if self.should_reconnect(&e) {
                    self.reconnect().await?;
                    let stmt = self.get_statement(key).await?;
                    let client = self.load_client().await?;
                    Ok(client.query(&stmt, params).await?)
                } else {
                    Err(anyhow!(e))
                }
            }
        }
    }

    pub async fn fetch_full_users_keys(&self) -> Result<Vec<(uuid::Uuid, Vec<Vec<u8>>)>> {
        let rows = self.query_prepared(StmtKey::FullUsersKeys, &[]).await?;
        let result = rows
            .into_iter()
            .map(|row| {
                let user_id: uuid::Uuid = row.get("id");
                let api_key_hashes: Vec<Vec<u8>> = row.get("api_key_hashes");
                (user_id, api_key_hashes)
            })
            .collect();
        Ok(result)
    }

    pub async fn fetch_delta_users_keys(
        &self,
        since: DateTime<Utc>,
        until: DateTime<Utc>,
    ) -> Result<Vec<(uuid::Uuid, Vec<Vec<u8>>)>> {
        let params: [&(dyn ToSql + Sync); 2] = [&since, &until];
        let rows = self
            .query_prepared(StmtKey::DeltaUsersKeys, &params)
            .await?;
        let result = rows
            .into_iter()
            .map(|row| {
                let user_id: uuid::Uuid = row.get("id");
                let api_key_hashes: Vec<Vec<u8>> = row.get("api_key_hashes");
                (user_id, api_key_hashes)
            })
            .collect();
        Ok(result)
    }

    pub async fn fetch_full_companies_meta(&self) -> Result<Vec<(uuid::Uuid, (String, u64, u64))>> {
        let rows = self.query_prepared(StmtKey::FullCompaniesMeta, &[]).await?;
        let result = rows
            .into_iter()
            .map(|row| {
                let id: uuid::Uuid = row.get("id");
                let name: String = row.get("name");
                let hourly: i32 = row.get("rate_limit_hourly");
                let daily: i32 = row.get("rate_limit_daily");
                (id, (name, hourly as u64, daily as u64))
            })
            .collect();
        Ok(result)
    }

    pub async fn fetch_delta_companies_meta(
        &self,
        since: DateTime<Utc>,
        until: DateTime<Utc>,
    ) -> Result<Vec<(uuid::Uuid, (String, u64, u64))>> {
        let params: [&(dyn ToSql + Sync); 2] = [&since, &until];
        let rows = self
            .query_prepared(StmtKey::DeltaCompaniesMeta, &params)
            .await?;
        let result = rows
            .into_iter()
            .map(|row| {
                let id: uuid::Uuid = row.get("id");
                let name: String = row.get("name");
                let hourly: i32 = row.get("rate_limit_hourly");
                let daily: i32 = row.get("rate_limit_daily");
                (id, (name, hourly as u64, daily as u64))
            })
            .collect();
        Ok(result)
    }

    pub async fn fetch_full_company_keys(&self) -> Result<Vec<(uuid::Uuid, Vec<u8>)>> {
        let rows = self.query_prepared(StmtKey::FullCompanyKeys, &[]).await?;
        let result = rows
            .into_iter()
            .map(|row| {
                let company_id: uuid::Uuid = row.get("company_id");
                let hash: Vec<u8> = row.get("api_key_hash");
                (company_id, hash)
            })
            .collect();
        Ok(result)
    }

    pub async fn fetch_delta_company_keys(
        &self,
        since: DateTime<Utc>,
        until: DateTime<Utc>,
    ) -> Result<Vec<(uuid::Uuid, Vec<Vec<u8>>)>> {
        let params: [&(dyn ToSql + Sync); 2] = [&since, &until];
        let rows = self
            .query_prepared(StmtKey::DeltaCompanyKeys, &params)
            .await?;
        let result = rows
            .into_iter()
            .map(|row| {
                let company_id: uuid::Uuid = row.get("company_id");
                let api_key_hashes: Vec<Vec<u8>> = row.get("api_key_hashes");
                (company_id, api_key_hashes)
            })
            .collect();
        Ok(result)
    }

    pub async fn cleanup_deleted_keys_before(&self, cutoff: DateTime<Utc>) -> Result<(u64, u64)> {
        let params: [&(dyn ToSql + Sync); 1] = [&cutoff];
        let rows = self
            .query_prepared(StmtKey::CleanupDeletedKeysBoth, &params)
            .await?;
        let row = rows
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("Cleanup query returned no rows"))?;
        let n1: i64 = row.get("n1");
        let n2: i64 = row.get("n2");
        Ok((n1 as u64, n2 as u64))
    }

    pub async fn server_time_utc(&self) -> Result<DateTime<Utc>> {
        let rows = self.query_prepared(StmtKey::ServerTimeUtc, &[]).await?;
        let row = rows
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("Server time query returned no rows"))?;
        let dt: DateTime<Utc> = row.get("server_time_utc");
        Ok(dt)
    }
}
