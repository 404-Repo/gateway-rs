use anyhow::{Result, anyhow};
use foldhash::fast::RandomState;
use rustls::{ClientConfig, RootCertStore};
use scc::HashMap as SccHashMap;
use sdd::{AtomicOwned, Guard, Owned, Tag};
use std::error::Error as _;
use std::io;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::{fs, task::JoinHandle};
use tokio_postgres::{Client, Config, Error, Row, Statement, types::ToSql};
use tokio_postgres_rustls::MakeRustlsConnect;
use tracing::{error, info};

use crate::common::cert::{load_certificates, load_private_key, parse_certificates_from_pem_bytes};
use crate::config::DbConfig;

use super::{Database, DatabaseBuilder};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) enum StmtKey {
    ServerTimeUtc,
    AllUserKeyHashes,
    DeltaUserKeyHashes,
    UserEmailById,
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

pub(super) struct AbortOnDropJoinHandle(JoinHandle<()>);

impl AbortOnDropJoinHandle {
    fn new(handle: JoinHandle<()>) -> Self {
        Self(handle)
    }
}

impl Drop for AbortOnDropJoinHandle {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn connect_postgres(
    config: &PostgresConnectionConfig<'_>,
) -> Result<(Arc<Client>, JoinHandle<()>)> {
    let tls_config = build_tls_config(config).await?;
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

    let connection_task = tokio::spawn(async move {
        if let Err(e) = connection.await {
            error!("Database connection error: {}", e);
        }
    });

    Ok((Arc::new(client), connection_task))
}

async fn build_tls_config(config: &PostgresConnectionConfig<'_>) -> Result<ClientConfig> {
    let ca_cert_bytes = fs::read(config.sslrootcert_path).await.map_err(|e| {
        anyhow!(
            "Failed to read CA certificate at {}: {}",
            config.sslrootcert_path,
            e
        )
    })?;
    let certs = parse_certificates_from_pem_bytes(&ca_cert_bytes)
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

    let tls_config = ClientConfig::builder_with_protocol_versions(&[
        &rustls::version::TLS13,
        &rustls::version::TLS12,
    ])
    .with_root_certificates(root_store)
    .with_client_auth_cert(client_certs, client_key)?;

    Ok(tls_config)
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
            events_copy_batch_size: None,
        }
    }

    pub fn from_config(config: &DbConfig) -> Self {
        Self::new()
            .host(&config.host)
            .port(config.port)
            .user(&config.user)
            .password(&config.password)
            .dbname(&config.db)
            .sslcert_path(&config.sslcert)
            .sslkey_path(&config.sslkey)
            .sslrootcert_path(&config.sslrootcert)
            .events_copy_batch_size(config.events_copy_batch_size)
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

    pub fn events_copy_batch_size(mut self, value: usize) -> Self {
        self.events_copy_batch_size = Some(value.max(1));
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
        let events_copy_batch_size = self.events_copy_batch_size.unwrap_or(1000).max(1);

        let (client, connection_task) = connect_postgres(&PostgresConnectionConfig {
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
            events_copy_batch_size,
            prepared: SccHashMap::with_capacity_and_hasher(16, RandomState::default()),
            connection_task: AtomicOwned::new(AbortOnDropJoinHandle::new(connection_task)),
        };

        db.prepare_all().await?;
        Ok(db)
    }
}

impl Default for DatabaseBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl Database {
    #[allow(dead_code)]
    pub fn new_mock() -> Self {
        Self {
            client: AtomicOwned::null(),
            config: PostgresConnectionConfigOwned {
                host: String::new(),
                port: 0,
                user: String::new(),
                password: String::new(),
                dbname: String::new(),
                sslcert_path: String::new(),
                sslkey_path: String::new(),
                sslrootcert_path: String::new(),
            },
            events_copy_batch_size: 1000,
            prepared: SccHashMap::with_capacity_and_hasher(0, RandomState::default()),
            connection_task: AtomicOwned::null(),
        }
    }

    pub(super) async fn load_client(&self) -> Result<Arc<Client>> {
        let guard = Guard::new();
        let shared = self.client.load(Ordering::Acquire, &guard);
        shared
            .as_ref()
            .cloned()
            .ok_or_else(|| anyhow!("Database client is not initialized"))
    }

    async fn upsert_stmt(&self, key: StmtKey, stmt: Arc<Statement>) {
        match self.prepared.entry_async(key).await {
            scc::hash_map::Entry::Occupied(mut entry) => {
                *entry.get_mut() = stmt;
            }
            scc::hash_map::Entry::Vacant(entry) => {
                entry.insert_entry(stmt);
            }
        }
    }

    async fn prepare_all(&self) -> Result<()> {
        let client = self.load_client().await?;
        self.prepared.clear_async().await;
        self.upsert_stmt(
            StmtKey::ServerTimeUtc,
            Arc::new(client.prepare(Self::Q_SERVER_TIME_UTC).await?),
        )
        .await;
        self.upsert_stmt(
            StmtKey::AllUserKeyHashes,
            Arc::new(client.prepare(Self::Q_ALL_USER_KEY_HASHES).await?),
        )
        .await;
        self.upsert_stmt(
            StmtKey::DeltaUserKeyHashes,
            Arc::new(client.prepare(Self::Q_DELTA_USER_KEY_HASHES).await?),
        )
        .await;
        self.upsert_stmt(
            StmtKey::UserEmailById,
            Arc::new(client.prepare(Self::Q_USER_EMAIL_BY_ID).await?),
        )
        .await;
        self.upsert_stmt(
            StmtKey::FullCompaniesMeta,
            Arc::new(client.prepare(Self::Q_FULL_COMPANIES_META).await?),
        )
        .await;
        self.upsert_stmt(
            StmtKey::DeltaCompaniesMeta,
            Arc::new(client.prepare(Self::Q_DELTA_COMPANIES_META).await?),
        )
        .await;
        self.upsert_stmt(
            StmtKey::FullCompanyKeys,
            Arc::new(client.prepare(Self::Q_FULL_COMPANY_KEYS).await?),
        )
        .await;
        self.upsert_stmt(
            StmtKey::DeltaCompanyKeys,
            Arc::new(client.prepare(Self::Q_DELTA_COMPANY_KEYS).await?),
        )
        .await;
        self.upsert_stmt(
            StmtKey::CompanyKeysForIds,
            Arc::new(client.prepare(Self::Q_COMPANY_KEYS_FOR_IDS).await?),
        )
        .await;
        self.upsert_stmt(
            StmtKey::CleanupDeletedKeysBoth,
            Arc::new(client.prepare(Self::Q_CLEANUP_DELETED_KEYS_BOTH).await?),
        )
        .await;
        Ok(())
    }

    async fn get_statement(&self, key: StmtKey) -> Result<Arc<Statement>> {
        if let Some(entry) = self.prepared.get_async(&key).await {
            return Ok(Arc::clone(entry.get()));
        }
        let client = self.load_client().await?;
        let sql = match key {
            StmtKey::ServerTimeUtc => Self::Q_SERVER_TIME_UTC,
            StmtKey::AllUserKeyHashes => Self::Q_ALL_USER_KEY_HASHES,
            StmtKey::DeltaUserKeyHashes => Self::Q_DELTA_USER_KEY_HASHES,
            StmtKey::UserEmailById => Self::Q_USER_EMAIL_BY_ID,
            StmtKey::FullCompaniesMeta => Self::Q_FULL_COMPANIES_META,
            StmtKey::DeltaCompaniesMeta => Self::Q_DELTA_COMPANIES_META,
            StmtKey::FullCompanyKeys => Self::Q_FULL_COMPANY_KEYS,
            StmtKey::DeltaCompanyKeys => Self::Q_DELTA_COMPANY_KEYS,
            StmtKey::CompanyKeysForIds => Self::Q_COMPANY_KEYS_FOR_IDS,
            StmtKey::CleanupDeletedKeysBoth => Self::Q_CLEANUP_DELETED_KEYS_BOTH,
        };
        let stmt = Arc::new(client.prepare(sql).await?);
        self.upsert_stmt(key, Arc::clone(&stmt)).await;
        Ok(stmt)
    }

    fn should_reconnect(&self, e: &Error) -> bool {
        if e.is_closed() {
            return true;
        }

        if let Some(src) = e.source()
            && let Some(io_err) = src.downcast_ref::<io::Error>()
        {
            return matches!(
                io_err.kind(),
                io::ErrorKind::TimedOut
                    | io::ErrorKind::ConnectionRefused
                    | io::ErrorKind::ConnectionReset
                    | io::ErrorKind::ConnectionAborted
            );
        }

        false
    }

    pub async fn reconnect(&self) -> Result<()> {
        let (new_client, new_connection_task) = connect_postgres(&self.config.borrow()).await?;

        if let Some(prev_task) = self
            .connection_task
            .swap(
                (
                    Some(Owned::new(AbortOnDropJoinHandle::new(new_connection_task))),
                    Tag::None,
                ),
                Ordering::AcqRel,
            )
            .0
        {
            drop(prev_task);
        }

        let _old = self
            .client
            .swap((Some(Owned::new(new_client)), Tag::None), Ordering::AcqRel);
        // Re-prepare statements on the new connection
        self.prepare_all().await?;
        Ok(())
    }

    pub(super) async fn query_prepared(
        &self,
        key: StmtKey,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<Row>> {
        let stmt = self.get_statement(key).await?;
        let client = self.load_client().await?;
        match client.query(stmt.as_ref(), params).await {
            Ok(rows) => Ok(rows),
            Err(e) => {
                info!("Database query failed: {}. Trying to reconnect...", e);
                if self.should_reconnect(&e) {
                    self.reconnect().await?;
                    let stmt = self.get_statement(key).await?;
                    let client = self.load_client().await?;
                    Ok(client.query(stmt.as_ref(), params).await?)
                } else {
                    Err(anyhow!(e))
                }
            }
        }
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        if let Some(handle) = self
            .connection_task
            .swap((None, Tag::None), Ordering::AcqRel)
            .0
        {
            drop(handle);
        }
    }
}
