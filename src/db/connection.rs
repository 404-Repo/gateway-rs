use anyhow::{Result, anyhow};
use foldhash::fast::RandomState;
use portable_atomic::{AtomicU8, AtomicUsize};
use rustls::{ClientConfig, RootCertStore};
use scc::HashMap as SccHashMap;
use sdd::{AtomicOwned, Guard, Owned, Tag};
use std::error::Error as _;
use std::io;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::sync::{Mutex, Notify};
use tokio::{fs, task::JoinHandle, time::sleep};
use tokio_postgres::{Client, Config, Error, NoTls, Row, Statement, types::ToSql};
use tokio_postgres_rustls::MakeRustlsConnect;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::common::cert::{load_certificates, load_private_key, parse_certificates_from_pem_bytes};
use crate::config::{DbConfig, TransportMode};

use super::{Database, DatabaseBuilder};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) enum StmtKey {
    ServerTimeMs,
    GatewaySettings,
    AllUserKeyHashes,
    DeltaUserKeyHashes,
    AllApiKeyBillingOwners,
    DeltaApiKeyBillingOwners,
    UserEmailById,
    FullCompaniesMeta,
    DeltaCompaniesMeta,
    CompanyMetaById,
    FullCompanyKeys,
    DeltaCompanyKeys,
    CompanyKeysForIds,
    CleanupDeletedKeysBoth,
    FetchApiKeyBillingOwner,
    GenerationSubmitTask,
    GenerationRecordTaskAssignments,
    GenerationFinalizeTaskAssignment,
    GenerationExpireTasks,
    GenerationPurgeTerminalTasks,
    GenerationTaskAccountId,
    GenerationTaskStatusSnapshot,
}

const POSTGRES_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const POSTGRES_STATEMENT_TIMEOUT_MS: u64 = 30_000;
const POSTGRES_LOCK_TIMEOUT_MS: u64 = 5_000;
const POSTGRES_RECONNECT_DELAY_MIN_MS: u64 = 200;
const POSTGRES_RECONNECT_DELAY_MAX_MS: u64 = 600;
const DEFAULT_DB_POOL_SIZE: usize = 4;
const SLOT_IDLE: u8 = 0;
const SLOT_BUSY: u8 = 1;
const SLOT_RECONNECTING: u8 = 2;
const STARTUP_STMT_KEYS: &[StmtKey] = &[
    StmtKey::ServerTimeMs,
    StmtKey::GatewaySettings,
    StmtKey::AllUserKeyHashes,
    StmtKey::DeltaUserKeyHashes,
    StmtKey::AllApiKeyBillingOwners,
    StmtKey::DeltaApiKeyBillingOwners,
    StmtKey::UserEmailById,
    StmtKey::FullCompaniesMeta,
    StmtKey::DeltaCompaniesMeta,
    StmtKey::CompanyMetaById,
    StmtKey::FullCompanyKeys,
    StmtKey::DeltaCompanyKeys,
    StmtKey::CompanyKeysForIds,
    StmtKey::CleanupDeletedKeysBoth,
    StmtKey::FetchApiKeyBillingOwner,
];

#[derive(Debug, Clone)]
pub struct PostgresConnectionConfig<'a> {
    pub transport: TransportMode,
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
    pub transport: TransportMode,
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
            transport: self.transport,
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

pub(super) async fn connect_postgres_owned(
    config: &PostgresConnectionConfig<'_>,
) -> Result<(Client, AbortOnDropJoinHandle)> {
    let mut pg_cfg = Config::new();
    pg_cfg.host(config.host);
    pg_cfg.port(config.port);
    pg_cfg.user(config.user);
    pg_cfg.password(config.password);
    pg_cfg.dbname(config.dbname);
    pg_cfg.connect_timeout(POSTGRES_CONNECT_TIMEOUT);

    let (client, connection_task) = match config.transport {
        TransportMode::Tls => {
            let tls_config = build_tls_config(config).await?;
            let tls_connector = MakeRustlsConnect::new(tls_config);
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
            (client, AbortOnDropJoinHandle::new(connection_task))
        }
        TransportMode::Plain => {
            pg_cfg.ssl_mode(tokio_postgres::config::SslMode::Disable);
            let (client, connection) = pg_cfg
                .connect(NoTls)
                .await
                .map_err(|e| anyhow!("Failed to connect to PostgreSQL without TLS: {}", e))?;
            let connection_task = tokio::spawn(async move {
                if let Err(e) = connection.await {
                    error!("Database connection error: {}", e);
                }
            });
            (client, AbortOnDropJoinHandle::new(connection_task))
        }
    };

    // tokio-postgres requires the connection future to be polled before client queries can make
    // progress. Start the driver task first, then configure the session.
    client
        .batch_execute(&format!(
            "SET statement_timeout = '{}ms'; SET lock_timeout = '{}ms';",
            POSTGRES_STATEMENT_TIMEOUT_MS, POSTGRES_LOCK_TIMEOUT_MS
        ))
        .await
        .map_err(|e| anyhow!("Failed to configure PostgreSQL session timeouts: {}", e))?;

    Ok((client, connection_task))
}

async fn connect_postgres(
    config: &PostgresConnectionConfig<'_>,
) -> Result<(Arc<Client>, AbortOnDropJoinHandle)> {
    let (client, connection_task) = connect_postgres_owned(config).await?;
    Ok((Arc::new(client), connection_task))
}

async fn connect_postgres_cancellable(
    config: PostgresConnectionConfigOwned,
    shutdown: Option<CancellationToken>,
) -> Result<(Arc<Client>, AbortOnDropJoinHandle)> {
    if let Some(token) = shutdown {
        let connect = async {
            let borrowed = config.borrow();
            connect_postgres(&borrowed).await
        };
        tokio::select! {
            _ = token.cancelled() => Err(anyhow!("Gateway shutdown requested")),
            res = connect => res,
        }
    } else {
        let borrowed = config.borrow();
        connect_postgres(&borrowed).await
    }
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

fn pool_mask(pool_size: usize) -> usize {
    if pool_size >= usize::BITS as usize {
        usize::MAX
    } else {
        (1usize << pool_size) - 1
    }
}

pub(super) fn reconnect_delay() -> Duration {
    Duration::from_millis(rand::random_range(
        POSTGRES_RECONNECT_DELAY_MIN_MS..=POSTGRES_RECONNECT_DELAY_MAX_MS,
    ))
}

pub(super) fn should_reconnect(e: &Error) -> bool {
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

fn ensure_running(shutdown: Option<&CancellationToken>) -> Result<()> {
    if shutdown.is_some_and(CancellationToken::is_cancelled) {
        return Err(anyhow!("Gateway shutdown requested"));
    }
    Ok(())
}

pub(super) async fn sleep_or_shutdown(
    shutdown: Option<&CancellationToken>,
    delay: Duration,
) -> Result<()> {
    if let Some(token) = shutdown {
        tokio::select! {
            _ = token.cancelled() => Err(anyhow!("Gateway shutdown requested")),
            _ = sleep(delay) => Ok(()),
        }
    } else {
        sleep(delay).await;
        Ok(())
    }
}

struct ClientCell {
    client: AtomicOwned<Arc<Client>>,
    connection_task: AtomicOwned<AbortOnDropJoinHandle>,
    config: PostgresConnectionConfigOwned,
    shutdown: Option<CancellationToken>,
}

impl ClientCell {
    async fn connect(
        config: PostgresConnectionConfigOwned,
        shutdown: Option<CancellationToken>,
    ) -> Result<Self> {
        let (client, connection_task) =
            connect_postgres_cancellable(config.clone(), shutdown.clone()).await?;
        Ok(Self {
            client: AtomicOwned::new(client),
            connection_task: AtomicOwned::new(connection_task),
            config,
            shutdown,
        })
    }

    fn empty(config: PostgresConnectionConfigOwned, shutdown: Option<CancellationToken>) -> Self {
        Self {
            client: AtomicOwned::null(),
            connection_task: AtomicOwned::null(),
            config,
            shutdown,
        }
    }

    fn shutdown_token(&self) -> Option<&CancellationToken> {
        self.shutdown.as_ref()
    }

    fn is_mock(&self) -> bool {
        let guard = Guard::new();
        self.client
            .load(Ordering::Acquire, &guard)
            .as_ref()
            .is_none()
    }

    async fn load_client(&self) -> Result<Arc<Client>> {
        let guard = Guard::new();
        let shared = self.client.load(Ordering::Acquire, &guard);
        shared
            .as_ref()
            .cloned()
            .ok_or_else(|| anyhow!("Database client is not initialized"))
    }

    async fn reconnect(&self) -> Result<Arc<Client>> {
        ensure_running(self.shutdown_token())?;
        let (new_client, new_connection_task) =
            connect_postgres_cancellable(self.config.clone(), self.shutdown.clone()).await?;

        if let Some(prev_task) = self
            .connection_task
            .swap(
                (Some(Owned::new(new_connection_task)), Tag::None),
                Ordering::AcqRel,
            )
            .0
        {
            drop(prev_task);
        }

        let _old = self.client.swap(
            (Some(Owned::new(Arc::clone(&new_client))), Tag::None),
            Ordering::AcqRel,
        );
        Ok(new_client)
    }
}

impl Drop for ClientCell {
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

struct MainPoolSlot {
    index: usize,
    state: AtomicU8,
    connection: ClientCell,
    prepared: SccHashMap<StmtKey, Arc<Statement>, RandomState>,
}

impl MainPoolSlot {
    async fn connect(
        index: usize,
        config: PostgresConnectionConfigOwned,
        shutdown: Option<CancellationToken>,
    ) -> Result<Self> {
        let slot = Self {
            index,
            state: AtomicU8::new(SLOT_IDLE),
            connection: ClientCell::connect(config, shutdown).await?,
            prepared: SccHashMap::with_capacity_and_hasher(32, RandomState::default()),
        };
        slot.prepare_startup_statements().await?;
        Ok(slot)
    }

    fn empty(
        index: usize,
        config: PostgresConnectionConfigOwned,
        shutdown: Option<CancellationToken>,
    ) -> Self {
        Self {
            index,
            state: AtomicU8::new(SLOT_IDLE),
            connection: ClientCell::empty(config, shutdown),
            prepared: SccHashMap::with_capacity_and_hasher(0, RandomState::default()),
        }
    }

    fn mark_idle(&self) {
        self.state.store(SLOT_IDLE, Ordering::Release);
    }

    fn mark_busy(&self) {
        self.state.store(SLOT_BUSY, Ordering::Release);
    }

    fn mark_reconnecting(&self) {
        self.state.store(SLOT_RECONNECTING, Ordering::Release);
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

    async fn prepare_startup_statements(&self) -> Result<()> {
        let client = self.connection.load_client().await?;
        self.prepared.clear_async().await;
        for &key in STARTUP_STMT_KEYS {
            let stmt = Arc::new(client.prepare(Database::sql_for_key(key)).await?);
            self.upsert_stmt(key, stmt).await;
        }
        Ok(())
    }

    async fn get_statement(&self, key: StmtKey) -> Result<Arc<Statement>> {
        if let Some(entry) = self.prepared.get_async(&key).await {
            return Ok(Arc::clone(entry.get()));
        }
        let client = self.connection.load_client().await?;
        let stmt = Arc::new(client.prepare(Database::sql_for_key(key)).await?);
        self.upsert_stmt(key, Arc::clone(&stmt)).await;
        Ok(stmt)
    }

    async fn reconnect(&self) -> Result<()> {
        self.mark_reconnecting();
        let result = async {
            self.connection.reconnect().await?;
            self.prepare_startup_statements().await
        }
        .await;
        self.mark_busy();
        result
    }
}

pub(super) struct MainPool {
    slots: Vec<Arc<MainPoolSlot>>,
    available_mask: AtomicUsize,
    next_slot: AtomicUsize,
    notify: Notify,
    shutdown: Option<CancellationToken>,
}

impl MainPool {
    async fn connect(
        config: PostgresConnectionConfigOwned,
        shutdown: Option<CancellationToken>,
        pool_size: usize,
    ) -> Result<Arc<Self>> {
        let mut slots = Vec::with_capacity(pool_size);
        for index in 0..pool_size {
            slots.push(Arc::new(
                MainPoolSlot::connect(index, config.clone(), shutdown.clone()).await?,
            ));
        }
        Ok(Arc::new(Self {
            slots,
            available_mask: AtomicUsize::new(pool_mask(pool_size)),
            next_slot: AtomicUsize::new(0),
            notify: Notify::new(),
            shutdown,
        }))
    }

    fn mock(pool_size: usize) -> Arc<Self> {
        let config = PostgresConnectionConfigOwned {
            transport: TransportMode::Plain,
            host: String::new(),
            port: 0,
            user: String::new(),
            password: String::new(),
            dbname: String::new(),
            sslcert_path: String::new(),
            sslkey_path: String::new(),
            sslrootcert_path: String::new(),
        };
        let slots = (0..pool_size)
            .map(|index| Arc::new(MainPoolSlot::empty(index, config.clone(), None)))
            .collect();
        Arc::new(Self {
            slots,
            available_mask: AtomicUsize::new(pool_mask(pool_size)),
            next_slot: AtomicUsize::new(0),
            notify: Notify::new(),
            shutdown: None,
        })
    }

    fn is_mock(&self) -> bool {
        self.slots
            .first()
            .is_none_or(|slot| slot.connection.is_mock())
    }

    fn choose_slot_index(&self, mask: usize) -> Option<usize> {
        if mask == 0 || self.slots.is_empty() {
            return None;
        }

        let start = self.next_slot.fetch_add(1, Ordering::Relaxed) % self.slots.len();
        for offset in 0..self.slots.len() {
            let index = (start + offset) % self.slots.len();
            let bit = 1usize << index;
            if mask & bit != 0 {
                return Some(index);
            }
        }
        None
    }

    fn release_slot(&self, slot: &MainPoolSlot) {
        slot.mark_idle();
        self.available_mask
            .fetch_or(1usize << slot.index, Ordering::AcqRel);
        self.notify.notify_one();
    }

    pub(super) async fn acquire(self: &Arc<Self>) -> Result<MainPoolLease> {
        loop {
            ensure_running(self.shutdown.as_ref())?;
            if let Some(index) = self.choose_slot_index(self.available_mask.load(Ordering::Acquire))
            {
                let bit = 1usize << index;
                let prev = self.available_mask.fetch_and(!bit, Ordering::AcqRel);
                if prev & bit != 0 {
                    let slot = Arc::clone(&self.slots[index]);
                    slot.mark_busy();
                    return Ok(MainPoolLease {
                        pool: Arc::clone(self),
                        slot,
                        released: false,
                    });
                }
            }

            let notified = self.notify.notified();
            if let Some(token) = self.shutdown.as_ref() {
                tokio::select! {
                    _ = token.cancelled() => return Err(anyhow!("Gateway shutdown requested")),
                    _ = notified => {}
                }
            } else {
                notified.await;
            }
        }
    }
}

pub(super) struct MainPoolLease {
    pool: Arc<MainPool>,
    slot: Arc<MainPoolSlot>,
    released: bool,
}

impl MainPoolLease {
    #[cfg(test)]
    fn slot_index(&self) -> usize {
        self.slot.index
    }

    async fn query_prepared(
        &self,
        key: StmtKey,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<Row>> {
        let stmt = self.slot.get_statement(key).await?;
        let client = self.slot.connection.load_client().await?;
        match client.query(stmt.as_ref(), params).await {
            Ok(rows) => Ok(rows),
            Err(e) => {
                if should_reconnect(&e) {
                    let delay = reconnect_delay();
                    info!(
                        "Database query failed on pooled slot {}: {}. Trying to reconnect after {:?}...",
                        self.slot.index, e, delay
                    );
                    sleep_or_shutdown(self.slot.connection.shutdown_token(), delay).await?;
                    self.slot.reconnect().await?;
                    let stmt = self.slot.get_statement(key).await?;
                    let client = self.slot.connection.load_client().await?;
                    Ok(client.query(stmt.as_ref(), params).await?)
                } else {
                    Err(anyhow!(e))
                }
            }
        }
    }
}

impl Drop for MainPoolLease {
    fn drop(&mut self) {
        if self.released {
            return;
        }
        self.released = true;
        self.pool.release_slot(&self.slot);
    }
}

pub(super) struct CopyLane {
    connection: ClientCell,
    gate: Mutex<()>,
}

impl CopyLane {
    async fn connect(
        config: PostgresConnectionConfigOwned,
        shutdown: Option<CancellationToken>,
    ) -> Result<Arc<Self>> {
        Ok(Arc::new(Self {
            connection: ClientCell::connect(config, shutdown).await?,
            gate: Mutex::new(()),
        }))
    }

    fn mock() -> Arc<Self> {
        let config = PostgresConnectionConfigOwned {
            transport: TransportMode::Plain,
            host: String::new(),
            port: 0,
            user: String::new(),
            password: String::new(),
            dbname: String::new(),
            sslcert_path: String::new(),
            sslkey_path: String::new(),
            sslrootcert_path: String::new(),
        };
        Arc::new(Self {
            connection: ClientCell::empty(config, None),
            gate: Mutex::new(()),
        })
    }

    pub(super) async fn lock(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.gate.lock().await
    }

    pub(super) async fn ensure_client(&self) -> Result<Arc<Client>> {
        match self.connection.load_client().await {
            Ok(client) => Ok(client),
            Err(_) => self.connection.reconnect().await,
        }
    }

    pub(super) async fn reconnect(&self) -> Result<Arc<Client>> {
        self.connection.reconnect().await
    }

    pub(super) fn shutdown_token(&self) -> Option<&CancellationToken> {
        self.connection.shutdown_token()
    }
}

impl DatabaseBuilder {
    pub fn new() -> Self {
        Self {
            transport: None,
            sslcert_path: None,
            sslkey_path: None,
            sslrootcert_path: None,
            host: None,
            port: None,
            user: None,
            password: None,
            dbname: None,
            events_copy_batch_size: None,
            pool_size: None,
            shutdown: None,
        }
    }

    pub fn from_config(config: &DbConfig) -> Self {
        Self::new()
            .transport(config.transport)
            .host(&config.host)
            .port(config.port)
            .user(&config.user)
            .password(&config.password)
            .dbname(&config.db)
            .sslcert_path(&config.sslcert)
            .sslkey_path(&config.sslkey)
            .sslrootcert_path(&config.sslrootcert)
            .events_copy_batch_size(config.events_copy_batch_size)
            .pool_size(config.pool_size)
    }

    pub fn shutdown_token(mut self, token: CancellationToken) -> Self {
        self.shutdown = Some(token);
        self
    }

    pub fn transport(mut self, transport: TransportMode) -> Self {
        self.transport = Some(transport);
        self
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

    pub fn pool_size(mut self, value: usize) -> Self {
        self.pool_size = Some(value.max(1));
        self
    }

    fn require_nonempty(opt: Option<String>, name: &str) -> Result<String> {
        opt.filter(|s| !s.trim().is_empty())
            .ok_or_else(|| anyhow!("{name} is required and cannot be empty"))
    }

    pub async fn build(self) -> Result<Database> {
        let transport = self.transport.unwrap_or_default();
        let (sslcert_path, sslkey_path, sslrootcert_path) = match transport {
            TransportMode::Tls => (
                Self::require_nonempty(self.sslcert_path, "sslcert path")?,
                Self::require_nonempty(self.sslkey_path, "sslkey path")?,
                Self::require_nonempty(self.sslrootcert_path, "sslrootcert path")?,
            ),
            TransportMode::Plain => (String::new(), String::new(), String::new()),
        };
        let host = Self::require_nonempty(self.host, "Host")?;
        let user = Self::require_nonempty(self.user, "User")?;
        let password = Self::require_nonempty(self.password, "Password")?;
        let dbname = Self::require_nonempty(self.dbname, "Database name")?;
        let port = self.port.ok_or_else(|| anyhow!("Port is required"))?;
        let events_copy_batch_size = self.events_copy_batch_size.unwrap_or(1000).max(1);
        let pool_size = self.pool_size.unwrap_or(DEFAULT_DB_POOL_SIZE).max(1);
        if pool_size > usize::BITS as usize {
            return Err(anyhow!(
                "db pool size {pool_size} exceeds supported capacity {}",
                usize::BITS
            ));
        }

        let shutdown = self.shutdown;
        let config = PostgresConnectionConfigOwned {
            transport,
            host,
            port,
            user,
            password,
            dbname,
            sslcert_path,
            sslkey_path,
            sslrootcert_path,
        };

        Ok(Database {
            main_pool: MainPool::connect(config.clone(), shutdown.clone(), pool_size).await?,
            events_copy_batch_size,
            activity_copy_lane: CopyLane::connect(config.clone(), shutdown.clone()).await?,
            worker_copy_lane: CopyLane::connect(config, shutdown).await?,
        })
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
            main_pool: MainPool::mock(1),
            events_copy_batch_size: 1000,
            activity_copy_lane: CopyLane::mock(),
            worker_copy_lane: CopyLane::mock(),
        }
    }

    pub(super) fn is_mock(&self) -> bool {
        self.main_pool.is_mock()
    }

    fn sql_for_key(key: StmtKey) -> &'static str {
        match key {
            StmtKey::ServerTimeMs => Self::Q_SERVER_TIME_MS,
            StmtKey::GatewaySettings => Self::Q_GATEWAY_SETTINGS,
            StmtKey::AllUserKeyHashes => Self::Q_ALL_USER_KEY_HASHES,
            StmtKey::DeltaUserKeyHashes => Self::Q_DELTA_USER_KEY_HASHES,
            StmtKey::AllApiKeyBillingOwners => Self::Q_ALL_API_KEY_BILLING_OWNERS,
            StmtKey::DeltaApiKeyBillingOwners => Self::Q_DELTA_API_KEY_BILLING_OWNERS,
            StmtKey::UserEmailById => Self::Q_USER_EMAIL_BY_ID,
            StmtKey::FullCompaniesMeta => Self::Q_FULL_COMPANIES_META,
            StmtKey::DeltaCompaniesMeta => Self::Q_DELTA_COMPANIES_META,
            StmtKey::CompanyMetaById => Self::Q_COMPANY_META_BY_ID,
            StmtKey::FullCompanyKeys => Self::Q_FULL_COMPANY_KEYS,
            StmtKey::DeltaCompanyKeys => Self::Q_DELTA_COMPANY_KEYS,
            StmtKey::CompanyKeysForIds => Self::Q_COMPANY_KEYS_FOR_IDS,
            StmtKey::CleanupDeletedKeysBoth => Self::Q_CLEANUP_DELETED_KEYS_BOTH,
            StmtKey::FetchApiKeyBillingOwner => Self::Q_FETCH_API_KEY_BILLING_OWNER,
            StmtKey::GenerationSubmitTask => Self::Q_GENERATION_SUBMIT_TASK,
            StmtKey::GenerationRecordTaskAssignments => Self::Q_GENERATION_RECORD_TASK_ASSIGNMENTS,
            StmtKey::GenerationFinalizeTaskAssignment => {
                Self::Q_GENERATION_FINALIZE_TASK_ASSIGNMENT
            }
            StmtKey::GenerationExpireTasks => Self::Q_GENERATION_EXPIRE_TASKS,
            StmtKey::GenerationPurgeTerminalTasks => Self::Q_GENERATION_PURGE_TERMINAL_TASKS,
            StmtKey::GenerationTaskAccountId => Self::Q_GENERATION_TASK_ACCOUNT_ID,
            StmtKey::GenerationTaskStatusSnapshot => Self::Q_GENERATION_TASK_STATUS_SNAPSHOT,
        }
    }

    pub(super) async fn query_prepared(
        &self,
        key: StmtKey,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<Row>> {
        let lease = self.main_pool.acquire().await?;
        lease.query_prepared(key, params).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    fn mock_connection_config() -> PostgresConnectionConfigOwned {
        PostgresConnectionConfigOwned {
            transport: TransportMode::Plain,
            host: String::new(),
            port: 0,
            user: String::new(),
            password: String::new(),
            dbname: String::new(),
            sslcert_path: String::new(),
            sslkey_path: String::new(),
            sslrootcert_path: String::new(),
        }
    }

    fn mock_pool_with_shutdown(
        pool_size: usize,
        shutdown: Option<CancellationToken>,
    ) -> Arc<MainPool> {
        let config = mock_connection_config();
        let slots = (0..pool_size)
            .map(|index| Arc::new(MainPoolSlot::empty(index, config.clone(), shutdown.clone())))
            .collect();
        Arc::new(MainPool {
            slots,
            available_mask: AtomicUsize::new(pool_mask(pool_size)),
            next_slot: AtomicUsize::new(0),
            notify: Notify::new(),
            shutdown,
        })
    }

    fn plain_builder() -> DatabaseBuilder {
        DatabaseBuilder::new()
            .transport(TransportMode::Plain)
            .host("127.0.0.1")
            .port(5432)
            .user("gateway")
            .password("gateway")
            .dbname("gateway")
    }

    fn tls_builder() -> DatabaseBuilder {
        DatabaseBuilder::new()
            .transport(TransportMode::Tls)
            .host("127.0.0.1")
            .port(5432)
            .user("gateway")
            .password("gateway")
            .dbname("gateway")
    }

    #[test]
    fn pool_mask_handles_zero_and_word_size_boundaries() {
        assert_eq!(pool_mask(0), 0);
        assert_eq!(pool_mask(1), 1);
        assert_eq!(pool_mask(usize::BITS as usize), usize::MAX);
        assert_eq!(pool_mask(usize::BITS as usize + 5), usize::MAX);
    }

    #[test]
    fn reconnect_delay_stays_within_configured_bounds() {
        for _ in 0..128 {
            let delay = reconnect_delay();
            assert!(
                delay >= Duration::from_millis(POSTGRES_RECONNECT_DELAY_MIN_MS)
                    && delay <= Duration::from_millis(POSTGRES_RECONNECT_DELAY_MAX_MS),
                "delay {:?} was outside [{}, {}] ms",
                delay,
                POSTGRES_RECONNECT_DELAY_MIN_MS,
                POSTGRES_RECONNECT_DELAY_MAX_MS
            );
        }
    }

    #[test]
    fn startup_prewarm_excludes_generation_lifecycle_statements() {
        assert!(STARTUP_STMT_KEYS.contains(&StmtKey::ServerTimeMs));
        assert!(STARTUP_STMT_KEYS.contains(&StmtKey::GatewaySettings));
        assert!(STARTUP_STMT_KEYS.contains(&StmtKey::FetchApiKeyBillingOwner));
        assert!(!STARTUP_STMT_KEYS.contains(&StmtKey::GenerationSubmitTask));
        assert!(!STARTUP_STMT_KEYS.contains(&StmtKey::GenerationRecordTaskAssignments));
        assert!(!STARTUP_STMT_KEYS.contains(&StmtKey::GenerationFinalizeTaskAssignment));
        assert!(!STARTUP_STMT_KEYS.contains(&StmtKey::GenerationExpireTasks));
        assert!(!STARTUP_STMT_KEYS.contains(&StmtKey::GenerationPurgeTerminalTasks));
        assert!(!STARTUP_STMT_KEYS.contains(&StmtKey::GenerationTaskAccountId));
        assert!(!STARTUP_STMT_KEYS.contains(&StmtKey::GenerationTaskStatusSnapshot));
    }

    #[tokio::test]
    async fn sleep_or_shutdown_returns_immediately_when_cancelled() {
        let token = CancellationToken::new();
        token.cancel();

        let started = Instant::now();
        let err = sleep_or_shutdown(Some(&token), Duration::from_secs(30))
            .await
            .expect_err("cancelled database sleep should return an error");
        assert!(
            err.to_string().contains("shutdown requested"),
            "unexpected error: {err}"
        );
        assert!(
            started.elapsed() < Duration::from_millis(100),
            "cancelled sleep should return immediately"
        );
    }

    #[test]
    fn choose_slot_index_returns_none_when_mask_is_empty() {
        let pool = MainPool::mock(3);
        assert_eq!(pool.choose_slot_index(0), None);
    }

    #[test]
    fn choose_slot_index_wraps_and_skips_unavailable_slots() {
        let pool = MainPool::mock(4);

        pool.next_slot.store(3, Ordering::Release);
        assert_eq!(pool.choose_slot_index(0b0001), Some(0));

        pool.next_slot.store(2, Ordering::Release);
        assert_eq!(pool.choose_slot_index(0b1010), Some(3));

        pool.next_slot.store(1, Ordering::Release);
        assert_eq!(pool.choose_slot_index(0b0010), Some(1));
    }

    #[tokio::test]
    async fn pooled_checkout_uses_distinct_slots_until_exhausted() {
        let pool = MainPool::mock(2);
        let lease_a = pool.acquire().await.expect("first lease");
        let lease_b = pool.acquire().await.expect("second lease");

        assert_ne!(lease_a.slot_index(), lease_b.slot_index());
    }

    #[tokio::test]
    async fn pooled_checkout_waits_for_release_when_pool_is_exhausted() {
        let pool = MainPool::mock(1);
        let lease = pool.acquire().await.expect("initial lease");
        let pool_for_waiter = Arc::clone(&pool);

        let waiter = tokio::spawn(async move {
            pool_for_waiter
                .acquire()
                .await
                .map(|lease| lease.slot_index())
        });
        sleep(Duration::from_millis(20)).await;
        assert!(
            !waiter.is_finished(),
            "second checkout should wait while the only slot is in use"
        );

        drop(lease);

        let slot_index = waiter
            .await
            .expect("waiter task should join")
            .expect("waiter should acquire after release");
        assert_eq!(slot_index, 0);
    }

    #[tokio::test]
    async fn pooled_checkout_returns_immediately_when_shutdown_is_already_requested() {
        let token = CancellationToken::new();
        token.cancel();
        let pool = mock_pool_with_shutdown(1, Some(token));

        let started = Instant::now();
        let err = match pool.acquire().await {
            Ok(_) => panic!("cancelled pool acquire should return an error"),
            Err(err) => err,
        };
        assert!(
            err.to_string().contains("shutdown requested"),
            "unexpected error: {err}"
        );
        assert!(
            started.elapsed() < Duration::from_millis(100),
            "cancelled acquire should return immediately"
        );
    }

    #[tokio::test]
    async fn pooled_checkout_returns_error_when_shutdown_arrives_while_waiting() {
        let token = CancellationToken::new();
        let pool = mock_pool_with_shutdown(1, Some(token.clone()));
        let lease = pool.acquire().await.expect("initial lease");
        let pool_for_waiter = Arc::clone(&pool);

        let waiter = tokio::spawn(async move { pool_for_waiter.acquire().await });
        sleep(Duration::from_millis(20)).await;
        assert!(
            !waiter.is_finished(),
            "second checkout should wait while the only slot is in use"
        );

        token.cancel();

        let err = match waiter.await.expect("waiter task should join") {
            Ok(_) => panic!("waiter should receive shutdown error"),
            Err(err) => err,
        };
        assert!(
            err.to_string().contains("shutdown requested"),
            "unexpected error: {err}"
        );

        drop(lease);
    }

    #[test]
    fn database_builder_clamps_zero_sizes_to_one() {
        let builder = DatabaseBuilder::new()
            .events_copy_batch_size(0)
            .pool_size(0);

        assert_eq!(builder.events_copy_batch_size, Some(1));
        assert_eq!(builder.pool_size, Some(1));
    }

    #[test]
    fn require_nonempty_rejects_blank_strings() {
        let err = DatabaseBuilder::require_nonempty(Some("   ".into()), "Host")
            .expect_err("blank values should be rejected");
        assert_eq!(err.to_string(), "Host is required and cannot be empty");
    }

    #[tokio::test]
    async fn database_builder_rejects_pool_sizes_beyond_supported_capacity() {
        let err = match plain_builder()
            .pool_size(usize::BITS as usize + 1)
            .build()
            .await
        {
            Ok(_) => panic!("oversized pools should be rejected before connect"),
            Err(err) => err,
        };

        assert!(
            err.to_string().contains(&format!(
                "db pool size {} exceeds supported capacity {}",
                usize::BITS as usize + 1,
                usize::BITS
            )),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn database_builder_tls_transport_requires_all_tls_paths() {
        let missing_cert = match tls_builder()
            .sslkey_path("/tmp/client.key")
            .sslrootcert_path("/tmp/ca.crt")
            .build()
            .await
        {
            Ok(_) => panic!("missing client cert should be rejected"),
            Err(err) => err,
        };
        assert!(
            missing_cert.to_string().contains("sslcert path"),
            "unexpected error: {missing_cert}"
        );

        let missing_key = match tls_builder()
            .sslcert_path("/tmp/client.crt")
            .sslrootcert_path("/tmp/ca.crt")
            .build()
            .await
        {
            Ok(_) => panic!("missing client key should be rejected"),
            Err(err) => err,
        };
        assert!(
            missing_key.to_string().contains("sslkey path"),
            "unexpected error: {missing_key}"
        );

        let missing_root = match tls_builder()
            .sslcert_path("/tmp/client.crt")
            .sslkey_path("/tmp/client.key")
            .build()
            .await
        {
            Ok(_) => panic!("missing root cert should be rejected"),
            Err(err) => err,
        };
        assert!(
            missing_root.to_string().contains("sslrootcert path"),
            "unexpected error: {missing_root}"
        );
    }

    #[test]
    fn database_new_mock_reports_mock_mode() {
        let db = Database::new_mock();
        assert!(db.is_mock());
    }
}
