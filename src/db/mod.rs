use anyhow::{anyhow, Context, Result};
use foldhash::fast::RandomState;
use foldhash::{HashMapExt as _, HashSetExt as _};
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

pub struct Database {
    client: AtomicOwned<Arc<Client>>,
    ca_pem_path: String,
    host: String,
    port: u16,
    user: String,
    password: String,
    dbname: String,
}

pub struct DatabaseBuilder {
    ca_pem_path: Option<String>,
    host: Option<String>,
    port: Option<u16>,
    user: Option<String>,
    password: Option<String>,
    dbname: Option<String>,
}

impl DatabaseBuilder {
    pub fn new() -> Self {
        Self {
            ca_pem_path: None,
            host: None,
            port: None,
            user: None,
            password: None,
            dbname: None,
        }
    }

    pub fn ca_pem_path(mut self, path: &str) -> Self {
        self.ca_pem_path = Some(path.into());
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
        let ca_pem_path = self
            .ca_pem_path
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| anyhow!("CA PEM path is required and cannot be empty"))?;
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

        let ca_cert_bytes = fs::read(&ca_pem_path)
            .await
            .with_context(|| format!("Failed to read CA certificate at {}", ca_pem_path))?;
        let mut reader = BufReader::new(&ca_cert_bytes[..]);
        let certs = certs(&mut reader)
            .collect::<std::io::Result<Vec<_>>>()
            .with_context(|| "Failed to parse CA certificate as PEM")?;
        let mut root_store = RootCertStore::empty();
        for cert in certs {
            root_store
                .add(cert)
                .with_context(|| "Failed to add CA certificate to root store")?;
        }

        let tls_config = ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth();
        let tls_connector = MakeRustlsConnect::new(tls_config);

        let mut pg_config = Config::new();
        pg_config.host(&host);
        pg_config.port(port);
        pg_config.user(&user);
        pg_config.password(&password);
        pg_config.dbname(&dbname);

        let (client, connection) = pg_config
            .connect(tls_connector)
            .await
            .with_context(|| "Failed to connect to PostgreSQL using rustls")?;
        let client = Arc::new(client);

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                error!("Database connection error: {}", e);
            }
        });

        Ok(Database {
            client: AtomicOwned::new(client),
            ca_pem_path,
            host,
            port,
            user,
            password,
            dbname,
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
        let ca_cert_bytes = fs::read(&self.ca_pem_path)
            .await
            .with_context(|| format!("Failed to read CA certificate at {}", self.ca_pem_path))?;
        let mut reader = BufReader::new(&ca_cert_bytes[..]);
        let certs = certs(&mut reader)
            .collect::<std::io::Result<Vec<_>>>()
            .with_context(|| "Failed to parse CA certificate as PEM")?;
        let mut root_store = RootCertStore::empty();
        for cert in certs {
            root_store
                .add(cert)
                .with_context(|| "Failed to add CA certificate to root store")?;
        }
        let tls_config = ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth();
        let tls_connector = MakeRustlsConnect::new(tls_config);

        let mut pg_config = Config::new();
        pg_config.host(&self.host);
        pg_config.port(self.port);
        pg_config.user(&self.user);
        pg_config.password(&self.password);
        pg_config.dbname(&self.dbname);

        let (new_client, connection) = pg_config.connect(tls_connector).await?;
        let new_client = Arc::new(new_client);

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                error!("Database connection error: {}", e);
            }
        });

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

pub struct KeysUpdater {
    db: Arc<Database>,
    // mapping from user_id -> list of api_keys
    users: scc::HashMap<Uuid, Vec<Uuid>, RandomState>,
    // reverse mapping: api_key -> user_id
    api_keys: scc::HashMap<Uuid, Uuid, RandomState>,
    update_interval: Duration,
}

impl KeysUpdater {
    pub fn new(db: Arc<Database>, update_interval: Duration) -> Self {
        Self {
            db,
            users: scc::HashMap::with_capacity_and_hasher(4096, RandomState::default()),
            api_keys: scc::HashMap::with_capacity_and_hasher(4096, RandomState::default()),
            update_interval,
        }
    }

    pub async fn run(self: Arc<Self>) {
        let mut interval = tokio::time::interval(self.update_interval);
        loop {
            if let Err(e) = self.update_keys().await {
                error!("Error updating keys: {:?}", e);
            }
            interval.tick().await;
        }
    }

    async fn update_keys(&self) -> Result<()> {
        let query = r#"
    SELECT
        u.id,
        COALESCE(array_agg(a.api_key) FILTER (WHERE a.api_key IS NOT NULL), '{}') AS api_keys
    FROM users u
    LEFT JOIN api_keys a ON u.id = a.user_id
    GROUP BY u.id;
"#;
        let rows = self.db.query(query, &[]).await?;

        let mut new_keys: foldhash::HashMap<Uuid, Vec<Uuid>> =
            foldhash::HashMap::with_capacity(rows.len());
        for row in rows {
            let user_id: Uuid = row.get("id");
            let api_keys: Vec<Uuid> = row.get("api_keys");
            new_keys.insert(user_id, api_keys);
        }

        info!("Retrieved {} API keys from the database", new_keys.len());

        let mut new_user_ids: foldhash::HashSet<&Uuid> =
            foldhash::HashSet::with_capacity(new_keys.len());
        for user_id in new_keys.keys() {
            new_user_ids.insert(user_id);
        }
        self.users
            .retain_async(|k, _| new_user_ids.contains(k))
            .await;
        for (user_id, new_api_keys) in &new_keys {
            self.users
                .entry(*user_id)
                .and_modify(|v| {
                    if *v != *new_api_keys {
                        *v = new_api_keys.clone();
                    }
                })
                .or_insert(new_api_keys.clone());
        }

        let total_api_keys: usize = new_keys.values().map(|v| v.len()).sum();
        let mut new_api_key_set: foldhash::HashSet<Uuid> =
            foldhash::HashSet::with_capacity(total_api_keys);
        new_api_key_set.extend(new_keys.values().flatten().cloned());

        self.api_keys
            .retain_async(|k, _| new_api_key_set.contains(k))
            .await;

        for (user_id, api_keys_vec) in &new_keys {
            for api_key in api_keys_vec {
                self.api_keys
                    .entry(*api_key)
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

    pub fn get_user_id(&self, api_key: &Uuid) -> Option<Uuid> {
        self.api_keys
            .read(api_key, |_, stored_user_id| *stored_user_id)
    }

    pub fn is_valid_api_key(&self, api_key: &Uuid) -> bool {
        self.api_keys.contains(api_key)
    }
}
