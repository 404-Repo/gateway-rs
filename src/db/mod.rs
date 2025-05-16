use anyhow::{Context, Result};
use foldhash::fast::RandomState;
use foldhash::{HashMapExt as _, HashSetExt as _};
use rustls::{ClientConfig, RootCertStore};
use rustls_pemfile::certs;
use std::time::Duration;
use std::{io::BufReader, sync::Arc};
use tokio::fs;
use tokio_postgres::Config;
use tokio_postgres_rustls::MakeRustlsConnect;
use tracing::{error, info};
use uuid::Uuid;

pub struct Database {
    client: tokio_postgres::Client,
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
        let ca_pem_path = match self.ca_pem_path {
            Some(s) if !s.trim().is_empty() => s,
            _ => {
                return Err(anyhow::anyhow!(
                    "CA PEM path is required and cannot be empty"
                ))
            }
        };
        let host = match self.host {
            Some(s) if !s.trim().is_empty() => s,
            _ => return Err(anyhow::anyhow!("Host is required and cannot be empty")),
        };
        let port = self
            .port
            .ok_or_else(|| anyhow::anyhow!("Port is required"))?;
        let user = match self.user {
            Some(s) if !s.trim().is_empty() => s,
            _ => return Err(anyhow::anyhow!("User is required and cannot be empty")),
        };
        let password = match self.password {
            Some(s) if !s.trim().is_empty() => s,
            _ => return Err(anyhow::anyhow!("Password is required and cannot be empty")),
        };
        let dbname = match self.dbname {
            Some(s) if !s.trim().is_empty() => s,
            _ => {
                return Err(anyhow::anyhow!(
                    "Database name is required and cannot be empty"
                ))
            }
        };

        let ca_cert_bytes = fs::read(&ca_pem_path)
            .await
            .with_context(|| format!("Failed to read CA certificate at {}", ca_pem_path))?;
        let mut reader = BufReader::new(&ca_cert_bytes[..]);
        let certs = certs(&mut reader)
            .collect::<Result<Vec<_>, _>>()
            .with_context(|| "Failed to parse CA certificate as PEM")?;
        let mut root_store = RootCertStore::empty();
        for cert in certs {
            root_store
                .add(cert)
                .with_context(|| "Failed to add CA certificate to root store")?;
        }

        let config = ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth();

        let tls_connector = MakeRustlsConnect::new(config);
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
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                error!("Database connection error: {}", e);
            }
        });
        Ok(Database { client })
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
        let rows = self.db.client.query(query, &[]).await?;

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

    pub fn is_valid_api_key(&self, api_key: &Uuid) -> bool {
        self.api_keys.contains(api_key)
    }
}
