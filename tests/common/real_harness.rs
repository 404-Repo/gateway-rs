use std::net::{TcpListener, UdpSocket};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use foldhash::HashSet;
use gateway::config::{NodeConfig, TransportMode};
use gateway::config_runtime::RuntimeConfigStore;
use gateway::crypto::crypto_provider::ApiKeyHasher;
use gateway::crypto::hotkey::Hotkey;
use gateway::raft::Gateway;
use gateway::raft::GatewayMode;
use gateway::test_support::load_test_single_node_config;
use http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode};
use reqwest::Client as HttpClient;
use serde::Serialize;
use serde_json::Value;
use tempfile::{NamedTempFile, TempDir};
use testcontainers_modules::postgres;
use testcontainers_modules::testcontainers::ImageExt;
use testcontainers_modules::testcontainers::core::ContainerAsync;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tokio::sync::{Mutex, OnceCell, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinHandle;
use tokio_postgres::{Client, NoTls};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const TEST_DB_ADMIN_NAME: &str = "postgres";
const TEST_DB_TEMPLATE_NAME: &str = "gateway_test_template";
const TEST_DB_PREFIX: &str = "gateway_test_";
const TEST_DB_USER: &str = "gateway_test";
const TEST_DB_PASSWORD: &str = "gateway_test";
const TEST_POSTGRES_TAG: &str = "17";
const DEFAULT_MAX_PARALLEL_HARNESSES: usize = 4;
const READY_RETRIES: usize = 80;
const READY_SLEEP: Duration = Duration::from_millis(50);
static SHARED_POSTGRES_ENV: OnceCell<Arc<SharedPostgresEnv>> = OnceCell::const_new();

#[derive(Debug, Clone)]
pub(crate) struct TestResponse {
    pub(crate) status: StatusCode,
    pub(crate) headers: HeaderMap,
    pub(crate) body: Vec<u8>,
}

#[derive(Clone)]
pub(crate) struct TestService {
    base_url: String,
    client: HttpClient,
}

impl TestService {
    pub(crate) fn new(base_url: String) -> Self {
        Self {
            base_url,
            client: HttpClient::new(),
        }
    }

    fn resolve_url(&self, raw: &str) -> String {
        if raw.starts_with("http://localhost") {
            raw.replacen("http://localhost", &self.base_url, 1)
        } else if raw.starts_with("https://localhost") {
            raw.replacen("https://localhost", &self.base_url, 1)
        } else if raw.starts_with('/') {
            format!("{}{}", self.base_url, raw)
        } else if raw.starts_with("http://") || raw.starts_with("https://") {
            raw.to_string()
        } else {
            format!("{}/{}", self.base_url, raw.trim_start_matches('/'))
        }
    }

    async fn get_json(&self, path: &str) -> Result<Value> {
        let response = self
            .client
            .get(self.resolve_url(path))
            .send()
            .await
            .context("send GET request")?;
        let status = response.status();
        let body = response.bytes().await.context("read GET response body")?;
        if !status.is_success() {
            return Err(anyhow!(
                "request to {} failed with status {}: {}",
                path,
                status,
                String::from_utf8_lossy(&body)
            ));
        }
        serde_json::from_slice(&body).context("decode JSON response")
    }
}

pub(crate) struct TestClient;

pub(crate) struct TestRequestBuilder {
    method: Method,
    url: String,
    headers: HeaderMap,
    body: Option<Vec<u8>>,
}

impl TestClient {
    pub(crate) fn get(url: impl Into<String>) -> TestRequestBuilder {
        TestRequestBuilder::new(Method::GET, url.into())
    }

    pub(crate) fn post(url: impl Into<String>) -> TestRequestBuilder {
        TestRequestBuilder::new(Method::POST, url.into())
    }
}

impl TestRequestBuilder {
    fn new(method: Method, url: String) -> Self {
        Self {
            method,
            url,
            headers: HeaderMap::new(),
            body: None,
        }
    }

    pub(crate) fn add_header(
        mut self,
        name: impl AsRef<str>,
        value: impl ToString,
        _overwrite: bool,
    ) -> Self {
        let header_name = HeaderName::from_bytes(name.as_ref().as_bytes()).expect("header name");
        let header_value = HeaderValue::from_str(&value.to_string()).expect("header value");
        self.headers.insert(header_name, header_value);
        self
    }

    pub(crate) fn json<T: Serialize>(mut self, value: &T) -> Self {
        self.body = Some(serde_json::to_vec(value).expect("serialize request JSON"));
        self.headers.insert(
            http::header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
        self
    }

    pub(crate) fn body(mut self, body: impl Into<Vec<u8>>) -> Self {
        self.body = Some(body.into());
        self
    }

    pub(crate) async fn send(self, service: &TestService) -> TestResponse {
        let url = service.resolve_url(&self.url);
        let mut request = service.client.request(self.method, url);
        for (name, value) in &self.headers {
            request = request.header(name, value);
        }
        if let Some(body) = self.body {
            request = request.body(body);
        }

        let response = request.send().await.expect("send test request");
        let status = response.status();
        let headers = response.headers().clone();
        let body = response
            .bytes()
            .await
            .expect("read test response body")
            .to_vec();
        TestResponse {
            status,
            headers,
            body,
        }
    }
}

#[derive(Clone)]
pub(crate) struct GatewayHarnessOptions {
    pub(crate) include_gateway_info: bool,
    pub(crate) worker_whitelist: Option<HashSet<Hotkey>>,
    pub(crate) taskmanager_cleanup_interval_secs: Option<u64>,
    pub(crate) taskmanager_result_lifetime_secs: Option<u64>,
    pub(crate) api_keys_update_interval_secs: Option<u64>,
}

impl Default for GatewayHarnessOptions {
    fn default() -> Self {
        Self {
            include_gateway_info: true,
            worker_whitelist: None,
            taskmanager_cleanup_interval_secs: None,
            taskmanager_result_lifetime_secs: None,
            api_keys_update_interval_secs: None,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct PersonalKeySeed<'a> {
    pub(crate) api_key: &'a str,
    pub(crate) user_email: Option<&'a str>,
    pub(crate) concurrent_limit: i32,
    pub(crate) daily_limit: i32,
    pub(crate) balance_cents: i64,
}

#[derive(Debug, Clone)]
pub(crate) struct CompanyKeySeed<'a> {
    pub(crate) api_key: &'a str,
    pub(crate) company_id: Uuid,
    pub(crate) company_name: &'a str,
    pub(crate) concurrent_limit: i32,
    pub(crate) daily_limit: i32,
    pub(crate) balance_cents: i64,
}

pub(crate) struct GatewayRuntimeHarness {
    pub(crate) service: TestService,
    pub(crate) config: Arc<NodeConfig>,
    pub(crate) gateway: Gateway,
    pub(crate) db_client: Client,
    _db_connection_task: JoinHandle<()>,
    _db_lease: TestDatabaseLease,
    _config_file: NamedTempFile,
    _temp_dir: TempDir,
    _shutdown: CancellationToken,
}

#[allow(dead_code)]
pub(crate) struct GatewayRuntimeStart {
    pub(crate) harness: GatewayRuntimeHarness,
    pub(crate) runtime_config: Arc<RuntimeConfigStore>,
    pub(crate) config_path: PathBuf,
    pub(crate) generic_key: Uuid,
    pub(crate) admin_key: Uuid,
    pub(crate) default_user_api_key: String,
    pub(crate) default_company_api_key: String,
    pub(crate) company_id: Uuid,
}

struct SharedPostgresEnv {
    host: String,
    port: u16,
    user: String,
    password: String,
    template_dbname: String,
    limiter: Arc<Semaphore>,
    clone_lock: Mutex<()>,
    _container: ContainerAsync<postgres::Postgres>,
}

struct TestDatabaseLease {
    host: String,
    port: u16,
    user: String,
    password: String,
    dbname: String,
    _permit: OwnedSemaphorePermit,
}

async fn shared_postgres_env() -> Arc<SharedPostgresEnv> {
    Arc::clone(
        SHARED_POSTGRES_ENV
            .get_or_init(|| async {
                Arc::new(
                    SharedPostgresEnv::start()
                        .await
                        .expect("start shared postgres test environment"),
                )
            })
            .await,
    )
}

impl SharedPostgresEnv {
    async fn start() -> Result<Self> {
        let container = postgres::Postgres::default()
            .with_db_name(TEST_DB_ADMIN_NAME)
            .with_user(TEST_DB_USER)
            .with_password(TEST_DB_PASSWORD)
            .with_tag(TEST_POSTGRES_TAG)
            .start()
            .await
            .context("start shared postgres container")?;
        let host = container
            .get_host()
            .await
            .context("read postgres container host")?
            .to_string();
        let port = container
            .get_host_port_ipv4(5432)
            .await
            .context("read postgres container port")?;
        let (admin_client, admin_connection_task) = connect_plain_postgres(
            host.as_str(),
            port,
            TEST_DB_USER,
            TEST_DB_PASSWORD,
            TEST_DB_ADMIN_NAME,
        )
        .await
        .context("connect to postgres admin database")?;
        create_template_database(&admin_client, TEST_DB_TEMPLATE_NAME)
            .await
            .context("create template database")?;
        drop(admin_client);
        admin_connection_task.abort();
        let (template_client, template_connection_task) = connect_plain_postgres(
            host.as_str(),
            port,
            TEST_DB_USER,
            TEST_DB_PASSWORD,
            TEST_DB_TEMPLATE_NAME,
        )
        .await
        .context("connect to template database")?;
        apply_gateway_dev_schema(&template_client)
            .await
            .context("apply gateway dev schema to template")?;
        drop(template_client);
        template_connection_task.abort();

        Ok(Self {
            host,
            port,
            user: TEST_DB_USER.to_string(),
            password: TEST_DB_PASSWORD.to_string(),
            template_dbname: TEST_DB_TEMPLATE_NAME.to_string(),
            limiter: Arc::new(Semaphore::new(test_db_parallelism())),
            clone_lock: Mutex::new(()),
            _container: container,
        })
    }

    async fn lease_database(&self) -> Result<TestDatabaseLease> {
        let permit = Arc::clone(&self.limiter)
            .acquire_owned()
            .await
            .context("acquire shared postgres test permit")?;
        let dbname = format!("{TEST_DB_PREFIX}{}", Uuid::new_v4().simple());
        let create_sql = format!(
            "CREATE DATABASE {} TEMPLATE {}",
            quote_ident(&dbname),
            quote_ident(&self.template_dbname)
        );
        let _clone_guard = self.clone_lock.lock().await;
        let (admin_client, admin_connection_task) = connect_plain_postgres(
            &self.host,
            self.port,
            &self.user,
            &self.password,
            TEST_DB_ADMIN_NAME,
        )
        .await
        .context("connect to postgres admin database")?;
        admin_client
            .batch_execute(&create_sql)
            .await
            .with_context(|| {
                format!(
                    "create isolated test database {dbname} from template {}",
                    self.template_dbname
                )
            })?;
        drop(admin_client);
        admin_connection_task.abort();
        Ok(TestDatabaseLease {
            host: self.host.clone(),
            port: self.port,
            user: self.user.clone(),
            password: self.password.clone(),
            dbname,
            _permit: permit,
        })
    }
}

impl GatewayRuntimeHarness {
    pub(crate) async fn start(options: GatewayHarnessOptions) -> GatewayRuntimeStart {
        let shared_env = shared_postgres_env().await;
        let db_lease = shared_env
            .lease_database()
            .await
            .expect("lease isolated test database");
        let (db_client, db_connection_task) = connect_plain_postgres(
            &db_lease.host,
            db_lease.port,
            &db_lease.user,
            &db_lease.password,
            &db_lease.dbname,
        )
        .await
        .expect("connect to isolated test database");

        let (mut config, _base_path) = load_test_single_node_config();
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let http_port = reserve_tcp_port().expect("reserve HTTP port");
        let raft_port = reserve_udp_port().expect("reserve raft port");
        std::fs::create_dir_all(temp_dir.path().join("snapshots")).expect("create snapshots dir");

        config.network.bind_ip = "127.0.0.1".to_string();
        config.network.external_ip = "127.0.0.1".to_string();
        config.network.domain = "localhost".to_string();
        config.network.server_port = raft_port;
        config.network.name = "test-node".to_string();
        config.network.node_dns_names.clear();
        config.http.port = http_port;
        config.http.transport = TransportMode::Plain;
        config.db.transport = TransportMode::Plain;
        config.db.host = db_lease.host.clone();
        config.db.port = db_lease.port;
        config.db.user = db_lease.user.clone();
        config.db.password = db_lease.password.clone();
        config.db.db = db_lease.dbname.clone();
        config.db.sslcert.clear();
        config.db.sslkey.clear();
        config.db.sslrootcert.clear();
        config.db.api_keys_update_interval = options.api_keys_update_interval_secs.unwrap_or(3_600);
        config.db.events_flush_interval_sec = 60;
        config.cert.cert_file_path.clear();
        config.cert.key_file_path.clear();
        config.raft.snapshot_dir = temp_dir.path().join("snapshots").display().to_string();
        config.log.path = temp_dir.path().join("gateway.log").display().to_string();
        config.basic.update_gateway_info_ms = if options.include_gateway_info {
            50
        } else {
            60_000
        };
        if let Some(whitelist) = options.worker_whitelist {
            config.http.worker_whitelist = whitelist;
        }
        if let Some(cleanup_secs) = options.taskmanager_cleanup_interval_secs {
            config.basic.taskmanager_cleanup_interval = cleanup_secs;
        }
        if let Some(result_lifetime_secs) = options.taskmanager_result_lifetime_secs {
            config.basic.taskmanager_result_lifetime = result_lifetime_secs;
        }

        let generic_key = config.http.generic_key.expect("generic key");
        let admin_key = config.http.admin_key;
        let default_user_api_key = Uuid::new_v4().to_string();
        let default_company_api_key = Uuid::new_v4().to_string();
        let default_company_id = Uuid::new_v4();
        let hasher = ApiKeyHasher::new(&config.http.api_key_secret).expect("api key hasher");

        update_app_settings_gateway_runtime(
            &db_client,
            GatewayRuntimeAppSettingsUpdate {
                generic_key,
                global_limit: 10_000,
                per_ip_limit: 10_000,
                unauthorized_per_ip_limit: config.http.add_task_unauthorized_per_ip_daily_rate_limit
                    as i32,
                rate_limit_whitelist: config.http.rate_limit_whitelist.iter().cloned().collect(),
                max_task_queue_len: config.http.max_task_queue_len as i32,
                request_file_size_limit: config.http.request_file_size_limit as i64,
                guest_generation_limit: 1,
                guest_window_ms: 86_400_000,
                registered_generation_limit: 0,
                registered_window_ms: 86_400_000,
            },
        )
        .await
        .expect("seed app settings");
        insert_personal_api_key(
            &db_client,
            &hasher,
            PersonalKeySeed {
                api_key: &default_user_api_key,
                user_email: Some("test-user@example.com"),
                concurrent_limit: 1,
                daily_limit: 10,
                balance_cents: 0,
            },
        )
        .await
        .expect("seed personal api key");
        insert_company_api_key(
            &db_client,
            &hasher,
            CompanyKeySeed {
                api_key: &default_company_api_key,
                company_id: default_company_id,
                company_name: "Seed Company",
                concurrent_limit: 100,
                daily_limit: 1_000,
                balance_cents: 100_000,
            },
        )
        .await
        .expect("seed company api key");

        let config_file = tempfile::Builder::new()
            .prefix("gateway-real-test-")
            .suffix(".toml")
            .tempfile()
            .expect("temp config file");
        let config_path = config_file.path().to_path_buf();
        std::fs::write(
            &config_path,
            toml::to_string(&config).expect("serialize test config"),
        )
        .expect("write test config");

        let runtime_config = Arc::new(
            RuntimeConfigStore::new(config_path.clone(), config.clone())
                .await
                .expect("runtime config"),
        );
        let shutdown = CancellationToken::new();
        let gateway = gateway::raft::start_gateway(
            GatewayMode::Single,
            Arc::clone(&runtime_config),
            shutdown.clone(),
        )
        .await
        .expect("start gateway");

        let service = TestService::new(format!("http://127.0.0.1:{http_port}"));
        wait_for_http_ready(&service)
            .await
            .expect("wait for HTTP server");
        gateway
            .sync_db_caches_for_test()
            .await
            .expect("sync db caches");
        if options.include_gateway_info {
            wait_for_gateway_info(&service)
                .await
                .expect("wait for gateway info");
        }

        GatewayRuntimeStart {
            harness: Self {
                service,
                config: Arc::new(config),
                gateway,
                db_client,
                _db_connection_task: db_connection_task,
                _db_lease: db_lease,
                _config_file: config_file,
                _temp_dir: temp_dir,
                _shutdown: shutdown,
            },
            runtime_config,
            config_path,
            generic_key,
            admin_key,
            default_user_api_key,
            default_company_api_key,
            company_id: default_company_id,
        }
    }
}

impl Drop for GatewayRuntimeHarness {
    fn drop(&mut self) {
        self._db_connection_task.abort();
    }
}

fn test_db_parallelism() -> usize {
    std::env::var("GATEWAY_TEST_DB_PARALLELISM")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_MAX_PARALLEL_HARNESSES)
}

fn quote_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

async fn create_template_database(client: &Client, dbname: &str) -> Result<()> {
    let sql = format!("CREATE DATABASE {}", quote_ident(dbname));
    client
        .batch_execute(&sql)
        .await
        .with_context(|| format!("create template database {dbname}"))?;
    Ok(())
}

async fn wait_for_http_ready(service: &TestService) -> Result<()> {
    for _ in 0..READY_RETRIES {
        if let Ok(response) = service.client.get(service.resolve_url("/id")).send().await
            && response.status() == StatusCode::OK
        {
            return Ok(());
        }
        tokio::time::sleep(READY_SLEEP).await;
    }
    Err(anyhow!("gateway HTTP endpoint did not become ready"))
}

async fn wait_for_gateway_info(service: &TestService) -> Result<()> {
    for _ in 0..READY_RETRIES {
        if let Ok(payload) = service.get_json("/get_load").await
            && payload
                .get("gateways")
                .and_then(|value| value.as_array())
                .is_some_and(|gateways| !gateways.is_empty())
        {
            return Ok(());
        }
        tokio::time::sleep(READY_SLEEP).await;
    }
    Err(anyhow!("gateway info did not become ready"))
}

async fn connect_plain_postgres(
    host: &str,
    port: u16,
    user: &str,
    password: &str,
    dbname: &str,
) -> Result<(Client, JoinHandle<()>)> {
    let mut cfg = tokio_postgres::Config::new();
    cfg.host(host);
    cfg.port(port);
    cfg.user(user);
    cfg.password(password);
    cfg.dbname(dbname);
    cfg.ssl_mode(tokio_postgres::config::SslMode::Disable);
    let (client, connection) = cfg
        .connect(NoTls)
        .await
        .with_context(|| format!("connect to postgres at {host}:{port}"))?;
    let task = tokio::spawn(async move {
        if let Err(err) = connection.await {
            panic!("postgres connection error: {err}");
        }
    });
    Ok((client, task))
}

async fn apply_gateway_dev_schema(client: &Client) -> Result<()> {
    let schema_path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("dev-env/init-scripts/init-schema.sql");
    let sql = std::fs::read_to_string(&schema_path)
        .with_context(|| format!("read schema snapshot {}", schema_path.display()))?;
    client
        .batch_execute(&sql)
        .await
        .with_context(|| format!("apply schema snapshot {}", schema_path.display()))?;
    Ok(())
}

#[allow(dead_code)]
pub(crate) async fn update_app_settings_generic_key(
    client: &Client,
    generic_key: Uuid,
    global_limit: i32,
    per_ip_limit: i32,
) -> Result<()> {
    let now = current_time_ms();
    client
        .execute(
            "UPDATE app_settings SET gateway_generic_key = $1, gateway_generic_global_daily_limit = $2, gateway_generic_per_ip_daily_limit = $3, updated_at = $4 WHERE id = 1",
            &[&generic_key, &global_limit, &per_ip_limit, &now],
        )
        .await
        .context("update app settings")?;
    Ok(())
}

pub(crate) struct GatewayRuntimeAppSettingsUpdate {
    pub(crate) generic_key: Uuid,
    pub(crate) global_limit: i32,
    pub(crate) per_ip_limit: i32,
    pub(crate) unauthorized_per_ip_limit: i32,
    pub(crate) rate_limit_whitelist: Vec<String>,
    pub(crate) max_task_queue_len: i32,
    pub(crate) request_file_size_limit: i64,
    pub(crate) guest_generation_limit: i32,
    pub(crate) guest_window_ms: i64,
    pub(crate) registered_generation_limit: i32,
    pub(crate) registered_window_ms: i64,
}

pub(crate) async fn update_app_settings_gateway_runtime(
    client: &Client,
    update: GatewayRuntimeAppSettingsUpdate,
) -> Result<()> {
    let now = current_time_ms();
    client
        .execute(
            "UPDATE app_settings SET gateway_generic_key = $1, gateway_generic_global_daily_limit = $2, gateway_generic_per_ip_daily_limit = $3, add_task_unauthorized_per_ip_daily_rate_limit = $4, rate_limit_whitelist = $5, max_task_queue_len = $6, request_file_size_limit = $7, guest_generation_limit = $8, guest_window_ms = $9, registered_generation_limit = $10, registered_window_ms = $11, updated_at = $12 WHERE id = 1",
            &[&update.generic_key, &update.global_limit, &update.per_ip_limit, &update.unauthorized_per_ip_limit, &update.rate_limit_whitelist, &update.max_task_queue_len, &update.request_file_size_limit, &update.guest_generation_limit, &update.guest_window_ms, &update.registered_generation_limit, &update.registered_window_ms, &now],
        )
        .await
        .context("update gateway runtime app settings")?;
    Ok(())
}

struct SeededApiKeyInsert<'a> {
    account_id: i64,
    key_scope: &'static str,
    user_id: Option<i64>,
    company_id: Option<Uuid>,
    name: &'a str,
    api_key_hash: &'a [u8],
    api_key_partial: &'a str,
    created_by_user_id: Option<i64>,
    now: i64,
    context: &'a str,
}

async fn insert_seeded_account(
    client: &Client,
    owner_kind: &'static str,
    user_id: Option<i64>,
    company_id: Option<Uuid>,
    now: i64,
    context: &str,
) -> Result<i64> {
    let account_row = client
        .query_one(
            "INSERT INTO accounts (owner_kind, user_id, company_id, balance_cents, currency, created_at, updated_at)
             VALUES ($1, $2, $3, $4, 'USD', $5, $5)
             RETURNING id",
            &[&owner_kind, &user_id, &company_id, &0_i64, &now],
        )
        .await
        .with_context(|| context.to_string())?;
    Ok(account_row.get("id"))
}

async fn insert_seeded_api_key(client: &Client, insert: SeededApiKeyInsert<'_>) -> Result<()> {
    client
        .execute(
            "INSERT INTO api_keys (
                account_id, key_scope, user_id, company_id, name, api_key_hash, api_key_encrypted,
                api_key_partial, is_primary, created_by_user_id, created_at, updated_at, last_used_at, revoked_at
             ) VALUES (
                $1, $2, $3, $4, $5, $6, NULL, $7, FALSE, $8, $9, $9, NULL, NULL
             )",
            &[
                &insert.account_id,
                &insert.key_scope,
                &insert.user_id,
                &insert.company_id,
                &insert.name,
                &insert.api_key_hash,
                &insert.api_key_partial,
                &insert.created_by_user_id,
                &insert.now,
            ],
        )
        .await
        .with_context(|| insert.context.to_string())?;
    Ok(())
}

pub(crate) async fn insert_personal_api_key(
    client: &Client,
    hasher: &ApiKeyHasher,
    seed: PersonalKeySeed<'_>,
) -> Result<()> {
    let now = current_time_ms();
    let api_key_hash = hasher.compute_hash_array(seed.api_key);
    let user_email = seed
        .user_email
        .map(str::to_string)
        .unwrap_or_else(|| format!("user-{}@example.com", Uuid::new_v4()));
    let user_row = client
        .query_one(
            "INSERT INTO users (email, display_name, task_limit_concurrent, task_limit_daily, created_at, updated_at, last_login_at)
             VALUES ($1, $2, $3, $4, $5, $5, $5)
             RETURNING id",
            &[
                &user_email,
                &"Test User",
                &seed.concurrent_limit,
                &seed.daily_limit,
                &now,
            ],
        )
        .await
        .context("insert user")?;
    let user_id: i64 = user_row.get("id");

    let account_id = insert_seeded_account(
        client,
        "user",
        Some(user_id),
        None,
        now,
        "insert user account",
    )
    .await?;
    if seed.balance_cents > 0 {
        apply_account_topup(client, account_id, seed.balance_cents, now).await?;
    }

    let api_key_partial = api_key_partial(seed.api_key);
    insert_seeded_api_key(
        client,
        SeededApiKeyInsert {
            account_id,
            key_scope: "personal",
            user_id: Some(user_id),
            company_id: None,
            name: "gateway-test-user-key",
            api_key_hash: &api_key_hash[..],
            api_key_partial: &api_key_partial,
            created_by_user_id: Some(user_id),
            now,
            context: "insert personal api key",
        },
    )
    .await?;
    Ok(())
}

pub(crate) async fn insert_company_api_key(
    client: &Client,
    hasher: &ApiKeyHasher,
    seed: CompanyKeySeed<'_>,
) -> Result<()> {
    let now = current_time_ms();
    let api_key_hash = hasher.compute_hash_array(seed.api_key);
    client
        .execute(
            "INSERT INTO companies (id, name, owner_email, vat, task_limit_concurrent, task_limit_daily, ownership_state, created_by_user_id, created_at, updated_at)
             VALUES ($1, $2, NULL, NULL, $3, $4, 'owned', NULL, $5, $5)",
            &[
                &seed.company_id,
                &seed.company_name,
                &seed.concurrent_limit,
                &seed.daily_limit,
                &now,
            ],
        )
        .await
        .context("insert company")?;

    let account_id = insert_seeded_account(
        client,
        "company",
        None,
        Some(seed.company_id),
        now,
        "insert company account",
    )
    .await?;
    if seed.balance_cents > 0 {
        apply_account_topup(client, account_id, seed.balance_cents, now).await?;
    }

    let api_key_partial = api_key_partial(seed.api_key);
    insert_seeded_api_key(
        client,
        SeededApiKeyInsert {
            account_id,
            key_scope: "company",
            user_id: None,
            company_id: Some(seed.company_id),
            name: "gateway-test-company-key",
            api_key_hash: &api_key_hash[..],
            api_key_partial: &api_key_partial,
            created_by_user_id: None,
            now,
            context: "insert company api key",
        },
    )
    .await?;
    Ok(())
}

async fn apply_account_topup(
    client: &Client,
    account_id: i64,
    amount_cents: i64,
    now: i64,
) -> Result<()> {
    let request_id = format!("gateway-test-topup-{account_id}-{now}");
    client
        .query(
            "SELECT ledger_id FROM gen_apply_account_topup($1, $2, NULL, $3, $4, $5)",
            &[
                &account_id,
                &amount_cents,
                &request_id,
                &"gateway integration seed",
                &now,
            ],
        )
        .await
        .context("apply seeded account topup")?;
    Ok(())
}

fn reserve_tcp_port() -> Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0").context("bind TCP listener")?;
    Ok(listener.local_addr().context("get TCP addr")?.port())
}

fn reserve_udp_port() -> Result<u16> {
    let socket = UdpSocket::bind("127.0.0.1:0").context("bind UDP socket")?;
    Ok(socket.local_addr().context("get UDP addr")?.port())
}

fn current_time_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_millis() as i64
}

fn api_key_partial(api_key: &str) -> String {
    api_key.chars().take(8).collect()
}
