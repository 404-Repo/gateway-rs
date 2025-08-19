use anyhow::anyhow;
use anyhow::Result;
use serde::{Deserialize, Deserializer, Serialize};
use std::str::FromStr;
use std::{fmt, path::Path};
use tracing::Level;
use uuid::Uuid;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BasicConfig {
    pub max_restart_attempts: usize,
    pub update_gateway_info_ms: u64,
    pub unique_validators_per_task: usize,
    pub taskmanager_initial_capacity: usize,
    pub taskmanager_cleanup_interval: u64,
    pub taskmanager_result_lifetime: u64,
    pub taskqueue_cleanup_interval: u64,
    pub taskqueue_task_ttl: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct NetworkConfig {
    pub bind_ip: String,
    // This IP will be used in the internal state
    pub external_ip: String,
    pub domain: String,
    pub server_port: u16,
    pub node_id: u64,
    pub node_dns_names: Vec<String>,
    pub node_id_discovery_sleep: u64,
    pub node_id_discovery_retries: usize,
    pub name: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RServerConfig {
    pub max_message_size: usize,
    pub receive_message_timeout_ms: u64,
    pub max_idle_timeout_sec: u64,
    pub keep_alive_interval_sec: u64,
}

impl Default for RServerConfig {
    fn default() -> Self {
        Self {
            max_message_size: 64 * 1024,
            receive_message_timeout_ms: 2000,
            max_idle_timeout_sec: 4,
            keep_alive_interval_sec: 1,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RClientConfig {
    pub max_idle_timeout_sec: u64,
    pub keep_alive_interval_sec: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LogConfig {
    pub path: String,
    pub level: LogLevel,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RaftConfig {
    pub cluster_name: String,
    pub election_timeout_min: u64,
    pub election_timeout_max: u64,
    pub heartbeat_interval: u64,
    pub max_payload_entries: u64,
    pub replication_lag_threshold: u64,
    pub snapshot_logs_since_last: u64,
    pub snapshot_max_chunk_size: u64,
    pub max_in_snapshot_log_to_keep: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct HTTPConfig {
    pub compression: bool,
    pub compression_lvl: u32,
    pub port: u16,
    // Rate limits
    pub basic_rate_limit: usize,
    pub write_rate_limit: usize,
    pub update_key_rate_limit: usize,
    pub add_task_generic_global_hourly_rate_limit: usize,
    pub add_task_generic_per_ip_hourly_rate_limit: usize,
    pub add_task_user_id_global_hourly_rate_limit: usize,
    pub add_task_user_id_per_user_hourly_rate_limit: usize,
    pub add_task_basic_per_ip_rate_limit: usize,
    pub load_rate_limit: usize,
    pub add_result_rate_limit: usize,
    pub leader_rate_limit: usize,
    pub metric_rate_limit: usize,
    pub get_status_rate_limit: usize,
    // Size limit for the request
    pub request_size_limit: u64,
    pub request_file_size_limit: u64,
    pub raft_write_size_limit: u64,
    pub wss_bittensor: String,
    pub wss_max_message_size: usize,
    pub signature_freshness_threshold: u64,
    // Subnet state updater settings
    pub subnet_number: u16,
    pub subnet_poll_interval_sec: u64,
    pub max_task_queue_len: usize,
    pub admin_key: Uuid,
    pub generic_key: Option<Uuid>,
    // HTTP/3 client timeouts
    pub post_timeout_sec: u64,
    pub forward_timeout_sec: u64,
    pub get_timeout_sec: u64,
    pub max_idle_timeout_sec: u64,
    pub keep_alive_interval_sec: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PromptConfig {
    pub min_len: usize,
    pub max_len: usize,
    pub allowed_pattern: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DbConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub db: String,
    pub sslcert: String,
    pub sslkey: String,
    pub sslrootcert: String,
    pub api_keys_update_interval: u64,
    pub keys_cache_ttl_sec: u64,
    pub keys_cache_max_capacity: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Certificate {
    pub dangerous_skip_verification: bool,
    pub cert_file_path: String,
    pub key_file_path: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct NodeConfig {
    pub basic: BasicConfig,
    pub network: NetworkConfig,
    pub rserver: RServerConfig,
    pub rclient: RClientConfig,
    pub http: HTTPConfig,
    pub prompt: PromptConfig,
    pub db: DbConfig,
    pub cert: Certificate,
    pub log: LogConfig,
    pub raft: RaftConfig,
}

#[derive(Debug, Clone, Serialize)]
pub enum LogLevel {
    /// Designates very low priority, often extremely verbose, information.
    Trace = 0,
    /// Designates lower priority information.
    Debug = 1,
    /// Designates useful information.
    Info = 2,
    /// Designates hazardous situations.
    Warn = 3,
    /// Designates very serious errors.
    Error = 4,
}

impl fmt::Display for LogLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let level_str = match self {
            LogLevel::Trace => "trace",
            LogLevel::Debug => "debug",
            LogLevel::Info => "info",
            LogLevel::Warn => "warn",
            LogLevel::Error => "error",
        };
        write!(f, "{}", level_str)
    }
}

impl From<&LogLevel> for Level {
    fn from(log_level: &LogLevel) -> Self {
        match log_level {
            LogLevel::Trace => Level::TRACE,
            LogLevel::Debug => Level::DEBUG,
            LogLevel::Info => Level::INFO,
            LogLevel::Warn => Level::WARN,
            LogLevel::Error => Level::ERROR,
        }
    }
}

impl<'de> Deserialize<'de> for LogLevel {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        LogLevel::from_str(&s.to_lowercase()).map_err(serde::de::Error::custom)
    }
}

impl FromStr for LogLevel {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "trace" => Ok(LogLevel::Trace),
            "debug" => Ok(LogLevel::Debug),
            "info" => Ok(LogLevel::Info),
            "warn" | "warning" => Ok(LogLevel::Warn),
            "error" => Ok(LogLevel::Error),
            _ => Err(format!("Invalid log level: {}", s)),
        }
    }
}

fn mask_string(s: &str, visible: usize) -> String {
    let char_count = s.chars().count();
    if char_count > visible {
        let visible_part: String = s.chars().take(visible).collect();
        let mask: String = "*".repeat(char_count - visible);
        format!("{}{}", visible_part, mask)
    } else {
        "*".repeat(char_count)
    }
}

fn mask_key_in_toml(toml_str: &mut String, key: &str, visible: usize) {
    if let Some(start) = toml_str.find(key) {
        let masked_key = mask_string(key, visible);
        toml_str.replace_range(start..start + key.len(), &masked_key);
    }
}

impl fmt::Display for NodeConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut masked = self.clone();
        masked.raft.cluster_name = mask_string(&masked.raft.cluster_name, 3);

        let mut toml_str = toml::to_string_pretty(&masked).map_err(|_| fmt::Error)?;

        let admin_key_str = self.http.admin_key.to_string();
        mask_key_in_toml(&mut toml_str, &admin_key_str, 6);

        if let Some(key) = self.http.generic_key {
            let generic_key_str = key.to_string();
            mask_key_in_toml(&mut toml_str, &generic_key_str, 6);
        }

        write!(f, "{}", toml_str)
    }
}

pub async fn read_config(path: Option<&String>) -> Result<NodeConfig> {
    if let Some(provided_path) = path {
        let provided_path = Path::new(&provided_path);
        if provided_path.exists() {
            return read_config_from_file(provided_path).await;
        } else {
            return Err(anyhow!("Provided configuration file path does not exist"));
        }
    }

    let toml_path = Path::new("config.toml");
    let json_path = Path::new("config.json");

    if toml_path.exists() {
        read_config_from_file(toml_path).await
    } else if json_path.exists() {
        read_config_from_file(json_path).await
    } else {
        Err(anyhow!("No configuration file found"))
    }
}

async fn read_config_from_file<P: AsRef<Path>>(path: P) -> Result<NodeConfig> {
    let contents = tokio::fs::read_to_string(&path).await?;

    match path.as_ref().extension().and_then(|ext| ext.to_str()) {
        Some("toml") => {
            let config: NodeConfig = toml::from_str(&contents)?;
            Ok(config)
        }
        Some("json") => {
            let config: NodeConfig = serde_json::from_str(&contents)?;
            Ok(config)
        }
        _ => Err(anyhow!("Unsupported file format")),
    }
}
