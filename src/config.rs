use anyhow::{Result, anyhow};
use foldhash::HashSet;
use serde::{Deserialize, Deserializer, Serialize};
use std::str::FromStr;
use std::{fmt, path::Path, path::PathBuf};
use tracing::Level;
use uuid::Uuid;

use crate::config_env::{
    ALLOW_DANGEROUS_SKIP_VERIFICATION_ENV, dangerous_skip_verification_allowed,
    parse_config_from_contents,
};
pub use crate::config_model::{ModelConfig, ModelOutput, ModelResolveError};
use crate::crypto::hotkey::Hotkey;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BasicConfig {
    pub max_restart_attempts: usize,
    pub update_gateway_info_ms: u64,
    #[serde(default = "default_max_rate_limit_deltas_per_batch")]
    pub max_rate_limit_deltas_per_batch: usize,
    #[serde(alias = "unique_validators_per_task")]
    pub unique_workers_per_task: usize,
    pub taskmanager_initial_capacity: usize,
    pub taskmanager_cleanup_interval: u64,
    pub taskmanager_result_lifetime: u64,
    pub taskqueue_cleanup_interval: u64,
    pub taskqueue_task_ttl: u64,
    #[serde(default = "default_generation_task_retention_sec")]
    pub generation_task_retention_sec: u64,
}

fn default_generation_task_retention_sec() -> u64 {
    86_400
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
    /// Additional IPs or hostnames whose resolved IPs are added to cluster_ips.
    /// Use this to whitelist Cloud NAT egress IPs so that cluster_check passes
    /// for cross-region Raft traffic without affecting Raft peer connection targets.
    /// These are NOT used for Raft peer connections — only for the cluster_ips whitelist.
    #[serde(default)]
    pub cluster_peer_egress_ips: Vec<String>,
    pub node_id_discovery_sleep: u64,
    pub node_id_discovery_retries: usize,
    pub name: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RServerConfig {
    pub max_message_size: usize,
    pub max_recv_buffer_size: usize,
    pub receive_message_timeout_ms: u64,
    pub max_idle_timeout_sec: u64,
    pub keep_alive_interval_sec: u64,
}

impl Default for RServerConfig {
    fn default() -> Self {
        Self {
            max_message_size: 256 * 1024,
            max_recv_buffer_size: 8 * 1024,
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
    #[serde(default = "default_snapshot_dir")]
    pub snapshot_dir: String,
    #[serde(default = "default_max_snapshots_to_keep")]
    pub max_snapshots_to_keep: usize,
    #[serde(default = "default_compaction_threshold_bytes")]
    pub compaction_threshold_bytes: u64,
    #[serde(default = "default_compaction_ops")]
    pub compaction_ops: u64,
    #[serde(default = "default_log_store_flush_interval_ms")]
    pub log_store_flush_interval_ms: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct HTTPConfig {
    pub compression: bool,
    pub compression_lvl: u32,
    pub port: u16,
    #[serde(default)]
    pub transport: TransportMode,
    #[serde(default = "default_tls_versions")]
    pub tls_versions: Vec<String>,
    #[serde(default)]
    pub allowed_origins: HashSet<String>,
    #[serde(default = "default_max_concurrent_image_uploads")]
    pub max_concurrent_image_uploads: usize,
    // Rate limits
    pub basic_rate_limit: usize,
    pub add_task_unauthorized_per_ip_daily_rate_limit: usize,
    pub rate_limit_whitelist: HashSet<String>,
    #[serde(default = "default_distributed_rate_limiter_max_capacity")]
    pub distributed_rate_limiter_max_capacity: usize,
    pub worker_per_minute_rate_limit: usize,
    pub get_status_rate_limit: usize,
    #[serde(default)]
    pub worker_whitelist: HashSet<Hotkey>,
    pub add_task_size_limit: u64,
    pub request_size_limit: u64,
    pub request_file_size_limit: u64,
    pub raft_write_size_limit: u64,
    pub signature_freshness_threshold: u64,
    pub max_task_queue_len: usize,
    pub admin_key: Uuid,
    pub generic_key: Option<Uuid>,
    #[serde(default = "default_generic_key_concurrent_limit")]
    pub generic_key_concurrent_limit: usize,
    pub api_key_secret: String,
    #[serde(default = "default_invalid_api_key_negative_cache_ttl_sec")]
    pub invalid_api_key_negative_cache_ttl_sec: u64,
    #[serde(default = "default_invalid_api_key_ip_miss_ttl_sec")]
    pub invalid_api_key_ip_miss_ttl_sec: u64,
    #[serde(default = "default_invalid_api_key_ip_cooldown_ttl_sec")]
    pub invalid_api_key_ip_cooldown_ttl_sec: u64,
    #[serde(default = "default_invalid_api_key_ip_cache_capacity")]
    pub invalid_api_key_ip_cache_capacity: u64,
    #[serde(default = "default_invalid_api_key_ip_miss_limit")]
    pub invalid_api_key_ip_miss_limit: u64,
    pub post_timeout_sec: u64,
    pub forward_timeout_sec: u64,
    pub get_timeout_sec: u64,
    pub max_idle_timeout_sec: u64,
    pub keep_alive_interval_sec: u64,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TransportMode {
    #[default]
    Tls,
    Plain,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PromptConfig {
    pub min_len: usize,
    pub max_len: usize,
    pub allowed_pattern: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ModelParamsConfig {
    #[serde(default = "default_model_params_max_len")]
    pub max_len: usize,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ImageConfig {
    pub max_width: u32,
    pub max_height: u32,
    pub max_size_bytes: usize,
    pub allowed_formats: foldhash::HashSet<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DbConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub db: String,
    #[serde(default)]
    pub transport: TransportMode,
    pub sslcert: String,
    pub sslkey: String,
    pub sslrootcert: String,
    pub api_keys_update_interval: u64,
    pub keys_cache_ttl_sec: u64,
    pub keys_cache_initial_capacity: usize,
    pub keys_cache_max_capacity: u64,
    #[serde(default = "default_deleted_keys_ttl_minutes")]
    pub deleted_keys_ttl_minutes: u64,
    #[serde(default = "default_events_flush_interval_sec")]
    pub events_flush_interval_sec: u64,
    #[serde(default = "default_events_copy_batch_size")]
    pub events_copy_batch_size: usize,
    #[serde(default = "default_db_pool_size")]
    pub pool_size: usize,
    #[serde(default = "default_events_queue_capacity")]
    pub events_queue_capacity: usize,
}

fn default_deleted_keys_ttl_minutes() -> u64 {
    60
}
fn default_events_flush_interval_sec() -> u64 {
    5
}
fn default_events_copy_batch_size() -> usize {
    1000
}
fn default_db_pool_size() -> usize {
    4
}
fn default_events_queue_capacity() -> usize {
    50_000
}
fn default_max_concurrent_image_uploads() -> usize {
    1024
}
fn default_distributed_rate_limiter_max_capacity() -> usize {
    4096
}
fn default_generic_key_concurrent_limit() -> usize {
    2
}
fn default_invalid_api_key_negative_cache_ttl_sec() -> u64 {
    5 * 60
}
fn default_invalid_api_key_ip_miss_ttl_sec() -> u64 {
    10 * 60
}
fn default_invalid_api_key_ip_cooldown_ttl_sec() -> u64 {
    5 * 60
}
fn default_invalid_api_key_ip_cache_capacity() -> u64 {
    200_000
}
fn default_invalid_api_key_ip_miss_limit() -> u64 {
    50
}
fn default_max_rate_limit_deltas_per_batch() -> usize {
    16_384
}
fn default_model_params_max_len() -> usize {
    1024
}
fn default_model_params_config() -> ModelParamsConfig {
    ModelParamsConfig {
        max_len: default_model_params_max_len(),
    }
}
fn default_snapshot_dir() -> String {
    "data/snapshots".to_string()
}
fn default_max_snapshots_to_keep() -> usize {
    5
}
fn default_compaction_threshold_bytes() -> u64 {
    4 * 1024 * 1024
}
fn default_compaction_ops() -> u64 {
    4096
}
fn default_log_store_flush_interval_ms() -> u64 {
    200
}
fn default_tls_versions() -> Vec<String> {
    vec!["1.2".to_string(), "1.3".to_string()]
}

fn validate_loaded_config(config: NodeConfig) -> Result<NodeConfig> {
    config
        .model_config
        .validate()
        .map_err(|e| anyhow!("Invalid model configuration: {}", e))?;
    validate_node_config(&config)?;
    Ok(config)
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Certificate {
    pub dangerous_skip_verification: bool,
    pub cert_file_path: String,
    pub key_file_path: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct NodeConfig {
    pub model_config: ModelConfig,
    pub basic: BasicConfig,
    pub network: NetworkConfig,
    pub rserver: RServerConfig,
    pub rclient: RClientConfig,
    pub http: HTTPConfig,
    pub prompt: PromptConfig,
    #[serde(default = "default_model_params_config")]
    pub model_params: ModelParamsConfig,
    pub image: ImageConfig,
    pub db: DbConfig,
    pub cert: Certificate,
    pub log: LogConfig,
    pub raft: RaftConfig,
}

#[derive(Debug, Clone, Serialize)]
pub enum LogLevel {
    Trace = 0,
    Debug = 1,
    Info = 2,
    Warn = 3,
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
    if char_count <= visible {
        return "*".repeat(char_count);
    }
    let mut result = String::with_capacity(s.len());
    result.extend(s.chars().take(visible));
    result.extend(std::iter::repeat_n('*', char_count - visible));
    result
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
        mask_key_in_toml(&mut toml_str, &self.http.api_key_secret, 3);
        mask_key_in_toml(&mut toml_str, &self.db.password, 0);
        write!(f, "{}", toml_str)
    }
}

pub fn validate_node_config(config: &NodeConfig) -> Result<()> {
    if config.http.transport == TransportMode::Plain {
        tracing::warn!(
            "HTTP transport is configured without TLS; only use this in local development"
        );
    }
    if config.db.transport == TransportMode::Plain {
        tracing::warn!(
            "Database transport is configured without TLS; only use this in local development"
        );
    }
    if config.cert.dangerous_skip_verification {
        let allow_override = dangerous_skip_verification_allowed();
        if !cfg!(debug_assertions) && !allow_override {
            return Err(anyhow!(
                "cert.dangerous_skip_verification can only be enabled in debug/test builds or when {}=1",
                ALLOW_DANGEROUS_SKIP_VERIFICATION_ENV
            ));
        }
        tracing::warn!("TLS server verification is disabled; only use this in local development");
    }
    Ok(())
}

pub async fn read_config(path: Option<&String>) -> Result<NodeConfig> {
    let path = resolve_config_path(path)?;
    read_config_from_path(&path).await
}

pub async fn read_config_from_path<P: AsRef<Path>>(path: P) -> Result<NodeConfig> {
    let config = read_config_from_file(path.as_ref()).await?;
    validate_loaded_config(config)
}

async fn read_config_from_file(path: &Path) -> Result<NodeConfig> {
    let contents = tokio::fs::read_to_string(&path).await?;
    parse_config_from_contents(path, &contents)
}

pub fn resolve_config_path(path: Option<&String>) -> Result<PathBuf> {
    if let Some(provided_path) = path {
        let provided_path = Path::new(&provided_path);
        if provided_path.exists() {
            return Ok(provided_path.to_path_buf());
        } else {
            return Err(anyhow!("Provided configuration file path does not exist"));
        }
    }
    let toml_path = Path::new("config.toml");
    let json_path = Path::new("config.json");
    if toml_path.exists() {
        Ok(toml_path.to_path_buf())
    } else if json_path.exists() {
        Ok(json_path.to_path_buf())
    } else {
        Err(anyhow!("No configuration file found"))
    }
}

#[cfg(test)]
mod tests {
    use super::NodeConfig;

    fn read_config_single() -> String {
        let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("dev-env/config/config-single.toml");
        std::fs::read_to_string(path).expect("read config-single.toml")
    }

    #[test]
    fn http_config_defaults_generic_key_concurrent_limit_when_missing() {
        let config_text = read_config_single().replace("generic_key_concurrent_limit = 2\n", "");
        let config: NodeConfig =
            toml::from_str(&config_text).expect("parse config without override");
        assert_eq!(config.http.generic_key_concurrent_limit, 2);
    }

    #[test]
    fn http_config_reads_generic_key_concurrent_limit_override() {
        let config_text = read_config_single().replacen(
            "generic_key_concurrent_limit = 2",
            "generic_key_concurrent_limit = 3",
            1,
        );
        let config: NodeConfig = toml::from_str(&config_text).expect("parse config with override");
        assert_eq!(config.http.generic_key_concurrent_limit, 3);
    }

    #[test]
    fn network_config_defaults_cluster_peer_egress_ips_when_missing() {
        let config_text = read_config_single();
        let config: NodeConfig = toml::from_str(&config_text).expect("parse config");
        assert!(config.network.cluster_peer_egress_ips.is_empty());
    }
}
