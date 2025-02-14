use anyhow::anyhow;
use anyhow::Result;
use serde::Deserialize;
use serde::Deserializer;
use std::str::FromStr;
use std::{fmt, path::Path};
use tracing::Level;

#[derive(Debug, Deserialize)]
pub struct BasicConfig {
    pub max_restart_attempts: usize,
}

#[derive(Debug, Deserialize)]
pub struct NetworkConfig {
    pub ip: String,
    pub server_port: usize,
    pub node_id: u64,
    pub node_endpoints: Vec<String>,
    pub node_ids: Vec<u64>,
    pub node_dns_names: Vec<String>,
    pub name: String,
}

#[derive(Debug, Deserialize)]
pub struct LogConfig {
    pub path: String,
    pub level: LogLevel,
}

#[derive(Debug, Deserialize)]
pub struct RaftConfig {
    pub cluster_name: String,
    pub election_timeout_min: u64,
    pub election_timeout_max: u64,
    pub heartbeat_interval: u64,
    pub max_payload_entries: u64,
    pub replication_lag_threshold: u64,
    pub snapshot_max_chunk_size: u64,
    pub max_in_snapshot_log_to_keep: u64,
}

#[derive(Debug, Deserialize)]
pub struct NodeConfig {
    pub basic: BasicConfig,
    pub network: NetworkConfig,
    pub log: LogConfig,
    pub raft: RaftConfig,
}

#[derive(Debug)]
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

impl fmt::Display for NodeConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "NodeConfig:\n\
            Basic:\n\
            - Max Restart Attempts: {}\n\
            Network:\n\
            - Name: {}\n\
            - IP: {}\n\
            - Server Port: {}\n\
            - Node ID: {}\n\
            - Node Endpoints: {:?}\n\
            - Node DNS Names: {:?}\n\
            Log:\n\
            - Path: {}\n\
            - Level: {:?}\n\
            Raft:\n\
            - Cluster Name: {}\n\
            - Election Timeout Min: {}\n\
            - Election Timeout Max: {}\n\
            - Heartbeat Interval: {}\n\
            - Max Payload Entries: {}\n\
            - Replication Lag Threshold: {}\n\
            - Snapshot Max Chunk Size: {}\n\
            - Max In Snapshot Log To Keep: {}",
            self.basic.max_restart_attempts,
            self.network.name,
            self.network.ip,
            self.network.server_port,
            self.network.node_id,
            self.network.node_endpoints,
            self.network.node_dns_names,
            self.log.path,
            self.log.level,
            self.raft.cluster_name,
            self.raft.election_timeout_min,
            self.raft.election_timeout_max,
            self.raft.heartbeat_interval,
            self.raft.max_payload_entries,
            self.raft.replication_lag_threshold,
            self.raft.snapshot_max_chunk_size,
            self.raft.max_in_snapshot_log_to_keep
        )
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
