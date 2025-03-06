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
    pub update_gateway_info_ms: u64,
    pub unique_validators_per_task: usize,
}

#[derive(Debug, Deserialize)]
pub struct NetworkConfig {
    pub ip: String,
    pub domain: String,
    pub server_port: u16,
    pub node_id: u64,
    pub node_endpoints: Vec<String>,
    pub node_ids: Vec<u64>,
    pub node_dns_names: Vec<String>,
    pub name: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ProtocolConfig {
    pub max_message_size: usize,
    pub send_timeout_ms: u64,
    pub receive_message_timeout_ms: u64,
}

impl Default for ProtocolConfig {
    fn default() -> Self {
        Self {
            max_message_size: 64 * 1024,
            send_timeout_ms: 300,
            receive_message_timeout_ms: 2000,
        }
    }
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

#[derive(Debug, Clone, Deserialize)]
pub struct HTTPConfig {
    pub compression: bool,
    pub compression_lvl: u32,
    pub port: u16,
    // Per minute rate limits
    pub load_rate_limit: usize,
    pub add_task_rate_limit: usize,
    pub leader_rate_limit: usize,
    // Size limit for the request
    pub request_size_limit: u64,
    pub bittensor_wss: String,
    pub signature_freshness_threshold: u64,
    // Subnet state updater settings
    pub subnet_number: u16,
    pub subnet_poll_interval_sec: u64,
}

#[derive(Debug, Deserialize)]
pub struct Certificate {
    pub dangerous_skip_verification: bool,
    pub cert_file_path: String,
    pub key_file_path: String,
}

#[derive(Debug, Deserialize)]
pub struct NodeConfig {
    pub basic: BasicConfig,
    pub network: NetworkConfig,
    pub protocol: ProtocolConfig,
    pub http: HTTPConfig,
    pub cert: Certificate,
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

fn mask_cluster_name(cluster_name: &str) -> String {
    let char_count = cluster_name.chars().count();
    if char_count < 3 {
        "*".to_owned()
    } else {
        let visible: String = cluster_name.chars().take(3).collect();
        let mask: String = "*".repeat(char_count - 3);
        format!("{}{}", visible, mask)
    }
}

impl fmt::Display for NodeConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Node Configuration:")?;
        writeln!(f, "  [Basic]")?;
        writeln!(
            f,
            "    Max Restart Attempts: {}",
            self.basic.max_restart_attempts
        )?;

        writeln!(f, "\n  [Network]")?;
        writeln!(f, "    Name            : {}", self.network.name)?;
        writeln!(f, "    IP              : {}", self.network.ip)?;
        writeln!(f, "    Domain          : {}", self.network.domain)?;
        writeln!(f, "    Server Port     : {}", self.network.server_port)?;
        writeln!(f, "    Node ID         : {}", self.network.node_id)?;
        writeln!(f, "    Node Endpoints  :")?;
        for endpoint in &self.network.node_endpoints {
            writeln!(f, "      - {}", endpoint)?;
        }
        writeln!(f, "    Node IDs        :")?;
        for nid in &self.network.node_ids {
            writeln!(f, "      - {}", nid)?;
        }
        writeln!(f, "    Node DNS Names  :")?;
        for dns in &self.network.node_dns_names {
            writeln!(f, "      - {}", dns)?;
        }

        writeln!(f, "\n  [Protocol]")?;
        writeln!(
            f,
            "    Max Message Size: {} bytes",
            self.protocol.max_message_size
        )?;

        writeln!(f, "\n  [HTTP]")?;
        writeln!(f, "    Port               : {}", self.http.port)?;
        writeln!(
            f,
            "    Compression        : {}",
            if self.http.compression {
                "enabled"
            } else {
                "disabled"
            }
        )?;
        writeln!(f, "    Compression Level  : {}", self.http.compression_lvl)?;
        writeln!(
            f,
            "    Load Rate Limit    : {} per minute",
            self.http.load_rate_limit
        )?;
        writeln!(
            f,
            "    Add Task Rate Limit: {} per minute",
            self.http.add_task_rate_limit
        )?;
        writeln!(
            f,
            "    Leader Rate Limit  : {} per minute",
            self.http.leader_rate_limit
        )?;
        writeln!(
            f,
            "    Request Size Limit : {} bytes",
            self.http.request_size_limit
        )?;

        writeln!(f, "\n  [Certificate]")?;
        writeln!(
            f,
            "    Dangerous Skip Verification: {}",
            self.cert.dangerous_skip_verification
        )?;
        writeln!(
            f,
            "    Cert File Path             : {}",
            self.cert.cert_file_path
        )?;
        writeln!(
            f,
            "    Key File Path              : {}",
            self.cert.key_file_path
        )?;

        writeln!(f, "\n  [Log]")?;
        writeln!(f, "    Path : {}", self.log.path)?;
        writeln!(f, "    Level: {}", self.log.level)?;

        writeln!(f, "\n  [Raft]")?;
        writeln!(
            f,
            "    Cluster Name                 : {}",
            mask_cluster_name(&self.raft.cluster_name)
        )?;
        writeln!(
            f,
            "    Election Timeout [Min, Max]  : {} ms, {} ms",
            self.raft.election_timeout_min, self.raft.election_timeout_max
        )?;
        writeln!(
            f,
            "    Heartbeat Interval           : {} ms",
            self.raft.heartbeat_interval
        )?;
        writeln!(
            f,
            "    Max Payload Entries          : {}",
            self.raft.max_payload_entries
        )?;
        writeln!(
            f,
            "    Replication Lag Threshold    : {} ms",
            self.raft.replication_lag_threshold
        )?;
        writeln!(
            f,
            "    Snapshot Max Chunk Size      : {} bytes",
            self.raft.snapshot_max_chunk_size
        )?;
        writeln!(
            f,
            "    Max In-Snapshot Log To Keep  : {}",
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
