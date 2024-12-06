// TODO: Remove this after the code is finalized
#![allow(dead_code)]

use anyhow::anyhow;
use anyhow::Result;
use serde::Deserialize;
use serde::Deserializer;
use std::str::FromStr;
use std::{fmt, path::Path};
use tracing::Level;

#[derive(Debug, Deserialize)]
pub struct NodeConfig {
    pub ip: String,
    pub port: usize,
    pub nodes: Vec<String>,
    pub name: String,
    pub log_path: String,
    pub log_level: LogLevel,
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
            "Running a node '{}' at {}:{}",
            self.name, self.ip, self.port
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
