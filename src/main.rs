use std::{env, sync::Arc};

use clap::{Arg, Command};
use common::init_tracing;
use config::{read_config, NodeConfig};

mod common;
mod config;
mod protocol;
mod raft;

use crate::common::log_app_config;
use crate::common::log_build_information;

#[tokio::main]
async fn main() {
    let matches = Command::new("CloudStorage")
        .version("1.0")
        .about("Cloud storage for assets")
        .arg(
            Arg::new("config")
                .short('c')
                .long("config")
                .value_name("FILE")
                .help("Sets a custom config file"),
        )
        .get_matches();

    let env_config = env::var("CLOUD_STORAGE_CONFIG").ok();
    let config_path: Option<&String> = matches.get_one::<String>("config").or(env_config.as_ref());

    let node_config: Arc<NodeConfig> = Arc::new(
        read_config(config_path)
            .await
            .unwrap_or_else(|e| panic!("Failed to load config file: {}", e)),
    );

    let _guards = init_tracing(&node_config.log_path, (&node_config.log_level).into());

    log_build_information();
    log_app_config(&node_config);
}
