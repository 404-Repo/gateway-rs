use std::time::Duration;
use std::{env, sync::Arc};

use clap::{Arg, Command};
use common::init_tracing;
use config::{read_config, NodeConfig};
use raft::start_gateway;
use tracing::info;

mod common;
mod config;
mod protocol;
mod raft;

use crate::common::log_app_config;
use crate::common::log_build_information;

#[tokio::main]
async fn main() {
    let matches = Command::new("Gateway")
        .version("1.0")
        .about("Gateway")
        .arg(
            Arg::new("config")
                .short('c')
                .long("config")
                .value_name("FILE")
                .help("Sets a custom config file"),
        )
        .get_matches();

    let env_config = env::var("GATEWAY_CONFIG").ok();
    let config_path: Option<&String> = matches.get_one::<String>("config").or(env_config.as_ref());

    let node_config: Arc<NodeConfig> = Arc::new(
        read_config(config_path)
            .await
            .unwrap_or_else(|e| panic!("Failed to load config file: {}", e)),
    );

    let _guards = init_tracing(&node_config.log.path, (&node_config.log.level).into());

    log_build_information();
    log_app_config(&node_config);

    let max_attempts = node_config.basic.max_restart_attempts;
    let mut attempts = 0;

    loop {
        match start_gateway(node_config.clone()).await {
            Ok(_) => {
                let _ = tokio::signal::ctrl_c().await;
                info!("Received CTRL+C, shutting down...");
                break;
            }
            Err(e) => {
                attempts += 1;
                info!(
                    "Failed to start gateway: {e}, attempt {}/{}",
                    attempts, max_attempts
                );

                if attempts >= max_attempts {
                    info!(
                        "Reached maximum restart attempts ({}). Stopping.",
                        max_attempts
                    );
                    break;
                } else {
                    info!("Retrying...");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }
}
