use clap::Parser;
use common::log::{init_tracing, log_app_config, log_build_information};
use std::{env, sync::Arc, time::Duration};

use config::{read_config, NodeConfig};
use raft::{start_gateway_single, start_gateway_vote};
use tracing::info;

mod api;
mod common;
mod config;
mod http3;
mod protocol;
mod raft;

#[derive(Parser, Debug)]
#[command(name = "Gateway", version = "1.0", about = "Gateway")]
struct Cli {
    #[arg(short, long, value_name = "FILE")]
    config: Option<String>,

    /// Run in single-node test mode
    #[arg(short, long)]
    test: bool,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    let env_config = env::var("GATEWAY_CONFIG").ok();
    let config_path: Option<&String> = cli.config.as_ref().or(env_config.as_ref());

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
        let result = if cli.test {
            start_gateway_single(node_config.clone()).await
        } else {
            start_gateway_vote(node_config.clone()).await
        };

        match result {
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
