mod api;
mod bittensor;
mod common;
mod config;
mod db;
mod http3;
mod metrics;
mod protocol;
mod raft;

use clap::Parser;
use clap::ValueEnum;
use common::log::{init_tracing, log_app_config, log_build_information};
use std::{env, sync::Arc, time::Duration};

use config::{read_config, NodeConfig};
use raft::{start_gateway, GatewayMode};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const RESTART_DELAY_SECS: u64 = 5;

#[cfg(unix)]
async fn wait_for_shutdown_signal() -> std::io::Result<()> {
    use tokio::signal::unix::{signal, SignalKind};

    let mut sigterm = signal(SignalKind::terminate())?;

    tokio::select! {
        res = tokio::signal::ctrl_c() => res,
        _ = sigterm.recv() => Ok(()),
    }
}

#[cfg(not(unix))]
async fn wait_for_shutdown_signal() -> std::io::Result<()> {
    tokio::signal::ctrl_c().await
}

#[derive(Parser, Debug)]
#[command(name = "Gateway", version = "1.0", about = "Gateway")]
struct Cli {
    #[arg(short, long, value_name = "FILE")]
    config: Option<String>,

    #[arg(long, value_enum, default_value_t = Mode::Vote)]
    mode: Mode,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum, Debug)]
enum Mode {
    Bootstrap,
    Vote,
    Single,
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

    let shutdown = CancellationToken::new();
    let signal_token = shutdown.clone();

    let signal_listener = tokio::spawn(async move {
        match wait_for_shutdown_signal().await {
            Ok(()) => info!("Received shutdown signal, shutting down..."),
            Err(e) => error!("Error while waiting for shutdown signal: {e}"),
        }
        signal_token.cancel();
    });

    let gateway_mode = match cli.mode {
        Mode::Bootstrap => GatewayMode::Bootstrap,
        Mode::Vote => GatewayMode::Vote,
        Mode::Single => GatewayMode::Single,
    };

    loop {
        if shutdown.is_cancelled() {
            break;
        }

        match start_gateway(gateway_mode, Arc::clone(&node_config), shutdown.clone()).await {
            Ok(gateway) => {
                tokio::select! {
                    _ = shutdown.cancelled() => {}
                }

                gateway.shutdown();
                drop(gateway);
                break;
            }
            Err(e) => {
                attempts += 1;
                error!(
                    "Failed to start gateway: {e}, attempt {}/{}",
                    attempts, max_attempts
                );

                if attempts >= max_attempts {
                    error!(
                        "Reached maximum restart attempts ({}). Stopping.",
                        max_attempts
                    );
                    shutdown.cancel();
                    break;
                }

                warn!("Retrying...");
                let sleep = tokio::time::sleep(Duration::from_secs(RESTART_DELAY_SECS));
                tokio::select! {
                    _ = shutdown.cancelled() => break,
                    _ = sleep => {}
                }
            }
        }
    }

    if !signal_listener.is_finished() {
        signal_listener.abort();
    }
    if let Err(e) = signal_listener.await {
        error!("Shutdown listener failed: {e}");
    }
}
