// TODO: Remove this after the code is finalized
#![allow(dead_code)]

use tracing::info;
use tracing::{level_filters::LevelFilter, Level};
use tracing_appender::{
    non_blocking::WorkerGuard,
    rolling::{RollingFileAppender, Rotation},
};
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{fmt, layer::SubscriberExt};

use crate::config::NodeConfig;

pub fn init_tracing(logs_path: &str, log_level: Level) -> (WorkerGuard, WorkerGuard) {
    let (non_blocking_stdout, stdout_guard) = tracing_appender::non_blocking(std::io::stdout());

    let stdout_layer = fmt::layer()
        .with_thread_names(true)
        .with_writer(non_blocking_stdout);

    let file_appender = RollingFileAppender::new(Rotation::DAILY, logs_path, "gateway.log");

    let (non_blocking_file, log_file_guard) = tracing_appender::non_blocking(file_appender);
    let file_layer = fmt::layer()
        .with_ansi(false)
        .with_thread_names(true)
        .with_writer(non_blocking_file);

    tracing_subscriber::registry()
        .with(LevelFilter::from_level(log_level))
        .with(stdout_layer)
        .with(file_layer)
        .init();

    (stdout_guard, log_file_guard)
}

macro_rules! process_env_var {
    ($mode:expr, $label:expr, $env_var:expr $(, $info:expr)*) => {
        if let Some(value) = option_env!($env_var) {
            match $mode {
                "log" => info!("{}: {}", $label, value),
                "collect" => {
                    $($info.push(format!("{}: {}", $label, value)));*
                },
                _ => {}
            }
        }
    };
}

pub fn log_build_information() {
    process_env_var!("log", "Git Branch", "VERGEN_GIT_BRANCH");
    process_env_var!("log", "Git Commit ID", "VERGEN_GIT_SHA");
    process_env_var!("log", "Git Commit Message", "VERGEN_GIT_COMMIT_MESSAGE");
    process_env_var!("log", "Build Date", "VERGEN_BUILD_DATE");
    process_env_var!("log", "Rust Channel", "VERGEN_RUSTC_CHANNEL");
    process_env_var!("log", "Rust Version", "VERGEN_RUSTC_SEMVER");
    process_env_var!("log", "Optimization Level", "VERGEN_CARGO_OPT_LEVEL");
}

pub fn get_build_information(html: bool) -> String {
    let mut info = Vec::new();

    process_env_var!("collect", "Git Branch", "VERGEN_GIT_BRANCH", info);
    process_env_var!("collect", "Git Commit ID", "VERGEN_GIT_SHA", info);
    process_env_var!(
        "collect",
        "Git Commit Message",
        "VERGEN_GIT_COMMIT_MESSAGE",
        info
    );
    process_env_var!("collect", "Build Date", "VERGEN_BUILD_DATE", info);
    process_env_var!("collect", "Rust Channel", "VERGEN_RUSTC_CHANNEL", info);
    process_env_var!("collect", "Rust Version", "VERGEN_RUSTC_SEMVER", info);
    process_env_var!(
        "collect",
        "Optimization Level",
        "VERGEN_CARGO_OPT_LEVEL",
        info
    );

    info.join(if html { "<br>" } else { "\n" })
}

pub fn log_app_config(app_config: &NodeConfig) {
    info!("Starting cloud storage services with the following config:");
    info!("{}", app_config);
}
