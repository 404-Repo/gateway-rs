use anyhow::{Result, anyhow};
use notify::{RecursiveMode, Watcher, recommended_watcher};
use regex::Regex;
use scc::Queue;
use sdd::{AtomicOwned, Guard, Owned, Tag};
use std::collections::HashSet;
use std::ffi::OsString;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering::{AcqRel, Acquire};
use std::time::{Duration, UNIX_EPOCH};
use tokio::sync::Notify;
use tracing::{info, warn};

use crate::config::{
    HTTPConfig, ImageConfig, ModelParamsConfig, NodeConfig, PromptConfig, read_config_from_path,
    validate_node_config,
};
use crate::http3::rate_limits::RateLimiters;
use crate::http3::upload_limiter::ImageUploadLimiter;
use crate::http3::whitelist::{
    RateLimitWhitelist, resolve_cluster_peer_ips, resolve_egress_ips, resolve_rate_limit_whitelist,
};
use crate::raft::rate_limit::RateLimitService;

pub struct RuntimeConfigSnapshot {
    pub raw: NodeConfig,
    pub prompt_regex: Regex,
    pub rate_limit_whitelist: RateLimitWhitelist,
    pub cluster_ips: HashSet<IpAddr>,
    pub rate_limiters: RateLimiters,
    pub rate_limit_service: RateLimitService,
    pub image_upload_limiter: ImageUploadLimiter,
}

#[derive(Clone)]
pub struct RuntimeConfigView {
    snapshot: Arc<RuntimeConfigSnapshot>,
}

impl RuntimeConfigView {
    pub fn node(&self) -> &NodeConfig {
        &self.snapshot.raw
    }

    pub fn http(&self) -> &HTTPConfig {
        &self.snapshot.raw.http
    }

    pub fn prompt(&self) -> &PromptConfig {
        &self.snapshot.raw.prompt
    }

    pub fn image(&self) -> &ImageConfig {
        &self.snapshot.raw.image
    }

    pub fn model_params(&self) -> &ModelParamsConfig {
        &self.snapshot.raw.model_params
    }

    pub fn prompt_regex(&self) -> &Regex {
        &self.snapshot.prompt_regex
    }

    pub fn rate_limit_whitelist(&self) -> &RateLimitWhitelist {
        &self.snapshot.rate_limit_whitelist
    }

    pub fn cluster_ips(&self) -> &HashSet<IpAddr> {
        &self.snapshot.cluster_ips
    }

    pub fn rate_limits(&self) -> &RateLimitService {
        &self.snapshot.rate_limit_service
    }

    pub fn ip_rate_limiters(&self) -> &RateLimiters {
        &self.snapshot.rate_limiters
    }

    pub fn image_upload_limiter(&self) -> &ImageUploadLimiter {
        &self.snapshot.image_upload_limiter
    }
}

pub struct RuntimeConfigStore {
    path: PathBuf,
    inner: AtomicOwned<Arc<RuntimeConfigSnapshot>>,
    fallback: Arc<RuntimeConfigSnapshot>,
}

const WATCHER_POLL_INTERVAL: Duration = Duration::from_secs(2);

impl RuntimeConfigStore {
    pub async fn new(path: PathBuf, initial: NodeConfig) -> Result<Self> {
        let initial_snapshot = Arc::new(build_runtime_snapshot(initial).await?);

        Ok(Self {
            path,
            inner: AtomicOwned::new(Arc::clone(&initial_snapshot)),
            fallback: initial_snapshot,
        })
    }

    pub fn snapshot(&self) -> RuntimeConfigView {
        let guard = Guard::new();
        let snapshot = self
            .inner
            .load(Acquire, &guard)
            .as_ref()
            .cloned()
            .unwrap_or_else(|| {
                warn!("Runtime config store was empty, using fallback snapshot");
                Arc::clone(&self.fallback)
            });
        RuntimeConfigView { snapshot }
    }

    pub async fn reload_from_disk(&self) -> bool {
        self.apply_reloaded_config(read_config_from_path(&self.path).await)
            .await
    }

    pub fn start_watcher(
        self: &Arc<Self>,
        handle: tokio::runtime::Handle,
    ) -> Result<RuntimeConfigWatcher> {
        let signals = Arc::new(Queue::<()>::default());
        let notify = Arc::new(Notify::new());

        let store = Arc::clone(self);
        let poll_path = self.path.clone();
        let task_signals = Arc::clone(&signals);
        let task_notify = Arc::clone(&notify);
        let task_handle = handle.spawn(async move {
            let mut poll = tokio::time::interval(WATCHER_POLL_INTERVAL);
            let mut last_polled_mtime = modified_millis_from_path(&poll_path).await;
            loop {
                tokio::select! {
                    _ = task_notify.notified() => {
                        while task_signals.pop().is_some() {}
                        if store.reload_from_disk().await {
                            last_polled_mtime = modified_millis_from_path(&poll_path).await;
                        }
                    }
                    _ = poll.tick() => {
                        let current_mtime = modified_millis_from_path(&poll_path).await;
                        if current_mtime.is_some() && current_mtime != last_polled_mtime {
                            let _ = store.reload_from_disk().await;
                            last_polled_mtime = current_mtime;
                        }
                    }
                }
            }
        });

        let watched_path = self.path.clone();
        let watch_root = watched_path
            .parent()
            .map(|p| p.to_path_buf())
            .unwrap_or_else(|| watched_path.clone());
        let watched_name: Option<OsString> = watched_path.file_name().map(|n| n.to_os_string());
        let callback_signals = Arc::clone(&signals);
        let callback_notify = Arc::clone(&notify);
        let mut watcher =
            recommended_watcher(move |res: notify::Result<notify::Event>| match res {
                Ok(event) => {
                    if !matches!(
                        event.kind,
                        notify::EventKind::Modify(_)
                            | notify::EventKind::Create(_)
                            | notify::EventKind::Remove(_)
                    ) {
                        return;
                    }

                    let matches_exact_path = event.paths.iter().any(|p| p == &watched_path);
                    let matches_file_name = watched_name.as_ref().is_some_and(|name| {
                        event
                            .paths
                            .iter()
                            .any(|p| p.file_name().is_some_and(|n| n == name))
                    });
                    if matches_exact_path || matches_file_name {
                        callback_signals.push(());
                        callback_notify.notify_one();
                    }
                }
                Err(err) => warn!("Runtime config watcher error: {}", err),
            })?;

        watcher.watch(&watch_root, RecursiveMode::NonRecursive)?;
        info!("Runtime config watcher started for {}", self.path.display());
        Ok(RuntimeConfigWatcher {
            _watcher: watcher,
            _signals: signals,
            _notify: notify,
            task_handle,
        })
    }

    #[cfg(test)]
    async fn reload_from_disk_with_env_overrides(&self, overrides: &[(&str, &str)]) -> bool {
        self.apply_reloaded_config(
            crate::config_env::read_config_from_path_with_overrides(&self.path, overrides).await,
        )
        .await
    }

    async fn apply_reloaded_config(&self, cfg_result: Result<NodeConfig>) -> bool {
        let cfg = match cfg_result {
            Ok(cfg) => cfg,
            Err(err) => {
                warn!(
                    "Failed to reload runtime config {}: {}",
                    self.path.display(),
                    err
                );
                return false;
            }
        };

        let current = self.snapshot();
        warn_restart_required_changes(current.node(), &cfg);

        let snapshot = match build_runtime_snapshot(cfg).await {
            Ok(snapshot) => snapshot,
            Err(err) => {
                warn!("Invalid runtime config reload: {}", err);
                return false;
            }
        };

        let previous = self
            .inner
            .swap((Some(Owned::new(Arc::new(snapshot))), Tag::None), AcqRel)
            .0;
        drop(previous);

        info!(
            "Runtime config reloaded successfully from {}",
            self.path.display()
        );

        true
    }
}

fn warn_restart_required_changes(current: &NodeConfig, updated: &NodeConfig) {
    if current.network.bind_ip != updated.network.bind_ip || current.http.port != updated.http.port
    {
        warn!(
            "Runtime config changed HTTP listen address from {}:{} to {}:{}; restart required",
            current.network.bind_ip, current.http.port, updated.network.bind_ip, updated.http.port
        );
    }

    if current.http.tls_versions != updated.http.tls_versions {
        warn!(
            "Runtime config changed TLS versions from {:?} to {:?}; restart required",
            current.http.tls_versions, updated.http.tls_versions
        );
    }

    if current.http.transport != updated.http.transport {
        warn!(
            "Runtime config changed HTTP transport mode from {:?} to {:?}; restart required",
            current.http.transport, updated.http.transport
        );
    }

    if current.http.compression != updated.http.compression
        || current.http.compression_lvl != updated.http.compression_lvl
    {
        warn!(
            "Runtime config changed HTTP compression settings (enabled={}, level={}) -> (enabled={}, level={}); restart required",
            current.http.compression,
            current.http.compression_lvl,
            updated.http.compression,
            updated.http.compression_lvl
        );
    }

    if current.cert.cert_file_path != updated.cert.cert_file_path
        || current.cert.key_file_path != updated.cert.key_file_path
        || current.cert.dangerous_skip_verification != updated.cert.dangerous_skip_verification
    {
        warn!("Runtime config changed TLS certificate settings; restart required to apply");
    }

    if current.db.transport != updated.db.transport {
        warn!(
            "Runtime config changed database transport mode from {:?} to {:?}; restart required",
            current.db.transport, updated.db.transport
        );
    }
}

pub struct RuntimeConfigWatcher {
    _watcher: notify::RecommendedWatcher,
    _signals: Arc<Queue<()>>,
    _notify: Arc<Notify>,
    task_handle: tokio::task::JoinHandle<()>,
}

impl Drop for RuntimeConfigWatcher {
    fn drop(&mut self) {
        self.task_handle.abort();
    }
}

async fn modified_millis_from_path(path: &Path) -> Option<u64> {
    match tokio::fs::metadata(path).await {
        Ok(metadata) => metadata
            .modified()
            .ok()
            .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
            .map(|duration| duration.as_millis() as u64),
        Err(_) => None,
    }
}

async fn build_runtime_snapshot(config: NodeConfig) -> Result<RuntimeConfigSnapshot> {
    config
        .model_config
        .validate()
        .map_err(|e| anyhow!("Invalid model configuration: {}", e))?;
    validate_node_config(&config)?;

    let prompt_regex = Regex::new(&config.prompt.allowed_pattern).map_err(|e| {
        anyhow!(
            "Invalid prompt regex '{}': {}",
            config.prompt.allowed_pattern,
            e
        )
    })?;

    let whitelist_ips = resolve_rate_limit_whitelist(&config.http.rate_limit_whitelist).await;

    // Resolve cluster_ips from node_dns_names (used for Raft peer connections)
    let mut cluster_ips =
        resolve_cluster_peer_ips(&config.network.domain, &config.network.node_dns_names).await;

    // Merge in cluster_peer_egress_ips (NAT egress IPs) if configured.
    // These are NOT used for Raft peer connections — only for cluster_check whitelist.
    if !config.network.cluster_peer_egress_ips.is_empty() {
        let egress_ips = resolve_egress_ips(&config.network.cluster_peer_egress_ips).await;
        cluster_ips.extend(egress_ips);
    }

    let rate_limit_service = RateLimitService::new(&config.http);
    let rate_limiters = RateLimiters::new(&config.http);
    let image_upload_limiter = ImageUploadLimiter::new(config.http.max_concurrent_image_uploads);

    Ok(RuntimeConfigSnapshot {
        raw: config,
        prompt_regex,
        rate_limit_whitelist: RateLimitWhitelist {
            ips: Arc::new(whitelist_ips),
        },
        cluster_ips,
        rate_limiters,
        rate_limit_service,
        image_upload_limiter,
    })
}

#[cfg(test)]
mod tests {
    use super::RuntimeConfigStore;
    use crate::config::NodeConfig;
    use crate::config_env::{
        ADMIN_KEY_ENV, API_KEY_SECRET_ENV, DB_HOST_ENV, DB_NAME_ENV, DB_PASSWORD_ENV, DB_USER_ENV,
        read_config_from_path_with_overrides,
    };
    use anyhow::Result;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::{Duration, Instant};
    use tempfile::Builder;

    const BASE_CONFIG: &str = include_str!("../dev-env/config/config1.toml");

    fn parse_node_config() -> Result<NodeConfig> {
        Ok(toml::from_str::<NodeConfig>(BASE_CONFIG)?)
    }

    fn remove_line(config_text: &str, line: &str) -> String {
        config_text.replacen(&format!("{line}\n"), "", 1)
    }

    fn remove_env_backed_fields(config_text: &str) -> String {
        let config_text = remove_line(
            config_text,
            "admin_key = \"b6c8597a-00e9-493a-b6cd-5dfc7244d46b\"",
        );
        let config_text = remove_line(
            &config_text,
            "api_key_secret = \"CHANGE_ME_IN_PRODUCTION_58392047\"",
        );
        let config_text = remove_line(&config_text, "host = \"db\"");
        let config_text = remove_line(&config_text, "user = \"postgres\"");
        let config_text = remove_line(&config_text, "password = \"api_keys_!54321\"");
        remove_line(&config_text, "db = \"api_keys_db\"")
    }

    fn env_backed_overrides() -> [(&'static str, &'static str); 6] {
        [
            (API_KEY_SECRET_ENV, "env-secret-for-hashing"),
            (ADMIN_KEY_ENV, "11111111-1111-1111-1111-111111111111"),
            (DB_HOST_ENV, "env-db-host"),
            (DB_USER_ENV, "env-db-user"),
            (DB_PASSWORD_ENV, "env-db-password"),
            (DB_NAME_ENV, "env-db-name"),
        ]
    }

    #[tokio::test]
    async fn reload_from_disk_is_atomic_and_updates_snapshot() -> Result<()> {
        let file = Builder::new().suffix(".toml").tempfile()?;
        std::fs::write(file.path(), BASE_CONFIG)?;
        let initial = parse_node_config()?;
        let store = RuntimeConfigStore::new(PathBuf::from(file.path()), initial).await?;
        let before = store.snapshot();
        assert_eq!(before.http().max_task_queue_len, 500);
        assert!(before.prompt_regex().is_match("HELLO 123"));
        let updated = BASE_CONFIG
            .replace("max_task_queue_len = 500", "max_task_queue_len = 777")
            .replace(
                "allowed_pattern = \"^[A-Za-z0-9 .,'():;/?!+%-]+$\"",
                "allowed_pattern = \"^[a-z]+$\"",
            );
        std::fs::write(file.path(), updated)?;
        assert!(store.reload_from_disk().await);
        assert_eq!(before.http().max_task_queue_len, 500);
        assert!(before.prompt_regex().is_match("HELLO 123"));
        let after = store.snapshot();
        assert_eq!(after.http().max_task_queue_len, 777);
        assert!(after.prompt_regex().is_match("hello"));
        assert!(!after.prompt_regex().is_match("HELLO 123"));
        Ok(())
    }

    #[tokio::test]
    async fn invalid_reload_keeps_previous_snapshot() -> Result<()> {
        let file = Builder::new().suffix(".toml").tempfile()?;
        std::fs::write(file.path(), BASE_CONFIG)?;
        let initial = parse_node_config()?;
        let store = RuntimeConfigStore::new(PathBuf::from(file.path()), initial).await?;
        let invalid = BASE_CONFIG.replace(
            "allowed_pattern = \"^[A-Za-z0-9 .,'():;/?!+%-]+$\"",
            "allowed_pattern = \"[\"",
        );
        std::fs::write(file.path(), invalid)?;
        assert!(!store.reload_from_disk().await);
        let snapshot = store.snapshot();
        assert_eq!(snapshot.http().max_task_queue_len, 500);
        assert!(snapshot.prompt_regex().is_match("HELLO 123"));
        Ok(())
    }

    #[tokio::test]
    async fn reload_updates_http_limits() -> Result<()> {
        let file = Builder::new().suffix(".toml").tempfile()?;
        std::fs::write(file.path(), BASE_CONFIG)?;
        let initial = parse_node_config()?;
        let store = RuntimeConfigStore::new(PathBuf::from(file.path()), initial).await?;
        let before = store.snapshot();
        assert_eq!(before.http().request_size_limit, 4096);
        let updated = BASE_CONFIG.replace("request_size_limit = 4096", "request_size_limit = 8192");
        std::fs::write(file.path(), updated)?;
        assert!(store.reload_from_disk().await);
        let after = store.snapshot();
        assert_eq!(after.http().request_size_limit, 8192);
        Ok(())
    }

    #[tokio::test]
    async fn reload_from_disk_uses_env_overrides_for_missing_fields() -> Result<()> {
        let file = Builder::new().suffix(".toml").tempfile()?;
        let overrides = env_backed_overrides();
        let initial_config = remove_env_backed_fields(BASE_CONFIG);
        std::fs::write(file.path(), &initial_config)?;
        let initial = read_config_from_path_with_overrides(file.path(), &overrides).await?;
        let store = RuntimeConfigStore::new(PathBuf::from(file.path()), initial).await?;
        let updated =
            initial_config.replace("max_task_queue_len = 500", "max_task_queue_len = 901");
        std::fs::write(file.path(), updated)?;
        assert!(store.reload_from_disk_with_env_overrides(&overrides).await);
        let snapshot = store.snapshot();
        assert_eq!(snapshot.http().max_task_queue_len, 901);
        assert_eq!(
            snapshot.node().http.api_key_secret,
            "env-secret-for-hashing"
        );
        assert_eq!(
            snapshot.node().http.admin_key.to_string(),
            "11111111-1111-1111-1111-111111111111"
        );
        assert_eq!(snapshot.node().db.host, "env-db-host");
        assert_eq!(snapshot.node().db.user, "env-db-user");
        assert_eq!(snapshot.node().db.password, "env-db-password");
        assert_eq!(snapshot.node().db.db, "env-db-name");
        Ok(())
    }

    #[tokio::test]
    async fn malformed_reload_keeps_previous_snapshot() -> Result<()> {
        let file = Builder::new().suffix(".toml").tempfile()?;
        std::fs::write(file.path(), BASE_CONFIG)?;
        let initial = parse_node_config()?;
        let store = RuntimeConfigStore::new(PathBuf::from(file.path()), initial).await?;
        std::fs::write(file.path(), "this is not valid toml = [")?;
        assert!(!store.reload_from_disk().await);
        let snapshot = store.snapshot();
        assert_eq!(snapshot.http().max_task_queue_len, 500);
        assert!(snapshot.prompt_regex().is_match("HELLO 123"));
        Ok(())
    }

    #[tokio::test]
    async fn missing_file_reload_keeps_previous_snapshot() -> Result<()> {
        let file = Builder::new().suffix(".toml").tempfile()?;
        std::fs::write(file.path(), BASE_CONFIG)?;
        let initial = parse_node_config()?;
        let store = RuntimeConfigStore::new(PathBuf::from(file.path()), initial).await?;
        std::fs::remove_file(file.path())?;
        assert!(!store.reload_from_disk().await);
        let snapshot = store.snapshot();
        assert_eq!(snapshot.http().max_task_queue_len, 500);
        assert!(snapshot.prompt_regex().is_match("HELLO 123"));
        Ok(())
    }

    #[tokio::test]
    async fn watcher_detects_file_change_and_reloads() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("runtime-config.toml");
        std::fs::write(&path, BASE_CONFIG)?;
        let initial = parse_node_config()?;
        let store = Arc::new(RuntimeConfigStore::new(path.clone(), initial).await?);
        let _watcher = store.start_watcher(tokio::runtime::Handle::current())?;
        let updated = BASE_CONFIG.replace("max_task_queue_len = 500", "max_task_queue_len = 888");
        std::fs::write(&path, updated)?;
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if store.snapshot().http().max_task_queue_len == 888 {
                break;
            }
            if Instant::now() >= deadline {
                panic!("watcher did not apply config change within timeout");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        Ok(())
    }
}
