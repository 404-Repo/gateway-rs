use anyhow::{Result, anyhow};
use foldhash::HashMap;
use notify::{EventKind, RecursiveMode, Watcher, recommended_watcher};
use sdd::{AtomicOwned, Guard, Owned, Tag};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::{AcqRel, Acquire, Release};
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::warn;

use crate::config::NodeConfig;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ModelDefinition {
    pub output: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ModelConfig {
    pub default_model: String,
    pub models: HashMap<String, ModelDefinition>,
}

#[derive(Debug, Clone, Copy)]
pub enum ModelOutput {
    Ply,
    Glb,
}

#[derive(Debug, Clone)]
pub enum ModelResolveError {
    EmptyConfig,
    MissingDefault {
        default_model: String,
        known: Vec<String>,
    },
    UnknownModel {
        model: String,
        known: Vec<String>,
    },
    InvalidOutput {
        model: String,
        output: String,
    },
}

#[derive(Debug, Clone)]
pub struct ResolvedModel {
    pub model: String,
}

impl ModelOutput {
    pub fn from_str(value: &str) -> Option<Self> {
        match value {
            "ply" => Some(Self::Ply),
            "glb" => Some(Self::Glb),
            _ => None,
        }
    }

    pub fn content_type(self) -> &'static str {
        match self {
            Self::Ply => "application/octet-stream",
            Self::Glb => "model/gltf-binary",
        }
    }
}

impl ModelConfig {
    pub fn validate(&self) -> Result<(), ModelResolveError> {
        if self.models.is_empty() {
            return Err(ModelResolveError::EmptyConfig);
        }

        if !self.models.contains_key(&self.default_model) {
            return Err(ModelResolveError::MissingDefault {
                default_model: self.default_model.clone(),
                known: self.known_models(),
            });
        }

        for (model, definition) in &self.models {
            if ModelOutput::from_str(&definition.output).is_none() {
                return Err(ModelResolveError::InvalidOutput {
                    model: model.clone(),
                    output: definition.output.clone(),
                });
            }
        }

        Ok(())
    }

    pub fn resolve_model(
        &self,
        requested: Option<&str>,
    ) -> Result<ResolvedModel, ModelResolveError> {
        if self.models.is_empty() {
            return Err(ModelResolveError::EmptyConfig);
        }

        let model = requested.unwrap_or(self.default_model.as_str());
        match self.output_for(model) {
            Ok(_) => Ok(ResolvedModel {
                model: model.to_string(),
            }),
            Err(ModelResolveError::UnknownModel { known, .. }) if requested.is_none() => {
                Err(ModelResolveError::MissingDefault {
                    default_model: self.default_model.clone(),
                    known,
                })
            }
            Err(err) => Err(err),
        }
    }

    pub fn output_for(&self, model: &str) -> Result<ModelOutput, ModelResolveError> {
        if self.models.is_empty() {
            return Err(ModelResolveError::EmptyConfig);
        }

        let def = self
            .models
            .get(model)
            .ok_or_else(|| ModelResolveError::UnknownModel {
                model: model.to_string(),
                known: self.known_models(),
            })?;

        let output = ModelOutput::from_str(def.output.as_str()).ok_or_else(|| {
            ModelResolveError::InvalidOutput {
                model: model.to_string(),
                output: def.output.clone(),
            }
        })?;

        Ok(output)
    }

    pub fn known_models(&self) -> Vec<String> {
        let mut models: Vec<String> = self.models.keys().cloned().collect();
        models.sort();
        models
    }
}

pub async fn read_model_config_from_file<P: AsRef<Path>>(path: P) -> Result<ModelConfig> {
    let contents = tokio::fs::read_to_string(&path).await?;

    let node_config = match path.as_ref().extension().and_then(|ext| ext.to_str()) {
        Some("toml") => toml::from_str::<NodeConfig>(&contents)?,
        Some("json") => serde_json::from_str::<NodeConfig>(&contents)?,
        _ => return Err(anyhow!("Unsupported file format")),
    };

    Ok(node_config.model_config)
}

fn system_time_millis(time: SystemTime) -> Option<u64> {
    time.duration_since(UNIX_EPOCH)
        .ok()
        .map(|duration| duration.as_millis() as u64)
}

pub struct ModelConfigStore {
    path: PathBuf,
    inner: AtomicOwned<ModelConfig>,
    last_modified_millis: AtomicU64,
}

impl ModelConfigStore {
    pub async fn new(path: PathBuf, initial: ModelConfig) -> Result<Self> {
        initial
            .validate()
            .map_err(|e| anyhow!("Invalid model configuration: {}", e))?;
        let last_modified_millis = match tokio::fs::metadata(&path).await {
            Ok(metadata) => metadata
                .modified()
                .ok()
                .and_then(system_time_millis)
                .unwrap_or(0),
            Err(_) => 0,
        };

        Ok(Self {
            path,
            inner: AtomicOwned::new(initial),
            last_modified_millis: AtomicU64::new(last_modified_millis),
        })
    }

    pub async fn get(&self) -> ModelConfig {
        self.maybe_reload().await;
        let guard = Guard::new();
        self.inner
            .load(Acquire, &guard)
            .as_ref()
            .expect("model config must be initialized")
            .clone()
    }

    pub fn start_watcher(
        self: &Arc<Self>,
        handle: tokio::runtime::Handle,
    ) -> Result<ModelConfigWatcher> {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<()>(16);
        let store = Arc::clone(self);
        let task_handle = handle.spawn(async move {
            while rx.recv().await.is_some() {
                store.reload_from_disk().await;
            }
        });

        let path = self.path.clone();
        let mut watcher =
            recommended_watcher(move |res: notify::Result<notify::Event>| match res {
                Ok(event) => {
                    if matches!(
                        event.kind,
                        EventKind::Modify(_) | EventKind::Create(_) | EventKind::Remove(_)
                    ) {
                        let _ = tx.try_send(());
                    }
                }
                Err(err) => {
                    warn!("Model config watcher error: {}", err);
                }
            })?;

        watcher.watch(&path, RecursiveMode::NonRecursive)?;
        Ok(ModelConfigWatcher {
            _watcher: watcher,
            task_handle,
        })
    }

    async fn maybe_reload(&self) {
        let Some(modified_millis) = self.modified_millis().await else {
            return;
        };

        let last_modified = self.last_modified_millis.load(Acquire);
        if modified_millis <= last_modified {
            return;
        }

        self.reload_with_millis(Some(modified_millis)).await;
    }

    async fn reload_from_disk(&self) -> bool {
        let modified_millis = self.modified_millis().await;
        self.reload_with_millis(modified_millis).await
    }

    async fn reload_with_millis(&self, modified_millis: Option<u64>) -> bool {
        let config = match read_model_config_from_file(&self.path).await {
            Ok(config) => config,
            Err(err) => {
                warn!(
                    "Failed to reload model config {}: {}",
                    self.path.display(),
                    err
                );
                return false;
            }
        };

        if let Err(err) = config.validate() {
            warn!("Invalid model config reload: {}", err);
            return false;
        }

        let previous = self
            .inner
            .swap((Some(Owned::new(config)), Tag::None), AcqRel)
            .0;
        drop(previous);
        if let Some(modified_millis) = modified_millis {
            self.last_modified_millis.store(modified_millis, Release);
        }
        true
    }

    async fn modified_millis(&self) -> Option<u64> {
        match tokio::fs::metadata(&self.path).await {
            Ok(metadata) => metadata.modified().ok().and_then(system_time_millis),
            Err(err) => {
                warn!(
                    "Failed to stat model config {}: {}",
                    self.path.display(),
                    err
                );
                None
            }
        }
    }
}

pub struct ModelConfigWatcher {
    _watcher: notify::RecommendedWatcher,
    task_handle: tokio::task::JoinHandle<()>,
}

impl Drop for ModelConfigWatcher {
    fn drop(&mut self) {
        self.task_handle.abort();
    }
}

impl fmt::Display for ModelResolveError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ModelResolveError::EmptyConfig => write!(f, "no models configured"),
            ModelResolveError::MissingDefault {
                default_model,
                known,
            } => {
                write!(
                    f,
                    "default model '{}' not configured (known models: {})",
                    default_model,
                    known.join(", ")
                )
            }
            ModelResolveError::UnknownModel { model, known } => {
                write!(
                    f,
                    "model '{}' not configured (known models: {})",
                    model,
                    known.join(", ")
                )
            }
            ModelResolveError::InvalidOutput { model, output } => {
                write!(
                    f,
                    "invalid output '{}' configured for model '{}'",
                    output, model
                )
            }
        }
    }
}

impl std::error::Error for ModelResolveError {}

#[cfg(test)]
mod tests {
    use super::{ModelConfigStore, NodeConfig};
    use anyhow::Result;
    use std::path::PathBuf;
    use tempfile::Builder;

    const BASE_CONFIG: &str = include_str!("../dev-env/config/config1.toml");

    fn parse_node_config() -> Result<NodeConfig> {
        Ok(toml::from_str::<NodeConfig>(BASE_CONFIG)?)
    }

    #[tokio::test]
    async fn reloads_model_config_on_update() -> Result<()> {
        let file = Builder::new().suffix(".toml").tempfile()?;
        std::fs::write(file.path(), BASE_CONFIG)?;

        let node_config = parse_node_config()?;
        let store =
            ModelConfigStore::new(PathBuf::from(file.path()), node_config.model_config.clone())
                .await?;

        let initial = store.get().await;
        assert_eq!(initial.default_model, "404-3dgs");

        let updated = BASE_CONFIG.replace(
            "default_model = \"404-3dgs\"",
            "default_model = \"404-mesh\"",
        );
        std::fs::write(file.path(), updated)?;

        assert!(store.reload_from_disk().await);

        let refreshed = store.get().await;
        assert_eq!(refreshed.default_model, "404-mesh");

        Ok(())
    }

    #[tokio::test]
    async fn keeps_old_model_config_on_invalid_update() -> Result<()> {
        let file = Builder::new().suffix(".toml").tempfile()?;
        std::fs::write(file.path(), BASE_CONFIG)?;

        let node_config = parse_node_config()?;
        let store =
            ModelConfigStore::new(PathBuf::from(file.path()), node_config.model_config.clone())
                .await?;

        let initial = store.get().await;
        assert_eq!(initial.default_model, "404-3dgs");

        let invalid = BASE_CONFIG.replace(
            "default_model = \"404-3dgs\"",
            "default_model = \"missing-model\"",
        );
        std::fs::write(file.path(), invalid)?;

        assert!(!store.reload_from_disk().await);

        let refreshed = store.get().await;
        assert_eq!(refreshed.default_model, "404-3dgs");

        Ok(())
    }
}
