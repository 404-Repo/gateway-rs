mod connection;
mod data_access;
mod event_recorder;
mod gateway_settings;
mod key_validator;
mod task_lifecycle;

use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value;
use std::sync::Arc;
#[cfg(feature = "test-support")]
use tokio::sync::Mutex;

use crate::config::TransportMode;

pub use event_recorder::EventRecorder;
pub use gateway_settings::{
    GatewayRuntimeSettingsConfig, GatewayRuntimeSettingsStore, gateway_settings_sync_interval,
};
pub use key_validator::{
    ApiKeyLookup, ApiKeyValidator, ApiKeyValidatorConfig, api_key_sync_interval,
};
pub use task_lifecycle::{
    CreateGenerationTaskInput, CreateGenerationTaskOutcome, CreateGenerationTaskRejection,
    FinalizeGenerationTaskAssignmentInput, FinalizeGenerationTaskAssignmentOutcome,
    GenerationBillingOwner, GenerationTaskAccessOwner, GenerationTaskStatusSnapshot,
    RecordedGenerationTaskAssignment, RecordedGenerationTaskAssignmentAction, TaskLifecycleStore,
    TaskLifecycleStoreHandle,
};

use connection::{CopyLane, MainPool};

pub struct Database {
    main_pool: Arc<MainPool>,
    events_copy_batch_size: usize,
    activity_copy_lane: Arc<CopyLane>,
    worker_copy_lane: Arc<CopyLane>,
}

#[derive(Clone)]
pub struct ActivityEventRow {
    pub user_id: Option<i64>,
    pub user_email: Option<String>,
    pub account_id: Option<i64>,
    pub company_id: Option<uuid::Uuid>,
    pub company_name: Option<String>,
    pub action: String,
    pub client_origin: String,
    pub task_kind: String,
    pub model: Option<String>,
    pub gateway_name: String,
    pub task_id: Option<uuid::Uuid>,
    pub created_at: DateTime<Utc>,
}

#[derive(Clone)]
pub struct WorkerEventRow {
    pub task_id: Option<uuid::Uuid>,
    #[allow(dead_code)]
    pub worker_id: Option<String>,
    pub action: String,
    pub task_kind: String,
    pub model: Option<String>,
    pub reason: Option<String>,
    pub gateway_name: String,
    pub metadata_json: Value,
    pub created_at: DateTime<Utc>,
}

pub struct DatabaseBuilder {
    transport: Option<TransportMode>,
    sslcert_path: Option<String>,
    sslkey_path: Option<String>,
    sslrootcert_path: Option<String>,
    host: Option<String>,
    port: Option<u16>,
    user: Option<String>,
    password: Option<String>,
    dbname: Option<String>,
    events_copy_batch_size: Option<usize>,
    pool_size: Option<usize>,
    shutdown: Option<tokio_util::sync::CancellationToken>,
}

#[async_trait]
pub trait EventSink: Send + Sync {
    async fn record_activity_events_batch(&self, rows: &[ActivityEventRow]) -> Result<()>;
    async fn record_worker_events_batch(&self, rows: &[WorkerEventRow]) -> Result<()>;
}

#[cfg(feature = "test-support")]
#[derive(Clone, Default)]
pub struct InMemoryEventSink {
    activity: Arc<Mutex<Vec<ActivityEventRow>>>,
    worker: Arc<Mutex<Vec<WorkerEventRow>>>,
}

#[cfg(feature = "test-support")]
impl InMemoryEventSink {
    #[allow(dead_code)]
    pub async fn activity_rows(&self) -> Vec<ActivityEventRow> {
        self.activity.lock().await.clone()
    }

    #[allow(dead_code)]
    pub async fn worker_rows(&self) -> Vec<WorkerEventRow> {
        self.worker.lock().await.clone()
    }
}

#[cfg(feature = "test-support")]
#[async_trait]
impl EventSink for InMemoryEventSink {
    async fn record_activity_events_batch(&self, rows: &[ActivityEventRow]) -> Result<()> {
        let mut guard = self.activity.lock().await;
        guard.extend(rows.iter().cloned());
        Ok(())
    }

    async fn record_worker_events_batch(&self, rows: &[WorkerEventRow]) -> Result<()> {
        let mut guard = self.worker.lock().await;
        guard.extend(rows.iter().cloned());
        Ok(())
    }
}

#[derive(Clone)]
#[allow(dead_code)]
pub enum EventSinkHandle {
    Database(Arc<Database>),
    #[cfg(feature = "test-support")]
    InMemory(InMemoryEventSink),
    Noop,
}

#[async_trait]
impl EventSink for EventSinkHandle {
    async fn record_activity_events_batch(&self, rows: &[ActivityEventRow]) -> Result<()> {
        match self {
            EventSinkHandle::Database(db) => db.record_activity_events_batch(rows).await,
            #[cfg(feature = "test-support")]
            EventSinkHandle::InMemory(sink) => sink.record_activity_events_batch(rows).await,
            EventSinkHandle::Noop => Ok(()),
        }
    }

    async fn record_worker_events_batch(&self, rows: &[WorkerEventRow]) -> Result<()> {
        match self {
            EventSinkHandle::Database(db) => db.record_worker_events_batch(rows).await,
            #[cfg(feature = "test-support")]
            EventSinkHandle::InMemory(sink) => sink.record_worker_events_batch(rows).await,
            EventSinkHandle::Noop => Ok(()),
        }
    }
}

#[async_trait]
impl EventSink for Database {
    async fn record_activity_events_batch(&self, rows: &[ActivityEventRow]) -> Result<()> {
        Database::record_activity_events_batch(self, rows).await
    }

    async fn record_worker_events_batch(&self, rows: &[WorkerEventRow]) -> Result<()> {
        Database::record_worker_events_batch(self, rows).await
    }
}
