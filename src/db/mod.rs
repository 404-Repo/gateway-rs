mod connection;
mod event_recorder;
mod key_validator;
mod repository;

use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use foldhash::fast::RandomState;
use scc::HashMap as SccHashMap;
use sdd::AtomicOwned;
use std::sync::Arc;
#[cfg(feature = "test-support")]
use tokio::sync::Mutex;
use tokio_postgres::{Client, Statement};

pub use connection::PostgresConnectionConfigOwned;
pub use event_recorder::EventRecorder;
pub use key_validator::{ApiKeyLookup, ApiKeyValidator};

use connection::{AbortOnDropJoinHandle, StmtKey};

pub struct Database {
    client: AtomicOwned<Arc<Client>>,
    config: PostgresConnectionConfigOwned,
    events_copy_batch_size: usize,
    prepared: SccHashMap<StmtKey, Arc<Statement>, RandomState>,
    connection_task: AtomicOwned<AbortOnDropJoinHandle>,
}

#[derive(Clone)]
pub struct ActivityEventRow {
    pub user_id: Option<uuid::Uuid>,
    pub user_email: Option<String>,
    pub company_id: Option<uuid::Uuid>,
    pub company_name: Option<String>,
    pub action: String,
    pub tool: String,
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
    pub reason: Option<String>,
    pub gateway_name: String,
    pub created_at: DateTime<Utc>,
}

pub struct DatabaseBuilder {
    sslcert_path: Option<String>,
    sslkey_path: Option<String>,
    sslrootcert_path: Option<String>,
    host: Option<String>,
    port: Option<u16>,
    user: Option<String>,
    password: Option<String>,
    dbname: Option<String>,
    events_copy_batch_size: Option<usize>,
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
