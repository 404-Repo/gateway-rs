mod connection;
mod event_recorder;
mod key_validator;
mod repository;
pub mod violation_tracker;

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
pub use violation_tracker::RateLimitViolationTracker;

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

// ---------------------------------------------------------------------------
// Rate-limit violation sink
// ---------------------------------------------------------------------------

/// Persists batched rate-limit violation snapshots.
#[async_trait]
pub trait RateLimitViolationSink: Send + Sync {
    async fn record_violations_batch(
        &self,
        gateway_name: &str,
        total_count: u64,
        details: &serde_json::Value,
    ) -> Result<()>;
}

#[cfg(feature = "test-support")]
#[derive(Clone)]
#[allow(dead_code)]
pub struct ViolationRow {
    pub gateway_name: String,
    pub total_count: u64,
    pub details: serde_json::Value,
}

#[cfg(feature = "test-support")]
#[derive(Clone, Default)]
pub struct InMemoryViolationSink {
    rows: Arc<Mutex<Vec<ViolationRow>>>,
}

#[cfg(feature = "test-support")]
impl InMemoryViolationSink {
    #[allow(dead_code)]
    pub async fn rows(&self) -> Vec<ViolationRow> {
        self.rows.lock().await.clone()
    }
}

#[cfg(feature = "test-support")]
#[async_trait]
impl RateLimitViolationSink for InMemoryViolationSink {
    async fn record_violations_batch(
        &self,
        gateway_name: &str,
        total_count: u64,
        details: &serde_json::Value,
    ) -> Result<()> {
        let mut guard = self.rows.lock().await;
        guard.push(ViolationRow {
            gateway_name: gateway_name.to_string(),
            total_count,
            details: details.clone(),
        });
        Ok(())
    }
}

#[derive(Clone)]
#[allow(dead_code)]
pub enum ViolationSinkHandle {
    Database(Arc<Database>),
    #[cfg(feature = "test-support")]
    InMemory(InMemoryViolationSink),
    Noop,
}

#[async_trait]
impl RateLimitViolationSink for ViolationSinkHandle {
    async fn record_violations_batch(
        &self,
        gateway_name: &str,
        total_count: u64,
        details: &serde_json::Value,
    ) -> Result<()> {
        match self {
            ViolationSinkHandle::Database(db) => {
                db.record_violations_batch(gateway_name, total_count, details)
                    .await
            }
            #[cfg(feature = "test-support")]
            ViolationSinkHandle::InMemory(sink) => {
                sink.record_violations_batch(gateway_name, total_count, details)
                    .await
            }
            ViolationSinkHandle::Noop => Ok(()),
        }
    }
}

#[async_trait]
impl RateLimitViolationSink for Database {
    async fn record_violations_batch(
        &self,
        gateway_name: &str,
        total_count: u64,
        details: &serde_json::Value,
    ) -> Result<()> {
        Database::record_violations_batch(self, gateway_name, total_count, details).await
    }
}
