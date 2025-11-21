use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use chrono::{NaiveDate, Utc};
use foldhash::fast::RandomState;
use tokio_util::sync::CancellationToken;
use tracing::error;
use uuid::Uuid;

use crate::db::Database;
use crate::metrics::TaskKind;

#[derive(Clone, Copy, Hash, PartialEq, Eq)]
struct UsageKey {
    company_id: Uuid,
    bucket: NaiveDate,
    task_kind: TaskKind,
}

#[derive(Clone)]
pub struct CompanyUsageRecorder {
    db: Arc<Database>,
    buffer: Arc<scc::HashMap<UsageKey, i64, RandomState>>,
}

impl CompanyUsageRecorder {
    pub fn new(db: Arc<Database>, flush_interval: Duration, shutdown: CancellationToken) -> Self {
        let recorder = Self {
            db,
            buffer: Arc::new(scc::HashMap::with_capacity_and_hasher(
                256,
                RandomState::default(),
            )),
        };
        recorder.spawn_flusher(flush_interval, shutdown);
        recorder
    }

    pub async fn record(&self, company_id: Uuid, task_kind: TaskKind) {
        let bucket = Utc::now().date_naive();
        let key = UsageKey {
            company_id,
            bucket,
            task_kind,
        };
        self.buffer
            .entry_async(key)
            .await
            .and_modify(|v| *v += 1)
            .or_insert(1);
    }

    fn spawn_flusher(&self, flush_interval: Duration, shutdown: CancellationToken) {
        let db = Arc::clone(&self.db);
        let buffer = Arc::clone(&self.buffer);
        let token = shutdown.child_token();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(flush_interval);
            loop {
                tokio::select! {
                    _ = token.cancelled() => {
                        if let Err(e) = Self::flush_once(&db, &buffer).await {
                            error!(error = ?e, "Failed to flush company usage stats on shutdown");
                        }
                        break;
                    }
                    _ = interval.tick() => {
                        if let Err(e) = Self::flush_once(&db, &buffer).await {
                            error!(error = ?e, "Failed to flush company usage stats");
                        }
                    }
                }
            }
        });
    }

    async fn flush_once(
        db: &Arc<Database>,
        buffer: &Arc<scc::HashMap<UsageKey, i64, RandomState>>,
    ) -> Result<()> {
        let mut pending = Vec::new();
        buffer
            .retain_async(|key, value| {
                if *value > 0 {
                    pending.push((*key, *value));
                    *value = 0;
                }
                true
            })
            .await;

        for (key, count) in pending {
            if let Err(e) = db
                .record_company_usage(key.company_id, key.bucket, key.task_kind.label(), count)
                .await
            {
                error!(
                    error = ?e,
                    company_id = %key.company_id,
                    bucket = %key.bucket,
                    task_kind = key.task_kind.label(),
                    "Failed to persist company usage stats, retrying on next flush"
                );
                buffer
                    .entry_async(key)
                    .await
                    .and_modify(|v| *v += count)
                    .or_insert(count);
            } else if let scc::hash_map::Entry::Occupied(entry) = buffer.entry_async(key).await {
                if *entry.get() == 0 {
                    let _ = entry.remove_entry();
                }
            }
        }

        Ok(())
    }
}
