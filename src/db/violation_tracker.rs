use std::sync::Arc;
use std::time::Duration;

use scc::HashMap as SccHashMap;
use serde_json::json;
use tokio_util::sync::CancellationToken;
use tracing::error;

use super::RateLimitViolationSink;
use super::ViolationSinkHandle;

/// Key identifying a unique (client, limiter) pair for violation counting.
///
/// `client_id` is either a source-IP string (per-IP limiters) or a
/// `"Subject:id"` string (distributed limiters).  `limiter` names the
/// rate-limiter that triggered (e.g. `"basic"`, `"distributed:User"`).
#[derive(Clone, Eq, PartialEq, Hash)]
struct ViolationKey {
    client_id: Arc<str>,
    limiter: Arc<str>,
}

/// Accumulates rate-limit violations in memory and periodically flushes
/// an aggregated snapshot to the configured sink.
///
/// Designed after [`super::EventRecorder`]: callers fire-and-forget via
/// [`record`], and a background task drains the counters on an interval.
/// Uses `scc::HashMap` with per-bucket locking for safe concurrent updates.
#[derive(Clone)]
pub struct RateLimitViolationTracker<S: RateLimitViolationSink = ViolationSinkHandle> {
    sink: Arc<S>,
    gateway_name: Arc<str>,
    counters: Arc<SccHashMap<ViolationKey, u64>>,
}

impl<S: RateLimitViolationSink + 'static> RateLimitViolationTracker<S> {
    pub fn new(
        sink: Arc<S>,
        gateway_name: Arc<str>,
        flush_interval: Duration,
        shutdown: CancellationToken,
    ) -> Self {
        let tracker = Self {
            sink,
            gateway_name,
            counters: Arc::new(SccHashMap::default()),
        };
        tracker.spawn_flusher(flush_interval, shutdown);
        tracker
    }

    /// Record a single rate-limit violation for the given limiter and client.
    ///
    /// Uses `entry_sync` which acquires a per-bucket lock so the
    /// increment is safe under concurrent access without atomics.
    pub fn record(&self, limiter: &str, client_id: &str) {
        let key = ViolationKey {
            client_id: Arc::from(client_id),
            limiter: Arc::from(limiter),
        };
        self.counters
            .entry_sync(key)
            .and_modify(|v| *v += 1)
            .or_insert(1);
    }

    fn spawn_flusher(&self, flush_interval: Duration, shutdown: CancellationToken) {
        let sink = Arc::clone(&self.sink);
        let counters = Arc::clone(&self.counters);
        let gateway_name = Arc::clone(&self.gateway_name);
        let token = shutdown.child_token();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(flush_interval);
            loop {
                tokio::select! {
                    _ = token.cancelled() => {
                        let _ = Self::flush_once(&sink, &counters, &gateway_name).await;
                        break;
                    }
                    _ = interval.tick() => {
                        let _ = Self::flush_once(&sink, &counters, &gateway_name).await;
                    }
                }
            }
        });
    }

    async fn flush_once(
        sink: &Arc<S>,
        counters: &Arc<SccHashMap<ViolationKey, u64>>,
        gateway_name: &str,
    ) -> anyhow::Result<()> {
        let mut details = serde_json::Map::new();
        let mut total: u64 = 0;

        // `retain_sync` visits every entry; returning `false` removes it.
        // This atomically drains the map while collecting per-key counts.
        counters.retain_sync(|key, val| {
            if *val > 0 {
                let label = format!("{}:{}", key.client_id, key.limiter);
                details.insert(label, json!(*val));
                total += *val;
            }
            false
        });

        if total == 0 {
            return Ok(());
        }

        let details_value = serde_json::Value::Object(details);
        if let Err(e) = sink
            .record_violations_batch(gateway_name, total, &details_value)
            .await
        {
            error!(error = ?e, "Failed to flush rate-limit violations");
        }
        Ok(())
    }

    #[cfg(any(test, feature = "test-support"))]
    #[allow(dead_code)]
    pub async fn flush_once_for_test(&self) {
        let _ = Self::flush_once(&self.sink, &self.counters, &self.gateway_name).await;
    }

    #[cfg(test)]
    fn pending_total(&self) -> u64 {
        let mut total = 0u64;
        // Use retain to iterate without removing (always return true).
        self.counters.retain_sync(|_, val| {
            total += *val;
            true
        });
        total
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use async_trait::async_trait;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    #[derive(Clone)]
    struct ViolationRow {
        pub gateway_name: String,
        pub total_count: u64,
        pub details: serde_json::Value,
    }

    #[derive(Clone, Default)]
    struct TestSink {
        rows: Arc<Mutex<Vec<ViolationRow>>>,
    }

    #[async_trait]
    impl RateLimitViolationSink for TestSink {
        async fn record_violations_batch(
            &self,
            gateway_name: &str,
            total_count: u64,
            details: &serde_json::Value,
        ) -> Result<()> {
            self.rows.lock().await.push(ViolationRow {
                gateway_name: gateway_name.to_string(),
                total_count,
                details: details.clone(),
            });
            Ok(())
        }
    }

    fn build_tracker(sink: Arc<TestSink>) -> RateLimitViolationTracker<TestSink> {
        let shutdown = CancellationToken::new();
        RateLimitViolationTracker::new(
            sink,
            Arc::from("test-gw"),
            Duration::from_secs(3600),
            shutdown,
        )
    }

    #[tokio::test]
    async fn record_accumulates_counts() {
        let sink = Arc::new(TestSink::default());
        let tracker = build_tracker(Arc::clone(&sink));

        tracker.record("basic", "10.0.0.1");
        tracker.record("basic", "10.0.0.1");
        tracker.record("result", "10.0.0.2");

        assert_eq!(tracker.pending_total(), 3);
    }

    #[tokio::test]
    async fn flush_writes_correct_snapshot() {
        let sink = Arc::new(TestSink::default());
        let tracker = build_tracker(Arc::clone(&sink));

        tracker.record("basic", "10.0.0.1");
        tracker.record("basic", "10.0.0.1");
        tracker.record("distributed:User", "user-abc");

        tracker.flush_once_for_test().await;

        let rows = sink.rows.lock().await;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].gateway_name, "test-gw");
        assert_eq!(rows[0].total_count, 3);

        let details = rows[0].details.as_object().expect("details is object");
        assert_eq!(details["10.0.0.1:basic"], json!(2));
        assert_eq!(details["user-abc:distributed:User"], json!(1));
    }

    #[tokio::test]
    async fn flush_clears_counters() {
        let sink = Arc::new(TestSink::default());
        let tracker = build_tracker(Arc::clone(&sink));

        tracker.record("basic", "10.0.0.1");
        tracker.flush_once_for_test().await;

        assert_eq!(tracker.pending_total(), 0);

        tracker.flush_once_for_test().await;
        let rows = sink.rows.lock().await;
        assert_eq!(rows.len(), 1, "empty flush should not produce a row");
    }

    #[tokio::test]
    async fn empty_flush_produces_no_row() {
        let sink = Arc::new(TestSink::default());
        let tracker = build_tracker(Arc::clone(&sink));

        tracker.flush_once_for_test().await;
        let rows = sink.rows.lock().await;
        assert!(rows.is_empty());
    }

    #[tokio::test]
    async fn concurrent_records_are_consistent() {
        let sink = Arc::new(TestSink::default());
        let tracker = build_tracker(Arc::clone(&sink));
        let n = 100u64;

        let mut handles = Vec::new();
        for _ in 0..n {
            let t = tracker.clone();
            handles.push(tokio::spawn(async move {
                t.record("basic", "10.0.0.1");
            }));
        }
        for h in handles {
            h.await.unwrap();
        }

        tracker.flush_once_for_test().await;

        let rows = sink.rows.lock().await;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].total_count, n);
        assert_eq!(rows[0].details["10.0.0.1:basic"], json!(n));
    }
}
