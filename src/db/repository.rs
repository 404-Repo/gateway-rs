use anyhow::{Result, anyhow};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures_util::SinkExt;
use tokio_postgres::types::ToSql;

use super::connection::StmtKey;
use super::{ActivityEventRow, Database, RateLimitViolationBatchRow, WorkerEventRow};

impl Database {
    pub(super) const Q_SERVER_TIME_UTC: &'static str = r#"
SELECT NOW()::timestamptz AS server_time_utc;
"#;

    pub(super) const Q_ALL_USER_KEY_HASHES: &'static str = r#"
SELECT u.id,
       u.email,
       COALESCE(
           array_agg(a.api_key_hash) FILTER (WHERE a.deleted_at IS NULL),
           '{}'::bytea[]
       ) AS api_key_hashes
FROM users u
LEFT JOIN api_keys a ON u.id = a.user_id
GROUP BY u.id, u.email;
"#;

    pub(super) const Q_DELTA_USER_KEY_HASHES: &'static str = r#"
WITH changed AS (
  SELECT DISTINCT user_id
  FROM api_keys
    WHERE (created_at > $1 AND created_at <= $2)
         OR (updated_at > $1 AND updated_at <= $2)
         OR (deleted_at IS NOT NULL AND deleted_at > $1 AND deleted_at <= $2)
)
SELECT c.user_id AS id,
       u.email,
       COALESCE(
           array_agg(a.api_key_hash) FILTER (WHERE a.deleted_at IS NULL),
           '{}'::bytea[]
       ) AS api_key_hashes
FROM changed c
JOIN users u ON u.id = c.user_id
LEFT JOIN api_keys a ON a.user_id = c.user_id
GROUP BY c.user_id, u.email;
"#;

    pub(super) const Q_USER_EMAIL_BY_ID: &'static str = r#"
SELECT email FROM users WHERE id = $1;
"#;

    pub(super) const Q_FULL_COMPANIES_META: &'static str = r#"
SELECT id, name, rate_limit_hourly, rate_limit_daily FROM companies;
"#;

    pub(super) const Q_DELTA_COMPANIES_META: &'static str = r#"
SELECT id, name, rate_limit_hourly, rate_limit_daily
FROM companies
WHERE (updated_at > $1 AND updated_at <= $2)
   OR (updated_at IS NULL AND created_at > $1 AND created_at <= $2);
"#;

    pub(super) const Q_FULL_COMPANY_KEYS: &'static str = r#"
SELECT company_id, api_key_hash FROM company_api_keys WHERE deleted_at IS NULL;
"#;

    pub(super) const Q_COMPANY_KEYS_FOR_IDS: &'static str = r#"
SELECT company_id, api_key_hash
FROM company_api_keys
WHERE deleted_at IS NULL AND company_id = ANY($1);
"#;

    pub(super) const Q_DELTA_COMPANY_KEYS: &'static str = r#"
WITH changed AS (
  SELECT DISTINCT company_id
  FROM company_api_keys
  WHERE (created_at > $1 AND created_at <= $2)
     OR (updated_at > $1 AND updated_at <= $2)
     OR (deleted_at IS NOT NULL AND deleted_at > $1 AND deleted_at <= $2)
)
SELECT c.company_id,
       COALESCE(
           array_agg(k.api_key_hash) FILTER (WHERE k.deleted_at IS NULL),
           '{}'::bytea[]
       ) AS api_key_hashes
FROM changed c
LEFT JOIN company_api_keys k ON k.company_id = c.company_id
GROUP BY c.company_id;
"#;

    pub(super) const Q_CLEANUP_DELETED_KEYS_BOTH: &'static str = r#"
WITH del_user AS (
  DELETE FROM api_keys
  WHERE deleted_at IS NOT NULL AND deleted_at < $1
  RETURNING 1
), del_company AS (
  DELETE FROM company_api_keys
  WHERE deleted_at IS NOT NULL AND deleted_at < $1
  RETURNING 1
)
SELECT
  (SELECT COUNT(*) FROM del_user) AS n1,
  (SELECT COUNT(*) FROM del_company) AS n2;
"#;

    pub(super) const COPY_ACTIVITY_EVENTS: &'static str = "\
COPY activity_events (\
user_id, \
user_email, \
company_id, \
company_name, \
action, \
tool, \
task_kind, \
model, \
gateway_name, \
task_id, \
created_at\
) FROM STDIN WITH (FORMAT text)";

    pub(super) const COPY_WORKER_EVENTS: &'static str = "\
COPY worker_events (\
task_id, \
worker_id, \
action, \
task_kind, \
reason, \
gateway_name, \
created_at\
) FROM STDIN WITH (FORMAT text)";

    pub(super) const COPY_RATE_LIMIT_VIOLATIONS: &'static str = "\
COPY rate_limit_violations (\
gateway_name, \
window_start, \
window_end, \
total_count, \
details, \
created_at\
) FROM STDIN WITH (FORMAT text)";

    pub async fn fetch_all_user_key_hashes(
        &self,
    ) -> Result<Vec<(uuid::Uuid, String, Vec<Vec<u8>>)>> {
        let rows = self.query_prepared(StmtKey::AllUserKeyHashes, &[]).await?;
        let result = rows
            .into_iter()
            .map(|row| {
                let user_id: uuid::Uuid = row.get("id");
                let email: String = row.get("email");
                let api_key_hashes: Vec<Option<Vec<u8>>> = row.get("api_key_hashes");
                (
                    user_id,
                    email,
                    api_key_hashes.into_iter().flatten().collect(),
                )
            })
            .collect();
        Ok(result)
    }

    pub async fn fetch_delta_user_key_hashes(
        &self,
        since: DateTime<Utc>,
        until: DateTime<Utc>,
    ) -> Result<Vec<(uuid::Uuid, String, Vec<Vec<u8>>)>> {
        let params: [&(dyn ToSql + Sync); 2] = [&since, &until];
        let rows = self
            .query_prepared(StmtKey::DeltaUserKeyHashes, &params)
            .await?;
        let result = rows
            .into_iter()
            .map(|row| {
                let user_id: uuid::Uuid = row.get("id");
                let email: String = row.get("email");
                let api_key_hashes: Vec<Option<Vec<u8>>> = row.get("api_key_hashes");
                (
                    user_id,
                    email,
                    api_key_hashes.into_iter().flatten().collect(),
                )
            })
            .collect();
        Ok(result)
    }

    pub async fn fetch_user_email(&self, user_id: uuid::Uuid) -> Result<Option<String>> {
        let params: [&(dyn ToSql + Sync); 1] = [&user_id];
        let rows = self.query_prepared(StmtKey::UserEmailById, &params).await?;
        Ok(rows.into_iter().next().map(|row| row.get("email")))
    }

    pub async fn fetch_full_companies_meta(&self) -> Result<Vec<(uuid::Uuid, (String, u64, u64))>> {
        let rows = self.query_prepared(StmtKey::FullCompaniesMeta, &[]).await?;
        let result = rows
            .into_iter()
            .map(|row| {
                let id: uuid::Uuid = row.get("id");
                let name: String = row.get("name");
                let hourly: i32 = row.get("rate_limit_hourly");
                let daily: i32 = row.get("rate_limit_daily");
                (id, (name, hourly as u64, daily as u64))
            })
            .collect();
        Ok(result)
    }

    pub async fn fetch_delta_companies_meta(
        &self,
        since: DateTime<Utc>,
        until: DateTime<Utc>,
    ) -> Result<Vec<(uuid::Uuid, (String, u64, u64))>> {
        let params: [&(dyn ToSql + Sync); 2] = [&since, &until];
        let rows = self
            .query_prepared(StmtKey::DeltaCompaniesMeta, &params)
            .await?;
        let result = rows
            .into_iter()
            .map(|row| {
                let id: uuid::Uuid = row.get("id");
                let name: String = row.get("name");
                let hourly: i32 = row.get("rate_limit_hourly");
                let daily: i32 = row.get("rate_limit_daily");
                (id, (name, hourly as u64, daily as u64))
            })
            .collect();
        Ok(result)
    }

    pub async fn fetch_full_company_keys(&self) -> Result<Vec<(uuid::Uuid, Vec<u8>)>> {
        let rows = self.query_prepared(StmtKey::FullCompanyKeys, &[]).await?;
        let result = rows
            .into_iter()
            .map(|row| {
                let company_id: uuid::Uuid = row.get("company_id");
                let hash: Vec<u8> = row.get("api_key_hash");
                (company_id, hash)
            })
            .collect();
        Ok(result)
    }

    pub async fn fetch_delta_company_keys(
        &self,
        since: DateTime<Utc>,
        until: DateTime<Utc>,
    ) -> Result<Vec<(uuid::Uuid, Vec<Vec<u8>>)>> {
        let params: [&(dyn ToSql + Sync); 2] = [&since, &until];
        let rows = self
            .query_prepared(StmtKey::DeltaCompanyKeys, &params)
            .await?;
        let result = rows
            .into_iter()
            .map(|row| {
                let company_id: uuid::Uuid = row.get("company_id");
                let api_key_hashes: Vec<Option<Vec<u8>>> = row.get("api_key_hashes");
                (company_id, api_key_hashes.into_iter().flatten().collect())
            })
            .collect();
        Ok(result)
    }

    pub async fn cleanup_deleted_keys_before(&self, cutoff: DateTime<Utc>) -> Result<(u64, u64)> {
        let params: [&(dyn ToSql + Sync); 1] = [&cutoff];
        let rows = self
            .query_prepared(StmtKey::CleanupDeletedKeysBoth, &params)
            .await?;
        let row = rows
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("Cleanup query returned no rows"))?;
        let n1: i64 = row.get("n1");
        let n2: i64 = row.get("n2");
        Ok((n1 as u64, n2 as u64))
    }

    pub async fn server_time_utc(&self) -> Result<DateTime<Utc>> {
        let rows = self.query_prepared(StmtKey::ServerTimeUtc, &[]).await?;
        let row = rows
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("Server time query returned no rows"))?;
        let dt: DateTime<Utc> = row.get("server_time_utc");
        Ok(dt)
    }

    pub async fn record_activity_events_batch(&self, rows: &[ActivityEventRow]) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let client = self.load_client().await?;
        client.batch_execute("BEGIN").await?;
        let result = async {
            for chunk in rows.chunks(self.events_copy_batch_size) {
                let mut buf = Vec::with_capacity(chunk.len() * 128);
                for row in chunk {
                    append_copy_field(&mut buf, row.user_id.map(|v| v.to_string()).as_deref());
                    buf.push(b'\t');
                    append_copy_field(&mut buf, row.user_email.as_deref());
                    buf.push(b'\t');
                    append_copy_field(&mut buf, row.company_id.map(|v| v.to_string()).as_deref());
                    buf.push(b'\t');
                    append_copy_field(&mut buf, row.company_name.as_deref());
                    buf.push(b'\t');
                    append_copy_field(&mut buf, Some(row.action.as_str()));
                    buf.push(b'\t');
                    append_copy_field(&mut buf, Some(row.tool.as_str()));
                    buf.push(b'\t');
                    append_copy_field(&mut buf, Some(row.task_kind.as_str()));
                    buf.push(b'\t');
                    append_copy_field(&mut buf, row.model.as_deref());
                    buf.push(b'\t');
                    append_copy_field(&mut buf, Some(row.gateway_name.as_str()));
                    buf.push(b'\t');
                    append_copy_field(&mut buf, row.task_id.map(|v| v.to_string()).as_deref());
                    buf.push(b'\t');
                    let created_at = row
                        .created_at
                        .naive_utc()
                        .format("%Y-%m-%d %H:%M:%S%.f")
                        .to_string();
                    append_copy_field(&mut buf, Some(created_at.as_str()));
                    buf.push(b'\n');
                }
                let sink = client.copy_in(Self::COPY_ACTIVITY_EVENTS).await?;
                let mut sink = std::pin::pin!(sink);
                sink.as_mut().send(Bytes::from(buf)).await?;
                sink.as_mut().finish().await?;
            }
            Ok::<(), anyhow::Error>(())
        }
        .await;

        match result {
            Ok(()) => {
                client.batch_execute("COMMIT").await?;
                Ok(())
            }
            Err(err) => {
                let _ = client.batch_execute("ROLLBACK").await;
                Err(err)
            }
        }
    }

    pub async fn record_worker_events_batch(&self, rows: &[WorkerEventRow]) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let client = self.load_client().await?;
        client.batch_execute("BEGIN").await?;
        let result = async {
            for chunk in rows.chunks(self.events_copy_batch_size) {
                let mut buf = Vec::with_capacity(chunk.len() * 128);
                for row in chunk {
                    append_copy_field(&mut buf, row.task_id.map(|v| v.to_string()).as_deref());
                    buf.push(b'\t');
                    append_copy_field(&mut buf, row.worker_id.as_deref());
                    buf.push(b'\t');
                    append_copy_field(&mut buf, Some(row.action.as_str()));
                    buf.push(b'\t');
                    append_copy_field(&mut buf, Some(row.task_kind.as_str()));
                    buf.push(b'\t');
                    append_copy_field(&mut buf, row.reason.as_deref());
                    buf.push(b'\t');
                    append_copy_field(&mut buf, Some(row.gateway_name.as_str()));
                    buf.push(b'\t');
                    let created_at = row
                        .created_at
                        .naive_utc()
                        .format("%Y-%m-%d %H:%M:%S%.f")
                        .to_string();
                    append_copy_field(&mut buf, Some(created_at.as_str()));
                    buf.push(b'\n');
                }
                let sink = client.copy_in(Self::COPY_WORKER_EVENTS).await?;
                let mut sink = std::pin::pin!(sink);
                sink.as_mut().send(Bytes::from(buf)).await?;
                sink.as_mut().finish().await?;
            }
            Ok::<(), anyhow::Error>(())
        }
        .await;

        match result {
            Ok(()) => {
                client.batch_execute("COMMIT").await?;
                Ok(())
            }
            Err(err) => {
                let _ = client.batch_execute("ROLLBACK").await;
                Err(err)
            }
        }
    }

    pub async fn record_rate_limit_violation_batches(
        &self,
        rows: &[RateLimitViolationBatchRow],
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let client = self.load_client().await?;
        client.batch_execute("BEGIN").await?;
        let result = async {
            for chunk in rows.chunks(self.events_copy_batch_size) {
                let mut buf = Vec::with_capacity(chunk.len() * 256);
                for row in chunk {
                    append_copy_field(&mut buf, Some(row.gateway_name.as_str()));
                    buf.push(b'\t');
                    let window_start = row
                        .window_start
                        .naive_utc()
                        .format("%Y-%m-%d %H:%M:%S%.f")
                        .to_string();
                    append_copy_field(&mut buf, Some(window_start.as_str()));
                    buf.push(b'\t');
                    let window_end = row
                        .window_end
                        .naive_utc()
                        .format("%Y-%m-%d %H:%M:%S%.f")
                        .to_string();
                    append_copy_field(&mut buf, Some(window_end.as_str()));
                    buf.push(b'\t');
                    append_copy_field(&mut buf, Some(row.total_count.to_string().as_str()));
                    buf.push(b'\t');
                    let details = serde_json::to_string(&row.details)?;
                    append_copy_field(&mut buf, Some(details.as_str()));
                    buf.push(b'\t');
                    let created_at = row
                        .created_at
                        .naive_utc()
                        .format("%Y-%m-%d %H:%M:%S%.f")
                        .to_string();
                    append_copy_field(&mut buf, Some(created_at.as_str()));
                    buf.push(b'\n');
                }
                let sink = client.copy_in(Self::COPY_RATE_LIMIT_VIOLATIONS).await?;
                let mut sink = std::pin::pin!(sink);
                sink.as_mut().send(Bytes::from(buf)).await?;
                sink.as_mut().finish().await?;
            }
            Ok::<(), anyhow::Error>(())
        }
        .await;

        match result {
            Ok(()) => {
                client.batch_execute("COMMIT").await?;
                Ok(())
            }
            Err(err) => {
                let _ = client.batch_execute("ROLLBACK").await;
                Err(err)
            }
        }
    }
}

fn append_copy_field(buf: &mut Vec<u8>, value: Option<&str>) {
    let Some(value) = value else {
        buf.extend_from_slice(b"\\N");
        return;
    };
    for &b in value.as_bytes() {
        match b {
            b'\\' => buf.extend_from_slice(b"\\\\"),
            b'\t' => buf.extend_from_slice(b"\\t"),
            b'\n' => buf.extend_from_slice(b"\\n"),
            b'\r' => buf.extend_from_slice(b"\\r"),
            _ => buf.push(b),
        }
    }
}
