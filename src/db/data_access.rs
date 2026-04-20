use anyhow::{Result, anyhow};
use bytes::Bytes;
use futures_util::SinkExt;
use tokio_postgres::{Client, Error as PgError, Row, types::ToSql};
use tracing::info;

use super::connection::StmtKey;
use super::task_lifecycle::GenerationBillingOwner;
use super::{ActivityEventRow, Database, WorkerEventRow};

#[derive(Debug, Clone)]
pub struct ApiKeyBillingOwnerRow {
    pub api_key_hash: Vec<u8>,
    pub owner: GenerationBillingOwner,
    pub revoked_at: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct GatewaySettingsRow {
    pub generic_key: uuid::Uuid,
    pub generic_global_daily_limit: u64,
    pub generic_per_ip_daily_limit: u64,
    pub generic_window_ms: u64,
    pub add_task_unauthorized_per_ip_daily_rate_limit: u64,
    pub rate_limit_whitelist: Vec<String>,
    pub max_task_queue_len: u64,
    pub request_file_size_limit: u64,
    pub guest_generation_limit: u64,
    pub guest_window_ms: u64,
    pub registered_generation_limit: u64,
    pub registered_window_ms: u64,
}

type UserKeyHashesRow = (i64, String, u64, u64, Vec<Vec<u8>>);
type UserMetaRow = (String, u64, u64);
type TaskLimitsRow = (String, u64, u64);
type CompanyMetaRow = (uuid::Uuid, TaskLimitsRow);
type CompanyKeyHashesRow = (uuid::Uuid, Vec<Vec<u8>>);

fn nonnegative_i32_to_u64(value: i32, field_name: &str) -> Result<u64> {
    u64::try_from(value).map_err(|_| anyhow!("Negative {} is not allowed: {}", field_name, value))
}

fn nonnegative_i64_to_u64(value: i64, field_name: &str) -> Result<u64> {
    u64::try_from(value).map_err(|_| anyhow!("Negative {} is not allowed: {}", field_name, value))
}

fn positive_i64_to_u64(value: i64, field_name: &str) -> Result<u64> {
    if value <= 0 {
        return Err(anyhow!(
            "Non-positive {} is not allowed: {}",
            field_name,
            value
        ));
    }
    Ok(value as u64)
}

fn decode_task_limits(name: String, concurrent: i32, daily: i32) -> Result<TaskLimitsRow> {
    Ok((
        name,
        nonnegative_i32_to_u64(concurrent, "task_limit_concurrent")?,
        nonnegative_i32_to_u64(daily, "task_limit_daily")?,
    ))
}

fn decode_user_key_hashes_row(row: Row) -> Result<UserKeyHashesRow> {
    let user_id: i64 = row.get("id");
    let email: String = row.get("email");
    let concurrent: i32 = row.get("task_limit_concurrent");
    let daily: i32 = row.get("task_limit_daily");
    let api_key_hashes: Vec<Option<Vec<u8>>> = row.get("api_key_hashes");
    Ok((
        user_id,
        email,
        nonnegative_i32_to_u64(concurrent, "task_limit_concurrent")?,
        nonnegative_i32_to_u64(daily, "task_limit_daily")?,
        api_key_hashes.into_iter().flatten().collect(),
    ))
}

fn decode_company_meta_row(row: Row) -> Result<CompanyMetaRow> {
    let id: uuid::Uuid = row.get("id");
    let name: String = row.get("name");
    let concurrent: i32 = row.get("task_limit_concurrent");
    let daily: i32 = row.get("task_limit_daily");
    Ok((id, decode_task_limits(name, concurrent, daily)?))
}

fn decode_company_limits_row(row: Row) -> Result<TaskLimitsRow> {
    let name: String = row.get("name");
    let concurrent: i32 = row.get("task_limit_concurrent");
    let daily: i32 = row.get("task_limit_daily");
    decode_task_limits(name, concurrent, daily)
}

fn decode_user_meta_row(row: Row) -> Result<UserMetaRow> {
    let email: String = row.get("email");
    let concurrent: i32 = row.get("task_limit_concurrent");
    let daily: i32 = row.get("task_limit_daily");
    Ok((
        email,
        nonnegative_i32_to_u64(concurrent, "task_limit_concurrent")?,
        nonnegative_i32_to_u64(daily, "task_limit_daily")?,
    ))
}

fn decode_company_key_hashes_row(row: Row) -> CompanyKeyHashesRow {
    let company_id: uuid::Uuid = row.get("company_id");
    let api_key_hashes: Vec<Option<Vec<u8>>> = row.get("api_key_hashes");
    (company_id, api_key_hashes.into_iter().flatten().collect())
}

fn decode_gateway_settings_row(row: Row) -> Result<GatewaySettingsRow> {
    let generic_global_daily_limit: i32 = row.get("gateway_generic_global_daily_limit");
    let generic_per_ip_daily_limit: i32 = row.get("gateway_generic_per_ip_daily_limit");
    let generic_window_ms: i64 = row.get("gateway_generic_window_ms");
    let add_task_unauthorized_per_ip_daily_rate_limit: i32 =
        row.get("add_task_unauthorized_per_ip_daily_rate_limit");
    let max_task_queue_len: i32 = row.get("max_task_queue_len");
    let request_file_size_limit: i64 = row.get("request_file_size_limit");
    let guest_generation_limit: i32 = row.get("guest_generation_limit");
    let guest_window_ms: i64 = row.get("guest_window_ms");
    let registered_generation_limit: i32 = row.get("registered_generation_limit");
    let registered_window_ms: i64 = row.get("registered_window_ms");
    Ok(GatewaySettingsRow {
        generic_key: row.get("gateway_generic_key"),
        generic_global_daily_limit: nonnegative_i32_to_u64(
            generic_global_daily_limit,
            "gateway_generic_global_daily_limit",
        )?,
        generic_per_ip_daily_limit: nonnegative_i32_to_u64(
            generic_per_ip_daily_limit,
            "gateway_generic_per_ip_daily_limit",
        )?,
        generic_window_ms: positive_i64_to_u64(generic_window_ms, "gateway_generic_window_ms")?,
        add_task_unauthorized_per_ip_daily_rate_limit: nonnegative_i32_to_u64(
            add_task_unauthorized_per_ip_daily_rate_limit,
            "add_task_unauthorized_per_ip_daily_rate_limit",
        )?,
        rate_limit_whitelist: row.get("rate_limit_whitelist"),
        max_task_queue_len: nonnegative_i32_to_u64(max_task_queue_len, "max_task_queue_len")?,
        request_file_size_limit: nonnegative_i64_to_u64(
            request_file_size_limit,
            "request_file_size_limit",
        )?,
        guest_generation_limit: nonnegative_i32_to_u64(
            guest_generation_limit,
            "guest_generation_limit",
        )?,
        guest_window_ms: positive_i64_to_u64(guest_window_ms, "guest_window_ms")?,
        registered_generation_limit: nonnegative_i32_to_u64(
            registered_generation_limit,
            "registered_generation_limit",
        )?,
        registered_window_ms: positive_i64_to_u64(registered_window_ms, "registered_window_ms")?,
    })
}

fn encode_activity_event_copy_row(buf: &mut Vec<u8>, row: &ActivityEventRow) {
    let metadata_json = serde_json::json!({
        "userEmail": row.user_email,
        "companyName": row.company_name,
    })
    .to_string();
    append_copy_field(buf, row.task_id.map(|v| v.to_string()).as_deref());
    buf.push(b'\t');
    append_copy_field(buf, None);
    buf.push(b'\t');
    append_copy_field(buf, row.user_id.map(|v| v.to_string()).as_deref());
    buf.push(b'\t');
    append_copy_field(buf, row.company_id.map(|v| v.to_string()).as_deref());
    buf.push(b'\t');
    append_copy_field(buf, None);
    buf.push(b'\t');
    append_copy_field(buf, None);
    buf.push(b'\t');
    append_copy_field(buf, Some(row.action.as_str()));
    buf.push(b'\t');
    append_copy_field(buf, Some("other"));
    buf.push(b'\t');
    append_copy_field(buf, Some(row.client_origin.as_str()));
    buf.push(b'\t');
    append_copy_field(buf, Some(row.task_kind.as_str()));
    buf.push(b'\t');
    append_copy_field(buf, row.model.as_deref());
    buf.push(b'\t');
    append_copy_field(buf, Some(row.gateway_name.as_str()));
    buf.push(b'\t');
    append_copy_field(buf, Some(metadata_json.as_str()));
    buf.push(b'\t');
    let created_at = row.created_at.timestamp_millis().to_string();
    append_copy_field(buf, Some(created_at.as_str()));
    buf.push(b'\n');
}

fn encode_worker_event_copy_row(buf: &mut Vec<u8>, row: &WorkerEventRow) {
    append_copy_field(buf, row.task_id.map(|v| v.to_string()).as_deref());
    buf.push(b'\t');
    append_copy_field(buf, row.worker_id.as_deref());
    buf.push(b'\t');
    append_copy_field(buf, Some(row.action.as_str()));
    buf.push(b'\t');
    append_copy_field(buf, Some(row.task_kind.as_str()));
    buf.push(b'\t');
    append_copy_field(buf, None);
    buf.push(b'\t');
    append_copy_field(buf, row.reason.as_deref());
    buf.push(b'\t');
    append_copy_field(buf, Some(row.gateway_name.as_str()));
    buf.push(b'\t');
    append_copy_field(buf, Some("{}"));
    buf.push(b'\t');
    let created_at = row.created_at.timestamp_millis().to_string();
    append_copy_field(buf, Some(created_at.as_str()));
    buf.push(b'\n');
}

async fn write_copy_batch<T>(
    client: &Client,
    rows: &[T],
    batch_size: usize,
    copy_statement: &'static str,
    encode_row: fn(&mut Vec<u8>, &T),
) -> std::result::Result<(), PgError> {
    client.batch_execute("BEGIN").await?;
    let result = async {
        for chunk in rows.chunks(batch_size) {
            let mut buf = Vec::with_capacity(chunk.len() * 128);
            for row in chunk {
                encode_row(&mut buf, row);
            }
            let sink = client.copy_in(copy_statement).await?;
            let mut sink = std::pin::pin!(sink);
            sink.as_mut().send(Bytes::from(buf)).await?;
            sink.as_mut().finish().await?;
        }
        client.batch_execute("COMMIT").await?;
        Ok(())
    }
    .await;

    if result.is_err() {
        let _ = client.batch_execute("ROLLBACK").await;
    }

    result
}

impl Database {
    async fn write_activity_events_batch(
        client: &Client,
        rows: &[ActivityEventRow],
        batch_size: usize,
    ) -> std::result::Result<(), PgError> {
        write_copy_batch(
            client,
            rows,
            batch_size,
            Self::COPY_ACTIVITY_EVENTS,
            encode_activity_event_copy_row,
        )
        .await
    }

    async fn write_worker_events_batch(
        client: &Client,
        rows: &[WorkerEventRow],
        batch_size: usize,
    ) -> std::result::Result<(), PgError> {
        write_copy_batch(
            client,
            rows,
            batch_size,
            Self::COPY_WORKER_EVENTS,
            encode_worker_event_copy_row,
        )
        .await
    }

    pub(super) const Q_SERVER_TIME_MS: &'static str = r#"
SELECT CAST(EXTRACT(EPOCH FROM clock_timestamp()) * 1000 AS BIGINT) AS server_time_ms;
"#;

    pub(super) const Q_GATEWAY_SETTINGS: &'static str = r#"
SELECT
  gateway_generic_key,
  gateway_generic_global_daily_limit,
  gateway_generic_per_ip_daily_limit,
  gateway_generic_window_ms,
  add_task_unauthorized_per_ip_daily_rate_limit,
  rate_limit_whitelist,
  max_task_queue_len,
  request_file_size_limit,
  guest_generation_limit,
  guest_window_ms,
  registered_generation_limit,
  registered_window_ms
FROM app_settings
WHERE id = 1
LIMIT 1;
"#;

    pub(super) const Q_ALL_USER_KEY_HASHES: &'static str = r#"
SELECT u.id,
       COALESCE(u.email, '') AS email,
       u.task_limit_concurrent,
       u.task_limit_daily,
       COALESCE(
           array_agg(a.api_key_hash) FILTER (
             WHERE a.key_scope = 'personal'
               AND a.revoked_at IS NULL
           ),
           '{}'::bytea[]
       ) AS api_key_hashes
FROM users u
LEFT JOIN api_keys a
  ON u.id = a.user_id
 AND a.key_scope = 'personal'
GROUP BY u.id, u.email, u.task_limit_concurrent, u.task_limit_daily;
"#;

    pub(super) const Q_DELTA_USER_KEY_HASHES: &'static str = r#"
WITH changed AS (
  SELECT user_id AS id
  FROM api_keys
  WHERE key_scope = 'personal'
    AND user_id IS NOT NULL
    AND (
      (created_at > $1 AND created_at <= $2)
      OR (updated_at > $1 AND updated_at <= $2)
      OR (revoked_at IS NOT NULL AND revoked_at > $1 AND revoked_at <= $2)
    )
  UNION
  SELECT id
  FROM users
  WHERE (created_at > $1 AND created_at <= $2)
     OR (updated_at > $1 AND updated_at <= $2)
)
SELECT c.id AS id,
       COALESCE(u.email, '') AS email,
       u.task_limit_concurrent,
       u.task_limit_daily,
       COALESCE(
           array_agg(a.api_key_hash) FILTER (
             WHERE a.key_scope = 'personal'
               AND a.revoked_at IS NULL
           ),
           '{}'::bytea[]
       ) AS api_key_hashes
FROM changed c
JOIN users u ON u.id = c.id
LEFT JOIN api_keys a
  ON a.user_id = c.id
 AND a.key_scope = 'personal'
GROUP BY c.id, u.email, u.task_limit_concurrent, u.task_limit_daily;
"#;

    pub(super) const Q_ALL_API_KEY_BILLING_OWNERS: &'static str = r#"
SELECT
  api_key_hash,
  id AS api_key_id,
  account_id,
  user_id,
  company_id
FROM api_keys
WHERE revoked_at IS NULL;
"#;

    pub(super) const Q_DELTA_API_KEY_BILLING_OWNERS: &'static str = r#"
SELECT
  api_key_hash,
  id AS api_key_id,
  account_id,
  user_id,
  company_id,
  revoked_at
FROM api_keys
WHERE (created_at > $1 AND created_at <= $2)
   OR (updated_at > $1 AND updated_at <= $2)
   OR (revoked_at IS NOT NULL AND revoked_at > $1 AND revoked_at <= $2);
"#;

    pub(super) const Q_USER_EMAIL_BY_ID: &'static str = r#"
SELECT
  COALESCE(email, '') AS email,
  task_limit_concurrent,
  task_limit_daily
FROM users
WHERE id = $1;
"#;

    pub(super) const Q_FULL_COMPANIES_META: &'static str = r#"
SELECT id, name, task_limit_concurrent, task_limit_daily FROM companies;
"#;

    pub(super) const Q_DELTA_COMPANIES_META: &'static str = r#"
SELECT id, name, task_limit_concurrent, task_limit_daily
FROM companies
WHERE (updated_at > $1 AND updated_at <= $2)
   OR (created_at > $1 AND created_at <= $2);
"#;

    pub(super) const Q_COMPANY_META_BY_ID: &'static str = r#"
SELECT id, name, task_limit_concurrent, task_limit_daily
FROM companies
WHERE id = $1
LIMIT 1;
"#;

    pub(super) const Q_FULL_COMPANY_KEYS: &'static str = r#"
SELECT company_id, api_key_hash
FROM api_keys
WHERE key_scope = 'company'
  AND revoked_at IS NULL
  AND company_id IS NOT NULL;
"#;

    pub(super) const Q_COMPANY_KEYS_FOR_IDS: &'static str = r#"
SELECT company_id, api_key_hash
FROM api_keys
WHERE key_scope = 'company'
  AND revoked_at IS NULL
  AND company_id = ANY($1);
"#;

    pub(super) const Q_DELTA_COMPANY_KEYS: &'static str = r#"
WITH changed AS (
  SELECT DISTINCT company_id
  FROM api_keys
  WHERE key_scope = 'company'
    AND company_id IS NOT NULL
    AND (
      (created_at > $1 AND created_at <= $2)
      OR (updated_at > $1 AND updated_at <= $2)
      OR (revoked_at IS NOT NULL AND revoked_at > $1 AND revoked_at <= $2)
    )
)
SELECT c.company_id,
       COALESCE(
           array_agg(k.api_key_hash) FILTER (
             WHERE k.key_scope = 'company'
               AND k.revoked_at IS NULL
           ),
           '{}'::bytea[]
       ) AS api_key_hashes
FROM changed c
LEFT JOIN api_keys k
  ON k.company_id = c.company_id
 AND k.key_scope = 'company'
GROUP BY c.company_id;
"#;

    pub(super) const COPY_ACTIVITY_EVENTS: &'static str = "\
COPY activity_events (\
task_id, \
account_id, \
user_id, \
company_id, \
api_key_id, \
session_id, \
action, \
event_family, \
client_origin, \
task_kind, \
model, \
gateway_name, \
metadata_json, \
created_at\
) FROM STDIN WITH (FORMAT text)";

    pub(super) const COPY_WORKER_EVENTS: &'static str = "\
COPY worker_events (\
task_id, \
worker_id, \
action, \
task_kind, \
model, \
reason, \
gateway_name, \
metadata_json, \
created_at\
) FROM STDIN WITH (FORMAT text)";

    pub async fn fetch_all_user_key_hashes(
        &self,
    ) -> Result<Vec<(i64, String, u64, u64, Vec<Vec<u8>>)>> {
        let rows = self.query_prepared(StmtKey::AllUserKeyHashes, &[]).await?;
        rows.into_iter()
            .map(decode_user_key_hashes_row)
            .collect::<Result<Vec<_>>>()
    }

    pub async fn fetch_delta_user_key_hashes(
        &self,
        since: i64,
        until: i64,
    ) -> Result<Vec<(i64, String, u64, u64, Vec<Vec<u8>>)>> {
        let params: [&(dyn ToSql + Sync); 2] = [&since, &until];
        let rows = self
            .query_prepared(StmtKey::DeltaUserKeyHashes, &params)
            .await?;
        rows.into_iter()
            .map(decode_user_key_hashes_row)
            .collect::<Result<Vec<_>>>()
    }

    pub async fn fetch_user_meta(&self, user_id: i64) -> Result<Option<(String, u64, u64)>> {
        let params: [&(dyn ToSql + Sync); 1] = [&user_id];
        let rows = self.query_prepared(StmtKey::UserEmailById, &params).await?;
        rows.into_iter()
            .next()
            .map(decode_user_meta_row)
            .transpose()
    }

    pub async fn fetch_all_api_key_billing_owners(&self) -> Result<Vec<ApiKeyBillingOwnerRow>> {
        let rows = self
            .query_prepared(StmtKey::AllApiKeyBillingOwners, &[])
            .await?;
        Ok(rows
            .into_iter()
            .map(|row| ApiKeyBillingOwnerRow {
                api_key_hash: row.get("api_key_hash"),
                owner: GenerationBillingOwner {
                    api_key_id: row.get("api_key_id"),
                    account_id: row.get("account_id"),
                    user_id: row.get("user_id"),
                    company_id: row.get("company_id"),
                },
                revoked_at: None,
            })
            .collect())
    }

    pub async fn fetch_delta_api_key_billing_owners(
        &self,
        since: i64,
        until: i64,
    ) -> Result<Vec<ApiKeyBillingOwnerRow>> {
        let params: [&(dyn ToSql + Sync); 2] = [&since, &until];
        let rows = self
            .query_prepared(StmtKey::DeltaApiKeyBillingOwners, &params)
            .await?;
        Ok(rows
            .into_iter()
            .map(|row| ApiKeyBillingOwnerRow {
                api_key_hash: row.get("api_key_hash"),
                owner: GenerationBillingOwner {
                    api_key_id: row.get("api_key_id"),
                    account_id: row.get("account_id"),
                    user_id: row.get("user_id"),
                    company_id: row.get("company_id"),
                },
                revoked_at: row.get("revoked_at"),
            })
            .collect())
    }

    pub async fn fetch_full_companies_meta(&self) -> Result<Vec<(uuid::Uuid, (String, u64, u64))>> {
        let rows = self.query_prepared(StmtKey::FullCompaniesMeta, &[]).await?;
        rows.into_iter()
            .map(decode_company_meta_row)
            .collect::<Result<Vec<_>>>()
    }

    pub async fn fetch_delta_companies_meta(
        &self,
        since: i64,
        until: i64,
    ) -> Result<Vec<(uuid::Uuid, (String, u64, u64))>> {
        let params: [&(dyn ToSql + Sync); 2] = [&since, &until];
        let rows = self
            .query_prepared(StmtKey::DeltaCompaniesMeta, &params)
            .await?;
        rows.into_iter()
            .map(decode_company_meta_row)
            .collect::<Result<Vec<_>>>()
    }

    pub async fn fetch_company_meta(
        &self,
        company_id: uuid::Uuid,
    ) -> Result<Option<(String, u64, u64)>> {
        let params: [&(dyn ToSql + Sync); 1] = [&company_id];
        let rows = self
            .query_prepared(StmtKey::CompanyMetaById, &params)
            .await?;
        rows.into_iter()
            .next()
            .map(decode_company_limits_row)
            .transpose()
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
        since: i64,
        until: i64,
    ) -> Result<Vec<(uuid::Uuid, Vec<Vec<u8>>)>> {
        let params: [&(dyn ToSql + Sync); 2] = [&since, &until];
        let rows = self
            .query_prepared(StmtKey::DeltaCompanyKeys, &params)
            .await?;
        Ok(rows
            .into_iter()
            .map(decode_company_key_hashes_row)
            .collect())
    }

    pub async fn fetch_gateway_settings(&self) -> Result<Option<GatewaySettingsRow>> {
        let rows = self.query_prepared(StmtKey::GatewaySettings, &[]).await?;
        rows.into_iter()
            .next()
            .map(decode_gateway_settings_row)
            .transpose()
    }

    pub async fn server_time_ms(&self) -> Result<i64> {
        let rows = self.query_prepared(StmtKey::ServerTimeMs, &[]).await?;
        let row = rows
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("Server time query returned no rows"))?;
        let value: i64 = row.get("server_time_ms");
        Ok(value)
    }

    pub async fn record_activity_events_batch(&self, rows: &[ActivityEventRow]) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let batch_size = self.events_copy_batch_size;
        let first_attempt = {
            let _guard = self.activity_copy_lane.lock().await;
            let client = self.activity_copy_lane.ensure_client().await?;
            Self::write_activity_events_batch(client.as_ref(), rows, batch_size).await
        };

        match first_attempt {
            Ok(()) => Ok(()),
            Err(error) => {
                let should_retry = super::connection::should_reconnect(&error)
                    || error
                        .to_string()
                        .contains("another command is already in progress");
                if !should_retry {
                    return Err(anyhow!(error));
                }

                let delay = super::connection::reconnect_delay();
                info!(
                    "Activity event COPY failed: {}. Trying to reconnect after {:?}...",
                    error, delay
                );
                // Release the lane while backing off so one failed batch does not block all
                // subsequent event flushes behind a timer wait.
                super::connection::sleep_or_shutdown(
                    self.activity_copy_lane.shutdown_token(),
                    delay,
                )
                .await?;

                {
                    let _guard = self.activity_copy_lane.lock().await;
                    let client = self.activity_copy_lane.reconnect().await?;
                    Self::write_activity_events_batch(client.as_ref(), rows, batch_size)
                        .await
                        .map_err(anyhow::Error::from)
                }
            }
        }
    }

    pub async fn record_worker_events_batch(&self, rows: &[WorkerEventRow]) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let batch_size = self.events_copy_batch_size;
        let first_attempt = {
            let _guard = self.worker_copy_lane.lock().await;
            let client = self.worker_copy_lane.ensure_client().await?;
            Self::write_worker_events_batch(client.as_ref(), rows, batch_size).await
        };

        match first_attempt {
            Ok(()) => Ok(()),
            Err(error) => {
                let should_retry = super::connection::should_reconnect(&error)
                    || error
                        .to_string()
                        .contains("another command is already in progress");
                if !should_retry {
                    return Err(anyhow!(error));
                }

                let delay = super::connection::reconnect_delay();
                info!(
                    "Worker event COPY failed: {}. Trying to reconnect after {:?}...",
                    error, delay
                );
                // Release the lane while backing off so one failed batch does not block all
                // subsequent event flushes behind a timer wait.
                super::connection::sleep_or_shutdown(self.worker_copy_lane.shutdown_token(), delay)
                    .await?;

                {
                    let _guard = self.worker_copy_lane.lock().await;
                    let client = self.worker_copy_lane.reconnect().await?;
                    Self::write_worker_events_batch(client.as_ref(), rows, batch_size)
                        .await
                        .map_err(anyhow::Error::from)
                }
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
