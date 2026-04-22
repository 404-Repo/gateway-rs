use anyhow::{Result, anyhow};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::Arc;
use tokio_postgres::types::ToSql;
use uuid::Uuid;

use super::{Database, connection::StmtKey};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GenerationBillingOwner {
    pub api_key_id: i64,
    pub account_id: i64,
    pub user_id: Option<i64>,
    pub company_id: Option<Uuid>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GenerationTaskAccessOwner {
    pub account_id: Option<i64>,
    pub user_id: Option<i64>,
    pub company_id: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateGenerationTaskInput {
    pub task_id: Uuid,
    pub account_id: Option<i64>,
    pub user_id: Option<i64>,
    pub company_id: Option<Uuid>,
    pub api_key_id: Option<i64>,
    pub task_kind: String,
    pub model: String,
    pub expected_results: i32,
    pub deadline_at_ms: i64,
    pub gateway_name: String,
    pub client_origin: String,
    pub request_json: String,
    // Registered-user free-tier settings forwarded into the billing/task lifecycle function.
    // `None` disables registered free billing for this task submission path.
    pub registered_generation_limit: Option<i32>,
    pub registered_window_ms: Option<i64>,
    pub now_ms: i64,
    // Guest free-tier settings forwarded into the billing/task lifecycle function.
    // `None` disables guest-free billing for this task submission path.
    pub guest_generation_limit: Option<i32>,
    pub guest_window_ms: Option<i64>,
    pub guest_key_hash: Option<Vec<u8>>,
    pub guest_access_mode: Option<String>,
    pub generic_global_limit: Option<i32>,
    pub generic_per_ip_limit: Option<i32>,
    pub generic_window_ms: Option<i64>,
    pub generic_key_hash: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateGenerationTaskRejection {
    pub error_code: String,
    pub error_message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CreateGenerationTaskOutcome {
    Created,
    Rejected(CreateGenerationTaskRejection),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FinalizeGenerationTaskAssignmentInput {
    pub task_id: Uuid,
    pub worker_hotkey: String,
    pub worker_id: String,
    pub assignment_token: Option<Uuid>,
    pub assignment_status: String,
    pub failure_reason: Option<String>,
    pub result_metadata_json: String,
    pub completed_at_ms: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RecordedGenerationTaskAssignmentAction {
    Assigned,
    Requeue,
    Retire,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordedGenerationTaskAssignment {
    pub task_id: Uuid,
    pub assignment_token: Option<Uuid>,
    pub action: RecordedGenerationTaskAssignmentAction,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FinalizeGenerationTaskAssignmentConflict {
    pub stored_assignment_status: String,
    pub stored_failure_reason: Option<String>,
    pub stored_result_metadata_json: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FinalizeGenerationTaskAssignmentOutcome {
    Applied,
    DuplicateReplay,
    DuplicateConflict(FinalizeGenerationTaskAssignmentConflict),
}

#[derive(Debug, Clone)]
pub struct GenerationTaskStatusSnapshot {
    pub status: String,
    pub finished_results_count: i32,
    pub success_count: i32,
    pub error_message: Option<String>,
    pub result_worker_id: Option<String>,
}

#[async_trait]
pub trait TaskLifecycleStore: Send + Sync + 'static {
    async fn create_generation_task(
        &self,
        input: &CreateGenerationTaskInput,
    ) -> Result<CreateGenerationTaskOutcome>;

    async fn record_generation_task_assignments(
        &self,
        task_ids: &[Uuid],
        worker_hotkey: &str,
        worker_id: &str,
        assigned_at_ms: i64,
    ) -> Result<Vec<RecordedGenerationTaskAssignment>>;

    async fn finalize_generation_task_assignment(
        &self,
        input: &FinalizeGenerationTaskAssignmentInput,
    ) -> Result<FinalizeGenerationTaskAssignmentOutcome>;

    async fn expire_generation_tasks(&self, limit: i32, now_ms: i64) -> Result<Vec<Uuid>>;

    async fn purge_terminal_generation_tasks(
        &self,
        limit: i32,
        completed_before_ms: i64,
    ) -> Result<Vec<Uuid>>;

    async fn get_generation_task_access_owner(
        &self,
        task_id: Uuid,
    ) -> Result<Option<GenerationTaskAccessOwner>>;

    async fn get_generation_task_status_snapshot(
        &self,
        task_id: Uuid,
    ) -> Result<Option<GenerationTaskStatusSnapshot>>;

    async fn get_generation_task_generic_key_hash(&self, task_id: Uuid)
    -> Result<Option<[u8; 16]>>;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct NoopTaskLifecycleStore;

#[async_trait]
impl TaskLifecycleStore for NoopTaskLifecycleStore {
    async fn create_generation_task(
        &self,
        _input: &CreateGenerationTaskInput,
    ) -> Result<CreateGenerationTaskOutcome> {
        Ok(CreateGenerationTaskOutcome::Created)
    }

    async fn record_generation_task_assignments(
        &self,
        task_ids: &[Uuid],
        _worker_hotkey: &str,
        _worker_id: &str,
        _assigned_at_ms: i64,
    ) -> Result<Vec<RecordedGenerationTaskAssignment>> {
        Ok(task_ids
            .iter()
            .copied()
            .map(|task_id| RecordedGenerationTaskAssignment {
                task_id,
                assignment_token: Some(Uuid::new_v4()),
                action: RecordedGenerationTaskAssignmentAction::Assigned,
            })
            .collect())
    }

    async fn finalize_generation_task_assignment(
        &self,
        _input: &FinalizeGenerationTaskAssignmentInput,
    ) -> Result<FinalizeGenerationTaskAssignmentOutcome> {
        Ok(FinalizeGenerationTaskAssignmentOutcome::Applied)
    }

    async fn expire_generation_tasks(&self, _limit: i32, _now_ms: i64) -> Result<Vec<Uuid>> {
        Ok(Vec::new())
    }

    async fn purge_terminal_generation_tasks(
        &self,
        _limit: i32,
        _completed_before_ms: i64,
    ) -> Result<Vec<Uuid>> {
        Ok(Vec::new())
    }

    async fn get_generation_task_access_owner(
        &self,
        _task_id: Uuid,
    ) -> Result<Option<GenerationTaskAccessOwner>> {
        Ok(None)
    }

    async fn get_generation_task_status_snapshot(
        &self,
        _task_id: Uuid,
    ) -> Result<Option<GenerationTaskStatusSnapshot>> {
        Ok(None)
    }

    async fn get_generation_task_generic_key_hash(
        &self,
        _task_id: Uuid,
    ) -> Result<Option<[u8; 16]>> {
        Ok(None)
    }
}

#[derive(Clone)]
pub enum TaskLifecycleStoreHandle {
    Database(Arc<Database>),
    Noop(NoopTaskLifecycleStore),
    #[cfg(test)]
    Shared(Arc<dyn TaskLifecycleStore>),
}

impl Default for TaskLifecycleStoreHandle {
    fn default() -> Self {
        Self::Noop(NoopTaskLifecycleStore)
    }
}

impl Database {
    pub(super) const Q_FETCH_API_KEY_BILLING_OWNER: &'static str = r#"
SELECT
  a.id AS api_key_id,
  a.account_id,
  a.user_id,
  a.company_id
FROM api_keys a
WHERE a.api_key_hash = $1
  AND a.revoked_at IS NULL
LIMIT 1;
"#;

    pub(super) const Q_GENERATION_SUBMIT_TASK: &'static str = r#"
SELECT
  ok,
  error_code,
  error_message,
  reservation_id,
  billing_mode,
  reservation_status,
  task_status
FROM generation_submit_task(
  $1,
  $2,
  $3,
  $4,
  $5,
  $6,
  $7,
  $8,
  $9,
  $10,
  $11,
  $12::TEXT::jsonb,
  $13,
  $14,
  $15,
  $16,
  $17,
  $18,
  $19,
  $20,
  $21,
  $22,
  $23
);
"#;

    pub(super) const Q_GENERATION_RECORD_TASK_ASSIGNMENTS: &'static str = r#"
SELECT task_id, assignment_id, assignment_token, task_status, delivery_action
FROM generation_record_task_assignments($1, $2, $3, $4);
"#;

    pub(super) const Q_GENERATION_FINALIZE_TASK_ASSIGNMENT: &'static str = r#"
SELECT
  assignment_outcome,
  stored_assignment_status,
  stored_failure_reason,
  stored_result_metadata_json::TEXT AS stored_result_metadata_json
FROM generation_finalize_task_assignment(
  $1,
  $2,
  $3,
  $4,
  $5,
  $6,
  $7::TEXT::jsonb,
  $8
);
"#;

    pub(super) const Q_GENERATION_EXPIRE_TASKS: &'static str = r#"
SELECT task_id
FROM generation_expire_tasks($1, $2);
"#;

    pub(super) const Q_GENERATION_PURGE_TERMINAL_TASKS: &'static str = r#"
SELECT task_id
FROM generation_purge_terminal_tasks($1, $2);
"#;

    pub(super) const Q_GENERATION_TASK_ACCOUNT_ID: &'static str = r#"
SELECT
  task_row.account_id,
  task_row.user_id,
  task_row.company_id
FROM generation_tasks AS task_row
WHERE task_row.id = $1
LIMIT 1;
"#;

    pub(super) const Q_GENERATION_TASK_STATUS_SNAPSHOT: &'static str = r#"
SELECT
  task_row.status,
  task_row.finished_results_count,
  task_row.success_count,
  task_row.error_message,
  task_row.result_json->>'worker_id' AS result_worker_id
FROM generation_tasks AS task_row
WHERE task_row.id = $1
LIMIT 1;
"#;

    pub(super) const Q_GENERATION_TASK_GENERIC_KEY_HASH: &'static str = r#"
SELECT reservation_row.guest_key_hash
FROM generation_tasks AS task_row
INNER JOIN generation_reservations AS reservation_row
  ON reservation_row.id = task_row.reservation_id
WHERE task_row.id = $1
  AND task_row.client_origin = 'guest_generic'
  AND reservation_row.billing_mode = 'guest_free'
  AND reservation_row.guest_key_hash IS NOT NULL
LIMIT 1;
"#;

    pub async fn fetch_api_key_billing_owner(
        &self,
        api_key_hash: &[u8; 32],
    ) -> Result<Option<GenerationBillingOwner>> {
        if self.is_mock() {
            return Ok(None);
        }
        let params: [&(dyn ToSql + Sync); 1] = [&api_key_hash.as_slice()];
        let row = self
            .query_prepared(StmtKey::FetchApiKeyBillingOwner, &params)
            .await?
            .into_iter()
            .next();

        Ok(row.map(|row| GenerationBillingOwner {
            api_key_id: row.get("api_key_id"),
            account_id: row.get("account_id"),
            user_id: row.get("user_id"),
            company_id: row.get("company_id"),
        }))
    }

    pub async fn create_generation_task(
        &self,
        input: &CreateGenerationTaskInput,
    ) -> Result<CreateGenerationTaskOutcome> {
        let params: [&(dyn ToSql + Sync); 23] = [
            &input.task_id,
            &input.account_id,
            &input.user_id,
            &input.company_id,
            &input.api_key_id,
            &input.task_kind,
            &input.model,
            &input.expected_results,
            &input.deadline_at_ms,
            &input.gateway_name,
            &input.client_origin,
            &input.request_json,
            &input.registered_generation_limit,
            &input.registered_window_ms,
            &input.now_ms,
            &input.guest_generation_limit,
            &input.guest_window_ms,
            &input.guest_key_hash,
            &input.guest_access_mode,
            &input.generic_global_limit,
            &input.generic_per_ip_limit,
            &input.generic_window_ms,
            &input.generic_key_hash,
        ];
        let row = self
            .query_prepared(StmtKey::GenerationSubmitTask, &params)
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("generation_submit_task returned no rows"))?;
        let created: bool = row.get("ok");
        if created {
            return Ok(CreateGenerationTaskOutcome::Created);
        }

        let error_code: Option<String> = row.get("error_code");
        let error_message: Option<String> = row.get("error_message");
        let error_code = error_code
            .ok_or_else(|| anyhow!("generation_submit_task rejected without error_code"))?;
        let error_message = error_message
            .ok_or_else(|| anyhow!("generation_submit_task rejected without error_message"))?;

        Ok(CreateGenerationTaskOutcome::Rejected(
            CreateGenerationTaskRejection {
                error_code,
                error_message,
            },
        ))
    }

    pub async fn record_generation_task_assignments(
        &self,
        task_ids: &[Uuid],
        worker_hotkey: &str,
        worker_id: &str,
        assigned_at_ms: i64,
    ) -> Result<Vec<RecordedGenerationTaskAssignment>> {
        if task_ids.is_empty() {
            return Ok(Vec::new());
        }
        let expected_task_ids: HashSet<Uuid> = task_ids.iter().copied().collect();
        let mut recorded_assignments = Vec::with_capacity(task_ids.len());
        let mut seen_task_ids = HashSet::with_capacity(task_ids.len());
        let params: [&(dyn ToSql + Sync); 4] =
            [&task_ids, &worker_hotkey, &worker_id, &assigned_at_ms];
        for row in self
            .query_prepared(StmtKey::GenerationRecordTaskAssignments, &params)
            .await?
        {
            let task_id: Uuid = row.get("task_id");
            if !expected_task_ids.contains(&task_id) {
                return Err(anyhow!(
                    "generation_record_task_assignments returned unexpected task_id: {}",
                    task_id
                ));
            }
            if !seen_task_ids.insert(task_id) {
                return Err(anyhow!(
                    "generation_record_task_assignments returned duplicate task_id: {}",
                    task_id
                ));
            }
            let task_status: String = row.get("task_status");
            let delivery_action = match row.get::<_, String>("delivery_action").as_str() {
                "assigned" => RecordedGenerationTaskAssignmentAction::Assigned,
                "requeue" => RecordedGenerationTaskAssignmentAction::Requeue,
                "retire" => RecordedGenerationTaskAssignmentAction::Retire,
                other => {
                    return Err(anyhow!(
                        "generation_record_task_assignments returned unexpected delivery_action: {}",
                        other
                    ));
                }
            };
            let assignment_id: Option<i64> = row.get("assignment_id");
            let assignment_token: Option<Uuid> = row.get("assignment_token");
            match delivery_action {
                RecordedGenerationTaskAssignmentAction::Assigned => {
                    if !matches!(task_status.as_str(), "queued" | "running") {
                        return Err(anyhow!(
                            "generation_record_task_assignments returned unexpected task_status for assigned task: {}",
                            task_status
                        ));
                    }
                    if assignment_id.is_none() || assignment_token.is_none() {
                        return Err(anyhow!(
                            "generation_record_task_assignments returned an assigned task without durable assignment data"
                        ));
                    }
                }
                RecordedGenerationTaskAssignmentAction::Requeue => {
                    if !matches!(task_status.as_str(), "queued" | "running") {
                        return Err(anyhow!(
                            "generation_record_task_assignments returned unexpected task_status for requeued task: {}",
                            task_status
                        ));
                    }
                    if assignment_id.is_some() || assignment_token.is_some() {
                        return Err(anyhow!(
                            "generation_record_task_assignments returned durable assignment data for a requeued task"
                        ));
                    }
                }
                RecordedGenerationTaskAssignmentAction::Retire => {
                    if assignment_id.is_some() || assignment_token.is_some() {
                        return Err(anyhow!(
                            "generation_record_task_assignments returned durable assignment data for a retired task"
                        ));
                    }
                }
            }
            recorded_assignments.push(RecordedGenerationTaskAssignment {
                task_id,
                assignment_token,
                action: delivery_action,
            });
        }
        if seen_task_ids != expected_task_ids {
            return Err(anyhow!(
                "generation_record_task_assignments did not return an outcome for every requested task"
            ));
        }
        Ok(recorded_assignments)
    }

    pub async fn finalize_generation_task_assignment(
        &self,
        input: &FinalizeGenerationTaskAssignmentInput,
    ) -> Result<FinalizeGenerationTaskAssignmentOutcome> {
        let params: [&(dyn ToSql + Sync); 8] = [
            &input.task_id,
            &input.worker_hotkey,
            &input.worker_id,
            &input.assignment_token,
            &input.assignment_status,
            &input.failure_reason.as_deref(),
            &input.result_metadata_json,
            &input.completed_at_ms,
        ];
        let row = self
            .query_prepared(StmtKey::GenerationFinalizeTaskAssignment, &params)
            .await?
            .into_iter()
            .next();
        let Some(row) = row else {
            return Err(anyhow!(
                "generation_finalize_task_assignment returned no rows"
            ));
        };
        let outcome: String = row.get("assignment_outcome");
        match outcome.as_str() {
            "applied" => Ok(FinalizeGenerationTaskAssignmentOutcome::Applied),
            "duplicate_replay" => Ok(FinalizeGenerationTaskAssignmentOutcome::DuplicateReplay),
            "duplicate_conflict" => Ok(FinalizeGenerationTaskAssignmentOutcome::DuplicateConflict(
                FinalizeGenerationTaskAssignmentConflict {
                    stored_assignment_status: row.get("stored_assignment_status"),
                    stored_failure_reason: row.get("stored_failure_reason"),
                    stored_result_metadata_json: row.get("stored_result_metadata_json"),
                },
            )),
            other => Err(anyhow!(
                "unsupported assignment finalize outcome: {}",
                other
            )),
        }
    }

    pub async fn expire_generation_tasks(&self, limit: i32, now_ms: i64) -> Result<Vec<Uuid>> {
        let params: [&(dyn ToSql + Sync); 2] = [&limit, &now_ms];
        let rows = self
            .query_prepared(StmtKey::GenerationExpireTasks, &params)
            .await?;
        Ok(rows.into_iter().map(|row| row.get("task_id")).collect())
    }

    pub async fn purge_terminal_generation_tasks(
        &self,
        limit: i32,
        completed_before_ms: i64,
    ) -> Result<Vec<Uuid>> {
        let params: [&(dyn ToSql + Sync); 2] = [&limit, &completed_before_ms];
        let rows = self
            .query_prepared(StmtKey::GenerationPurgeTerminalTasks, &params)
            .await?;
        Ok(rows.into_iter().map(|row| row.get("task_id")).collect())
    }

    pub async fn get_generation_task_access_owner(
        &self,
        task_id: Uuid,
    ) -> Result<Option<GenerationTaskAccessOwner>> {
        let params: [&(dyn ToSql + Sync); 1] = [&task_id];
        let row = self
            .query_prepared(StmtKey::GenerationTaskAccountId, &params)
            .await?
            .into_iter()
            .next();
        Ok(row.and_then(|row| {
            let owner = GenerationTaskAccessOwner {
                account_id: row.get("account_id"),
                user_id: row.get("user_id"),
                company_id: row.get("company_id"),
            };
            if owner.account_id.is_none() && owner.user_id.is_none() && owner.company_id.is_none() {
                None
            } else {
                Some(owner)
            }
        }))
    }

    pub async fn get_generation_task_status_snapshot(
        &self,
        task_id: Uuid,
    ) -> Result<Option<GenerationTaskStatusSnapshot>> {
        let params: [&(dyn ToSql + Sync); 1] = [&task_id];
        let row = self
            .query_prepared(StmtKey::GenerationTaskStatusSnapshot, &params)
            .await?
            .into_iter()
            .next();

        Ok(row.map(|row| GenerationTaskStatusSnapshot {
            status: row.get("status"),
            finished_results_count: row.get("finished_results_count"),
            success_count: row.get("success_count"),
            error_message: row.get("error_message"),
            result_worker_id: row.get("result_worker_id"),
        }))
    }

    pub async fn get_generation_task_generic_key_hash(
        &self,
        task_id: Uuid,
    ) -> Result<Option<[u8; 16]>> {
        let params: [&(dyn ToSql + Sync); 1] = [&task_id];
        let row = self
            .query_prepared(StmtKey::GenerationTaskGenericKeyHash, &params)
            .await?
            .into_iter()
            .next();

        Ok(row.and_then(|row| {
            let bytes: Vec<u8> = row.get("guest_key_hash");
            if bytes.len() != 16 {
                return None;
            }
            let mut hash = [0u8; 16];
            hash.copy_from_slice(&bytes);
            Some(hash)
        }))
    }
}

#[async_trait]
impl TaskLifecycleStore for Database {
    async fn create_generation_task(
        &self,
        input: &CreateGenerationTaskInput,
    ) -> Result<CreateGenerationTaskOutcome> {
        Database::create_generation_task(self, input).await
    }

    async fn record_generation_task_assignments(
        &self,
        task_ids: &[Uuid],
        worker_hotkey: &str,
        worker_id: &str,
        assigned_at_ms: i64,
    ) -> Result<Vec<RecordedGenerationTaskAssignment>> {
        Database::record_generation_task_assignments(
            self,
            task_ids,
            worker_hotkey,
            worker_id,
            assigned_at_ms,
        )
        .await
    }

    async fn finalize_generation_task_assignment(
        &self,
        input: &FinalizeGenerationTaskAssignmentInput,
    ) -> Result<FinalizeGenerationTaskAssignmentOutcome> {
        Database::finalize_generation_task_assignment(self, input).await
    }

    async fn expire_generation_tasks(&self, limit: i32, now_ms: i64) -> Result<Vec<Uuid>> {
        Database::expire_generation_tasks(self, limit, now_ms).await
    }

    async fn purge_terminal_generation_tasks(
        &self,
        limit: i32,
        completed_before_ms: i64,
    ) -> Result<Vec<Uuid>> {
        Database::purge_terminal_generation_tasks(self, limit, completed_before_ms).await
    }

    async fn get_generation_task_access_owner(
        &self,
        task_id: Uuid,
    ) -> Result<Option<GenerationTaskAccessOwner>> {
        Database::get_generation_task_access_owner(self, task_id).await
    }

    async fn get_generation_task_status_snapshot(
        &self,
        task_id: Uuid,
    ) -> Result<Option<GenerationTaskStatusSnapshot>> {
        Database::get_generation_task_status_snapshot(self, task_id).await
    }

    async fn get_generation_task_generic_key_hash(
        &self,
        task_id: Uuid,
    ) -> Result<Option<[u8; 16]>> {
        Database::get_generation_task_generic_key_hash(self, task_id).await
    }
}

#[async_trait]
impl TaskLifecycleStore for TaskLifecycleStoreHandle {
    async fn create_generation_task(
        &self,
        input: &CreateGenerationTaskInput,
    ) -> Result<CreateGenerationTaskOutcome> {
        match self {
            TaskLifecycleStoreHandle::Database(db) => db.create_generation_task(input).await,
            TaskLifecycleStoreHandle::Noop(store) => store.create_generation_task(input).await,
            #[cfg(test)]
            TaskLifecycleStoreHandle::Shared(store) => store.create_generation_task(input).await,
        }
    }

    async fn record_generation_task_assignments(
        &self,
        task_ids: &[Uuid],
        worker_hotkey: &str,
        worker_id: &str,
        assigned_at_ms: i64,
    ) -> Result<Vec<RecordedGenerationTaskAssignment>> {
        match self {
            TaskLifecycleStoreHandle::Database(db) => {
                db.record_generation_task_assignments(
                    task_ids,
                    worker_hotkey,
                    worker_id,
                    assigned_at_ms,
                )
                .await
            }
            TaskLifecycleStoreHandle::Noop(store) => {
                store
                    .record_generation_task_assignments(
                        task_ids,
                        worker_hotkey,
                        worker_id,
                        assigned_at_ms,
                    )
                    .await
            }
            #[cfg(test)]
            TaskLifecycleStoreHandle::Shared(store) => {
                store
                    .record_generation_task_assignments(
                        task_ids,
                        worker_hotkey,
                        worker_id,
                        assigned_at_ms,
                    )
                    .await
            }
        }
    }

    async fn finalize_generation_task_assignment(
        &self,
        input: &FinalizeGenerationTaskAssignmentInput,
    ) -> Result<FinalizeGenerationTaskAssignmentOutcome> {
        match self {
            TaskLifecycleStoreHandle::Database(db) => {
                db.finalize_generation_task_assignment(input).await
            }
            TaskLifecycleStoreHandle::Noop(store) => {
                store.finalize_generation_task_assignment(input).await
            }
            #[cfg(test)]
            TaskLifecycleStoreHandle::Shared(store) => {
                store.finalize_generation_task_assignment(input).await
            }
        }
    }

    async fn expire_generation_tasks(&self, limit: i32, now_ms: i64) -> Result<Vec<Uuid>> {
        match self {
            TaskLifecycleStoreHandle::Database(db) => {
                db.expire_generation_tasks(limit, now_ms).await
            }
            TaskLifecycleStoreHandle::Noop(store) => {
                store.expire_generation_tasks(limit, now_ms).await
            }
            #[cfg(test)]
            TaskLifecycleStoreHandle::Shared(store) => {
                store.expire_generation_tasks(limit, now_ms).await
            }
        }
    }

    async fn purge_terminal_generation_tasks(
        &self,
        limit: i32,
        completed_before_ms: i64,
    ) -> Result<Vec<Uuid>> {
        match self {
            TaskLifecycleStoreHandle::Database(db) => {
                db.purge_terminal_generation_tasks(limit, completed_before_ms)
                    .await
            }
            TaskLifecycleStoreHandle::Noop(store) => {
                store
                    .purge_terminal_generation_tasks(limit, completed_before_ms)
                    .await
            }
            #[cfg(test)]
            TaskLifecycleStoreHandle::Shared(store) => {
                store
                    .purge_terminal_generation_tasks(limit, completed_before_ms)
                    .await
            }
        }
    }

    async fn get_generation_task_access_owner(
        &self,
        task_id: Uuid,
    ) -> Result<Option<GenerationTaskAccessOwner>> {
        match self {
            TaskLifecycleStoreHandle::Database(db) => {
                db.get_generation_task_access_owner(task_id).await
            }
            TaskLifecycleStoreHandle::Noop(store) => {
                store.get_generation_task_access_owner(task_id).await
            }
            #[cfg(test)]
            TaskLifecycleStoreHandle::Shared(store) => {
                store.get_generation_task_access_owner(task_id).await
            }
        }
    }

    async fn get_generation_task_status_snapshot(
        &self,
        task_id: Uuid,
    ) -> Result<Option<GenerationTaskStatusSnapshot>> {
        match self {
            TaskLifecycleStoreHandle::Database(db) => {
                db.get_generation_task_status_snapshot(task_id).await
            }
            TaskLifecycleStoreHandle::Noop(store) => {
                store.get_generation_task_status_snapshot(task_id).await
            }
            #[cfg(test)]
            TaskLifecycleStoreHandle::Shared(store) => {
                store.get_generation_task_status_snapshot(task_id).await
            }
        }
    }

    async fn get_generation_task_generic_key_hash(
        &self,
        task_id: Uuid,
    ) -> Result<Option<[u8; 16]>> {
        match self {
            TaskLifecycleStoreHandle::Database(db) => {
                db.get_generation_task_generic_key_hash(task_id).await
            }
            TaskLifecycleStoreHandle::Noop(store) => {
                store.get_generation_task_generic_key_hash(task_id).await
            }
            #[cfg(test)]
            TaskLifecycleStoreHandle::Shared(store) => {
                store.get_generation_task_generic_key_hash(task_id).await
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn noop_task_lifecycle_store_helpers_are_noops() {
        let store = NoopTaskLifecycleStore;
        let task_id = Uuid::new_v4();
        let create_input = CreateGenerationTaskInput {
            task_id,
            account_id: Some(1),
            user_id: Some(1),
            company_id: None,
            api_key_id: Some(1),
            task_kind: "text_to_3d".to_string(),
            model: "test-model".to_string(),
            expected_results: 1,
            deadline_at_ms: 1000,
            gateway_name: "gateway".to_string(),
            client_origin: "gen".to_string(),
            request_json: "{}".to_string(),
            registered_generation_limit: Some(1),
            registered_window_ms: Some(60_000),
            now_ms: 1000,
            guest_generation_limit: None,
            guest_window_ms: None,
            guest_key_hash: None,
            guest_access_mode: None,
            generic_global_limit: None,
            generic_per_ip_limit: None,
            generic_window_ms: None,
            generic_key_hash: None,
        };

        assert_eq!(
            store
                .create_generation_task(&create_input)
                .await
                .expect("noop create should return created"),
            CreateGenerationTaskOutcome::Created
        );

        let recorded_assignments = store
            .record_generation_task_assignments(&[task_id], "worker-hotkey", "worker-id", 1000)
            .await
            .expect("noop record assignments should be a no-op");
        assert_eq!(recorded_assignments.len(), 1);
        assert_eq!(recorded_assignments[0].task_id, task_id);

        store
            .finalize_generation_task_assignment(&FinalizeGenerationTaskAssignmentInput {
                task_id,
                worker_hotkey: "worker-hotkey".to_string(),
                worker_id: "worker-id".to_string(),
                assignment_token: Some(Uuid::new_v4()),
                assignment_status: "completed".to_string(),
                failure_reason: None,
                result_metadata_json: "{}".to_string(),
                completed_at_ms: 2000,
            })
            .await
            .expect("noop finalize should be a no-op");

        assert!(
            store
                .get_generation_task_access_owner(task_id)
                .await
                .expect("noop task owner lookup should be a no-op")
                .is_none(),
            "noop task owner lookup should return none"
        );

        assert!(
            store
                .get_generation_task_status_snapshot(task_id)
                .await
                .expect("noop status snapshot should be a no-op")
                .is_none(),
            "noop status snapshot should return none"
        );

        assert!(
            store
                .expire_generation_tasks(16, 3000)
                .await
                .expect("noop expire should succeed")
                .is_empty()
        );
        assert!(
            store
                .purge_terminal_generation_tasks(16, 4000)
                .await
                .expect("noop purge should succeed")
                .is_empty()
        );
    }
}
