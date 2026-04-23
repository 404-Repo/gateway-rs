use super::store::Request;
use super::store::{RateLimitMutation, RateLimitMutationBatch, Subject, rate_limit_key};
use super::{NodeId, Raft, StateMachineStore};
use crate::api::request::GatewayInfoExtRef;
use crate::api::response::GatewayInfo;
use crate::api::response::GatewayInfoRef;
use crate::config_runtime::RuntimeConfigStore;
use crate::db::{
    ApiKeyLookup, ApiKeyValidator, CreateGenerationTaskInput, CreateGenerationTaskOutcome,
    EventRecorder, FinalizeGenerationTaskAssignmentInput, FinalizeGenerationTaskAssignmentOutcome,
    GatewayRuntimeSettingsStore, GenerationTaskAccessOwner, GenerationTaskStatusSnapshot,
    RecordedGenerationTaskAssignment, TaskLifecycleStore, TaskLifecycleStoreHandle,
};
use crate::http3::client::{Http3Client, Http3ClientBuilder};
use crate::task::TaskManager;
use anyhow::Result;
use backon::{ExponentialBuilder, Retryable};
use bytes::Bytes;
use openraft::{BasicNode, RaftMetrics};
use rmp_serde;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::net::IpAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{Mutex, watch};
use tracing::error;
use uuid::Uuid;

pub struct ActivityEventRef<'a> {
    pub user_id: Option<i64>,
    pub user_email: Option<&'a str>,
    pub account_id: Option<i64>,
    pub company_id: Option<Uuid>,
    pub company_name: Option<&'a str>,
    pub action: &'a str,
    pub client_origin: &'a str,
    pub task_kind: &'a str,
    pub model: Option<&'a str>,
    pub task_id: Option<Uuid>,
}

pub struct WorkerEventRef<'a> {
    pub task_id: Option<Uuid>,
    pub worker_id: Option<&'a str>,
    pub action: &'a str,
    pub task_kind: &'a str,
    pub model: Option<&'a str>,
    pub reason: Option<&'a str>,
    pub metadata_json: Option<serde_json::Value>,
}

#[derive(Serialize)]
enum RateLimitRequestRef<'a> {
    RateLimitMutations {
        request_id: u128,
        mutations: &'a [RateLimitMutation],
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TaskLifecycleWriteRequest {
    CreateGenerationTask {
        input: Box<CreateGenerationTaskInput>,
    },
    RecordGenerationTaskAssignments {
        task_ids: Vec<Uuid>,
        worker_hotkey: String,
        worker_id: String,
        assigned_at_ms: i64,
    },
    FinalizeGenerationTaskAssignment {
        input: Box<FinalizeGenerationTaskAssignmentInput>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TaskLifecycleWriteResponse {
    Ack,
    CreateGenerationTask(CreateGenerationTaskOutcome),
    RecordGenerationTaskAssignments(Vec<RecordedGenerationTaskAssignment>),
    FinalizeGenerationTaskAssignment(FinalizeGenerationTaskAssignmentOutcome),
}

#[derive(Debug)]
pub enum GatewayStateError {
    NotFound(String),
    DeserializeError(rmp_serde::decode::Error),
}

impl fmt::Display for GatewayStateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GatewayStateError::NotFound(key) => {
                write!(f, "No gateway info found for key '{}'", key)
            }
            GatewayStateError::DeserializeError(e) => {
                write!(f, "Error deserializing gateway info: {}", e)
            }
        }
    }
}

impl std::error::Error for GatewayStateError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            GatewayStateError::NotFound(_) => None,
            GatewayStateError::DeserializeError(e) => Some(e),
        }
    }
}

struct GatewayStateInner<S = TaskLifecycleStoreHandle>
where
    S: TaskLifecycleStore,
{
    state: Arc<StateMachineStore>,
    raft: Raft,
    last_task_acquisition: Arc<AtomicU64>,
    task_lifecycle_store: S,
    key_validator: Arc<ApiKeyValidator>,
    gateway_settings: Arc<GatewayRuntimeSettingsStore>,
    task_manager: TaskManager,
    config: Arc<RuntimeConfigStore>,
    event_recorder: EventRecorder,
    leader_client: Mutex<Option<CachedLeaderClient>>,
}

#[derive(Clone)]
struct CachedLeaderClient {
    leader_id: u64,
    client: Http3Client,
}

#[derive(Clone)]
pub struct GatewayState<S = TaskLifecycleStoreHandle>
where
    S: TaskLifecycleStore,
{
    internal: Arc<GatewayStateInner<S>>,
}

pub struct GatewayStateInit<S = TaskLifecycleStoreHandle>
where
    S: TaskLifecycleStore,
{
    pub state: Arc<StateMachineStore>,
    pub raft: Raft,
    pub last_task_acquisition: Arc<AtomicU64>,
    pub task_lifecycle_store: S,
    pub api_key_validator: Arc<ApiKeyValidator>,
    pub task_manager: TaskManager,
    pub config: Arc<RuntimeConfigStore>,
    pub event_recorder: EventRecorder,
}

impl<S> GatewayState<S>
where
    S: TaskLifecycleStore,
{
    pub(crate) fn admin_key(&self) -> String {
        self.internal.config.snapshot().http().admin_key.to_string()
    }

    pub fn current_node_id(&self) -> u64 {
        self.internal.config.snapshot().node().network.node_id
    }

    pub(crate) fn leader_server_addr(&self, info: &GatewayInfo) -> String {
        format!("{}:{}", info.ip, info.http_port)
    }

    fn leader_endpoint_url(&self, info: &GatewayInfo, endpoint_path: &str) -> String {
        format!(
            "https://{}:{}{}",
            info.domain, info.http_port, endpoint_path
        )
    }

    pub(crate) async fn build_leader_client(
        &self,
        info: &GatewayInfo,
    ) -> anyhow::Result<Http3Client> {
        let cfg = self.internal.config.snapshot();
        (|| async {
            Http3ClientBuilder::new()
                .server_domain(&info.domain)
                .server_ip(self.leader_server_addr(info))
                .max_idle_timeout_sec(cfg.http().max_idle_timeout_sec)
                .keep_alive_interval(cfg.http().keep_alive_interval_sec)
                .dangerous_skip_verification(cfg.node().cert.dangerous_skip_verification)
                .build()
                .await
                .map_err(|e| anyhow::anyhow!("Failed to create HTTP3 client to leader: {:?}", e))
        })
        .retry(
            ExponentialBuilder::default()
                .with_min_delay(Duration::from_millis(200))
                .with_max_times(5),
        )
        .await
        .map_err(|e| {
            anyhow::anyhow!(
                "Failed to create HTTP3 client to leader after retries: {}",
                e
            )
        })
    }

    async fn shared_leader_client(
        &self,
        leader_id: u64,
        leader_info: &GatewayInfo,
    ) -> anyhow::Result<Http3Client> {
        if let Some(client) = {
            let guard = self.internal.leader_client.lock().await;
            guard
                .as_ref()
                .filter(|cached| cached.leader_id == leader_id)
                .map(|cached| cached.client.clone())
        } {
            return Ok(client);
        }

        let client = self.build_leader_client(leader_info).await?;
        let mut guard = self.internal.leader_client.lock().await;
        if let Some(cached) = guard.as_ref()
            && cached.leader_id == leader_id
        {
            return Ok(cached.client.clone());
        }

        *guard = Some(CachedLeaderClient {
            leader_id,
            client: client.clone(),
        });
        Ok(client)
    }

    async fn forward_to_leader_endpoint(
        &self,
        current_node_id: u64,
        endpoint_path: &str,
        payload: Bytes,
        admin_key: Option<&str>,
        client: Option<&Http3Client>,
        timeout: Duration,
    ) -> Result<bool> {
        let Some(leader_id) = self.leader().await else {
            return Err(anyhow::anyhow!("No leader elected"));
        };
        if leader_id == current_node_id {
            return Ok(false);
        }

        let leader_info = self
            .gateway(leader_id)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to obtain leader info: {:?}", e))?;

        let client_storage;
        let client = match client {
            Some(client) => client,
            None => {
                client_storage = self.shared_leader_client(leader_id, &leader_info).await?;
                &client_storage
            }
        };

        let url = self.leader_endpoint_url(&leader_info, endpoint_path);
        let headers_vec: Vec<(&str, &str)> = admin_key
            .map(|key| vec![("x-admin-key", key)])
            .unwrap_or_default();
        let extra_headers = (!headers_vec.is_empty()).then_some(headers_vec.as_slice());

        match client
            .post(&url, payload, extra_headers, Some(timeout))
            .await
        {
            Ok((status, _body)) if status.is_success() => Ok(true),
            Ok((status, body)) => Err(anyhow::anyhow!(
                "Failed to forward to leader: {} {:?}",
                status,
                String::from_utf8_lossy(&body)
            )),
            Err(e) => Err(anyhow::anyhow!("Failed to forward to leader: {:?}", e)),
        }
    }

    pub fn new(args: GatewayStateInit<S>) -> Self {
        let GatewayStateInit {
            state,
            raft,
            last_task_acquisition,
            task_lifecycle_store,
            api_key_validator,
            task_manager,
            config,
            event_recorder,
        } = args;
        let gateway_settings = api_key_validator.gateway_settings();
        Self {
            internal: Arc::new(GatewayStateInner {
                state,
                raft,
                last_task_acquisition,
                task_lifecycle_store,
                key_validator: api_key_validator,
                gateway_settings,
                task_manager,
                config,
                event_recorder,
                leader_client: Mutex::new(None),
            }),
        }
    }

    pub fn update_task_acquisition(&self) -> Result<()> {
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        self.internal
            .last_task_acquisition
            .store(timestamp, Ordering::Relaxed);
        Ok(())
    }

    pub async fn leader(&self) -> Option<u64> {
        self.internal.raft.metrics().borrow().current_leader
    }

    pub async fn metrics(&self) -> watch::Receiver<RaftMetrics<NodeId, BasicNode>> {
        self.internal.raft.metrics()
    }

    pub async fn membership(&self) -> Vec<u64> {
        let metrics = self.internal.raft.metrics();
        let borrowed = metrics.borrow();
        borrowed
            .membership_config
            .membership()
            .nodes()
            .map(|(&id, _)| id)
            .collect()
    }

    pub async fn gateway(&self, n: u64) -> Result<GatewayInfo, GatewayStateError> {
        let key = n.to_string();
        let value = {
            self.internal
                .state
                .get_raw(&key)
                .await
                .ok_or_else(|| GatewayStateError::NotFound(key.clone()))?
        };

        rmp_serde::from_slice::<GatewayInfo>(&value).map_err(GatewayStateError::DeserializeError)
    }

    pub async fn gateways(&self) -> Result<Vec<GatewayInfo>, GatewayStateError> {
        let nodes = self.membership().await;
        let mut infos = Vec::with_capacity(nodes.len());
        for n in nodes {
            infos.push(self.gateway(n).await?);
        }
        Ok(infos)
    }

    pub async fn set_gateway_info(&self, info: GatewayInfoRef<'_>) -> Result<()> {
        let gateway_info_bytes = rmp_serde::to_vec(&info)?;

        let request = Request::Set {
            request_id: Uuid::new_v4().as_u128(),
            key: info.node_id.to_string(),
            value: gateway_info_bytes,
        };

        self.internal.raft.client_write(request).await?;

        Ok(())
    }

    pub async fn submit_gateway_info_ext(
        &self,
        info: GatewayInfoExtRef<'_>,
        client: Option<&Http3Client>,
    ) -> Result<bool> {
        let current_node_id = self.internal.config.snapshot().node().network.node_id;
        if self.leader().await == Some(current_node_id) {
            self.set_gateway_info(GatewayInfoRef {
                node_id: info.node_id,
                domain: info.domain,
                ip: info.ip,
                name: info.name,
                http_port: info.http_port,
                available_tasks: info.available_tasks,
                last_task_acquisition: info.last_task_acquisition,
                last_update: info.last_update,
            })
            .await?;
            return Ok(false);
        }

        let payload = rmp_serde::to_vec(&info)
            .map_err(|e| anyhow::anyhow!("Failed to serialize gateway info: {}", e))?;
        let admin_key = self.admin_key();
        self.forward_to_leader_endpoint(
            current_node_id,
            "/write",
            Bytes::from(payload),
            Some(admin_key.as_str()),
            client,
            Duration::from_secs(self.internal.config.snapshot().http().post_timeout_sec),
        )
        .await
    }

    pub fn generic_key(&self) -> Option<Uuid> {
        self.internal
            .gateway_settings
            .generic_key()
            .or_else(|| self.internal.state.get_cached_generic_key())
    }

    pub fn is_generic_key(&self, api_key: &Uuid) -> bool {
        self.internal.gateway_settings.is_generic_key(api_key)
            || self
                .internal
                .state
                .get_cached_generic_key()
                .is_some_and(|generic_key| &generic_key == api_key)
    }

    pub fn has_database_gateway_settings(&self) -> bool {
        self.internal
            .gateway_settings
            .has_database_gateway_settings()
    }

    pub fn add_task_unauthorized_per_ip_daily_rate_limit(&self) -> u64 {
        if self.has_database_gateway_settings() {
            return self
                .internal
                .gateway_settings
                .add_task_unauthorized_per_ip_daily_rate_limit();
        }
        self.internal
            .config
            .snapshot()
            .http()
            .add_task_unauthorized_per_ip_daily_rate_limit as u64
    }

    pub fn max_task_queue_len(&self) -> usize {
        if self.has_database_gateway_settings() {
            return usize::try_from(self.internal.gateway_settings.max_task_queue_len())
                .unwrap_or(usize::MAX);
        }
        self.internal.config.snapshot().http().max_task_queue_len
    }

    pub fn request_file_size_limit(&self) -> u64 {
        if self.has_database_gateway_settings() {
            return self.internal.gateway_settings.request_file_size_limit();
        }
        self.internal
            .config
            .snapshot()
            .http()
            .request_file_size_limit
    }

    pub fn guest_generation_limit(&self) -> u64 {
        self.internal.gateway_settings.guest_generation_limit()
    }

    pub fn guest_window_ms(&self) -> u64 {
        self.internal.gateway_settings.guest_window_ms()
    }

    pub fn generic_global_daily_limit(&self) -> u64 {
        self.internal.gateway_settings.generic_global_daily_limit()
    }

    pub fn generic_per_ip_daily_limit(&self) -> u64 {
        self.internal.gateway_settings.generic_per_ip_daily_limit()
    }

    pub fn generic_window_ms(&self) -> u64 {
        self.internal.gateway_settings.generic_window_ms()
    }

    pub fn registered_generation_limit(&self) -> u64 {
        self.internal.gateway_settings.registered_generation_limit()
    }

    pub fn registered_window_ms(&self) -> u64 {
        self.internal.gateway_settings.registered_window_ms()
    }

    pub fn is_rate_limit_whitelisted_ip(&self, ip: &IpAddr) -> bool {
        if self.has_database_gateway_settings() {
            return self
                .internal
                .gateway_settings
                .is_rate_limit_whitelisted_ip(ip);
        }
        self.internal
            .config
            .snapshot()
            .rate_limit_whitelist()
            .ips
            .contains(ip)
    }

    pub async fn lookup_api_key(&self, api_key: &str) -> ApiKeyLookup {
        self.internal.key_validator.lookup(api_key).await
    }

    pub async fn lookup_api_key_with_unknown_key_guard(
        &self,
        api_key: &str,
        source_key: Option<Arc<str>>,
    ) -> ApiKeyLookup {
        self.internal
            .key_validator
            .lookup_with_unknown_key_guard(api_key, source_key)
            .await
    }

    pub async fn apply_task_lifecycle_write(
        &self,
        request: TaskLifecycleWriteRequest,
    ) -> Result<TaskLifecycleWriteResponse> {
        match request {
            TaskLifecycleWriteRequest::CreateGenerationTask { input } => {
                let outcome = self
                    .internal
                    .task_lifecycle_store
                    .create_generation_task(input.as_ref())
                    .await?;
                Ok(TaskLifecycleWriteResponse::CreateGenerationTask(outcome))
            }
            TaskLifecycleWriteRequest::RecordGenerationTaskAssignments {
                task_ids,
                worker_hotkey,
                worker_id,
                assigned_at_ms,
            } => {
                let assigned_task_ids = self
                    .internal
                    .task_lifecycle_store
                    .record_generation_task_assignments(
                        task_ids.as_slice(),
                        worker_hotkey.as_str(),
                        worker_id.as_str(),
                        assigned_at_ms,
                    )
                    .await?;
                Ok(TaskLifecycleWriteResponse::RecordGenerationTaskAssignments(
                    assigned_task_ids,
                ))
            }
            TaskLifecycleWriteRequest::FinalizeGenerationTaskAssignment { input } => {
                let outcome = self
                    .internal
                    .task_lifecycle_store
                    .finalize_generation_task_assignment(input.as_ref())
                    .await?;
                Ok(TaskLifecycleWriteResponse::FinalizeGenerationTaskAssignment(outcome))
            }
        }
    }

    async fn submit_task_lifecycle_write(
        &self,
        request: TaskLifecycleWriteRequest,
    ) -> Result<TaskLifecycleWriteResponse> {
        self.apply_task_lifecycle_write(request).await
    }

    pub async fn create_generation_task(
        &self,
        input: &CreateGenerationTaskInput,
    ) -> Result<CreateGenerationTaskOutcome> {
        match self
            .submit_task_lifecycle_write(TaskLifecycleWriteRequest::CreateGenerationTask {
                input: Box::new(input.clone()),
            })
            .await?
        {
            TaskLifecycleWriteResponse::CreateGenerationTask(outcome) => Ok(outcome),
            TaskLifecycleWriteResponse::Ack => {
                Err(anyhow::anyhow!("missing generation task create outcome"))
            }
            TaskLifecycleWriteResponse::RecordGenerationTaskAssignments(_) => Err(anyhow::anyhow!(
                "unexpected generation task assignment outcome while creating generation task"
            )),
            TaskLifecycleWriteResponse::FinalizeGenerationTaskAssignment(_) => {
                Err(anyhow::anyhow!(
                    "unexpected generation task finalize outcome while creating generation task"
                ))
            }
        }
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
        match self
            .submit_task_lifecycle_write(
                TaskLifecycleWriteRequest::RecordGenerationTaskAssignments {
                    task_ids: task_ids.to_vec(),
                    worker_hotkey: worker_hotkey.to_owned(),
                    worker_id: worker_id.to_owned(),
                    assigned_at_ms,
                },
            )
            .await?
        {
            TaskLifecycleWriteResponse::RecordGenerationTaskAssignments(assigned_assignments) => {
                Ok(assigned_assignments)
            }
            TaskLifecycleWriteResponse::Ack => Err(anyhow::anyhow!(
                "missing generation task assignment outcome"
            )),
            TaskLifecycleWriteResponse::CreateGenerationTask(_) => Err(anyhow::anyhow!(
                "unexpected generation task create outcome while recording task assignments"
            )),
            TaskLifecycleWriteResponse::FinalizeGenerationTaskAssignment(_) => {
                Err(anyhow::anyhow!(
                    "unexpected generation task finalize outcome while recording task assignments"
                ))
            }
        }
    }

    pub async fn finalize_generation_task_assignment(
        &self,
        input: FinalizeGenerationTaskAssignmentInput,
    ) -> Result<FinalizeGenerationTaskAssignmentOutcome> {
        match self
            .submit_task_lifecycle_write(
                TaskLifecycleWriteRequest::FinalizeGenerationTaskAssignment {
                    input: Box::new(input),
                },
            )
            .await?
        {
            TaskLifecycleWriteResponse::FinalizeGenerationTaskAssignment(outcome) => Ok(outcome),
            TaskLifecycleWriteResponse::Ack => {
                Err(anyhow::anyhow!("missing generation task finalize outcome"))
            }
            TaskLifecycleWriteResponse::RecordGenerationTaskAssignments(_) => Err(anyhow::anyhow!(
                "unexpected generation task assignment outcome while finalizing assignment"
            )),
            TaskLifecycleWriteResponse::CreateGenerationTask(_) => Err(anyhow::anyhow!(
                "unexpected generation task create outcome while finalizing assignment"
            )),
        }
    }

    pub async fn expire_generation_tasks(&self, limit: i32, now_ms: i64) -> Result<Vec<Uuid>> {
        self.internal
            .task_lifecycle_store
            .expire_generation_tasks(limit, now_ms)
            .await
    }

    pub async fn purge_terminal_generation_tasks(
        &self,
        limit: i32,
        completed_before_ms: i64,
    ) -> Result<Vec<Uuid>> {
        self.internal
            .task_lifecycle_store
            .purge_terminal_generation_tasks(limit, completed_before_ms)
            .await
    }

    pub async fn get_generation_task_access_owner(
        &self,
        task_id: Uuid,
    ) -> Result<Option<GenerationTaskAccessOwner>> {
        self.internal
            .task_lifecycle_store
            .get_generation_task_access_owner(task_id)
            .await
    }

    pub async fn get_generation_task_status_snapshot(
        &self,
        task_id: Uuid,
    ) -> Result<Option<GenerationTaskStatusSnapshot>> {
        self.internal
            .task_lifecycle_store
            .get_generation_task_status_snapshot(task_id)
            .await
    }

    pub async fn get_generation_task_generic_key_hash(
        &self,
        task_id: Uuid,
    ) -> Result<Option<[u8; 16]>> {
        self.internal
            .task_lifecycle_store
            .get_generation_task_generic_key_hash(task_id)
            .await
    }

    pub fn cluster_name_matches(&self, cluster_name: &str) -> bool {
        self.internal
            .config
            .snapshot()
            .node()
            .raft
            .cluster_name
            .as_str()
            == cluster_name
    }

    pub fn task_manager(&self) -> TaskManager {
        self.internal.task_manager.clone()
    }

    pub fn record_activity_event(&self, event: ActivityEventRef<'_>) {
        self.internal.event_recorder.record_activity(
            event.user_id,
            event.user_email,
            event.account_id,
            event.company_id,
            event.company_name,
            event.action,
            event.client_origin,
            event.task_kind,
            event.model,
            event.task_id,
        );
    }

    pub fn record_worker_event(&self, event: WorkerEventRef<'_>) {
        self.internal.event_recorder.record_worker_event(
            event.task_id,
            event.worker_id,
            event.action,
            event.task_kind,
            event.model,
            event.reason,
            event.metadata_json.as_ref(),
        );
    }

    pub async fn submit_rate_limit_mutation_batch(
        &self,
        batch: &RateLimitMutationBatch,
        client: Option<&Http3Client>,
    ) -> Result<()> {
        self.submit_rate_limit_mutations_with_request_id(batch.request_id, &batch.mutations, client)
            .await
    }

    pub async fn submit_rate_limit_mutations_with_request_id(
        &self,
        request_id: u128,
        mutations: &[RateLimitMutation],
        client: Option<&Http3Client>,
    ) -> Result<()> {
        let cfg = self.internal.config.snapshot();
        let current_node_id = cfg.node().network.node_id;
        let timeout = Duration::from_secs(cfg.http().forward_timeout_sec);

        let forwarded = (|| async {
            if let Some(leader_id) = self.leader().await
                && leader_id != current_node_id
            {
                let payload = rmp_serde::to_vec(&RateLimitRequestRef::RateLimitMutations {
                    request_id,
                    mutations,
                })
                .map_err(|e| anyhow::anyhow!("Failed to serialize mutations: {}", e))?;

                let admin_key = self.admin_key();
                return self
                    .forward_to_leader_endpoint(
                        current_node_id,
                        "/write",
                        Bytes::from(payload),
                        Some(admin_key.as_str()),
                        client,
                        timeout,
                    )
                    .await;
            }

            Ok(false)
        })
        .retry(ExponentialBuilder::default().with_max_times(5))
        .await
        .map_err(|e| {
            error!(
                "Rate limit mutations submission failed after retries (request_id: {}): {}",
                request_id, e
            );
            anyhow::anyhow!(
                "Rate limit mutations submission failed after retries: {}",
                e
            )
        })?;

        if forwarded {
            Ok(())
        } else {
            self.apply_rate_limit_mutation_batch(RateLimitMutationBatch::new(
                request_id,
                mutations.to_vec(),
            ))
            .await
        }
    }

    pub async fn apply_rate_limit_mutation_batch(
        &self,
        batch: RateLimitMutationBatch,
    ) -> Result<()> {
        self.internal
            .raft
            .client_write(Request::RateLimitMutations {
                request_id: batch.request_id,
                mutations: batch.mutations,
            })
            .await
            .map(|_r| ())
            .map_err(|e| anyhow::anyhow!("Failed to complete client_write: {}", e))
    }

    #[cfg(any(test, feature = "test-support"))]
    #[allow(dead_code)]
    pub async fn apply_rate_limit_mutations(
        &self,
        request_id: u128,
        mutations: Vec<RateLimitMutation>,
    ) -> Result<()> {
        self.apply_rate_limit_mutation_batch(RateLimitMutationBatch::new(request_id, mutations))
            .await
    }

    pub async fn get_cluster_rate_usage(
        &self,
        subject: Subject,
        id: u128,
        day_epoch: u64,
    ) -> (u64, u64) {
        let key = rate_limit_key(subject, id);
        self.internal
            .state
            .get_rate_limit_usage(&key, day_epoch)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::store::RateLimitDelta;

    #[test]
    fn rate_limit_request_ref_encodes_compatibly_with_request() {
        let request_id = 42u128;
        let delta = RateLimitDelta {
            subject: Subject::User,
            id: 7u128,
            day_epoch: 1,
            add_active: 0,
            add_day: 1,
        };
        let mutation_delta = RateLimitMutation::from_delta(delta);
        let mutation_refund = RateLimitMutation::refund_from_delta(delta);

        let payload = rmp_serde::to_vec(&RateLimitRequestRef::RateLimitMutations {
            request_id,
            mutations: &[mutation_delta, mutation_refund],
        })
        .expect("serialize mutation request");
        match rmp_serde::from_slice::<Request>(&payload).expect("decode mutation request") {
            Request::RateLimitMutations {
                request_id: decoded_id,
                mutations,
            } => {
                assert_eq!(decoded_id, request_id);
                assert_eq!(mutations.len(), 2);
                assert_eq!(mutations[0].subject, Subject::User);
                assert_eq!(mutations[0].id, 7u128);
                assert_eq!(mutations[0].day_delta, 1);
                assert_eq!(mutations[1].day_delta, -1);
            }
            other => panic!("unexpected decoded variant: {:?}", other),
        }
    }
}
