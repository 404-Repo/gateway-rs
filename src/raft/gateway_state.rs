use super::store::Request;
use super::store::{RateLimitDelta, Subject, rate_limit_key};
use super::{NodeId, Raft, StateMachineStore};
use crate::api::request::GatewayInfoExtRef;
use crate::api::response::GatewayInfo;
use crate::api::response::GatewayInfoRef;
use crate::config::NodeConfig;
use crate::db::{ApiKeyLookup, ApiKeyValidator, EventRecorder};
use crate::http3::client::{Http3Client, Http3ClientBuilder};
use crate::task::TaskManager;
use anyhow::Result;
use backon::{ExponentialBuilder, Retryable};
use bytes::Bytes;
use openraft::{BasicNode, RaftMetrics};
use rmp_serde;
use scc::Queue;
use serde::Serialize;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::watch;
use tracing::{error, info};
use uuid::Uuid;

type RateLimitDeltaBatch = Arc<Vec<RateLimitDelta>>;

pub struct ActivityEventRef<'a> {
    pub user_id: Option<Uuid>,
    pub user_email: Option<&'a str>,
    pub company_id: Option<Uuid>,
    pub company_name: Option<&'a str>,
    pub action: &'a str,
    pub tool: &'a str,
    pub task_kind: &'a str,
    pub model: Option<&'a str>,
    pub task_id: Option<Uuid>,
}

pub struct WorkerEventRef<'a> {
    pub task_id: Option<Uuid>,
    pub worker_id: Option<&'a str>,
    pub action: &'a str,
    pub task_kind: &'a str,
    pub reason: Option<&'a str>,
}

#[derive(Serialize)]
enum RateLimitRequestRef<'a> {
    RateLimitDeltas {
        request_id: u128,
        deltas: &'a [RateLimitDelta],
    },
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

struct GatewayStateInner {
    state: Arc<StateMachineStore>,
    raft: Raft,
    last_task_acquisition: Arc<AtomicU64>,
    key_validator: Arc<ApiKeyValidator>,
    task_manager: TaskManager,
    config: Arc<NodeConfig>,
    rate_limit_queue: Arc<Queue<RateLimitDelta>>,
    event_recorder: EventRecorder,
}

#[derive(Clone)]
pub struct GatewayState {
    internal: Arc<GatewayStateInner>,
}

pub struct GatewayStateInit {
    pub state: Arc<StateMachineStore>,
    pub raft: Raft,
    pub last_task_acquisition: Arc<AtomicU64>,
    pub key_validator_updater: Arc<ApiKeyValidator>,
    pub task_manager: TaskManager,
    pub config: Arc<NodeConfig>,
    pub rate_limit_queue: Arc<Queue<RateLimitDelta>>,
    pub event_recorder: EventRecorder,
}

impl GatewayState {
    fn key_prefix(key: &Uuid) -> String {
        key.to_string().chars().take(6).collect()
    }

    pub(crate) fn admin_key(&self) -> String {
        self.internal.config.http.admin_key.to_string()
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
        (|| async {
            Http3ClientBuilder::new()
                .server_domain(&info.domain)
                .server_ip(self.leader_server_addr(info))
                .max_idle_timeout_sec(self.internal.config.http.max_idle_timeout_sec)
                .keep_alive_interval(self.internal.config.http.keep_alive_interval_sec)
                .dangerous_skip_verification(self.internal.config.cert.dangerous_skip_verification)
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
                client_storage = self.build_leader_client(&leader_info).await?;
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

    pub fn new(args: GatewayStateInit) -> Self {
        let GatewayStateInit {
            state,
            raft,
            last_task_acquisition,
            key_validator_updater,
            task_manager,
            config,
            rate_limit_queue,
            event_recorder,
        } = args;
        Self {
            internal: Arc::new(GatewayStateInner {
                state,
                raft,
                last_task_acquisition,
                key_validator: key_validator_updater,
                task_manager,
                config,
                rate_limit_queue,
                event_recorder,
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
        let current_node_id = self.internal.config.network.node_id;
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
            Duration::from_secs(self.internal.config.http.post_timeout_sec),
        )
        .await
    }

    pub async fn update_gateway_generic_key(
        &self,
        current_node_id: u64,
        new_key: Option<Uuid>,
        admin_key: Option<&str>,
    ) -> Result<()> {
        let key_to_set = if self.generic_key().await.is_none() {
            new_key.unwrap_or_else(Uuid::new_v4)
        } else if let Some(key) = new_key {
            key
        } else {
            return Ok(());
        };

        let serialized_key = rmp_serde::to_vec(&key_to_set)
            .map_err(|e| anyhow::anyhow!("Failed to serialize UUID to rmp: {}", e))?;

        let payload = serde_json::to_vec(&serde_json::json!({ "generic_key": key_to_set }))
            .map_err(|e| anyhow::anyhow!("Failed to serialize payload: {}", e))?;
        let forwarded = self
            .forward_to_leader_endpoint(
                current_node_id,
                "/update_key",
                Bytes::from(payload),
                admin_key,
                None,
                Duration::from_secs(self.internal.config.http.forward_timeout_sec),
            )
            .await?;
        if forwarded {
            info!(
                "Gateway generic key updated (forwarded), prefix: {}",
                Self::key_prefix(&key_to_set)
            );
            return Ok(());
        }

        {
            self.internal
                .raft
                .client_write(Request::Set {
                    request_id: Uuid::new_v4().as_u128(),
                    key: "generic_key".to_string(),
                    value: serialized_key,
                })
                .await
                .map_err(|e| anyhow::anyhow!("Failed to complete client_write: {}", e))?;
        }

        info!(
            "Gateway generic key updated (leader), prefix: {}",
            Self::key_prefix(&key_to_set)
        );
        Ok(())
    }

    pub async fn generic_key(&self) -> Option<Uuid> {
        self.internal
            .state
            .get_raw("generic_key")
            .await
            .and_then(|v| rmp_serde::from_slice::<Uuid>(&v).ok())
    }

    pub async fn is_generic_key(&self, api_key: &Uuid) -> bool {
        self.generic_key()
            .await
            .is_some_and(|generic_key| &generic_key == api_key)
    }

    pub async fn lookup_api_key(&self, api_key: &str) -> ApiKeyLookup {
        self.internal.key_validator.lookup(api_key).await
    }

    pub fn cluster_name(&self) -> &str {
        &self.internal.config.raft.cluster_name
    }

    pub fn task_manager(&self) -> TaskManager {
        self.internal.task_manager.clone()
    }

    pub fn preconfigured_generic_key(&self) -> Option<Uuid> {
        self.internal.config.http.generic_key
    }

    pub fn enqueue_rate_limit_delta(&self, delta: RateLimitDelta) {
        self.internal.rate_limit_queue.push(delta);
    }

    pub fn record_activity_event(&self, event: ActivityEventRef<'_>) {
        self.internal.event_recorder.record_activity(
            event.user_id,
            event.user_email,
            event.company_id,
            event.company_name,
            event.action,
            event.tool,
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
            event.reason,
        );
    }

    pub async fn submit_rate_limit_deltas(
        &self,
        deltas: RateLimitDeltaBatch,
        client: Option<&Http3Client>,
    ) -> Result<()> {
        let request_id = Uuid::new_v4().as_u128();
        let current_node_id = self.internal.config.network.node_id;
        let timeout = Duration::from_secs(self.internal.config.http.forward_timeout_sec);

        let forwarded = (|| async {
            if let Some(leader_id) = self.leader().await
                && leader_id != current_node_id
            {
                let payload = rmp_serde::to_vec(&RateLimitRequestRef::RateLimitDeltas {
                    request_id,
                    deltas: deltas.as_slice(),
                })
                .map_err(|e| anyhow::anyhow!("Failed to serialize deltas: {}", e))?;

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
                "Rate limit deltas submission failed after retries (request_id: {}): {}",
                request_id, e
            );
            anyhow::anyhow!("Rate limit deltas submission failed after retries: {}", e)
        })?;

        if forwarded {
            Ok(())
        } else {
            let owned_deltas = match Arc::try_unwrap(deltas) {
                Ok(vec) => vec,
                Err(arc) => arc.as_ref().clone(),
            };
            self.apply_rate_limit_deltas(request_id, owned_deltas).await
        }
    }

    pub async fn apply_rate_limit_deltas(
        &self,
        request_id: u128,
        deltas: Vec<RateLimitDelta>,
    ) -> Result<()> {
        self.internal
            .raft
            .client_write(Request::RateLimitDeltas { request_id, deltas })
            .await
            .map(|_r| ())
            .map_err(|e| anyhow::anyhow!("Failed to complete client_write: {}", e))
    }

    pub async fn get_cluster_rate_usage(
        &self,
        subject: Subject,
        id: u128,
        hour_epoch: u64,
        day_epoch: u64,
    ) -> (u64, u64) {
        let key = rate_limit_key(subject, id);
        self.internal
            .state
            .get_rate_limit_usage(&key, hour_epoch, day_epoch)
            .await
    }
}
