use super::store::Request;
use super::store::{rate_limit_key, RateLimitDelta, RateLimitWindow, Subject};
use super::{NodeId, Raft, StateMachineStore};
use crate::api::response::GatewayInfo;
use crate::api::response::GatewayInfoRef;
use crate::common::company_usage::CompanyUsageRecorder;
use crate::config::NodeConfig;
use crate::db::ApiKeyValidator;
use crate::http3::client::{Http3Client, Http3ClientBuilder};
use crate::metrics::TaskKind;
use crate::task::TaskManager;
use anyhow::Result;
use backon::{ExponentialBuilder, Retryable};
use bytes::Bytes;
use openraft::{BasicNode, RaftMetrics};
use rmp_serde;
use scc::Queue;
use serde::Serialize;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::watch;
use tracing::{error, info};
use uuid::Uuid;

type RateLimitDeltaBatch = Arc<Vec<RateLimitDelta>>;

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
    usage_recorder: CompanyUsageRecorder,
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
    pub usage_recorder: CompanyUsageRecorder,
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

    pub(crate) fn leader_write_url(&self, info: &GatewayInfo) -> String {
        format!("https://{}:{}/write", info.domain, info.http_port)
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

    pub fn new(args: GatewayStateInit) -> Self {
        let GatewayStateInit {
            state,
            raft,
            last_task_acquisition,
            key_validator_updater,
            task_manager,
            config,
            rate_limit_queue,
            usage_recorder,
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
                usage_recorder,
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
        let membership_config = {
            self.internal
                .raft
                .metrics()
                .borrow()
                .membership_config
                .clone()
        };

        membership_config
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

        let leader_id = self.leader().await;

        if leader_id != Some(current_node_id) {
            let leader_id = leader_id.ok_or_else(|| anyhow::anyhow!("No leader elected"))?;
            let leader_info = self
                .gateway(leader_id)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to obtain leader info: {:?}", e))?;

            let server_ip = format!("{}:{}", leader_info.ip, leader_info.http_port);
            let url = format!(
                "https://{}:{}/update_key",
                leader_info.domain, leader_info.http_port
            );

            let client = Http3ClientBuilder::new()
                .server_domain(&leader_info.domain)
                .server_ip(&server_ip)
                .dangerous_skip_verification(self.internal.config.cert.dangerous_skip_verification)
                .build()
                .await
                .map_err(|e| anyhow::anyhow!("Failed to create HTTP3 client to leader: {:?}", e))?;

            let payload = serde_json::to_vec(&serde_json::json!({ "generic_key": key_to_set }))
                .map_err(|e| anyhow::anyhow!("Failed to serialize payload: {}", e))?;
            let headers_vec: Vec<(&str, &str)> = admin_key
                .map(|k| vec![("x-admin-key", k)])
                .unwrap_or_default();
            let extra_headers = (!headers_vec.is_empty()).then_some(headers_vec.as_slice());

            match client
                .post(
                    &url,
                    Bytes::from(payload),
                    extra_headers,
                    Some(Duration::from_secs(
                        self.internal.config.http.forward_timeout_sec,
                    )),
                )
                .await
            {
                Ok((status, _body)) if status.is_success() => {
                    info!(
                        "Gateway generic key updated (forwarded), prefix: {}",
                        Self::key_prefix(&key_to_set)
                    );
                    return Ok(());
                }
                Ok((status, body)) => {
                    return Err(anyhow::anyhow!(
                        "Failed to forward to leader: {} {:?}",
                        status,
                        String::from_utf8_lossy(&body)
                    ))
                }
                Err(e) => return Err(anyhow::anyhow!("Failed to forward to leader: {:?}", e)),
            }
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

    pub async fn is_valid_api_key(&self, api_key: &str) -> bool {
        self.internal.key_validator.is_valid_api_key(api_key).await
    }

    pub async fn is_company_key(&self, api_key: &str) -> bool {
        self.internal.key_validator.is_company_key(api_key).await
    }

    pub async fn get_user_id(&self, api_key: &str) -> Option<Uuid> {
        self.internal.key_validator.get_user_id(api_key).await
    }

    pub async fn get_company_info_from_key(
        &self,
        api_key: &str,
    ) -> Option<(Uuid, (String, u64, u64))> {
        self.internal
            .key_validator
            .get_company_info_from_key(api_key)
            .await
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

    pub async fn record_company_usage(&self, company_id: Uuid, task_kind: TaskKind) {
        self.internal
            .usage_recorder
            .record(company_id, task_kind)
            .await;
    }

    pub async fn submit_rate_limit_deltas(
        &self,
        deltas: RateLimitDeltaBatch,
        client: Option<&Http3Client>,
    ) -> Result<()> {
        let request_id = Uuid::new_v4().as_u128();
        let current_node_id = self.internal.config.network.node_id;

        let forwarded = (|| async {
            if let Some(leader_id) = self.leader().await {
                if leader_id != current_node_id {
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
                    let url = self.leader_write_url(&leader_info);

                    let payload = rmp_serde::to_vec(&RateLimitRequestRef::RateLimitDeltas {
                        request_id,
                        deltas: deltas.as_slice(),
                    })
                    .map_err(|e| anyhow::anyhow!("Failed to serialize deltas: {}", e))?;

                    let admin_key = self.admin_key();
                    let headers = [("x-admin-key", admin_key.as_str())];
                    match client
                        .post(
                            &url,
                            Bytes::from(payload),
                            Some(&headers),
                            Some(Duration::from_secs(
                                self.internal.config.http.forward_timeout_sec,
                            )),
                        )
                        .await
                    {
                        Ok((status, _body)) if status.is_success() => {
                            return Ok(true);
                        }
                        Ok((status, body)) => {
                            return Err(anyhow::anyhow!(
                                "Failed to forward to leader: {} {:?}",
                                status,
                                String::from_utf8_lossy(&body)
                            ));
                        }
                        Err(e) => {
                            return Err(anyhow::anyhow!("Failed to forward to leader: {:?}", e))
                        }
                    }
                }
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

    pub async fn get_cluster_rate_window(
        &self,
        subject: Subject,
        id: u128,
    ) -> Option<RateLimitWindow> {
        let key = rate_limit_key(subject, id);
        self.internal.state.get_rate_limit_window(&key).await
    }
}
