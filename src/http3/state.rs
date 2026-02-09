use std::collections::HashSet;
use std::net::IpAddr;
use std::sync::Arc;

use regex::Regex;

use crate::api::Task;
use crate::common::queue::DupQueue;
use crate::config::{HTTPConfig, ImageConfig, ModelConfigStore, NodeConfig, PromptConfig};
use crate::http3::rate_limits::RateLimitService;
use crate::http3::upload_limiter::ImageUploadLimiter;
use crate::http3::whitelist::RateLimitWhitelist;
use crate::metrics::Metrics;
use crate::raft::gateway_state::GatewayState;

#[derive(Clone)]
pub struct HttpState {
    config: Arc<NodeConfig>,
    model_store: Arc<ModelConfigStore>,
    gateway_state: GatewayState,
    task_queue: DupQueue<Task>,
    metrics: Metrics,
    rate_limit_whitelist: RateLimitWhitelist,
    cluster_ips: Arc<HashSet<IpAddr>>,
    image_upload_limiter: ImageUploadLimiter,
    prompt_regex: Arc<Regex>,
    rate_limits: RateLimitService,
}

pub struct HttpStateInit {
    pub config: Arc<NodeConfig>,
    pub model_store: Arc<ModelConfigStore>,
    pub gateway_state: GatewayState,
    pub task_queue: DupQueue<Task>,
    pub metrics: Metrics,
    pub rate_limit_whitelist: RateLimitWhitelist,
    pub cluster_ips: HashSet<IpAddr>,
    pub image_upload_limiter: ImageUploadLimiter,
    pub prompt_regex: Regex,
    pub rate_limits: RateLimitService,
}

impl HttpState {
    pub fn new(init: HttpStateInit) -> Self {
        let HttpStateInit {
            config,
            model_store,
            gateway_state,
            task_queue,
            metrics,
            rate_limit_whitelist,
            cluster_ips,
            image_upload_limiter,
            prompt_regex,
            rate_limits,
        } = init;
        Self {
            config,
            model_store,
            gateway_state,
            task_queue,
            metrics,
            rate_limit_whitelist,
            cluster_ips: Arc::new(cluster_ips),
            image_upload_limiter,
            prompt_regex: Arc::new(prompt_regex),
            rate_limits,
        }
    }

    pub fn http_config(&self) -> &HTTPConfig {
        &self.config.http
    }

    pub fn prompt_config(&self) -> &PromptConfig {
        &self.config.prompt
    }

    pub fn image_config(&self) -> &ImageConfig {
        &self.config.image
    }

    pub fn node_id(&self) -> usize {
        self.config.network.node_id as usize
    }

    pub fn model_store(&self) -> &Arc<ModelConfigStore> {
        &self.model_store
    }

    pub fn gateway_state(&self) -> &GatewayState {
        &self.gateway_state
    }

    pub fn task_queue(&self) -> &DupQueue<Task> {
        &self.task_queue
    }

    pub fn metrics(&self) -> &Metrics {
        &self.metrics
    }

    pub fn rate_limit_whitelist(&self) -> &RateLimitWhitelist {
        &self.rate_limit_whitelist
    }

    pub fn cluster_ips(&self) -> &HashSet<IpAddr> {
        &self.cluster_ips
    }

    pub fn image_upload_limiter(&self) -> &ImageUploadLimiter {
        &self.image_upload_limiter
    }

    pub fn prompt_regex(&self) -> &Regex {
        &self.prompt_regex
    }

    pub fn rate_limits(&self) -> &RateLimitService {
        &self.rate_limits
    }
}
