use salvo::prelude::*;
use salvo::rate_limiter::{
    BasicQuota, FixedGuard, MokaStore, RateIssuer, RateLimiter, RemoteIpIssuer,
};
use uuid::Uuid;

use crate::config::HTTPConfig;
use crate::http3::error::ServerError;
use crate::http3::whitelist::is_whitelisted_ip;
use crate::raft::gateway_state::GatewayState;

#[derive(Clone, Debug, Default)]
pub struct RateLimitContext {
    pub is_whitelisted_ip: bool,
    pub has_valid_api_key: bool,
    pub is_generic_key: bool,
    pub is_company_key: bool,
    pub user_id: Option<Uuid>,
    pub key_is_uuid: bool,
}

fn decimal_ip_from_req(req: &mut Request) -> Option<String> {
    match req.remote_addr() {
        salvo::conn::SocketAddr::IPv4(addr) => {
            let bits = addr.ip().to_bits();
            let mut buf = itoa::Buffer::new();
            Some(buf.format(bits).to_owned())
        }
        salvo::conn::SocketAddr::IPv6(addr) => {
            let bits = addr.ip().to_bits();
            let mut buf = itoa::Buffer::new();
            Some(buf.format(bits).to_owned())
        }
        _ => None,
    }
}

impl RateLimitContext {
    pub fn has_authorized_key(&self) -> bool {
        self.is_generic_key
            || self.is_company_key
            || self.has_valid_api_key
            || self.user_id.is_some()
    }
}

#[handler]
pub async fn prepare_rate_limit_context(
    depot: &mut Depot,
    req: &mut Request,
) -> Result<(), ServerError> {
    let gs = depot
        .obtain::<GatewayState>()
        .map_err(|e| ServerError::Internal(format!("Failed to obtain GatewayState: {:?}", e)))?;

    let mut context = RateLimitContext {
        is_whitelisted_ip: is_whitelisted_ip(req, depot),
        ..RateLimitContext::default()
    };

    if let Some(key_str) = req.headers().get("x-api-key").and_then(|v| v.to_str().ok()) {
        if key_str.len() == uuid::fmt::Hyphenated::LENGTH {
            if let Ok(uuid) = Uuid::parse_str(key_str) {
                context.key_is_uuid = true;
                context.has_valid_api_key = gs.is_valid_api_key(key_str).await;
                context.is_company_key = gs.is_company_key(key_str).await;
                context.user_id = gs.get_user_id(key_str).await;
                context.is_generic_key = gs.is_generic_key(&uuid).await;
            }
        }
    }

    depot.inject(context);
    Ok(())
}

pub type PerIPRateLimiter = RateLimiter<
    FixedGuard,
    MokaStore<<RemoteIpIssuer as RateIssuer>::Key, FixedGuard>,
    RemoteIpIssuer,
    BasicQuota,
>;

pub type GenericKeyPerIpRateLimiter = RateLimiter<
    FixedGuard,
    MokaStore<<GenericKeyPerIpIssuer as RateIssuer>::Key, FixedGuard>,
    GenericKeyPerIpIssuer,
    BasicQuota,
>;

pub type GlobalGenericKeyRateLimiter = RateLimiter<
    FixedGuard,
    MokaStore<<GlobalGenericKeyIssuer as RateIssuer>::Key, FixedGuard>,
    GlobalGenericKeyIssuer,
    BasicQuota,
>;

pub type UnauthorizedOnlyRateLimiter = RateLimiter<
    FixedGuard,
    MokaStore<<UnauthorizedOnlyIssuer as RateIssuer>::Key, FixedGuard>,
    UnauthorizedOnlyIssuer,
    BasicQuota,
>;

pub struct GenericKeyPerIpIssuer;

impl RateIssuer for GenericKeyPerIpIssuer {
    type Key = String;

    async fn issue(&self, req: &mut Request, _depot: &Depot) -> Option<Self::Key> {
        let ip_dec = decimal_ip_from_req(req)?;
        let mut key = String::with_capacity(2 + ip_dec.len());
        key.push_str("g_");
        key.push_str(&ip_dec);
        Some(key)
    }
}

pub struct GlobalGenericKeyIssuer;
impl RateIssuer for GlobalGenericKeyIssuer {
    type Key = String;

    async fn issue(&self, _req: &mut Request, _depot: &Depot) -> Option<Self::Key> {
        Some("g_gk".to_string())
    }
}

pub struct UnauthorizedOnlyIssuer;

impl RateIssuer for UnauthorizedOnlyIssuer {
    type Key = String;

    async fn issue(&self, req: &mut Request, _depot: &Depot) -> Option<Self::Key> {
        let ip = decimal_ip_from_req(req)?;
        Some(ip)
    }
}

pub struct RateLimits {
    pub basic_limiter: PerIPRateLimiter,
    pub update_limiter: PerIPRateLimiter,
    // This only prevents spam per IP (for unauthenticated users).
    pub unauthorized_only_limiter: UnauthorizedOnlyRateLimiter,
    pub generic_global_limiter: GlobalGenericKeyRateLimiter,
    pub generic_per_ip_limiter: GenericKeyPerIpRateLimiter,
    pub read_limiter: PerIPRateLimiter,
    pub result_limiter: PerIPRateLimiter,
    pub load_limiter: PerIPRateLimiter,
    pub leader_limiter: PerIPRateLimiter,
    pub metric_limiter: PerIPRateLimiter,
    pub status_limiter: PerIPRateLimiter,
}

impl RateLimits {
    pub fn new(http_config: &HTTPConfig) -> Self {
        let basic_limiter = Self::create_ip_rate_limiter(http_config.basic_rate_limit);
        let update_limiter = Self::create_ip_rate_limiter(http_config.update_key_rate_limit);
        let unauthorized_only_limiter = UnauthorizedOnlyRateLimiter::new(
            FixedGuard::new(),
            MokaStore::new(),
            UnauthorizedOnlyIssuer,
            BasicQuota::per_hour(http_config.add_task_basic_per_ip_rate_limit),
        )
        .with_skipper(|_: &mut Request, depot: &Depot| {
            depot
                .obtain::<RateLimitContext>()
                .map(|ctx| ctx.has_authorized_key())
                .unwrap_or(false)
        });
        let generic_global_limiter = GlobalGenericKeyRateLimiter::new(
            FixedGuard::new(),
            MokaStore::new(),
            GlobalGenericKeyIssuer,
            BasicQuota::per_hour(http_config.add_task_generic_global_hourly_rate_limit),
        )
        .with_skipper(|_: &mut Request, depot: &Depot| {
            depot
                .obtain::<RateLimitContext>()
                .map(|ctx| !ctx.is_generic_key)
                .unwrap_or(false)
        });
        let generic_per_ip_limiter = GenericKeyPerIpRateLimiter::new(
            FixedGuard::new(),
            MokaStore::new(),
            GenericKeyPerIpIssuer,
            BasicQuota::per_hour(http_config.add_task_generic_per_ip_hourly_rate_limit),
        )
        .with_skipper(|_: &mut Request, depot: &Depot| {
            depot
                .obtain::<RateLimitContext>()
                .map(|ctx| !ctx.is_generic_key)
                .unwrap_or(false)
        });

        let result_limiter = Self::create_ip_rate_limiter(http_config.add_result_rate_limit);
        let read_limiter = Self::create_ip_rate_limiter(http_config.basic_rate_limit);
        let load_limiter = Self::create_ip_rate_limiter(http_config.load_rate_limit);
        let leader_limiter = Self::create_ip_rate_limiter(http_config.leader_rate_limit);
        let metric_limiter = Self::create_ip_rate_limiter(http_config.metric_rate_limit);
        let status_limiter = Self::create_ip_rate_limiter(http_config.get_status_rate_limit);

        Self {
            basic_limiter,
            update_limiter,
            unauthorized_only_limiter,
            generic_global_limiter,
            generic_per_ip_limiter,
            read_limiter,
            result_limiter,
            load_limiter,
            leader_limiter,
            metric_limiter,
            status_limiter,
        }
    }

    fn create_ip_rate_limiter(
        quota: usize,
    ) -> RateLimiter<
        FixedGuard,
        MokaStore<<RemoteIpIssuer as RateIssuer>::Key, FixedGuard>,
        RemoteIpIssuer,
        BasicQuota,
    > {
        RateLimiter::new(
            FixedGuard::new(),
            MokaStore::<<RemoteIpIssuer as RateIssuer>::Key, FixedGuard>::new(),
            RemoteIpIssuer,
            BasicQuota::per_minute(quota),
        )
    }
}
