use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};

use salvo::prelude::*;
use salvo::rate_limiter::{
    BasicQuota, FixedGuard, MokaStore, RateIssuer, RateLimiter, RemoteIpIssuer,
};
use uuid::Uuid;

use crate::config::HTTPConfig;
use crate::raft::gateway_state::GatewayState;

static BYPASS_COUNTER: AtomicU64 = AtomicU64::new(0);

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

pub type GlobalUserIDRateLimiter = RateLimiter<
    FixedGuard,
    MokaStore<<GlobalUserIDIssuer as RateIssuer>::Key, FixedGuard>,
    GlobalUserIDIssuer,
    BasicQuota,
>;

pub type UserIDLimiter = RateLimiter<
    FixedGuard,
    MokaStore<<UserIDLimitIssuer as RateIssuer>::Key, FixedGuard>,
    UserIDLimitIssuer,
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

    async fn issue(&self, req: &mut Request, depot: &Depot) -> Option<Self::Key> {
        let key_header = req.headers().get("x-api-key")?.to_str().ok()?;
        let gs = depot.obtain::<GatewayState>().ok()?;
        let uuid = Uuid::parse_str(key_header).ok()?;

        let ip_str = match req.remote_addr() {
            salvo::conn::SocketAddr::IPv4(addr) => addr.ip().to_string(),
            salvo::conn::SocketAddr::IPv6(addr) => addr.ip().to_string(),
            _ => return None,
        };

        if gs.is_generic_key(&uuid).await {
            return Some(format!("g_{}", ip_str));
        }

        let counter = BYPASS_COUNTER.fetch_add(1, Ordering::Relaxed);
        Some(counter.to_string())
    }
}

pub struct GlobalGenericKeyIssuer;
impl RateIssuer for GlobalGenericKeyIssuer {
    type Key = String;

    async fn issue(&self, req: &mut Request, depot: &Depot) -> Option<Self::Key> {
        let key_header = req.headers().get("x-api-key")?.to_str().ok()?;
        let gs = depot.obtain::<GatewayState>().ok()?;
        let uuid = Uuid::from_str(key_header).ok()?;

        if gs.is_generic_key(&uuid).await {
            return Some("g_gk".to_string());
        }

        let counter = BYPASS_COUNTER.fetch_add(1, Ordering::Relaxed);
        Some(counter.to_string())
    }
}

pub struct GlobalUserIDIssuer;
impl RateIssuer for GlobalUserIDIssuer {
    type Key = String;

    async fn issue(&self, req: &mut Request, depot: &Depot) -> Option<Self::Key> {
        let key_header = req.headers().get("x-api-key")?.to_str().ok()?;
        let gs = depot.obtain::<GatewayState>().ok()?;

        if gs.get_user_id(key_header).is_some() {
            return Some("g_uid".to_string());
        }

        let counter = BYPASS_COUNTER.fetch_add(1, Ordering::Relaxed);
        Some(counter.to_string())
    }
}

pub struct UserIDLimitIssuer;
impl RateIssuer for UserIDLimitIssuer {
    type Key = String;

    async fn issue(&self, req: &mut Request, depot: &Depot) -> Option<Self::Key> {
        let key_header = req.headers().get("x-api-key")?.to_str().ok()?;
        let gs = depot.obtain::<GatewayState>().ok()?;

        if let Some(user_id) = gs.get_user_id(key_header) {
            return Some(format!("u_{}", user_id));
        }

        let counter = BYPASS_COUNTER.fetch_add(1, Ordering::Relaxed);
        Some(counter.to_string())
    }
}

pub struct UnauthorizedOnlyIssuer;

impl RateIssuer for UnauthorizedOnlyIssuer {
    type Key = String;

    async fn issue(&self, req: &mut Request, depot: &Depot) -> Option<Self::Key> {
        let ip_str = || match req.remote_addr() {
            salvo::conn::SocketAddr::IPv4(addr) => Some(addr.ip().to_string()),
            salvo::conn::SocketAddr::IPv6(addr) => Some(addr.ip().to_string()),
            _ => None,
        };

        let gs = depot.obtain::<GatewayState>().ok()?;

        if let Some(key_str) = req.headers().get("x-api-key").and_then(|h| h.to_str().ok()) {
            if let Ok(uuid) = Uuid::parse_str(key_str) {
                let is_authorized = gs.is_valid_api_key(key_str)
                    || gs.is_generic_key(&uuid).await
                    || gs.is_company_key(key_str);

                if is_authorized {
                    // Authorized, bypass rate limiting
                    let counter = BYPASS_COUNTER.fetch_add(1, Ordering::Relaxed);
                    return Some(counter.to_string());
                }
            }
        }

        // For any case that is not a valid, authorized key, we rate limit by IP
        ip_str()
    }
}

pub struct RateLimits {
    pub basic_limiter: PerIPRateLimiter,
    pub write_limiter: PerIPRateLimiter,
    pub update_limiter: PerIPRateLimiter,
    // This only prevents spam per IP (for unauthenticated users).
    pub unauthorized_only_limiter: UnauthorizedOnlyRateLimiter,
    pub generic_global_limiter: GlobalGenericKeyRateLimiter,
    pub generic_per_ip_limiter: GenericKeyPerIpRateLimiter,
    pub user_id_global_limiter: GlobalUserIDRateLimiter,
    pub user_id_per_user_limiter: UserIDLimiter,
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
        let write_limiter = Self::create_ip_rate_limiter(http_config.write_rate_limit);
        let update_limiter = Self::create_ip_rate_limiter(http_config.update_key_rate_limit);
        let unauthorized_only_limiter = UnauthorizedOnlyRateLimiter::new(
            FixedGuard::new(),
            MokaStore::new(),
            UnauthorizedOnlyIssuer,
            BasicQuota::per_hour(http_config.add_task_basic_per_ip_rate_limit),
        );

        let generic_global_limiter = GlobalGenericKeyRateLimiter::new(
            FixedGuard::new(),
            MokaStore::new(),
            GlobalGenericKeyIssuer,
            BasicQuota::per_hour(http_config.add_task_generic_global_hourly_rate_limit),
        );
        let generic_per_ip_limiter = GenericKeyPerIpRateLimiter::new(
            FixedGuard::new(),
            MokaStore::new(),
            GenericKeyPerIpIssuer,
            BasicQuota::per_hour(http_config.add_task_generic_per_ip_hourly_rate_limit),
        );
        let user_id_global_limiter = GlobalUserIDRateLimiter::new(
            FixedGuard::new(),
            MokaStore::new(),
            GlobalUserIDIssuer,
            BasicQuota::per_hour(http_config.add_task_user_id_global_hourly_rate_limit),
        );
        let user_id_per_user_limiter = UserIDLimiter::new(
            FixedGuard::new(),
            MokaStore::new(),
            UserIDLimitIssuer,
            BasicQuota::per_hour(http_config.add_task_user_id_per_user_hourly_rate_limit),
        );

        let result_limiter = Self::create_ip_rate_limiter(http_config.add_result_rate_limit);
        let read_limiter = Self::create_ip_rate_limiter(http_config.write_rate_limit);
        let load_limiter = Self::create_ip_rate_limiter(http_config.load_rate_limit);
        let leader_limiter = Self::create_ip_rate_limiter(http_config.leader_rate_limit);
        let metric_limiter = Self::create_ip_rate_limiter(http_config.metric_rate_limit);
        let status_limiter = Self::create_ip_rate_limiter(http_config.get_status_rate_limit);

        Self {
            basic_limiter,
            write_limiter,
            update_limiter,
            unauthorized_only_limiter,
            generic_global_limiter,
            generic_per_ip_limiter,
            user_id_global_limiter,
            user_id_per_user_limiter,
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
