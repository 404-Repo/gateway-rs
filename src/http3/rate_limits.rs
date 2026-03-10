use salvo::prelude::*;
use salvo::rate_limiter::{BasicQuota, FixedGuard, MokaStore, RateIssuer, RateLimiter};
use std::sync::Arc;
use uuid::Uuid;

use crate::config::HTTPConfig;
use crate::http3::depot_ext::DepotExt;
use crate::http3::distributed_rate_limiter::DistributedRateLimiter;
use crate::http3::error::ServerError;
use crate::http3::state::HttpState;
use crate::http3::whitelist::is_whitelisted_ip;
use crate::raft::gateway_state::GatewayState;
use crate::raft::store::{RateLimitDelta, Subject};

#[derive(Clone, Debug, Default)]
pub struct RateLimitContext {
    pub is_whitelisted_ip: bool,
    pub has_valid_api_key: bool,
    pub is_generic_key: bool,
    pub is_company_key: bool,
    pub user_id: Option<Uuid>,
    pub user_email: Option<Arc<str>>,
    pub key_is_uuid: bool,
    pub company: Option<CompanyRateLimit>,
    pub decimal_ip: Option<Arc<str>>,
}

#[derive(Clone, Debug)]
pub struct CompanyRateLimit {
    pub id: Uuid,
    pub name: Arc<str>,
    pub hourly_limit: u64,
    pub daily_limit: u64,
}

#[derive(Copy, Clone, Debug)]
pub struct RateLimitPolicies {
    pub user_hourly_limit: u64,
}

impl RateLimitPolicies {
    fn from_config(cfg: &HTTPConfig) -> Self {
        Self {
            user_hourly_limit: cfg.add_task_authenticated_per_user_hourly_rate_limit as u64,
        }
    }
}

#[derive(Clone)]
pub struct RateLimitService {
    distributed: DistributedRateLimiter,
    policies: RateLimitPolicies,
}

impl RateLimitService {
    pub fn new(http_config: &HTTPConfig) -> (Self, RateLimiters) {
        let service = Self {
            distributed: DistributedRateLimiter::new(
                http_config.distributed_rate_limiter_max_capacity,
            ),
            policies: RateLimitPolicies::from_config(http_config),
        };
        let limiters = RateLimiters::new(http_config);
        (service, limiters)
    }

    pub fn distributed(&self) -> &DistributedRateLimiter {
        &self.distributed
    }

    pub fn policies(&self) -> RateLimitPolicies {
        self.policies
    }
}

fn decimal_ip_from_req(req: &mut Request) -> Option<Arc<str>> {
    match req.remote_addr() {
        salvo::conn::SocketAddr::IPv4(addr) => {
            let bits = addr.ip().to_bits();
            let mut buf = itoa::Buffer::new();
            Some(Arc::<str>::from(buf.format(bits)))
        }
        salvo::conn::SocketAddr::IPv6(addr) => {
            let bits = addr.ip().to_bits();
            let mut buf = itoa::Buffer::new();
            Some(Arc::<str>::from(buf.format(bits)))
        }
        _ => None,
    }
}

fn cached_decimal_ip(req: &mut Request, depot: &Depot) -> Option<Arc<str>> {
    if let Ok(ctx) = depot.obtain::<RateLimitContext>()
        && let Some(ip) = ctx.decimal_ip.as_ref()
    {
        return Some(Arc::clone(ip));
    }
    decimal_ip_from_req(req)
}

impl RateLimitContext {
    pub fn has_authorized_key(&self) -> bool {
        self.is_generic_key
            || self.is_company_key
            || self.has_valid_api_key
            || self.user_id.is_some()
    }
}

/// Builds the identity key used for batched rate-limit violation aggregation.
///
/// Priority:
/// 1. company id
/// 2. user id
/// 3. decimal source IP
/// 4. unknown
fn violation_client_key(ctx: &RateLimitContext) -> String {
    if let Some(company) = ctx.company.as_ref() {
        return format!("company:{}", company.id);
    }
    if let Some(user_id) = ctx.user_id {
        return format!("user:{user_id}");
    }
    if let Some(ip) = ctx.decimal_ip.as_ref() {
        return format!("ip:{ip}");
    }
    "unknown".to_string()
}

fn violation_client_key_for_subject(subject: Subject, id: u128) -> String {
    match subject {
        Subject::Company => format!("company:{}", Uuid::from_u128(id)),
        Subject::User => format!("user:{}", Uuid::from_u128(id)),
    }
}

fn maybe_record_local_violation(state: &HttpState, depot: &Depot, res: &Response) {
    if res.status_code != Some(salvo::http::StatusCode::TOO_MANY_REQUESTS) {
        return;
    }
    if let Ok(ctx) = depot.obtain::<RateLimitContext>() {
        state
            .gateway_state()
            .record_rate_limit_violation(violation_client_key(ctx).as_str());
    } else {
        state.gateway_state().record_rate_limit_violation("unknown");
    }
}

#[handler]
pub async fn prepare_rate_limit_context(
    depot: &mut Depot,
    req: &mut Request,
) -> Result<(), ServerError> {
    let state = depot.require::<HttpState>()?.clone();
    let gateway_state = state.gateway_state().clone();

    let mut context = RateLimitContext {
        is_whitelisted_ip: is_whitelisted_ip(req, &state),
        ..RateLimitContext::default()
    };
    context.decimal_ip = decimal_ip_from_req(req);

    if let Some(key_str) = req.headers().get("x-api-key").and_then(|v| v.to_str().ok())
        && key_str.len() == uuid::fmt::Hyphenated::LENGTH
        && let Ok(uuid) = Uuid::parse_str(key_str)
    {
        context.key_is_uuid = true;
        let lookup = gateway_state.lookup_api_key(key_str).await;
        context.has_valid_api_key = lookup.user_id.is_some();
        context.is_company_key = lookup.company_id.is_some();
        context.user_id = lookup.user_id;
        context.user_email = lookup.user_email;
        if let (Some(cid), Some((name, hourly, daily))) = (lookup.company_id, lookup.company_info) {
            context.company = Some(CompanyRateLimit {
                id: cid,
                name,
                hourly_limit: hourly,
                daily_limit: daily,
            });
        }
        context.is_generic_key = gateway_state.is_generic_key(&uuid).await;
    }

    depot.inject(context);
    Ok(())
}

pub type PerIPRateLimiter = RateLimiter<
    FixedGuard,
    MokaStore<<CachedIpIssuer as RateIssuer>::Key, FixedGuard>,
    CachedIpIssuer,
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

    async fn issue(&self, req: &mut Request, depot: &Depot) -> Option<Self::Key> {
        let ip_dec = cached_decimal_ip(req, depot)?;
        let mut key = String::with_capacity(2 + ip_dec.len());
        key.push_str("g_");
        key.push_str(ip_dec.as_ref());
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
    type Key = Arc<str>;

    async fn issue(&self, req: &mut Request, depot: &Depot) -> Option<Self::Key> {
        cached_decimal_ip(req, depot)
    }
}

pub struct CachedIpIssuer;

impl RateIssuer for CachedIpIssuer {
    type Key = Arc<str>;

    async fn issue(&self, req: &mut Request, depot: &Depot) -> Option<Self::Key> {
        cached_decimal_ip(req, depot)
    }
}

pub struct RateLimiters {
    pub basic_limiter: PerIPRateLimiter,
    pub update_limiter: PerIPRateLimiter,
    // /add_task only: applies before auth check and only for unauthorized attempts.
    // It is strictly per source IP; there is no unauthorized global bucket.
    pub unauthorized_only_limiter: UnauthorizedOnlyRateLimiter,
    // /add_task only: one shared bucket for generic-key requests from all IPs.
    pub generic_global_limiter: GlobalGenericKeyRateLimiter,
    // /add_task only: generic-key bucket scoped by source IP.
    pub generic_per_ip_limiter: GenericKeyPerIpRateLimiter,
    pub read_limiter: PerIPRateLimiter,
    pub result_limiter: PerIPRateLimiter,
    pub load_limiter: PerIPRateLimiter,
    pub leader_limiter: PerIPRateLimiter,
    pub metric_limiter: PerIPRateLimiter,
    pub status_limiter: PerIPRateLimiter,
}

impl RateLimiters {
    pub fn new(http_config: &HTTPConfig) -> Self {
        let basic_limiter =
            Self::create_ip_rate_limiter_with_whitelist_skip(http_config.basic_rate_limit);
        let update_limiter =
            Self::create_ip_rate_limiter_with_whitelist_skip(http_config.update_key_rate_limit);
        let unauthorized_only_limiter = UnauthorizedOnlyRateLimiter::new(
            FixedGuard::new(),
            MokaStore::new(),
            UnauthorizedOnlyIssuer,
            BasicQuota::per_hour(http_config.add_task_unauthorized_per_ip_hourly_rate_limit),
        )
        .with_skipper(|_: &mut Request, depot: &Depot| {
            depot
                .obtain::<RateLimitContext>()
                .map(|ctx| ctx.is_whitelisted_ip || ctx.has_authorized_key())
                .unwrap_or(false)
        });
        let generic_global_limiter = GlobalGenericKeyRateLimiter::new(
            FixedGuard::new(),
            MokaStore::new(),
            GlobalGenericKeyIssuer,
            // One key ("g_gk"), so this quota is aggregated across all source IPs.
            BasicQuota::per_hour(http_config.add_task_generic_key_global_hourly_rate_limit),
        )
        .with_skipper(|_: &mut Request, depot: &Depot| {
            depot
                .obtain::<RateLimitContext>()
                .map(|ctx| ctx.is_whitelisted_ip || !ctx.is_generic_key)
                .unwrap_or(false)
        });
        let generic_per_ip_limiter = GenericKeyPerIpRateLimiter::new(
            FixedGuard::new(),
            MokaStore::new(),
            GenericKeyPerIpIssuer,
            BasicQuota::per_hour(http_config.add_task_generic_key_per_ip_hourly_rate_limit),
        )
        .with_skipper(|_: &mut Request, depot: &Depot| {
            depot
                .obtain::<RateLimitContext>()
                .map(|ctx| ctx.is_whitelisted_ip || !ctx.is_generic_key)
                .unwrap_or(false)
        });

        let result_limiter =
            Self::create_ip_rate_limiter_with_whitelist_skip(http_config.add_result_rate_limit);
        let read_limiter =
            Self::create_ip_rate_limiter_with_whitelist_skip(http_config.basic_rate_limit);
        let load_limiter =
            Self::create_ip_rate_limiter_with_whitelist_skip(http_config.load_rate_limit);
        let leader_limiter =
            Self::create_ip_rate_limiter_with_whitelist_skip(http_config.leader_rate_limit);
        let metric_limiter =
            Self::create_ip_rate_limiter_with_whitelist_skip(http_config.metric_rate_limit);
        let status_limiter =
            Self::create_ip_rate_limiter_with_whitelist_skip(http_config.get_status_rate_limit);

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

    fn create_ip_rate_limiter_with_whitelist_skip(
        quota: usize,
    ) -> RateLimiter<
        FixedGuard,
        MokaStore<<CachedIpIssuer as RateIssuer>::Key, FixedGuard>,
        CachedIpIssuer,
        BasicQuota,
    > {
        RateLimiter::new(
            FixedGuard::new(),
            MokaStore::<<CachedIpIssuer as RateIssuer>::Key, FixedGuard>::new(),
            CachedIpIssuer,
            BasicQuota::per_minute(quota),
        )
        .with_skipper(|_: &mut Request, depot: &Depot| {
            depot
                .obtain::<RateLimitContext>()
                .map(|ctx| ctx.is_whitelisted_ip)
                .unwrap_or(false)
        })
    }
}

#[handler]
pub async fn basic_rate_limit(
    depot: &mut Depot,
    req: &mut Request,
    res: &mut Response,
    ctrl: &mut FlowCtrl,
) -> Result<(), ServerError> {
    let state = depot.require::<HttpState>()?.clone();
    let cfg = state.config();
    cfg.ip_rate_limiters()
        .basic_limiter
        .handle(req, depot, res, ctrl)
        .await;
    maybe_record_local_violation(&state, depot, res);
    Ok(())
}

#[handler]
pub async fn update_key_rate_limit(
    depot: &mut Depot,
    req: &mut Request,
    res: &mut Response,
    ctrl: &mut FlowCtrl,
) -> Result<(), ServerError> {
    let state = depot.require::<HttpState>()?.clone();
    let cfg = state.config();
    cfg.ip_rate_limiters()
        .update_limiter
        .handle(req, depot, res, ctrl)
        .await;
    maybe_record_local_violation(&state, depot, res);
    Ok(())
}

#[handler]
pub async fn unauthorized_only_rate_limit(
    depot: &mut Depot,
    req: &mut Request,
    res: &mut Response,
    ctrl: &mut FlowCtrl,
) -> Result<(), ServerError> {
    let state = depot.require::<HttpState>()?.clone();
    let cfg = state.config();
    cfg.ip_rate_limiters()
        .unauthorized_only_limiter
        .handle(req, depot, res, ctrl)
        .await;
    maybe_record_local_violation(&state, depot, res);
    Ok(())
}

#[handler]
pub async fn generic_global_rate_limit(
    depot: &mut Depot,
    req: &mut Request,
    res: &mut Response,
    ctrl: &mut FlowCtrl,
) -> Result<(), ServerError> {
    let state = depot.require::<HttpState>()?.clone();
    let cfg = state.config();
    cfg.ip_rate_limiters()
        .generic_global_limiter
        .handle(req, depot, res, ctrl)
        .await;
    maybe_record_local_violation(&state, depot, res);
    Ok(())
}

#[handler]
pub async fn generic_per_ip_rate_limit(
    depot: &mut Depot,
    req: &mut Request,
    res: &mut Response,
    ctrl: &mut FlowCtrl,
) -> Result<(), ServerError> {
    let state = depot.require::<HttpState>()?.clone();
    let cfg = state.config();
    cfg.ip_rate_limiters()
        .generic_per_ip_limiter
        .handle(req, depot, res, ctrl)
        .await;
    maybe_record_local_violation(&state, depot, res);
    Ok(())
}

#[handler]
pub async fn read_rate_limit(
    depot: &mut Depot,
    req: &mut Request,
    res: &mut Response,
    ctrl: &mut FlowCtrl,
) -> Result<(), ServerError> {
    let state = depot.require::<HttpState>()?.clone();
    let cfg = state.config();
    cfg.ip_rate_limiters()
        .read_limiter
        .handle(req, depot, res, ctrl)
        .await;
    maybe_record_local_violation(&state, depot, res);
    Ok(())
}

#[handler]
pub async fn result_rate_limit(
    depot: &mut Depot,
    req: &mut Request,
    res: &mut Response,
    ctrl: &mut FlowCtrl,
) -> Result<(), ServerError> {
    let state = depot.require::<HttpState>()?.clone();
    let cfg = state.config();
    cfg.ip_rate_limiters()
        .result_limiter
        .handle(req, depot, res, ctrl)
        .await;
    maybe_record_local_violation(&state, depot, res);
    Ok(())
}

#[handler]
pub async fn load_rate_limit(
    depot: &mut Depot,
    req: &mut Request,
    res: &mut Response,
    ctrl: &mut FlowCtrl,
) -> Result<(), ServerError> {
    let state = depot.require::<HttpState>()?.clone();
    let cfg = state.config();
    cfg.ip_rate_limiters()
        .load_limiter
        .handle(req, depot, res, ctrl)
        .await;
    maybe_record_local_violation(&state, depot, res);
    Ok(())
}

#[handler]
pub async fn leader_rate_limit(
    depot: &mut Depot,
    req: &mut Request,
    res: &mut Response,
    ctrl: &mut FlowCtrl,
) -> Result<(), ServerError> {
    let state = depot.require::<HttpState>()?.clone();
    let cfg = state.config();
    cfg.ip_rate_limiters()
        .leader_limiter
        .handle(req, depot, res, ctrl)
        .await;
    maybe_record_local_violation(&state, depot, res);
    Ok(())
}

#[handler]
pub async fn metric_rate_limit(
    depot: &mut Depot,
    req: &mut Request,
    res: &mut Response,
    ctrl: &mut FlowCtrl,
) -> Result<(), ServerError> {
    let state = depot.require::<HttpState>()?.clone();
    let cfg = state.config();
    cfg.ip_rate_limiters()
        .metric_limiter
        .handle(req, depot, res, ctrl)
        .await;
    maybe_record_local_violation(&state, depot, res);
    Ok(())
}

#[handler]
pub async fn status_rate_limit(
    depot: &mut Depot,
    req: &mut Request,
    res: &mut Response,
    ctrl: &mut FlowCtrl,
) -> Result<(), ServerError> {
    let state = depot.require::<HttpState>()?.clone();
    let cfg = state.config();
    cfg.ip_rate_limiters()
        .status_limiter
        .handle(req, depot, res, ctrl)
        .await;
    maybe_record_local_violation(&state, depot, res);
    Ok(())
}

struct SubjectParams<'a> {
    subject: Subject,
    id: u128,
    hourly_limit: u64,
    daily_limit: u64,
    add_day: u16,
    error_msg: &'a str,
    require_day_match: bool,
}

async fn enforce_subject(
    limiter: &DistributedRateLimiter,
    gs: &GatewayState,
    epochs: (u64, u64),
    params: SubjectParams<'_>,
) -> Result<(), ServerError> {
    let SubjectParams {
        subject,
        id,
        hourly_limit,
        daily_limit,
        add_day,
        error_msg,
        require_day_match,
    } = params;

    let (hour_epoch, day_epoch) = epochs;
    let has_limits = hourly_limit > 0 || daily_limit > 0;
    let (cluster_hour, cluster_day) = if has_limits {
        limiter
            .cluster_usage(
                gs,
                crate::http3::distributed_rate_limiter::ClusterUsageParams {
                    subject,
                    id,
                    hourly_limit,
                    daily_limit,
                    require_day_match,
                    epochs,
                },
            )
            .await
    } else {
        (0, 0)
    };

    if !limiter
        .check_and_incr(
            crate::http3::distributed_rate_limiter::ClientKey { subject, id },
            hourly_limit,
            daily_limit,
            cluster_hour,
            cluster_day,
            (hour_epoch, day_epoch),
        )
        .await
    {
        gs.record_rate_limit_violation(violation_client_key_for_subject(subject, id).as_str());
        return Err(ServerError::TooManyRequests(error_msg.to_string()));
    }

    if has_limits {
        let delta = RateLimitDelta {
            subject,
            id,
            hour_epoch,
            day_epoch,
            add_hour: 1,
            add_day: add_day as u32,
        };
        gs.enqueue_rate_limit_delta(delta);
    }

    Ok(())
}

#[handler]
pub async fn enforce_rate_limit(depot: &mut Depot, _req: &mut Request) -> Result<(), ServerError> {
    let ctx = depot
        .obtain::<RateLimitContext>()
        .map_err(|e| ServerError::Internal(format!("RateLimitContext missing: {:?}", e)))?;

    if ctx.is_whitelisted_ip {
        return Ok(());
    }

    let state = depot.require::<HttpState>()?.clone();
    let cfg = state.config();
    let rate_limits = cfg.rate_limits();
    let limiter = rate_limits.distributed();
    let gs = state.gateway_state().clone();
    let policies = rate_limits.policies();

    let epochs = limiter.epochs();

    // Company keys first, enforce their limits if present
    if let Some(company) = ctx.company.as_ref() {
        enforce_subject(
            limiter,
            &gs,
            epochs,
            SubjectParams {
                subject: Subject::Company,
                id: company.id.as_u128(),
                hourly_limit: company.hourly_limit,
                daily_limit: company.daily_limit,
                add_day: 1,
                error_msg: "Company rate limit exceeded",
                require_day_match: true,
            },
        )
        .await?;
        return Ok(());
    }

    // Otherwise fall back to per-user quota when we have a user id
    if let Some(user_id) = ctx.user_id {
        enforce_subject(
            limiter,
            &gs,
            epochs,
            SubjectParams {
                subject: Subject::User,
                id: user_id.as_u128(),
                hourly_limit: policies.user_hourly_limit,
                daily_limit: 0,
                add_day: 0,
                error_msg: "User rate limit exceeded",
                require_day_match: false,
            },
        )
        .await?;
    }

    Ok(())
}
