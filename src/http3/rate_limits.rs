use salvo::prelude::*;
use salvo::rate_limiter::{BasicQuota, FixedGuard, MokaStore, RateIssuer, RateLimiter};
use std::sync::Arc;
use uuid::Uuid;

use crate::config::HTTPConfig;
use crate::http3::depot_ext::DepotExt;
use crate::http3::error::ServerError;
use crate::http3::state::HttpState;
use crate::http3::whitelist::is_whitelisted_ip;
use crate::raft::gateway_state::GatewayState;
use crate::raft::rate_limit::{ClientKey, ClusterUsageParams, DistributedRateLimiter};
use crate::raft::store::{RateLimitDelta, RateLimitMutation, Subject};

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
    pub source_addr: Option<Arc<str>>,
}

#[derive(Clone, Debug)]
pub struct CompanyRateLimit {
    pub id: Uuid,
    pub name: Arc<str>,
    pub hourly_limit: u64,
    pub daily_limit: u64,
}

#[derive(Clone)]
pub struct RateLimitReservation {
    deltas: Vec<RateLimitDelta>,
    limiter: DistributedRateLimiter,
}

impl RateLimitReservation {
    pub fn new(limiter: DistributedRateLimiter, deltas: Vec<RateLimitDelta>) -> Self {
        Self { deltas, limiter }
    }

    pub fn charge_mutations(&self) -> Vec<RateLimitMutation> {
        self.deltas
            .iter()
            .copied()
            .map(RateLimitMutation::from_delta)
            .collect()
    }

    pub fn refund_mutations(&self) -> Vec<RateLimitMutation> {
        self.deltas
            .iter()
            .copied()
            .map(RateLimitMutation::refund_from_delta)
            .collect()
    }

    pub async fn rollback_pending(&self) {
        for delta in &self.deltas {
            self.limiter
                .rollback_pending(
                    ClientKey {
                        subject: delta.subject,
                        id: delta.id,
                    },
                    delta.add_hour,
                    delta.add_day,
                    (delta.hour_epoch, delta.day_epoch),
                )
                .await;
        }
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

fn source_addr_from_req(req: &mut Request) -> Option<Arc<str>> {
    match req.remote_addr() {
        salvo::conn::SocketAddr::IPv4(addr) => Some(Arc::<str>::from(addr.ip().to_string())),
        salvo::conn::SocketAddr::IPv6(addr) => Some(Arc::<str>::from(addr.ip().to_string())),
        _ => Some(Arc::<str>::from(req.remote_addr().to_string())),
    }
}

fn cached_decimal_ip(req: &mut Request, depot: &Depot) -> Option<Arc<str>> {
    if let Ok(ctx) = depot.obtain::<RateLimitContext>() {
        if let Some(ip) = ctx.decimal_ip.as_ref() {
            return Some(Arc::clone(ip));
        }
        if let Some(addr) = ctx.source_addr.as_ref() {
            return Some(Arc::clone(addr));
        }
    }
    decimal_ip_from_req(req).or_else(|| source_addr_from_req(req))
}

const GENERIC_GLOBAL_SUBJECT_ID: u128 = 0;

fn generic_ip_subject_id(ctx: &RateLimitContext) -> u128 {
    if let Some(ip) = ctx.decimal_ip.as_ref()
        && let Ok(id) = ip.parse::<u128>()
    {
        return id;
    }

    if let Some(addr) = ctx.source_addr.as_ref() {
        let digest = blake3::hash(addr.as_bytes());
        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(&digest.as_bytes()[..16]);
        return u128::from_le_bytes(bytes);
    }

    0
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
    if context.decimal_ip.is_none() {
        context.source_addr = source_addr_from_req(req);
    }

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

pub type UnauthorizedOnlyRateLimiter = RateLimiter<
    FixedGuard,
    MokaStore<<UnauthorizedOnlyIssuer as RateIssuer>::Key, FixedGuard>,
    UnauthorizedOnlyIssuer,
    BasicQuota,
>;

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
    error_msg: &'a str,
    require_day_match: bool,
}

fn pending_increments(params: &SubjectParams<'_>) -> (u32, u32) {
    (
        u32::from(params.hourly_limit > 0),
        u32::from(params.daily_limit > 0),
    )
}

fn subject_delta(epochs: (u64, u64), params: &SubjectParams<'_>) -> Option<RateLimitDelta> {
    let (add_hour, add_day) = pending_increments(params);
    let has_limits = add_hour > 0 || add_day > 0;
    if !has_limits {
        return None;
    }

    Some(RateLimitDelta {
        subject: params.subject,
        id: params.id,
        hour_epoch: epochs.0,
        day_epoch: epochs.1,
        add_hour,
        add_day,
    })
}

async fn check_subject_limit(
    limiter: &DistributedRateLimiter,
    gs: &GatewayState,
    epochs: (u64, u64),
    params: &SubjectParams<'_>,
) -> Result<(), ServerError> {
    let subject = params.subject;
    let id = params.id;
    let hourly_limit = params.hourly_limit;
    let daily_limit = params.daily_limit;
    let error_msg = params.error_msg;
    let require_day_match = params.require_day_match;

    let (hour_epoch, day_epoch) = epochs;
    let has_limits = hourly_limit > 0 || daily_limit > 0;
    let (cluster_hour, cluster_day) = if has_limits {
        limiter
            .cluster_usage(
                gs,
                ClusterUsageParams {
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
            ClientKey { subject, id },
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

    Ok(())
}

pub async fn reserve_add_task_rate_limit(
    depot: &mut Depot,
) -> Result<Option<RateLimitReservation>, ServerError> {
    let ctx = depot
        .obtain::<RateLimitContext>()
        .map_err(|e| ServerError::Internal(format!("RateLimitContext missing: {:?}", e)))?;

    if ctx.is_whitelisted_ip {
        return Ok(None);
    }

    let state = depot.require::<HttpState>()?.clone();
    let cfg = state.config();
    let rate_limits = cfg.rate_limits();
    let limiter = rate_limits.distributed().clone();
    let gs = state.gateway_state().clone();
    let policies = rate_limits.policies();

    let epochs = limiter.epochs();
    let mut subjects = Vec::with_capacity(3);

    if ctx.is_generic_key {
        subjects.push(SubjectParams {
            subject: Subject::GenericGlobal,
            id: GENERIC_GLOBAL_SUBJECT_ID,
            hourly_limit: policies.generic_global_hourly_limit,
            daily_limit: 0,
            error_msg: "Generic key global rate limit exceeded",
            require_day_match: false,
        });
        subjects.push(SubjectParams {
            subject: Subject::GenericIp,
            id: generic_ip_subject_id(ctx),
            hourly_limit: policies.generic_per_ip_hourly_limit,
            daily_limit: 0,
            error_msg: "Generic key per-IP rate limit exceeded",
            require_day_match: false,
        });
    }

    // Company keys first, enforce their limits if present
    if let Some(company) = ctx.company.as_ref() {
        subjects.push(SubjectParams {
            subject: Subject::Company,
            id: company.id.as_u128(),
            hourly_limit: company.hourly_limit,
            daily_limit: company.daily_limit,
            error_msg: "Company rate limit exceeded",
            require_day_match: company.daily_limit > 0,
        });
    } else if let Some(user_id) = ctx.user_id {
        // Otherwise fall back to per-user quota when we have a user id
        subjects.push(SubjectParams {
            subject: Subject::User,
            id: user_id.as_u128(),
            hourly_limit: policies.user_hourly_limit,
            daily_limit: 0,
            error_msg: "User rate limit exceeded",
            require_day_match: false,
        });
    }

    let mut succeeded = Vec::with_capacity(subjects.len());
    for params in &subjects {
        if let Err(err) = check_subject_limit(&limiter, &gs, epochs, params).await {
            for (key, (hour_decrement, day_decrement)) in succeeded.into_iter().rev() {
                limiter
                    .rollback_pending(key, hour_decrement, day_decrement, epochs)
                    .await;
            }
            return Err(err);
        }
        succeeded.push((
            ClientKey {
                subject: params.subject,
                id: params.id,
            },
            pending_increments(params),
        ));
    }

    let mut deltas = Vec::with_capacity(subjects.len());
    for params in &subjects {
        if let Some(delta) = subject_delta(epochs, params) {
            deltas.push(delta);
        }
    }
    if deltas.is_empty() {
        return Ok(None);
    }

    Ok(Some(RateLimitReservation::new(limiter, deltas)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn reservation_rollback_uses_bound_limiter_instance() {
        let bound_limiter = DistributedRateLimiter::new(64);
        let other_limiter = DistributedRateLimiter::new(64);
        let key = ClientKey {
            subject: Subject::User,
            id: 55u128,
        };
        let epochs = (700u64, 70u64);

        assert!(bound_limiter.check_and_incr(key, 1, 0, 0, 0, epochs).await);
        assert!(!bound_limiter.check_and_incr(key, 1, 0, 0, 0, epochs).await);
        assert!(other_limiter.check_and_incr(key, 1, 0, 0, 0, epochs).await);
        assert!(!other_limiter.check_and_incr(key, 1, 0, 0, 0, epochs).await);

        let reservation = RateLimitReservation::new(
            bound_limiter.clone(),
            vec![RateLimitDelta {
                subject: key.subject,
                id: key.id,
                hour_epoch: epochs.0,
                day_epoch: epochs.1,
                add_hour: 1,
                add_day: 0,
            }],
        );

        reservation.rollback_pending().await;

        assert!(bound_limiter.check_and_incr(key, 1, 0, 0, 0, epochs).await);
        assert!(
            !other_limiter.check_and_incr(key, 1, 0, 0, 0, epochs).await,
            "rollback must not touch unrelated limiter instances"
        );
    }

    #[test]
    fn subject_delta_uses_same_pending_shape_as_local_increment() {
        let params = SubjectParams {
            subject: Subject::Company,
            id: 123u128,
            hourly_limit: 0,
            daily_limit: 10,
            error_msg: "Company rate limit exceeded",
            require_day_match: true,
        };

        let (hour_pending, day_pending) = pending_increments(&params);
        assert_eq!(hour_pending, 0);
        assert_eq!(day_pending, 1);

        let delta = subject_delta((10, 2), &params).expect("delta should exist");
        assert_eq!(delta.add_hour, hour_pending);
        assert_eq!(delta.add_day, day_pending);
    }

    #[test]
    fn subject_delta_disables_day_increment_when_daily_limit_is_zero() {
        let params = SubjectParams {
            subject: Subject::Company,
            id: 321u128,
            hourly_limit: 10,
            daily_limit: 0,
            error_msg: "Company rate limit exceeded",
            require_day_match: false,
        };

        let (hour_pending, day_pending) = pending_increments(&params);
        assert_eq!(hour_pending, 1);
        assert_eq!(day_pending, 0);

        let delta = subject_delta((11, 3), &params).expect("delta should exist");
        assert_eq!(delta.add_hour, hour_pending);
        assert_eq!(delta.add_day, day_pending);
    }
}
