use foldhash::fast::RandomState;
use http::StatusCode;
use moka::future::Cache;
use salvo::prelude::*;
use salvo::rate_limiter::{BasicQuota, FixedGuard, MokaStore, RateIssuer, RateLimiter};
use serde_json::json;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use uuid::Uuid;

use crate::config::HTTPConfig;
use crate::db::GenerationBillingOwner;
use crate::http3::depot_ext::DepotExt;
use crate::http3::error::ServerError;
use crate::http3::state::HttpState;
use crate::http3::whitelist::is_whitelisted_ip;
use crate::raft::gateway_state::GatewayState;
use crate::raft::rate_limit::{
    CheckAndIncrParams, ClientKey, ClusterUsageParams, DistributedRateLimiter, RateLimitRejection,
};
use crate::raft::store::{RateLimitDelta, RateLimitMutation, RateLimitMutationBatch, Subject};

#[derive(Clone, Debug, Default)]
pub struct RateLimitContext {
    pub is_whitelisted_ip: bool,
    pub has_valid_api_key: bool,
    pub is_generic_key: bool,
    pub is_company_key: bool,
    pub user_id: Option<i64>,
    pub user_email: Option<Arc<str>>,
    pub user_limits: Option<(u64, u64)>,
    pub billing_owner: Option<GenerationBillingOwner>,
    pub key_is_uuid: bool,
    pub auth_lookup_blocked: bool,
    pub company: Option<CompanyRateLimit>,
    pub decimal_ip: Option<Arc<str>>,
    pub source_addr: Option<Arc<str>>,
    pub auth_context_ready: bool,
}

#[derive(Clone, Debug)]
pub struct CompanyRateLimit {
    pub id: Uuid,
    pub name: Arc<str>,
    pub concurrent_limit: u64,
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

    fn batch_from_mutations_with_request_id(
        request_id: u128,
        mutations: Vec<RateLimitMutation>,
    ) -> Option<RateLimitMutationBatch> {
        (!mutations.is_empty()).then(|| RateLimitMutationBatch::new(request_id, mutations))
    }

    fn batch_from_mutations(mutations: Vec<RateLimitMutation>) -> Option<RateLimitMutationBatch> {
        RateLimitMutationBatch::with_generated_request_id(mutations)
    }

    pub fn accepted_charge_mutations(&self) -> Vec<RateLimitMutation> {
        self.deltas
            .iter()
            .filter_map(|delta| {
                let mutation = RateLimitMutation::from_delta(*delta);
                (mutation.active_delta != 0 || mutation.day_delta != 0).then_some(mutation)
            })
            .collect()
    }

    pub fn accepted_charge_batch(&self) -> Option<RateLimitMutationBatch> {
        Self::batch_from_mutations(self.accepted_charge_mutations())
    }

    #[cfg(test)]
    pub fn active_charge_mutations(&self) -> Vec<RateLimitMutation> {
        self.deltas
            .iter()
            .filter_map(|delta| {
                (delta.add_active > 0).then_some(RateLimitMutation {
                    subject: delta.subject,
                    id: delta.id,
                    day_epoch: delta.day_epoch,
                    active_delta: delta.add_active as i64,
                    day_delta: 0,
                })
            })
            .collect()
    }

    #[cfg(test)]
    pub fn deferred_charge_mutations(&self) -> Vec<RateLimitMutation> {
        self.deltas
            .iter()
            .filter_map(|delta| {
                let mutation = RateLimitMutation {
                    subject: delta.subject,
                    id: delta.id,
                    day_epoch: delta.day_epoch,
                    active_delta: 0,
                    day_delta: delta.add_day as i64,
                };
                (mutation.day_delta != 0).then_some(mutation)
            })
            .collect()
    }

    pub fn refund_mutations(&self) -> Vec<RateLimitMutation> {
        self.deltas
            .iter()
            .copied()
            .map(RateLimitMutation::refund_from_delta)
            .collect()
    }

    pub fn refund_batch(&self) -> Option<RateLimitMutationBatch> {
        Self::batch_from_mutations(self.refund_mutations())
    }

    pub fn refund_batch_with_request_id(&self, request_id: u128) -> Option<RateLimitMutationBatch> {
        Self::batch_from_mutations_with_request_id(request_id, self.refund_mutations())
    }

    pub fn success_mutations(&self) -> Vec<RateLimitMutation> {
        self.active_release_mutations()
    }

    pub fn success_batch(&self) -> Option<RateLimitMutationBatch> {
        Self::batch_from_mutations(self.success_mutations())
    }

    pub fn success_batch_with_request_id(
        &self,
        request_id: u128,
    ) -> Option<RateLimitMutationBatch> {
        Self::batch_from_mutations_with_request_id(request_id, self.success_mutations())
    }

    pub fn active_release_mutations(&self) -> Vec<RateLimitMutation> {
        self.deltas
            .iter()
            .filter_map(|delta| {
                (delta.add_active > 0).then_some(RateLimitMutation {
                    subject: delta.subject,
                    id: delta.id,
                    day_epoch: delta.day_epoch,
                    active_delta: -(delta.add_active as i64),
                    day_delta: 0,
                })
            })
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
                    delta.add_active,
                    delta.add_day,
                    delta.day_epoch,
                )
                .await;
        }
    }

    pub async fn rollback_pending_success(&self) {
        for delta in &self.deltas {
            if delta.add_active == 0 {
                continue;
            }
            self.limiter
                .rollback_pending(
                    ClientKey {
                        subject: delta.subject,
                        id: delta.id,
                    },
                    delta.add_active,
                    0,
                    delta.day_epoch,
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

const UNAUTHORIZED_DAILY_COUNTER_TTL: Duration = Duration::from_secs(60 * 60 * 48);
const UNAUTHORIZED_DAILY_COUNTER_CAPACITY: u64 = 200_000;

#[derive(Clone)]
pub struct UnauthorizedDailyLimiter {
    counters: Cache<Arc<str>, Arc<AtomicU64>, RandomState>,
}

impl UnauthorizedDailyLimiter {
    pub fn new() -> Self {
        Self {
            counters: Cache::builder()
                .max_capacity(UNAUTHORIZED_DAILY_COUNTER_CAPACITY)
                .time_to_live(UNAUTHORIZED_DAILY_COUNTER_TTL)
                .build_with_hasher(RandomState::default()),
        }
    }

    pub async fn try_acquire(&self, subject: Arc<str>, day_epoch: u64, limit: u64) -> bool {
        if limit == 0 {
            return false;
        }

        let cache_key = Arc::<str>::from(format!("{day_epoch}:{subject}"));
        let counter = self
            .counters
            .get_with(cache_key, async { Arc::new(AtomicU64::new(0)) })
            .await;

        loop {
            let current = counter.load(Ordering::Acquire);
            if current >= limit {
                return false;
            }
            if counter
                .compare_exchange(current, current + 1, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return true;
            }
        }
    }
}

impl Default for UnauthorizedDailyLimiter {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone)]
pub struct AdminKeyFailureLimiter {
    misses: Cache<Arc<str>, Arc<AtomicU64>, RandomState>,
    cooldowns: Cache<Arc<str>, (), RandomState>,
    miss_limit: u64,
}

impl AdminKeyFailureLimiter {
    pub fn new(miss_ttl_sec: u64, cooldown_ttl_sec: u64, capacity: u64, miss_limit: u64) -> Self {
        Self {
            misses: Cache::builder()
                .max_capacity(capacity.max(1))
                .time_to_live(Duration::from_secs(miss_ttl_sec.max(1)))
                .build_with_hasher(RandomState::default()),
            cooldowns: Cache::builder()
                .max_capacity(capacity.max(1))
                .time_to_live(Duration::from_secs(cooldown_ttl_sec.max(1)))
                .build_with_hasher(RandomState::default()),
            miss_limit: miss_limit.max(1),
        }
    }

    pub async fn is_blocked(&self, source_key: &Arc<str>) -> bool {
        self.cooldowns.get(source_key).await.is_some()
    }

    pub async fn record_miss(&self, source_key: Arc<str>) -> bool {
        let misses = self
            .misses
            .get_with(source_key.clone(), async { Arc::new(AtomicU64::new(0)) })
            .await;
        let attempts = misses.fetch_add(1, Ordering::AcqRel) + 1;
        if attempts >= self.miss_limit {
            self.cooldowns.insert(source_key, ()).await;
            true
        } else {
            false
        }
    }
}

fn user_subject_id(user_id: i64) -> u128 {
    u128::from(user_id.max(0) as u64)
}

const GENERIC_GLOBAL_SUBJECT_ID: u128 = 0;

impl RateLimitContext {
    pub fn has_authorized_key(&self) -> bool {
        self.is_generic_key
            || self.is_company_key
            || self.has_valid_api_key
            || self.user_id.is_some()
    }

    fn auth_guard_source(&self) -> Option<Arc<str>> {
        self.decimal_ip
            .as_ref()
            .map(Arc::clone)
            .or_else(|| self.source_addr.as_ref().map(Arc::clone))
    }
}

fn base_rate_limit_context(req: &mut Request, state: &HttpState) -> RateLimitContext {
    let mut context = RateLimitContext {
        is_whitelisted_ip: is_whitelisted_ip(req, state),
        ..RateLimitContext::default()
    };
    context.decimal_ip = decimal_ip_from_req(req);
    if context.decimal_ip.is_none() {
        context.source_addr = source_addr_from_req(req);
    }
    context
}

async fn populate_api_key_context(
    context: &mut RateLimitContext,
    req: &Request,
    gateway_state: &GatewayState,
) {
    context.auth_context_ready = true;

    if let Some(key_str) = req.headers().get("x-api-key").and_then(|v| v.to_str().ok())
        && key_str.len() == uuid::fmt::Hyphenated::LENGTH
        && let Ok(uuid) = Uuid::parse_str(key_str)
    {
        context.key_is_uuid = true;
        context.is_generic_key = gateway_state.is_generic_key(&uuid);
        if !context.is_generic_key {
            let source_key = if context.is_whitelisted_ip {
                None
            } else {
                context.auth_guard_source()
            };
            let lookup = gateway_state
                .lookup_api_key_with_unknown_key_guard(key_str, source_key)
                .await;
            context.has_valid_api_key = lookup.user_id.is_some();
            context.is_company_key = lookup.company_id.is_some();
            context.user_id = lookup.user_id;
            context.user_email = lookup.user_email;
            context.user_limits = lookup.user_limits;
            context.billing_owner = lookup.billing_owner;
            context.auth_lookup_blocked = lookup.auth_lookup_blocked;
            if let (Some(cid), Some((name, concurrent, daily))) =
                (lookup.company_id, lookup.company_info)
            {
                context.company = Some(CompanyRateLimit {
                    id: cid,
                    name,
                    concurrent_limit: concurrent,
                    daily_limit: daily,
                });
            }
        }
    }
}

#[handler]
pub async fn prepare_rate_limit_context(
    depot: &mut Depot,
    req: &mut Request,
) -> Result<(), ServerError> {
    let state = depot.require::<HttpState>()?.clone();
    depot.inject(base_rate_limit_context(req, &state));
    Ok(())
}

pub async fn ensure_api_key_context(
    depot: &mut Depot,
    req: &mut Request,
) -> Result<(), ServerError> {
    let state = depot.require::<HttpState>()?.clone();
    let gateway_state = state.gateway_state().clone();
    let mut context = match depot.obtain::<RateLimitContext>() {
        Ok(existing) => existing.clone(),
        Err(_) => base_rate_limit_context(req, &state),
    };
    if context.auth_context_ready {
        return Ok(());
    }

    populate_api_key_context(&mut context, req, &gateway_state).await;
    depot.inject(context);
    Ok(())
}

pub type PerIPRateLimiter = RateLimiter<
    FixedGuard,
    MokaStore<<CachedIpIssuer as RateIssuer>::Key, FixedGuard>,
    CachedIpIssuer,
    BasicQuota,
>;

pub struct CachedIpIssuer;

impl RateIssuer for CachedIpIssuer {
    type Key = Arc<str>;

    async fn issue(&self, req: &mut Request, depot: &Depot) -> Option<Self::Key> {
        cached_decimal_ip(req, depot)
    }
}

pub struct RateLimiters {
    pub basic_limiter: PerIPRateLimiter,
    // Shared limiter for worker-facing endpoints: /add_result, /get_load, /get_leader.
    pub worker_limiter: PerIPRateLimiter,
    pub status_limiter: PerIPRateLimiter,
}

impl RateLimiters {
    pub fn new(http_config: &HTTPConfig) -> Self {
        let basic_limiter =
            Self::create_ip_rate_limiter_with_whitelist_skip(http_config.basic_rate_limit);
        let worker_limiter = Self::create_ip_rate_limiter_with_whitelist_skip(
            http_config.worker_per_minute_rate_limit,
        );
        let status_limiter =
            Self::create_ip_rate_limiter_with_whitelist_skip(http_config.get_status_rate_limit);

        Self {
            basic_limiter,
            worker_limiter,
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
    Ok(())
}

#[handler]
pub async fn unauthorized_only_rate_limit(
    depot: &mut Depot,
    req: &mut Request,
    _res: &mut Response,
    _ctrl: &mut FlowCtrl,
) -> Result<(), ServerError> {
    ensure_api_key_context(depot, req).await?;
    let state = depot.require::<HttpState>()?.clone();
    let ctx = depot
        .obtain::<RateLimitContext>()
        .map_err(|e| ServerError::Internal(format!("RateLimitContext missing: {:?}", e)))?;
    if ctx.is_whitelisted_ip || ctx.has_authorized_key() {
        return Ok(());
    }

    let Some(subject) = cached_decimal_ip(req, depot) else {
        return Ok(());
    };
    let allowed = state
        .unauthorized_daily_limiter()
        .try_acquire(
            subject,
            DistributedRateLimiter::current_day_epoch(),
            state
                .gateway_state()
                .add_task_unauthorized_per_ip_daily_rate_limit(),
        )
        .await;
    if allowed {
        return Ok(());
    }

    Err(ServerError::Json(
        StatusCode::TOO_MANY_REQUESTS,
        json!({
            "error": "unauthorized_daily_limit",
            "message": "Unauthorized per-IP daily rate limit exceeded."
        }),
    ))
}

#[handler]
pub async fn worker_rate_limit(
    depot: &mut Depot,
    req: &mut Request,
    res: &mut Response,
    ctrl: &mut FlowCtrl,
) -> Result<(), ServerError> {
    let state = depot.require::<HttpState>()?.clone();
    let cfg = state.config();
    cfg.ip_rate_limiters()
        .worker_limiter
        .handle(req, depot, res, ctrl)
        .await;
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
    Ok(())
}

struct SubjectParams<'a> {
    subject: Subject,
    id: u128,
    active_limit: u64,
    daily_limit: u64,
    scope_label: &'a str,
    require_day_match: bool,
}

fn pending_increments(params: &SubjectParams<'_>) -> (u32, u32) {
    (
        u32::from(params.active_limit > 0),
        u32::from(params.daily_limit > 0),
    )
}

fn subject_delta(day_epoch: u64, params: &SubjectParams<'_>) -> Option<RateLimitDelta> {
    let (add_active, add_day) = pending_increments(params);
    let has_limits = add_active > 0 || add_day > 0;
    if !has_limits {
        return None;
    }

    Some(RateLimitDelta {
        subject: params.subject,
        id: params.id,
        day_epoch,
        add_active,
        add_day,
    })
}

async fn check_subject_limit(
    limiter: &DistributedRateLimiter,
    gs: &GatewayState,
    day_epoch: u64,
    params: &SubjectParams<'_>,
) -> Result<(), ServerError> {
    let subject = params.subject;
    let id = params.id;
    let active_limit = params.active_limit;
    let daily_limit = params.daily_limit;
    let scope_label = params.scope_label;
    let require_day_match = params.require_day_match;

    let has_limits = active_limit > 0 || daily_limit > 0;
    let (cluster_active, cluster_day) = if has_limits {
        limiter
            .cluster_usage(
                gs,
                ClusterUsageParams {
                    subject,
                    id,
                    active_limit,
                    daily_limit,
                    require_day_match,
                    day_epoch,
                },
            )
            .await
    } else {
        (0, 0)
    };

    if let Err(rejection) = limiter
        .check_and_incr(CheckAndIncrParams {
            key: ClientKey { subject, id },
            active_limit,
            daily_limit,
            cluster_active,
            cluster_day,
            day_epoch,
        })
        .await
    {
        let message = match rejection {
            RateLimitRejection::Active => {
                format!("{} concurrent task limit exceeded.", scope_label)
            }
            RateLimitRejection::Daily => {
                format!("{} daily task limit exceeded.", scope_label)
            }
        };
        return Err(match rejection {
            RateLimitRejection::Active => ServerError::Json(
                StatusCode::TOO_MANY_REQUESTS,
                json!({ "error": "concurrent_limit", "message": message }),
            ),
            RateLimitRejection::Daily => ServerError::Json(
                StatusCode::TOO_MANY_REQUESTS,
                json!({ "error": "daily_limit", "message": message }),
            ),
        });
    }

    Ok(())
}

pub async fn reserve_add_task_rate_limit(
    depot: &mut Depot,
) -> Result<Option<RateLimitReservation>, ServerError> {
    let ctx = depot
        .obtain::<RateLimitContext>()
        .map_err(|e| ServerError::Internal(format!("RateLimitContext missing: {:?}", e)))?;

    // Whitelisted IPs bypass the distributed /add_task quota checks too.
    if ctx.is_whitelisted_ip {
        return Ok(None);
    }

    let state = depot.require::<HttpState>()?.clone();
    let cfg = state.config();
    let rate_limits = cfg.rate_limits();
    let limiter = rate_limits.distributed().clone();
    let gs = state.gateway_state().clone();

    let day_epoch = limiter.day_epoch();
    let mut subjects = Vec::with_capacity(3);

    // Company keys first, enforce their limits if present
    if let Some(company) = ctx.company.as_ref() {
        subjects.push(SubjectParams {
            subject: Subject::Company,
            id: company.id.as_u128(),
            active_limit: company.concurrent_limit,
            daily_limit: company.daily_limit,
            scope_label: company.name.as_ref(),
            require_day_match: company.daily_limit > 0,
        });
    } else if let (Some(user_id), Some((concurrent_limit, daily_limit))) =
        (ctx.user_id, ctx.user_limits)
    {
        // Otherwise fall back to per-user quota when we have a user id
        subjects.push(SubjectParams {
            subject: Subject::User,
            id: user_subject_id(user_id),
            active_limit: concurrent_limit,
            daily_limit,
            scope_label: "User",
            require_day_match: daily_limit > 0,
        });
    } else if ctx.key_is_uuid && ctx.is_generic_key {
        subjects.push(SubjectParams {
            subject: Subject::GenericGlobal,
            id: GENERIC_GLOBAL_SUBJECT_ID,
            active_limit: cfg.http().generic_key_concurrent_limit as u64,
            daily_limit: 0,
            scope_label: "Generic key",
            require_day_match: false,
        });
    }

    let mut succeeded = Vec::with_capacity(subjects.len());
    for params in &subjects {
        if let Err(err) = check_subject_limit(&limiter, &gs, day_epoch, params).await {
            for (key, (active_decrement, day_decrement)) in succeeded.into_iter().rev() {
                limiter
                    .rollback_pending(key, active_decrement, day_decrement, day_epoch)
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
        if let Some(delta) = subject_delta(day_epoch, params) {
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
        let day_epoch = 70u64;

        assert!(
            bound_limiter
                .check_and_incr(CheckAndIncrParams {
                    key,
                    active_limit: 1,
                    daily_limit: 0,
                    cluster_active: 0,
                    cluster_day: 0,
                    day_epoch,
                })
                .await
                .is_ok()
        );
        assert!(
            bound_limiter
                .check_and_incr(CheckAndIncrParams {
                    key,
                    active_limit: 1,
                    daily_limit: 0,
                    cluster_active: 0,
                    cluster_day: 0,
                    day_epoch,
                })
                .await
                .is_err()
        );
        assert!(
            other_limiter
                .check_and_incr(CheckAndIncrParams {
                    key,
                    active_limit: 1,
                    daily_limit: 0,
                    cluster_active: 0,
                    cluster_day: 0,
                    day_epoch,
                })
                .await
                .is_ok()
        );
        assert!(
            other_limiter
                .check_and_incr(CheckAndIncrParams {
                    key,
                    active_limit: 1,
                    daily_limit: 0,
                    cluster_active: 0,
                    cluster_day: 0,
                    day_epoch,
                })
                .await
                .is_err()
        );

        let reservation = RateLimitReservation::new(
            bound_limiter.clone(),
            vec![RateLimitDelta {
                subject: key.subject,
                id: key.id,
                day_epoch,
                add_active: 1,
                add_day: 0,
            }],
        );

        reservation.rollback_pending().await;

        assert!(
            bound_limiter
                .check_and_incr(CheckAndIncrParams {
                    key,
                    active_limit: 1,
                    daily_limit: 0,
                    cluster_active: 0,
                    cluster_day: 0,
                    day_epoch,
                })
                .await
                .is_ok()
        );
        assert!(
            other_limiter
                .check_and_incr(CheckAndIncrParams {
                    key,
                    active_limit: 1,
                    daily_limit: 0,
                    cluster_active: 0,
                    cluster_day: 0,
                    day_epoch,
                })
                .await
                .is_err(),
            "rollback must not touch unrelated limiter instances"
        );
    }

    #[test]
    fn subject_delta_uses_same_pending_shape_as_local_increment() {
        let params = SubjectParams {
            subject: Subject::Company,
            id: 123u128,
            active_limit: 0,
            daily_limit: 10,
            scope_label: "Company",
            require_day_match: true,
        };

        let (active_pending, day_pending) = pending_increments(&params);
        assert_eq!(active_pending, 0);
        assert_eq!(day_pending, 1);

        let delta = subject_delta(2, &params).expect("delta should exist");
        assert_eq!(delta.add_day, day_pending);
    }

    #[test]
    fn subject_delta_disables_day_increment_when_daily_limit_is_zero() {
        let params = SubjectParams {
            subject: Subject::Company,
            id: 321u128,
            active_limit: 0,
            daily_limit: 0,
            scope_label: "Company",
            require_day_match: false,
        };

        let (active_pending, day_pending) = pending_increments(&params);
        assert_eq!(active_pending, 0);
        assert_eq!(day_pending, 0);

        assert!(subject_delta(3, &params).is_none());
    }

    #[test]
    fn active_charge_and_deferred_charge_are_split_cleanly() {
        let reservation = RateLimitReservation::new(
            DistributedRateLimiter::new(64),
            vec![
                RateLimitDelta {
                    subject: Subject::User,
                    id: 7u128,
                    day_epoch: 2,
                    add_active: 1,
                    add_day: 1,
                },
                RateLimitDelta {
                    subject: Subject::GenericGlobal,
                    id: 0u128,
                    day_epoch: 2,
                    add_active: 0,
                    add_day: 1,
                },
            ],
        );

        let active = reservation.active_charge_mutations();
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].active_delta, 1);
        assert_eq!(active[0].day_delta, 0);

        let deferred = reservation.deferred_charge_mutations();
        assert_eq!(deferred.len(), 2);
        assert_eq!(deferred[0].active_delta, 0);
        assert_eq!(deferred[0].day_delta, 1);
        assert_eq!(deferred[1].day_delta, 1);
    }

    #[test]
    fn accepted_charge_keeps_active_and_day_mutations_together() {
        let reservation = RateLimitReservation::new(
            DistributedRateLimiter::new(64),
            vec![
                RateLimitDelta {
                    subject: Subject::User,
                    id: 7u128,
                    day_epoch: 2,
                    add_active: 1,
                    add_day: 1,
                },
                RateLimitDelta {
                    subject: Subject::GenericGlobal,
                    id: 0u128,
                    day_epoch: 2,
                    add_active: 0,
                    add_day: 1,
                },
            ],
        );

        let accepted = reservation.accepted_charge_mutations();
        assert_eq!(accepted.len(), 2);
        assert_eq!(accepted[0].active_delta, 1);
        assert_eq!(accepted[0].day_delta, 1);
        assert_eq!(accepted[1].active_delta, 0);
        assert_eq!(accepted[1].day_delta, 1);
    }
}
