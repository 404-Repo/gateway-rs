use std::sync::Arc;

use uuid::Uuid;

use crate::http3::rate_limits::RateLimitContext;
use crate::raft::gateway_state::{ActivityEventRef, GatewayState};

pub struct TaskActivityContext<'a> {
    pub gateway_state: &'a GatewayState,
    pub rate_ctx: &'a RateLimitContext,
    pub origin: &'a str,
    pub task_kind: &'a str,
    pub model: Option<&'a str>,
    pub task_id: Option<Uuid>,
}

pub fn record_task_activity(ctx: TaskActivityContext<'_>, action: &str) {
    let mut company_id = None;
    let mut company_name: Option<Arc<str>> = None;
    if ctx.rate_ctx.is_company_key
        && let Some(company) = ctx.rate_ctx.company.as_ref()
    {
        company_id = Some(company.id);
        company_name = Some(company.name.clone());
    }
    ctx.gateway_state.record_activity_event(ActivityEventRef {
        user_id: ctx.rate_ctx.user_id,
        user_email: ctx.rate_ctx.user_email.as_deref(),
        company_id,
        company_name: company_name.as_deref(),
        action,
        tool: ctx.origin,
        task_kind: ctx.task_kind,
        model: ctx.model,
        task_id: ctx.task_id,
    });
}
