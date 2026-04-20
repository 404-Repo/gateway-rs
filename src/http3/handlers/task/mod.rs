mod add_task;
mod add_task_billing;
mod add_task_log;
mod add_task_parse;
mod add_task_rate_limit;
mod get_load;
mod get_tasks;

use std::time::{SystemTime, UNIX_EPOCH};

use crate::api::Task;

pub use add_task::add_task_handler;
pub use get_load::get_load_handler;
pub use get_tasks::get_tasks_handler;

pub(super) fn task_kind_label(task: &Task) -> &'static str {
    if task.image.is_some() {
        "img3d"
    } else {
        "txt3d"
    }
}

pub(super) fn current_time_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}
