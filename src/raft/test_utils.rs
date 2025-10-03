use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

static COUNTER: AtomicU64 = AtomicU64::new(0);

pub(crate) fn unique_path(dir: &Path, prefix: &str, suffix: &str) -> PathBuf {
    let id = COUNTER.fetch_add(1, Ordering::Relaxed);
    dir.join(format!("{prefix}{id:016x}{suffix}"))
}
