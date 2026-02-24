use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Once};
use std::time::Instant;

use anyhow::{Result, bail};
use foldhash::fast::RandomState;
use openraft::{BasicNode, LogId};
use tokio::time::Duration;

use crate::raft::{NodeId, Raft, client::RClient};

static COUNTER: AtomicU64 = AtomicU64::new(0);
static TRACING: Once = Once::new();

pub(crate) fn unique_path(dir: &Path, prefix: &str, suffix: &str) -> PathBuf {
    let id = COUNTER.fetch_add(1, Ordering::Relaxed);
    dir.join(format!("{prefix}{id:016x}{suffix}"))
}

pub(crate) fn init_tracing() {
    TRACING.call_once(|| {
        tracing_subscriber::FmtSubscriber::builder()
            .with_max_level(tracing::Level::DEBUG)
            .with_test_writer()
            .init();
    });
}

pub(crate) fn membership_from(
    node_configs: &[(u64, &str)],
) -> std::collections::BTreeMap<NodeId, BasicNode> {
    node_configs
        .iter()
        .map(|(node_id, addr)| {
            (
                *node_id,
                BasicNode {
                    addr: (*addr).to_string(),
                },
            )
        })
        .collect()
}

pub(crate) async fn wait_for_snapshot_file(
    dir: &Path,
    timeout: Duration,
) -> anyhow::Result<PathBuf> {
    let start = Instant::now();
    loop {
        if dir.exists()
            && let Ok(entries) = std::fs::read_dir(dir)
        {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_file() {
                    return Ok(path);
                }
            }
        }

        if start.elapsed() > timeout {
            anyhow::bail!("Snapshot file was not created within {:?}", timeout);
        }

        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

pub(crate) async fn wait_for_node_to_be_voter(
    raft: &Raft,
    node_id: NodeId,
    timeout: Duration,
) -> anyhow::Result<()> {
    let start = Instant::now();
    loop {
        {
            let metrics = raft.metrics();
            let membership = metrics.borrow().membership_config.clone();
            if membership.voter_ids().any(|id| id == node_id) {
                return Ok(());
            }
        }

        if start.elapsed() > timeout {
            anyhow::bail!("Node {node_id} did not regain voter status before timeout");
        }

        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

pub(crate) async fn wait_for_log_commit(
    nodes: &[Raft],
    target_log_id: LogId<NodeId>,
) -> Result<()> {
    let start = Instant::now();
    let timeout = Duration::from_secs(20);

    loop {
        let mut all_synced = true;
        let mut status = String::new();

        for (i, node) in nodes.iter().enumerate() {
            let metrics_ref = node.metrics();
            let metrics = metrics_ref.borrow();
            let last_applied = metrics.last_applied;
            let last_index = metrics.last_log_index;
            let current_leader = metrics.current_leader;

            status.push_str(&format!(
                "\nNode {} - last_applied: {:?}, last_index: {:?}, leader: {:?}, target: {:?}",
                i + 1,
                last_applied,
                last_index,
                current_leader,
                target_log_id
            ));

            if last_applied.is_none_or(|id| id < target_log_id) {
                all_synced = false;
            }
        }

        if all_synced {
            println!("All nodes synced successfully");
            return Ok(());
        }

        if start.elapsed() > timeout {
            println!("Sync status before timeout:{}", status);
            bail!(
                "Nodes failed to sync within {} seconds. Status:{}",
                timeout.as_secs(),
                status
            );
        }

        println!("Waiting for sync... Current status:{}", status);
        tokio::time::sleep(Duration::from_millis(1000)).await;
    }
}

pub(crate) fn make_node_clients(len: usize) -> Arc<scc::HashMap<String, RClient, RandomState>> {
    Arc::new(scc::HashMap::with_capacity_and_hasher(
        len,
        RandomState::default(),
    ))
}
