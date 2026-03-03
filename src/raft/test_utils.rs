use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Once};
use std::time::Instant;
use std::{io::ErrorKind, time::Duration as StdDuration};

use anyhow::{Result, bail};
use foldhash::fast::RandomState;
use openraft::{BasicNode, LogId};
use tokio::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::config::RServerConfig;
use crate::raft::client::{RClient, RClientBuilder};
use crate::raft::server::RServer;
use crate::raft::{LogStore, Network, NodeId, Raft, StateMachineStore, TypeConfig};

static COUNTER: AtomicU64 = AtomicU64::new(0);
static TRACING: Once = Once::new();
static CRYPTO_PROVIDER_INIT: Once = Once::new();

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

pub(crate) fn reserve_udp_addresses(
    count: usize,
) -> Result<(Vec<String>, Vec<std::net::UdpSocket>)> {
    let mut addrs = Vec::with_capacity(count);
    let mut sockets = Vec::with_capacity(count);
    for _ in 0..count {
        let socket = std::net::UdpSocket::bind("127.0.0.1:0")?;
        let addr = socket.local_addr()?.to_string();
        addrs.push(addr);
        sockets.push(socket);
    }
    Ok((addrs, sockets))
}

pub(crate) async fn setup_standalone_raft_node(
    node_id: u64,
) -> Result<(openraft::Raft<TypeConfig>, Arc<StateMachineStore>)> {
    CRYPTO_PROVIDER_INIT.call_once(|| {
        crate::raft::init_crypto_provider().expect("crypto provider init must succeed");
    });

    let log_store = LogStore::default();
    let state_machine_store = Arc::new(StateMachineStore::default());
    let node_clients = scc::HashMap::with_capacity_and_hasher(5, RandomState::default());
    let network = Network::new(Arc::new(node_clients));

    let config = Arc::new(
        openraft::Config {
            heartbeat_interval: 100,
            election_timeout_min: 300,
            election_timeout_max: 600,
            ..Default::default()
        }
        .validate()?,
    );

    let raft = openraft::Raft::new(
        node_id,
        Arc::clone(&config),
        network,
        log_store,
        state_machine_store.clone(),
    )
    .await?;

    Ok((raft, state_machine_store))
}

pub(crate) async fn start_server_on_reserved_addr(
    raft: Raft,
    pcfg: &RServerConfig,
) -> Result<(String, RServer)> {
    let (addrs, addr_reservations) = reserve_udp_addresses(1)?;
    let addr = addrs[0].clone();
    drop(addr_reservations);
    let server = start_test_rserver(addr.as_str(), raft, pcfg).await?;

    tokio::time::sleep(StdDuration::from_millis(100)).await;

    Ok((addr, server))
}

pub(crate) async fn start_test_rserver(
    bind_addr: &str,
    raft: Raft,
    pcfg: &RServerConfig,
) -> Result<RServer> {
    const MAX_BIND_RETRIES: usize = 50;
    const BIND_RETRY_DELAY: Duration = Duration::from_millis(100);

    let mut last_addr_in_use = None;
    for attempt in 1..=MAX_BIND_RETRIES {
        match RServer::new(
            bind_addr,
            None,
            raft.clone(),
            pcfg.clone(),
            CancellationToken::new(),
        )
        .await
        {
            Ok(server) => return Ok(server),
            Err(err) if is_addr_in_use(&err) && attempt < MAX_BIND_RETRIES => {
                warn!(
                    "RServer bind addr {bind_addr} in use (attempt {attempt}/{MAX_BIND_RETRIES}); retrying"
                );
                last_addr_in_use = Some(err);
                tokio::time::sleep(BIND_RETRY_DELAY).await;
            }
            Err(err) => return Err(err),
        }
    }

    Err(last_addr_in_use.expect("addr-in-use retry loop must capture last error"))
}

fn is_addr_in_use(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| {
        cause
            .downcast_ref::<std::io::Error>()
            .is_some_and(|io_err| io_err.kind() == ErrorKind::AddrInUse)
    })
}

pub(crate) async fn create_default_local_rclient(
    remote_addr: &str,
    pcfg: &RServerConfig,
) -> Result<RClient> {
    RClientBuilder::new()
        .remote_addr(remote_addr)
        .server_name("localhost")
        .local_bind_addr("127.0.0.1:0")
        .dangerous_skip_verification(true)
        .protocol_cfg(pcfg.clone())
        .build()
        .await
}
