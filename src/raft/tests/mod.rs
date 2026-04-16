#![cfg(test)]
use crate::raft::LogStore;
use crate::raft::Network;
use crate::raft::Raft;
use crate::raft::Request;
use crate::raft::StateMachineStore;
use crate::raft::client::RClientBuilder;
use crate::raft::test_utils::{
    ensure_crypto_provider_for_tests, init_tracing, make_node_clients, membership_from,
    reserve_udp_addresses, start_test_rserver, unique_path, wait_for_log_commit,
    wait_for_node_to_be_voter, wait_for_snapshot_file,
};
use crate::{
    config::RServerConfig,
    raft::{client::RClient, server::RServer},
};
use anyhow::{Result, anyhow, bail};
use foldhash::fast::RandomState;
use openraft::storage::RaftLogStorage;
use openraft::{BasicNode, Config};
use std::{fs, path::PathBuf, sync::Arc};
use tempfile::{Builder, TempDir};
use tokio::time::Duration;
use tracing::info;
use uuid::Uuid;

mod cluster_setup {
    use super::*;
    use std::sync::OnceLock;

    static LOG_STORE_REGISTRY: OnceLock<scc::HashMap<String, LogStore, RandomState>> =
        OnceLock::new();

    pub(super) struct NodeStoragePaths {
        pub(super) snapshot_dir: PathBuf,
        pub(super) log_path: PathBuf,
        pub(super) retention: usize,
        _temp_dir: TempDir,
    }

    impl NodeStoragePaths {
        fn new(temp_dir: TempDir, retention: usize) -> anyhow::Result<Self> {
            let base_dir = temp_dir.path();
            let snapshot_dir = base_dir.join("snapshots");
            fs::create_dir_all(&snapshot_dir)?;
            let log_path = unique_path(base_dir, "persistent_log_store_", ".bin");
            Ok(Self {
                snapshot_dir,
                log_path,
                retention,
                _temp_dir: temp_dir,
            })
        }

        pub(super) fn for_node(node_id: u64, retention: usize) -> anyhow::Result<Self> {
            let temp_dir = Builder::new()
                .prefix(&format!("gateway_persistent_test_node_{node_id}_"))
                .tempdir()?;
            Self::new(temp_dir, retention)
        }
    }

    fn register_log_store(key: &str, log_store: &LogStore) {
        let registry = LOG_STORE_REGISTRY
            .get_or_init(|| scc::HashMap::with_capacity_and_hasher(16, RandomState::default()));
        let _ = registry.insert_sync(key.to_string(), log_store.clone());
    }

    pub(super) fn get_log_store_handle(key: &str) -> Result<LogStore> {
        let registry = LOG_STORE_REGISTRY
            .get()
            .ok_or_else(|| anyhow!("log store registry not initialized"))?;

        registry
            .get_sync(key)
            .map(|ref_| (*ref_).clone())
            .ok_or_else(|| anyhow!("log store not found for key {key}"))
    }

    pub(super) async fn setup_node(
        node_id: u64,
        addr: &str,
        config: Arc<openraft::Config>,
        node_clients: Arc<scc::HashMap<String, RClient, RandomState>>,
        storage: Option<&NodeStoragePaths>,
    ) -> Result<(Raft, Arc<StateMachineStore>, RServer)> {
        let network = Network::new(node_clients.clone());
        let (log_store, state_machine_store) = match storage {
            Some(paths) => {
                let log_store = LogStore::with_persistence(&paths.log_path)?;
                let state_machine_store = Arc::new(StateMachineStore::with_persistence(
                    &paths.snapshot_dir,
                    paths.retention,
                    Some(paths.log_path.clone()),
                )?);
                (log_store, state_machine_store)
            }
            None => (LogStore::default(), Arc::new(StateMachineStore::default())),
        };
        register_log_store(addr, &log_store);
        let pcfg = RServerConfig::default();
        let raft = openraft::Raft::new(
            node_id,
            Arc::clone(&config),
            network,
            log_store,
            state_machine_store.clone(),
        )
        .await?;
        let server = start_test_rserver(addr, raft.clone(), &pcfg).await?;
        Ok((raft, state_machine_store, server))
    }

    pub(super) async fn setup_cluster(
        node_configs: &[(u64, &str)],
        node_clients: Arc<scc::HashMap<String, RClient, RandomState>>,
        storage_paths: Option<&[NodeStoragePaths]>,
    ) -> Result<(
        Arc<Config>,
        RServerConfig,
        Vec<Raft>,
        Vec<Arc<StateMachineStore>>,
        Vec<RServer>,
    )> {
        ensure_crypto_provider_for_tests();

        let pcfg = RServerConfig::default();
        let config = Arc::new(
            openraft::Config {
                heartbeat_interval: 500,
                election_timeout_min: 5000,
                election_timeout_max: 10000,
                install_snapshot_timeout: 500,
                ..Default::default()
            }
            .validate()?,
        );
        let mut raft_nodes = Vec::new();
        let mut state_machines = Vec::new();
        let mut server_handles = Vec::new();
        for (idx, &(node_id, addr)) in node_configs.iter().enumerate() {
            let storage = storage_paths.and_then(|paths| paths.get(idx));
            let (raft, sm, server) =
                setup_node(node_id, addr, config.clone(), node_clients.clone(), storage).await?;
            raft_nodes.push(raft);
            state_machines.push(sm);
            server_handles.push(server);
        }
        Ok((config, pcfg, raft_nodes, state_machines, server_handles))
    }

    pub(super) async fn connect_clients<F>(
        node_configs: &[(u64, &str)],
        node_clients: &Arc<scc::HashMap<String, RClient, RandomState>>,
        pcfg: &RServerConfig,
        mut bind_addr_fn: F,
    ) -> Result<()>
    where
        F: FnMut(usize, &(u64, &str)) -> String,
    {
        for (idx, &(node_id, server_addr)) in node_configs.iter().enumerate() {
            let local_bind_addr = bind_addr_fn(idx, &(node_id, server_addr));
            let client = RClientBuilder::new()
                .remote_addr(server_addr)
                .server_name("localhost")
                .local_bind_addr(&local_bind_addr)
                .dangerous_skip_verification(true)
                .protocol_cfg(pcfg.clone())
                .build()
                .await?;
            node_clients
                .insert_sync(server_addr.to_string(), client)
                .map_err(|e| anyhow::anyhow!("{e:?}"))?;
        }
        Ok(())
    }

    pub(super) type OwnedNodeConfig = (u64, String);

    pub(super) fn reserve_node_configs(node_count: usize) -> Result<Vec<OwnedNodeConfig>> {
        let (node_addrs, node_addr_reservations) = reserve_udp_addresses(node_count)?;
        drop(node_addr_reservations);

        Ok(node_addrs
            .into_iter()
            .enumerate()
            .map(|(idx, addr)| (idx as u64 + 1, addr))
            .collect())
    }

    pub(super) fn node_config_refs(node_configs: &[OwnedNodeConfig]) -> Vec<(u64, &str)> {
        node_configs
            .iter()
            .map(|(id, addr)| (*id, addr.as_str()))
            .collect()
    }

    pub(super) fn node_index(node_configs: &[OwnedNodeConfig], node_id: u64) -> usize {
        node_configs
            .iter()
            .position(|(id, _)| *id == node_id)
            .expect("node id must exist in node config")
    }

    pub(super) async fn setup_connected_cluster(
        node_configs: &[OwnedNodeConfig],
        node_clients: Arc<scc::HashMap<String, RClient, RandomState>>,
        storage_paths: Option<&[NodeStoragePaths]>,
    ) -> Result<(
        Arc<Config>,
        RServerConfig,
        Vec<Raft>,
        Vec<Arc<StateMachineStore>>,
        Vec<RServer>,
    )> {
        let refs = node_config_refs(node_configs);
        let (config, pcfg, raft_nodes, state_machines, server_handles) =
            setup_cluster(&refs, node_clients.clone(), storage_paths).await?;

        tokio::time::sleep(Duration::from_secs(2)).await;
        connect_clients(&refs, &node_clients, &pcfg, |_, _| {
            "127.0.0.1:0".to_string()
        })
        .await?;

        Ok((config, pcfg, raft_nodes, state_machines, server_handles))
    }

    pub(super) async fn initialize_first_node_membership(
        raft_nodes: &[Raft],
        node_configs: &[OwnedNodeConfig],
    ) -> Result<()> {
        let refs = node_config_refs(node_configs);
        let initial_members = membership_from(&refs);
        raft_nodes[0].initialize(initial_members).await?;
        Ok(())
    }

    pub(super) fn initialize_all_nodes_membership(
        raft_nodes: &[Raft],
        node_configs: &[OwnedNodeConfig],
    ) {
        let refs = node_config_refs(node_configs);
        for raft in raft_nodes {
            let raft = raft.clone();
            let initial_nodes = membership_from(&refs);
            tokio::spawn(async move {
                raft.initialize(initial_nodes).await.unwrap();
            });
        }
    }

    pub(super) fn assert_all_nodes_see_leader(raft_nodes: &[Raft], leader_id: u64) {
        for (i, raft) in raft_nodes.iter().enumerate() {
            let observed_leader = raft.metrics().borrow().current_leader;
            assert_eq!(
                observed_leader,
                Some(leader_id),
                "Node {} sees a different leader",
                i + 1
            );
        }
    }

    pub(super) async fn assert_state_machine_value(
        state_machines: &[Arc<StateMachineStore>],
        key: &str,
        expected: &str,
    ) {
        for state_machine in state_machines {
            let sm = state_machine.state_machine.read().await;
            let value = sm.data.get(key).unwrap();
            let value_str: String = rmp_serde::from_slice(value).unwrap();
            assert_eq!(value_str, expected);
        }
    }

    pub(super) fn set_request(key: &str, value: &str) -> Request {
        Request::Set {
            request_id: Uuid::new_v4().as_u128(),
            key: key.to_string(),
            value: rmp_serde::to_vec(value).unwrap(),
        }
    }

    pub(super) async fn client_write_and_wait(
        raft: &Raft,
        nodes: &[Raft],
        request: Request,
    ) -> Result<()> {
        let write_result = raft.client_write(request).await?;
        wait_for_log_commit(nodes, write_result.log_id).await
    }

    pub(super) async fn add_learner_and_wait(
        leader: &Raft,
        nodes: &[Raft],
        node_id: u64,
        addr: &str,
    ) -> Result<()> {
        let resp = leader
            .add_learner(
                node_id,
                BasicNode {
                    addr: addr.to_string(),
                },
                false,
            )
            .await?;
        wait_for_log_commit(nodes, resp.log_id).await
    }

    pub(super) async fn create_rclient(
        remote_addr: &str,
        local_bind_addr: &str,
        pcfg: &RServerConfig,
    ) -> Result<RClient> {
        RClientBuilder::new()
            .remote_addr(remote_addr)
            .server_name("localhost")
            .local_bind_addr(local_bind_addr)
            .dangerous_skip_verification(true)
            .protocol_cfg(pcfg.clone())
            .build()
            .await
    }
}

mod cluster_wait {
    use super::*;

    pub(super) async fn wait_for_leader(nodes: &[Raft], timeout: Duration) -> Result<u64> {
        let start = std::time::Instant::now();

        loop {
            for node in nodes {
                match node.metrics().borrow().current_leader {
                    Some(leader_id) => return Ok(leader_id),
                    None => continue,
                }
            }

            if start.elapsed() > timeout {
                bail!("No leader elected within timeout period");
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    pub(super) async fn wait_for_leader_consistent(
        nodes: &[Raft],
        timeout: Duration,
    ) -> anyhow::Result<u64> {
        let start = std::time::Instant::now();

        while start.elapsed() <= timeout {
            let mut maybe_leader = None;
            let mut all_agree = true;

            for node in nodes {
                if let Some(current_leader) = node.metrics().borrow().current_leader {
                    match maybe_leader {
                        Some(existing_leader) if existing_leader != current_leader => {
                            all_agree = false;
                            break;
                        }
                        Some(_) => {}
                        None => maybe_leader = Some(current_leader),
                    }
                } else {
                    all_agree = false;
                    break;
                }
            }

            if all_agree && let Some(final_leader) = maybe_leader {
                return Ok(final_leader);
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        anyhow::bail!("Not all nodes saw a consistent leader within the timeout");
    }

    pub(super) async fn wait_for_leader_consistent_excluding(
        nodes: &[Raft],
        exclude_node: u64,
        timeout: Duration,
    ) -> anyhow::Result<u64> {
        let start = std::time::Instant::now();

        while start.elapsed() <= timeout {
            let mut maybe_leader = None;
            let mut all_agree = true;

            for node in nodes {
                if let Some(current_leader) = node.metrics().borrow().current_leader {
                    if current_leader == exclude_node {
                        all_agree = false;
                        break;
                    }

                    match maybe_leader {
                        Some(existing_leader) if existing_leader != current_leader => {
                            all_agree = false;
                            break;
                        }
                        Some(_) => {}
                        None => maybe_leader = Some(current_leader),
                    }
                } else {
                    all_agree = false;
                    break;
                }
            }

            if all_agree && let Some(final_leader) = maybe_leader {
                return Ok(final_leader);
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        anyhow::bail!("Not all nodes saw a consistent leader within the timeout");
    }
}

use cluster_setup::*;
use cluster_wait::*;

async fn wait_for_consistent_leader_index(
    raft_nodes: &[Raft],
    node_configs: &[OwnedNodeConfig],
    timeout: Duration,
) -> Result<(u64, usize)> {
    let leader_id = wait_for_leader_consistent(raft_nodes, timeout).await?;
    let leader_index = node_index(node_configs, leader_id);
    Ok((leader_id, leader_index))
}

mod cluster_bootstrap;
mod cross_gateway_add_task;
mod leader_failover;
mod membership_flows;
mod rate_limit_queue_flush;
mod rate_limit_sync;
mod snapshot_flows;
