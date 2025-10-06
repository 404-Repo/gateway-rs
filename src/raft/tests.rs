#[cfg(test)]
#[allow(clippy::module_inception)]
mod tests {
    use crate::raft::client::RClientBuilder;
    use crate::raft::init_crypto_provider;
    use crate::raft::test_utils::unique_path;
    use crate::raft::LogStore;
    use crate::raft::Network;
    use crate::raft::NodeId;
    use crate::raft::Raft;
    use crate::raft::Request;
    use crate::raft::StateMachineStore;
    use crate::{
        config::RServerConfig,
        raft::{client::RClient, server::RServer},
    };
    use anyhow::{anyhow, bail, Result};
    use core::panic;
    use foldhash::fast::RandomState;
    use openraft::storage::RaftLogStorage;
    use openraft::{BasicNode, Config, LogId, Membership};
    use std::{
        collections::{BTreeMap, BTreeSet},
        fs,
        path::{Path, PathBuf},
        sync::{Arc, Once, OnceLock},
        time::Instant,
    };
    use tempfile::{Builder, TempDir};
    use tokio::time::Duration;
    use tokio_util::sync::CancellationToken;
    use tracing::info;

    static TRACING: Once = Once::new();
    static LOG_STORE_REGISTRY: OnceLock<scc::HashMap<String, LogStore, RandomState>> =
        OnceLock::new();

    pub fn init_tracing() {
        TRACING.call_once(|| {
            tracing_subscriber::FmtSubscriber::builder()
                .with_max_level(tracing::Level::DEBUG)
                .with_test_writer()
                .init();
        });
    }

    fn register_log_store(key: &str, log_store: &LogStore) {
        let registry = LOG_STORE_REGISTRY
            .get_or_init(|| scc::HashMap::with_capacity_and_hasher(16, RandomState::default()));
        // Best-effort insert; overwrite if present.
        let _ = registry.insert_sync(key.to_string(), log_store.clone());
    }

    fn get_log_store_handle(key: &str) -> Result<LogStore> {
        let registry = LOG_STORE_REGISTRY
            .get()
            .ok_or_else(|| anyhow!("log store registry not initialized"))?;

        registry
            .get_sync(key)
            .map(|ref_| (*ref_).clone())
            .ok_or_else(|| anyhow!("log store not found for key {key}"))
    }

    struct NodeStoragePaths {
        snapshot_dir: PathBuf,
        log_path: PathBuf,
        retention: usize,
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

        fn for_node(node_id: u64, retention: usize) -> anyhow::Result<Self> {
            let temp_dir = Builder::new()
                .prefix(&format!("gateway_persistent_test_node_{node_id}_"))
                .tempdir()?;
            Self::new(temp_dir, retention)
        }
    }

    async fn setup_node(
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
        let server = RServer::new(addr, None, raft.clone(), pcfg, CancellationToken::new()).await?;
        Ok((raft, state_machine_store, server))
    }

    async fn setup_cluster(
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
        init_crypto_provider().unwrap();

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

    async fn connect_clients<F>(
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

    fn membership_from(node_configs: &[(u64, &str)]) -> BTreeMap<NodeId, BasicNode> {
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

    async fn assert_state_machine_value(
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

    async fn wait_for_snapshot_file(dir: &Path, timeout: Duration) -> anyhow::Result<PathBuf> {
        let start = Instant::now();
        loop {
            if dir.exists() {
                if let Ok(entries) = fs::read_dir(dir) {
                    for entry in entries.flatten() {
                        let path = entry.path();
                        if path.is_file() {
                            return Ok(path);
                        }
                    }
                }
            }

            if start.elapsed() > timeout {
                anyhow::bail!("Snapshot file was not created within {:?}", timeout);
            }

            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    async fn wait_for_node_to_be_voter(
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

    async fn wait_for_log_commit(nodes: &[Raft], target_log_id: LogId<NodeId>) -> Result<()> {
        let start = std::time::Instant::now();
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

    fn make_node_clients(len: usize) -> Arc<scc::HashMap<String, RClient, RandomState>> {
        Arc::new(scc::HashMap::with_capacity_and_hasher(
            len,
            RandomState::default(),
        ))
    }

    fn set_request(key: &str, value: &str) -> Request {
        Request::Set {
            key: key.to_string(),
            value: rmp_serde::to_vec(value).unwrap(),
        }
    }

    async fn client_write_and_wait(raft: &Raft, nodes: &[Raft], request: Request) -> Result<()> {
        let write_result = raft.client_write(request).await?;
        wait_for_log_commit(nodes, write_result.log_id).await
    }

    async fn add_learner_and_wait(
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

    async fn create_rclient(
        remote_addr: &str,
        local_bind_addr: &str,
        pcfg: &RServerConfig,
    ) -> Result<RClient> {
        Ok(RClientBuilder::new()
            .remote_addr(remote_addr)
            .server_name("localhost")
            .local_bind_addr(local_bind_addr)
            .dangerous_skip_verification(true)
            .protocol_cfg(pcfg.clone())
            .build()
            .await?)
    }

    async fn wait_for_leader(nodes: &[Raft], timeout: Duration) -> Result<u64> {
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

    async fn wait_for_leader_consistent(nodes: &[Raft], timeout: Duration) -> anyhow::Result<u64> {
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

            if all_agree {
                if let Some(final_leader) = maybe_leader {
                    return Ok(final_leader);
                }
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        anyhow::bail!("Not all nodes saw a consistent leader within the timeout");
    }

    async fn wait_for_leader_consistent_excluding(
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
                    // Skip if the current leader is the excluded node
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

            if all_agree {
                if let Some(final_leader) = maybe_leader {
                    return Ok(final_leader);
                }
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        anyhow::bail!("Not all nodes saw a consistent leader within the timeout");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn test_three_node_cluster() -> Result<()> {
        init_tracing();

        let node_configs = vec![
            (1, "127.0.0.1:21001"),
            (2, "127.0.0.1:21002"),
            (3, "127.0.0.1:21003"),
        ];
        let node_clients = make_node_clients(node_configs.len());

        let (_config, pcfg, raft_nodes, state_machines, _server_handles) =
            setup_cluster(&node_configs, node_clients.clone(), None).await?;

        tokio::time::sleep(Duration::from_secs(2)).await;

        connect_clients(&node_configs, &node_clients, &pcfg, |i, _| {
            format!("127.0.0.1:{}", 22001 + i)
        })
        .await?;

        // Initialize and configure the first node as leader
        let mut initial_nodes = BTreeMap::new();
        initial_nodes.insert(
            1,
            BasicNode {
                addr: node_configs[0].1.to_string(),
            },
        );

        raft_nodes[0].initialize(initial_nodes).await?;

        wait_for_leader(&raft_nodes, Duration::from_secs(10)).await?;

        // Add learner nodes and wait for synchronization
        let last_log_id = {
            let mut last_log_id = None;
            for (_i, (node_id, addr)) in node_configs.iter().enumerate().skip(1) {
                let node = BasicNode {
                    addr: addr.to_string(),
                };
                let add_learner_result = raft_nodes[0].add_learner(*node_id, node, false).await?;
                last_log_id = Some(add_learner_result.log_id);
            }
            last_log_id
        };

        if let Some(log_id) = last_log_id {
            wait_for_log_commit(&raft_nodes, log_id).await?;
        } else {
            panic!("log_id must not be None");
        }

        // Write and verify test data
        client_write_and_wait(
            &raft_nodes[0],
            &raft_nodes,
            set_request("test_key", "test_value"),
        )
        .await?;

        assert_state_machine_value(&state_machines, "test_key", "test_value").await;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 5)]
    async fn test_vote_mode_all_initialized() -> Result<()> {
        init_tracing();

        let node_configs = vec![
            (1, "127.0.0.1:24004"),
            (2, "127.0.0.1:24005"),
            (3, "127.0.0.1:24006"),
        ];
        let node_clients = make_node_clients(node_configs.len());

        let (_config, pcfg, raft_nodes, state_machines, _server_handles) =
            setup_cluster(&node_configs, node_clients.clone(), None).await?;

        tokio::time::sleep(Duration::from_secs(2)).await;

        connect_clients(&node_configs, &node_clients, &pcfg, |i, _| {
            format!("127.0.0.1:{}", 25001 + i as u16)
        })
        .await?;

        for (i, raft) in raft_nodes.clone().into_iter().enumerate() {
            let initial_nodes = membership_from(&node_configs);
            let membership: Membership<_, _> = Membership::from(initial_nodes.clone());
            info!(
                "Node {} initializing with membership configuration: {}",
                node_configs[i].0, membership
            );

            tokio::spawn(async move {
                raft.initialize(initial_nodes).await.unwrap();
            });
        }

        // Wait until a leader is elected.
        let leader_id = wait_for_leader_consistent(&raft_nodes, Duration::from_secs(10)).await?;

        info!("Leader elected: {:?}", leader_id);
        for (i, raft) in raft_nodes.iter().enumerate() {
            let observed_leader = raft.metrics().borrow().current_leader;
            assert_eq!(
                observed_leader,
                Some(leader_id),
                "Node {} sees a different leader",
                i + 1
            );
        }

        let leader_index = node_configs
            .iter()
            .position(|(id, _)| *id == leader_id)
            .expect("Leader must be one of the nodes");
        client_write_and_wait(
            &raft_nodes[leader_index],
            &raft_nodes,
            set_request("vote_mode_key", "vote_mode_value"),
        )
        .await?;

        // Verify that the write has propagated to the state machines of all nodes.
        assert_state_machine_value(&state_machines, "vote_mode_key", "vote_mode_value").await;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 5)]
    async fn test_leader_failover() -> anyhow::Result<()> {
        init_tracing();

        let node_configs = vec![
            (1, "127.0.0.1:24007"),
            (2, "127.0.0.1:24008"),
            (3, "127.0.0.1:24009"),
            (4, "127.0.0.1:24010"),
            (5, "127.0.0.1:24011"),
        ];
        let node_clients = make_node_clients(node_configs.len());

        let (_config, pcfg, mut raft_nodes, mut state_machines, mut server_handles) =
            setup_cluster(&node_configs, node_clients.clone(), None).await?;

        // Give servers a moment to start up.
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Connect a client to each node.
        connect_clients(&node_configs, &node_clients, &pcfg, |i, _| {
            format!("127.0.0.1:{}", 26001 + i as u16)
        })
        .await?;

        // Initialize the cluster membership on every node.
        for (i, raft) in raft_nodes.clone().into_iter().enumerate() {
            let initial_nodes = membership_from(&node_configs);
            let membership = openraft::Membership::from(initial_nodes.clone());
            info!(
                "Node {} initializing with membership configuration: {:?}",
                node_configs[i].0, membership
            );

            let raft_clone = raft.clone();
            tokio::spawn(async move {
                raft_clone.initialize(initial_nodes).await.unwrap();
            });
        }

        // Wait until all nodes agree on the leader.
        let old_leader = wait_for_leader_consistent(&raft_nodes, Duration::from_secs(10)).await?;
        info!("Initial leader elected: {:?}", old_leader);

        // Verify that all nodes see the elected leader.
        for (i, raft) in raft_nodes.iter().enumerate() {
            let observed_leader = raft.metrics().borrow().current_leader;
            assert_eq!(
                observed_leader,
                Some(old_leader),
                "Node {} sees a different leader",
                i + 1
            );
        }

        // Simulate a leader failure by "killing" the leader's server.
        let leader_index = node_configs
            .iter()
            .position(|(id, _)| *id == old_leader)
            .expect("Leader must be one of the nodes");

        info!(
            "Simulating leader failure: killing leader node {} at {}",
            old_leader, node_configs[leader_index].1
        );
        // Dropping the server handle simulates a crash.
        server_handles.remove(leader_index).abort();
        {
            let _ = raft_nodes.remove(leader_index);
            let _ = state_machines.remove(leader_index);
        }

        // Wait for a new leader to be elected and for all nodes to agree.
        let new_leader =
            wait_for_leader_consistent_excluding(&raft_nodes, old_leader, Duration::from_secs(30))
                .await?;
        info!("New leader elected: {:?}", new_leader);
        assert_ne!(
            new_leader, old_leader,
            "The new leader should be different from the old leader."
        );

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 5)]
    async fn test_node_restart_from_snapshot_retains_voter() -> anyhow::Result<()> {
        init_tracing();

        let node_configs = vec![
            (1, "127.0.0.1:31001"),
            (2, "127.0.0.1:31002"),
            (3, "127.0.0.1:31003"),
        ];

        let storage_paths: Vec<NodeStoragePaths> = node_configs
            .iter()
            .map(|(id, _)| NodeStoragePaths::for_node(*id, 3))
            .collect::<Result<Vec<_>>>()?;

        let node_clients = make_node_clients(node_configs.len());

        let (config, pcfg, mut raft_nodes, mut state_machines, mut server_handles) =
            setup_cluster(&node_configs, node_clients.clone(), Some(&storage_paths)).await?;

        tokio::time::sleep(Duration::from_secs(2)).await;

        connect_clients(&node_configs, &node_clients, &pcfg, |_, _| {
            "127.0.0.1:0".to_string()
        })
        .await?;

        let initial_members = membership_from(&node_configs);
        raft_nodes[0].initialize(initial_members).await?;

        let leader_id = wait_for_leader_consistent(&raft_nodes, Duration::from_secs(10)).await?;
        let leader_index = node_configs
            .iter()
            .position(|(id, _)| *id == leader_id)
            .expect("Leader must be part of node configs");

        for (idx, (node_id, _)) in node_configs.iter().enumerate() {
            wait_for_node_to_be_voter(&raft_nodes[idx], *node_id, Duration::from_secs(10)).await?;
        }

        let snapshot_key = "snapshot_test_key";
        client_write_and_wait(
            &raft_nodes[leader_index],
            &raft_nodes,
            set_request(snapshot_key, "snapshot_value"),
        )
        .await?;

        raft_nodes[leader_index].trigger().snapshot().await?;
        wait_for_snapshot_file(
            &storage_paths[leader_index].snapshot_dir,
            Duration::from_secs(10),
        )
        .await?;

        {
            let leader_server = server_handles.remove(leader_index);
            leader_server.abort();

            let _ = raft_nodes.remove(leader_index);
            let _ = state_machines.remove(leader_index);
        }

        tokio::time::sleep(Duration::from_secs(5)).await;

        let (restarted_raft, restarted_sm, restarted_server) = setup_node(
            leader_id,
            node_configs[leader_index].1,
            Arc::clone(&config),
            node_clients.clone(),
            Some(&storage_paths[leader_index]),
        )
        .await?;

        raft_nodes.insert(leader_index, restarted_raft.clone());
        state_machines.insert(leader_index, restarted_sm.clone());
        server_handles.insert(leader_index, restarted_server);

        let server_addr = node_configs[leader_index].1;
        let server_key = server_addr.to_string();
        let replacement_client = RClientBuilder::new()
            .remote_addr(server_addr)
            .server_name("localhost")
            .local_bind_addr("127.0.0.1:0")
            .dangerous_skip_verification(true)
            .protocol_cfg(pcfg.clone())
            .build()
            .await?;
        node_clients
            .entry_async(server_key.clone())
            .await
            .and_modify(|existing| {
                *existing = replacement_client.clone();
            })
            .or_insert_with(|| replacement_client.clone());

        wait_for_leader_consistent(&raft_nodes, Duration::from_secs(15)).await?;
        wait_for_node_to_be_voter(
            &raft_nodes[leader_index],
            leader_id,
            Duration::from_secs(10),
        )
        .await?;

        let sm = state_machines[leader_index].state_machine.read().await;
        let value = sm
            .data
            .get(snapshot_key)
            .expect("Value should persist across restart");
        let value_str: String = rmp_serde::from_slice(value).unwrap();
        assert_eq!(value_str, "snapshot_value");

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 5)]
    async fn test_add_node_to_three_node_cluster() -> Result<()> {
        init_tracing();

        // Set up a three-node cluster.
        let initial_configs = vec![
            (1, "127.0.0.1:27001"),
            (2, "127.0.0.1:27002"),
            (3, "127.0.0.1:27003"),
        ];
        let node_clients = make_node_clients(initial_configs.len());

        let (config, pcfg, mut raft_nodes, mut state_machines, mut server_handles) =
            setup_cluster(&initial_configs, node_clients.clone(), None).await?;

        tokio::time::sleep(Duration::from_secs(2)).await;

        connect_clients(&initial_configs, &node_clients, &pcfg, |i, _| {
            format!("127.0.0.1:{}", 28001 + i as u16)
        })
        .await?;

        // Initialize the cluster membership with all three nodes as voters.
        let initial_members = membership_from(&initial_configs);
        raft_nodes[0].initialize(initial_members).await?;

        // Wait for the leader to be elected.
        let leader_id = wait_for_leader_consistent(&raft_nodes, Duration::from_secs(10)).await?;
        let leader_index = initial_configs
            .iter()
            .position(|(id, _)| *id == leader_id)
            .expect("Leader must be one of the initial nodes");

        // Issue a client write to set a key/value pair.
        client_write_and_wait(
            &raft_nodes[leader_index],
            &raft_nodes,
            set_request("test_key", "test_value"),
        )
        .await?;

        // Verify that all initial nodes applied the command.
        assert_state_machine_value(&state_machines, "test_key", "test_value").await;

        // Now, add a new node (node 4) to the existing three-node cluster.
        let new_node_id = 4;
        let new_node_addr = "127.0.0.1:27004";
        let (new_raft, new_sm, new_server) = setup_node(
            new_node_id,
            new_node_addr,
            Arc::clone(&config),
            node_clients.clone(),
            None,
        )
        .await?;
        raft_nodes.push(new_raft);
        state_machines.push(new_sm);
        server_handles.push(new_server);

        // Create a client connection for the new node.
        let new_client_addr = "127.0.0.1:28004";
        let new_client = create_rclient(new_node_addr, new_client_addr, &pcfg).await?;
        node_clients
            .insert_sync(new_node_addr.to_string(), new_client)
            .map_err(|e| anyhow::anyhow!("{:?}", e))?;

        // Add the new node as a learner using the current leader.
        add_learner_and_wait(
            &raft_nodes[leader_index],
            &raft_nodes,
            new_node_id,
            new_node_addr,
        )
        .await?;

        // Verify that the new node's state machine caught up with the previously applied log.
        let new_node_sm = state_machines.last().unwrap();
        let sm = new_node_sm.state_machine.read().await;
        let value = sm.data.get("test_key").unwrap();
        let value_str: String = rmp_serde::from_slice(value).unwrap();
        assert_eq!(value_str, "test_value");

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 5)]
    async fn test_add_voter_node_to_three_node_cluster_change_membership() -> Result<()> {
        init_tracing();

        // Set up an initial three-node cluster.
        let initial_configs = vec![
            (1, "127.0.0.1:29001"),
            (2, "127.0.0.1:29002"),
            (3, "127.0.0.1:29003"),
        ];

        let node_clients = make_node_clients(initial_configs.len());

        let (config, pcfg, mut raft_nodes, mut state_machines, mut server_handles) =
            setup_cluster(&initial_configs, node_clients.clone(), None).await?;

        // Allow the servers time to start up.
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Establish client connections for the initial nodes.
        connect_clients(&initial_configs, &node_clients, &pcfg, |i, _| {
            format!("127.0.0.1:{}", 30001 + i as u16)
        })
        .await?;

        // Initialize the cluster membership for the initial three nodes.
        let initial_members = membership_from(&initial_configs);
        raft_nodes[0].initialize(initial_members).await?;

        // Wait until a leader is elected.
        let leader_id = wait_for_leader_consistent(&raft_nodes, Duration::from_secs(10)).await?;
        let leader_index = initial_configs
            .iter()
            .position(|(id, _)| *id == leader_id)
            .expect("Leader must be one of the nodes");

        // Bring up a new node (node 4) that will be added as a voter.
        let new_node_id = 4;
        let new_node_addr = "127.0.0.1:29004";
        let (new_raft, new_sm, new_server) = setup_node(
            new_node_id,
            new_node_addr,
            Arc::clone(&config),
            node_clients.clone(),
            None,
        )
        .await?;
        raft_nodes.push(new_raft);
        state_machines.push(new_sm);
        server_handles.push(new_server);

        // Create a client connection for the new node.
        let new_client_addr = "127.0.0.1:30004";
        let new_client = create_rclient(new_node_addr, new_client_addr, &pcfg).await?;

        node_clients
            .insert_sync(new_node_addr.to_string(), new_client)
            .map_err(|e| anyhow::anyhow!("{:?}", e))?;

        // Add the new node as a learner before promoting it to a voter.
        add_learner_and_wait(
            &raft_nodes[leader_index],
            &raft_nodes,
            new_node_id,
            new_node_addr,
        )
        .await?;

        // Build the new membership configuration as a set of node IDs.
        // Since node 4 is already added as a learner, this promotion will convert it to a voter.
        let new_members: BTreeSet<u64> = initial_configs
            .iter()
            .map(|(node_id, _)| *node_id)
            .chain(std::iter::once(new_node_id))
            .collect();

        // Issue the membership change to promote the learner to a voter.
        let change_resp = raft_nodes[leader_index]
            .change_membership(new_members, false)
            .await?;
        wait_for_log_commit(&raft_nodes, change_resp.log_id).await?;

        // Issue a client write after the membership change.
        client_write_and_wait(
            &raft_nodes[leader_index],
            &raft_nodes,
            set_request("test_key", "test_value"),
        )
        .await?;

        // Verify that every node in the cluster, including the newly promoted voter, has applied the command.
        assert_state_machine_value(&state_machines, "test_key", "test_value").await;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 5)]
    async fn test_add_voter_node_after_snapshot_compaction() -> Result<()> {
        init_tracing();

        // Set up an initial three-node cluster with persistent storage so snapshots
        // produce on-disk artifacts and purge earlier log entries.
        let initial_configs = vec![
            (1, "127.0.0.1:33001"),
            (2, "127.0.0.1:33002"),
            (3, "127.0.0.1:33003"),
        ];

        let mut storage_paths: Vec<NodeStoragePaths> = initial_configs
            .iter()
            .map(|(id, _)| NodeStoragePaths::for_node(*id, 3))
            .collect::<Result<Vec<_>>>()?;

        let node_clients = make_node_clients(initial_configs.len() + 1);

        let (config, pcfg, mut raft_nodes, mut state_machines, mut server_handles) =
            setup_cluster(&initial_configs, node_clients.clone(), Some(&storage_paths)).await?;

        tokio::time::sleep(Duration::from_secs(2)).await;

        connect_clients(&initial_configs, &node_clients, &pcfg, |i, _| {
            format!("127.0.0.1:{}", 34001 + i as u16)
        })
        .await?;

        // Initialize the cluster membership for the initial three nodes.
        let initial_members = membership_from(&initial_configs);
        raft_nodes[0].initialize(initial_members).await?;

        // Wait until a leader is elected.
        let leader_id = wait_for_leader_consistent(&raft_nodes, Duration::from_secs(10)).await?;
        let leader_index = initial_configs
            .iter()
            .position(|(id, _)| *id == leader_id)
            .expect("Leader must be one of the nodes");

        // Issue a client write that will later be recovered via snapshot installation.
        let pre_snapshot_write = raft_nodes[leader_index]
            .client_write(set_request("pre_snapshot_key", "pre_snapshot_value"))
            .await?;
        let pre_snapshot_log_id = pre_snapshot_write.log_id;
        wait_for_log_commit(&raft_nodes, pre_snapshot_log_id).await?;

        assert_state_machine_value(&state_machines, "pre_snapshot_key", "pre_snapshot_value").await;

        // Trigger a snapshot to compact the log and remove earlier entries from the log store.
        raft_nodes[leader_index].trigger().snapshot().await?;
        wait_for_snapshot_file(
            &storage_paths[leader_index].snapshot_dir,
            Duration::from_secs(10),
        )
        .await?;

        raft_nodes[leader_index]
            .trigger()
            .purge_log(pre_snapshot_log_id.index)
            .await?;

        let purge_timeout = Duration::from_secs(10);
        let purge_start = Instant::now();
        loop {
            let mut log_store = get_log_store_handle(initial_configs[leader_index].1)?;
            let log_state = log_store.get_log_state().await?;

            if log_state.last_purged_log_id == Some(pre_snapshot_log_id) {
                break;
            }

            if purge_start.elapsed() > purge_timeout {
                bail!(
                    "Leader log store was not purged to {:?} within {:?}",
                    pre_snapshot_log_id,
                    purge_timeout
                );
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // Bring up a new node that will be added as a voter after compaction.
        let new_node_id = 4;
        let new_node_addr = "127.0.0.1:33004";
        let new_node_paths = NodeStoragePaths::for_node(new_node_id, 3)?;
        let new_snapshot_dir = new_node_paths.snapshot_dir.clone();
        let (new_raft, new_sm, new_server) = setup_node(
            new_node_id,
            new_node_addr,
            Arc::clone(&config),
            node_clients.clone(),
            Some(&new_node_paths),
        )
        .await?;
        storage_paths.push(new_node_paths);
        raft_nodes.push(new_raft);
        state_machines.push(new_sm);
        server_handles.push(new_server);

        // Create a client connection for the new node.
        let new_client_addr = "127.0.0.1:34004";
        let new_client = create_rclient(new_node_addr, new_client_addr, &pcfg).await?;
        node_clients
            .insert_sync(new_node_addr.to_string(), new_client)
            .map_err(|e| anyhow::anyhow!("{:?}", e))?;

        // Add the new node as a learner; this should trigger a snapshot transfer since the
        // earlier log entries have been purged by compaction.
        add_learner_and_wait(
            &raft_nodes[leader_index],
            &raft_nodes,
            new_node_id,
            new_node_addr,
        )
        .await?;

        wait_for_snapshot_file(&new_snapshot_dir, Duration::from_secs(20)).await?;

        // Ensure the new node replayed the snapshot and now contains the data written before
        // it joined the cluster.
        let expected_key = "pre_snapshot_key";
        let expected_value = "pre_snapshot_value";
        let start = Instant::now();
        let timeout = Duration::from_secs(10);
        loop {
            {
                let new_node_state = state_machines
                    .last()
                    .expect("new node state machine should be present")
                    .state_machine
                    .read()
                    .await;
                if let Some(bytes) = new_node_state.data.get(expected_key) {
                    let value: String = rmp_serde::from_slice(bytes).unwrap();
                    if value == expected_value {
                        break;
                    }
                }
            }

            if start.elapsed() > timeout {
                bail!(
                    "New node did not receive expected snapshot data within {:?}",
                    timeout
                );
            }

            tokio::time::sleep(Duration::from_millis(200)).await;
        }

        // Promote the new node to a voter via membership change.
        let new_members: BTreeSet<u64> = initial_configs
            .iter()
            .map(|(node_id, _)| *node_id)
            .chain(std::iter::once(new_node_id))
            .collect();

        let change_resp = raft_nodes[leader_index]
            .change_membership(new_members, false)
            .await?;
        wait_for_log_commit(&raft_nodes, change_resp.log_id).await?;

        // Issue a client write after the membership change so the new voter applies
        // post-compaction log entries too.
        client_write_and_wait(
            &raft_nodes[leader_index],
            &raft_nodes,
            set_request("post_snapshot_key", "post_snapshot_value"),
        )
        .await?;

        assert_state_machine_value(&state_machines, "pre_snapshot_key", "pre_snapshot_value").await;
        assert_state_machine_value(&state_machines, "post_snapshot_key", "post_snapshot_value")
            .await;

        Ok(())
    }
}
