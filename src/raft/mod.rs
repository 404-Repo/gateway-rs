// TODO: Remove this after the code is finalized
#![allow(dead_code)]

pub mod client;
pub mod memstore;
pub mod network;
pub mod server;
pub mod store;

use anyhow::Result;
use dashmap::DashMap;
use network::Network;
use openraft::BasicNode;
use server::RServer;
use std::collections::BTreeMap;
use std::io::Cursor;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;

use crate::config::NodeConfig;
use crate::raft::client::RClient;
use crate::raft::store::Request;
use crate::raft::store::Response;

pub type NodeId = u64;
pub type LogStore = store::LogStore;
pub type StateMachineStore = store::StateMachineStore;
pub type Raft = openraft::Raft<TypeConfig>;

pub struct Gateway {
    pub id: NodeId,
    pub server: RServer,
    pub log_store: LogStore,
    pub config: Arc<NodeConfig>,
}

openraft::declare_raft_types!(
    pub TypeConfig:
        D = Request,
        R = Response,
        NodeId = NodeId,
);

pub mod typ {
    use openraft::BasicNode;

    use crate::raft::NodeId;

    pub type RaftError<E = openraft::error::Infallible> = openraft::error::RaftError<NodeId, E>;
    pub type RPCError<E = openraft::error::Infallible> =
        openraft::error::RPCError<NodeId, BasicNode, RaftError<E>>;
}

pub async fn start_gateway_bootstrap(config: Arc<NodeConfig>) -> Result<Gateway> {
    let raft_config = Arc::new(
        openraft::Config {
            cluster_name: config.raft.cluster_name.clone(),
            heartbeat_interval: config.raft.heartbeat_interval,
            election_timeout_min: config.raft.election_timeout_min,
            election_timeout_max: config.raft.election_timeout_max,
            max_payload_entries: config.raft.max_payload_entries,
            replication_lag_threshold: config.raft.replication_lag_threshold,
            snapshot_max_chunk_size: config.raft.snapshot_max_chunk_size,
            max_in_snapshot_log_to_keep: config.raft.max_in_snapshot_log_to_keep,
            ..Default::default()
        }
        .validate()?,
    );

    let log_store = LogStore::default();
    let state_machine_store = Arc::new(StateMachineStore::default());
    let clients_map = Arc::new(DashMap::new());
    let network = Network {
        clients: clients_map.clone(),
    };

    let raft = Arc::new(RwLock::new(
        openraft::Raft::new(
            config.network.node_id,
            raft_config.clone(),
            network,
            log_store.clone(),
            state_machine_store,
        )
        .await?,
    ));

    let server = RServer::new(
        &format!("{}:{}", config.network.ip, config.network.server_port),
        None,
        raft.clone(),
    )
    .await?;

    // Set up clients for communication with other nodes
    for (endpoint, dns_name) in config
        .network
        .node_endpoints
        .iter()
        .zip(config.network.node_dns_names.iter())
    {
        let client = RClient::new(
            endpoint,
            dns_name,
            &format!("{}:{}", config.network.ip, 0),
            true,
        )
        .await?;
        clients_map.insert(endpoint.clone(), client);
    }

    let node_id = config.network.node_id;
    info!("Starting node {}", node_id);

    // If this node is node_id == 1, initialize the cluster.
    if node_id == 1 {
        let r = raft.write().await;
        info!("Initializing the cluster with node {}", node_id);

        let mut initial_nodes = BTreeMap::new();
        initial_nodes.insert(
            1,
            BasicNode {
                addr: format!("{}:{}", config.network.ip, config.network.server_port),
            },
        );

        r.initialize(initial_nodes).await?;

        for ((id, endpoint), _dns_name) in config
            .network
            .node_ids
            .iter()
            .zip(config.network.node_endpoints.iter())
            .zip(config.network.node_dns_names.iter())
        {
            let node = BasicNode {
                addr: endpoint.clone(),
            };
            info!(
                "Adding node {} as a learner with endpoint: {}",
                id, endpoint
            );
            r.add_learner(*id, node, true).await?;
        }
    } else {
        info!("Node {} started as a non-initialized node. It will be added as a learner by the leader.", node_id);
    }

    let gateway = Gateway {
        config: config.clone(),
        id: node_id,
        server,
        log_store,
    };

    Ok(gateway)
}

pub async fn start_gateway_vote(config: Arc<NodeConfig>) -> Result<Gateway> {
    let raft_config = Arc::new(
        openraft::Config {
            cluster_name: config.raft.cluster_name.clone(),
            heartbeat_interval: config.raft.heartbeat_interval,
            election_timeout_min: config.raft.election_timeout_min,
            election_timeout_max: config.raft.election_timeout_max,
            max_payload_entries: config.raft.max_payload_entries,
            replication_lag_threshold: config.raft.replication_lag_threshold,
            snapshot_max_chunk_size: config.raft.snapshot_max_chunk_size,
            max_in_snapshot_log_to_keep: config.raft.max_in_snapshot_log_to_keep,
            ..Default::default()
        }
        .validate()?,
    );

    let log_store = LogStore::default();
    let state_machine_store = Arc::new(StateMachineStore::default());

    let clients_map = Arc::new(DashMap::new());
    let network = Network {
        clients: clients_map.clone(),
    };

    let raft = Arc::new(RwLock::new(
        openraft::Raft::new(
            config.network.node_id,
            raft_config.clone(),
            network,
            log_store.clone(),
            state_machine_store,
        )
        .await?,
    ));

    let server_addr = format!("{}:{}", config.network.ip, config.network.server_port);
    let server = RServer::new(&server_addr, None, raft.clone()).await?;

    // Set up client connections to remote nodes.
    // Note: The config.node_endpoints contains only remote nodes.
    for (endpoint, dns_name) in config
        .network
        .node_endpoints
        .iter()
        .zip(config.network.node_dns_names.iter())
    {
        let client = RClient::new(
            endpoint,
            dns_name,
            &format!("{}:{}", config.network.ip, 0),
            true,
        )
        .await?;
        clients_map.insert(endpoint.clone(), client);
    }

    let node_id = config.network.node_id;
    info!("Starting node {} in vote mode", node_id);

    let mut membership_nodes = BTreeMap::new();

    membership_nodes.insert(
        node_id,
        BasicNode {
            addr: format!("{}:{}", config.network.ip, config.network.server_port),
        },
    );

    for (&peer_node_id, peer_endpoint) in config
        .network
        .node_ids
        .iter()
        .zip(config.network.node_endpoints.iter())
    {
        membership_nodes.insert(
            peer_node_id,
            BasicNode {
                addr: peer_endpoint.clone(),
            },
        );
    }

    info!(
        "Node {} initializing with membership configuration: {:?}",
        node_id, membership_nodes
    );

    match raft.write().await.initialize(membership_nodes).await {
        Ok(_) => info!("Node {} successfully initialized in vote mode", node_id),
        Err(e) => info!(
            "Warning Node {} not initialized (likely already initialized): {:?}",
            node_id, e
        ),
    }

    tokio::spawn(async move {
        let mut metrics_rx = { raft.read().await.metrics() };
        let mut last_leader = None;

        loop {
            let current_leader = {
                metrics_rx
                    .changed()
                    .await
                    .expect("Failed to listen for metric changes");
                let metrics = metrics_rx.borrow();
                metrics.current_leader
            };

            if current_leader != last_leader {
                if let Some(leader_id) = current_leader {
                    info!("Leader changed to node {}", leader_id);
                } else {
                    info!("Leadership changed, but no leader is elected yet");
                }

                last_leader = current_leader;
            }
        }
    });

    let gateway = Gateway {
        config,
        id: node_id,
        server,
        log_store,
    };

    Ok(gateway)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::server::RServer;
    use anyhow::{bail, Result};
    use core::panic;
    use openraft::{BasicNode, Config, LogId, Membership};
    use std::{
        collections::{BTreeMap, BTreeSet},
        sync::Arc,
        sync::Once,
    };
    use tokio::{sync::RwLock, time::Duration};

    static TRACING: Once = Once::new();

    pub fn init_tracing() {
        TRACING.call_once(|| {
            tracing_subscriber::FmtSubscriber::builder()
                .with_max_level(tracing::Level::DEBUG)
                .with_test_writer()
                .init();
        });
    }

    async fn setup_node(
        node_id: u64,
        addr: &str,
        config: Arc<openraft::Config>,
        node_clients: Arc<DashMap<String, RClient>>,
    ) -> Result<(Arc<RwLock<Raft>>, Arc<StateMachineStore>, RServer)> {
        let network = Network::new(node_clients.clone());
        let log_store = LogStore::default();
        let state_machine_store = Arc::new(StateMachineStore::default());
        let raft = Arc::new(RwLock::new(
            openraft::Raft::new(
                node_id,
                config.clone(),
                network,
                log_store,
                state_machine_store.clone(),
            )
            .await?,
        ));
        let server = RServer::new(addr, None, raft.clone()).await?;
        Ok((raft, state_machine_store, server))
    }

    async fn setup_cluster(
        node_configs: &[(u64, &str)],
        node_clients: Arc<DashMap<String, RClient>>,
    ) -> Result<(
        Arc<Config>,
        Vec<Arc<RwLock<Raft>>>,
        Vec<Arc<StateMachineStore>>,
        Vec<RServer>,
    )> {
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
        for &(node_id, addr) in node_configs {
            let (raft, sm, server) =
                setup_node(node_id, addr, config.clone(), node_clients.clone()).await?;
            raft_nodes.push(raft);
            state_machines.push(sm);
            server_handles.push(server);
        }
        Ok((config, raft_nodes, state_machines, server_handles))
    }

    async fn wait_for_log_commit(
        nodes: &[Arc<RwLock<Raft>>],
        target_log_id: LogId<NodeId>,
    ) -> Result<()> {
        let start = std::time::Instant::now();
        let timeout = Duration::from_secs(10);

        loop {
            let mut all_synced = true;
            let mut status = String::new();

            for (i, node) in nodes.iter().enumerate() {
                let node = node.read().await;
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

                if last_applied.map_or(true, |id| id < target_log_id) {
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

    async fn wait_for_leader(nodes: &[Arc<RwLock<Raft>>], timeout: Duration) -> Result<u64> {
        let start = std::time::Instant::now();

        loop {
            for node in nodes {
                let node = node.read().await;
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

    async fn wait_for_leader_consistent(
        nodes: &[Arc<RwLock<Raft>>],
        timeout: Duration,
    ) -> anyhow::Result<u64> {
        let start = std::time::Instant::now();

        while start.elapsed() <= timeout {
            let mut maybe_leader = None;
            let mut all_agree = true;

            for node in nodes {
                let node_guard = node.read().await;
                if let Some(current_leader) = node_guard.metrics().borrow().current_leader {
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
        nodes: &[Arc<RwLock<Raft>>],
        exclude_node: u64,
        timeout: Duration,
    ) -> anyhow::Result<u64> {
        let start = std::time::Instant::now();

        while start.elapsed() <= timeout {
            let mut maybe_leader = None;
            let mut all_agree = true;

            for node in nodes {
                let node_guard = node.read().await;
                if let Some(current_leader) = node_guard.metrics().borrow().current_leader {
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
        let node_clients = Arc::new(DashMap::new());

        let (_config, raft_nodes, state_machines, _server_handles) =
            setup_cluster(&node_configs, node_clients.clone()).await?;

        tokio::time::sleep(Duration::from_secs(2)).await;

        for (i, &(_, server_addr)) in node_configs.iter().enumerate() {
            let client_port = 22001 + i;
            let client_addr = format!("127.0.0.1:{}", client_port);
            let client = RClient::new(server_addr, "localhost", &client_addr, true).await?;
            node_clients.insert(server_addr.to_string(), client);
        }

        // Initialize and configure the first node as leader
        let mut initial_nodes = BTreeMap::new();
        initial_nodes.insert(
            1,
            BasicNode {
                addr: node_configs[0].1.to_string(),
            },
        );

        raft_nodes[0]
            .write()
            .await
            .initialize(initial_nodes)
            .await?;

        wait_for_leader(&raft_nodes, Duration::from_secs(10)).await?;

        // Add learner nodes and wait for synchronization
        let last_log_id = {
            let mut last_log_id = None;
            for (_i, (node_id, addr)) in node_configs.iter().enumerate().skip(1) {
                let node = BasicNode {
                    addr: addr.to_string(),
                };
                let add_learner_result = raft_nodes[0]
                    .write()
                    .await
                    .add_learner(*node_id, node, false)
                    .await?;
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
        let test_request = Request::Set {
            key: "test_key".to_string(),
            value: "test_value".to_string(),
        };
        let write_result = raft_nodes[0]
            .write()
            .await
            .client_write(test_request)
            .await?;
        wait_for_log_commit(&raft_nodes, write_result.log_id).await?;

        for state_machine in &state_machines {
            let sm = state_machine.state_machine.read().await;
            assert_eq!(sm.data.get("test_key").unwrap(), "test_value");
        }

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
        let node_clients = Arc::new(DashMap::new());

        let (_config, raft_nodes, state_machines, _server_handles) =
            setup_cluster(&node_configs, node_clients.clone()).await?;

        tokio::time::sleep(Duration::from_secs(2)).await;

        for (i, &(_, server_addr)) in node_configs.iter().enumerate() {
            let client_port = 25001 + i as u16;
            let client_addr = format!("127.0.0.1:{}", client_port);
            let client = RClient::new(server_addr, "localhost", &client_addr, true).await?;
            node_clients.insert(server_addr.to_string(), client);
        }

        for (i, raft) in raft_nodes.clone().into_iter().enumerate() {
            let mut initial_nodes = BTreeMap::new();
            for (node_id, addr) in &node_configs {
                initial_nodes.insert(
                    *node_id,
                    BasicNode {
                        addr: addr.to_string(),
                    },
                );
            }
            let membership: Membership<_, _> = Membership::from(initial_nodes);
            info!(
                "Node {} initializing with membership configuration: {}",
                node_configs[i].0, membership
            );

            let init_members: BTreeMap<_, _> = membership
                .nodes()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();

            tokio::spawn(async move {
                raft.write().await.initialize(init_members).await.unwrap();
            });
        }

        // Wait until a leader is elected.
        let leader_id = wait_for_leader_consistent(&raft_nodes, Duration::from_secs(10)).await?;

        info!("Leader elected: {:?}", leader_id);
        for (i, raft) in raft_nodes.iter().enumerate() {
            let observed_leader = raft.read().await.metrics().borrow().current_leader;
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
        let test_request = Request::Set {
            key: "vote_mode_key".into(),
            value: "vote_mode_value".into(),
        };
        let write_result = raft_nodes[leader_index]
            .write()
            .await
            .client_write(test_request)
            .await?;
        wait_for_log_commit(&raft_nodes, write_result.log_id).await?;

        // Verify that the write has propagated to the state machines of all nodes.
        for state_machine in state_machines.iter() {
            let sm = state_machine.state_machine.read().await;
            assert_eq!(
                sm.data.get("vote_mode_key").unwrap(),
                "vote_mode_value",
                "State machine did not record the correct value"
            );
        }

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
        let node_clients = Arc::new(DashMap::new());

        let (_config, mut raft_nodes, mut state_machines, mut server_handles) =
            setup_cluster(&node_configs, node_clients.clone()).await?;

        // Give servers a moment to start up.
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Connect a client to each node.
        for (i, &(_, server_addr)) in node_configs.iter().enumerate() {
            let client_port = 26001 + i as u16;
            let client_addr = format!("127.0.0.1:{}", client_port);
            let client = RClient::new(server_addr, "localhost", &client_addr, true).await?;
            node_clients.insert(server_addr.to_string(), client);
        }

        // Initialize the cluster membership on every node.
        for (i, raft) in raft_nodes.clone().into_iter().enumerate() {
            let mut initial_nodes = BTreeMap::new();
            for (node_id, addr) in &node_configs {
                initial_nodes.insert(
                    *node_id,
                    BasicNode {
                        addr: addr.to_string(),
                    },
                );
            }
            let membership = openraft::Membership::from(initial_nodes);
            info!(
                "Node {} initializing with membership configuration: {:?}",
                node_configs[i].0, membership
            );

            let init_members: BTreeMap<_, _> = membership
                .nodes()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();

            let raft_clone = raft.clone();
            tokio::spawn(async move {
                raft_clone
                    .write()
                    .await
                    .initialize(init_members)
                    .await
                    .unwrap();
            });
        }

        // Wait until all nodes agree on the leader.
        let old_leader = wait_for_leader_consistent(&raft_nodes, Duration::from_secs(10)).await?;
        info!("Initial leader elected: {:?}", old_leader);

        // Verify that all nodes see the elected leader.
        for (i, raft) in raft_nodes.iter().enumerate() {
            let observed_leader = raft.read().await.metrics().borrow().current_leader;
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
        let node = server_handles.remove(leader_index);
        node.abort().await;
        drop(node);

        let raft = raft_nodes.remove(leader_index);
        drop(raft);

        let state_machine = state_machines.remove(leader_index);
        drop(state_machine);

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
    async fn test_add_node_to_three_node_cluster() -> Result<()> {
        init_tracing();

        // Set up a three-node cluster.
        let initial_configs = vec![
            (1, "127.0.0.1:27001"),
            (2, "127.0.0.1:27002"),
            (3, "127.0.0.1:27003"),
        ];
        let node_clients = Arc::new(DashMap::new());

        let (config, mut raft_nodes, mut state_machines, mut server_handles) =
            setup_cluster(&initial_configs, node_clients.clone()).await?;

        tokio::time::sleep(Duration::from_secs(2)).await;

        for (i, &(_, server_addr)) in initial_configs.iter().enumerate() {
            let client_port = 28001 + i as u16;
            let client_addr = format!("127.0.0.1:{}", client_port);
            let client = RClient::new(server_addr, "localhost", &client_addr, true).await?;
            node_clients.insert(server_addr.to_string(), client);
        }

        // Initialize the cluster membership with all three nodes as voters.
        let mut initial_members = BTreeMap::new();
        for (node_id, addr) in &initial_configs {
            initial_members.insert(
                *node_id,
                BasicNode {
                    addr: addr.to_string(),
                },
            );
        }
        raft_nodes[0]
            .write()
            .await
            .initialize(initial_members)
            .await?;

        // Wait for the leader to be elected.
        let leader_id = wait_for_leader_consistent(&raft_nodes, Duration::from_secs(10)).await?;
        let leader_index = initial_configs
            .iter()
            .position(|(id, _)| *id == leader_id)
            .expect("Leader must be one of the initial nodes");

        // Issue a client write to set a key/value pair.
        let test_request = Request::Set {
            key: "test_key".to_string(),
            value: "test_value".to_string(),
        };
        let write_result = raft_nodes[leader_index]
            .write()
            .await
            .client_write(test_request)
            .await?;
        wait_for_log_commit(&raft_nodes, write_result.log_id).await?;

        // Verify that all initial nodes applied the command.
        for state_machine in &state_machines {
            let sm = state_machine.state_machine.read().await;
            assert_eq!(sm.data.get("test_key").unwrap(), "test_value");
        }

        // Now, add a new node (node 4) to the existing three-node cluster.
        let new_node_id = 4;
        let new_node_addr = "127.0.0.1:27004";
        let (new_raft, new_sm, new_server) = setup_node(
            new_node_id,
            new_node_addr,
            config.clone(),
            node_clients.clone(),
        )
        .await?;
        raft_nodes.push(new_raft);
        state_machines.push(new_sm);
        server_handles.push(new_server);

        // Create a client connection for the new node.
        let new_client_addr = "127.0.0.1:28004".to_string();
        let new_client = RClient::new(new_node_addr, "localhost", &new_client_addr, true).await?;
        node_clients.insert(new_node_addr.to_string(), new_client);

        // Add the new node as a learner using the current leader.
        let add_learner_result = raft_nodes[leader_index]
            .write()
            .await
            .add_learner(
                new_node_id,
                BasicNode {
                    addr: new_node_addr.to_string(),
                },
                false,
            )
            .await?;
        wait_for_log_commit(&raft_nodes, add_learner_result.log_id).await?;

        // Verify that the new node's state machine caught up with the previously applied log.
        let new_node_sm = state_machines.last().unwrap();
        let sm = new_node_sm.state_machine.read().await;
        assert_eq!(sm.data.get("test_key").unwrap(), "test_value");

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

        let node_clients = Arc::new(DashMap::new());

        let (config, mut raft_nodes, mut state_machines, mut server_handles) =
            setup_cluster(&initial_configs, node_clients.clone()).await?;

        // Allow the servers time to start up.
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Establish client connections for the initial nodes.
        for (i, &(_, server_addr)) in initial_configs.iter().enumerate() {
            let client_port = 30001 + i as u16;
            let client_addr = format!("127.0.0.1:{}", client_port);
            let client = RClient::new(server_addr, "localhost", &client_addr, true).await?;
            node_clients.insert(server_addr.to_string(), client);
        }

        // Initialize the cluster membership for the initial three nodes.
        let mut initial_members = BTreeMap::new();
        for (node_id, addr) in &initial_configs {
            initial_members.insert(
                *node_id,
                BasicNode {
                    addr: addr.to_string(),
                },
            );
        }
        raft_nodes[0]
            .write()
            .await
            .initialize(initial_members)
            .await?;

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
            config.clone(),
            node_clients.clone(),
        )
        .await?;
        raft_nodes.push(new_raft);
        state_machines.push(new_sm);
        server_handles.push(new_server);

        // Create a client connection for the new node.
        let new_client_addr = "127.0.0.1:30004".to_string();
        let new_client = RClient::new(new_node_addr, "localhost", &new_client_addr, true).await?;
        node_clients.insert(new_node_addr.to_string(), new_client);

        // Add the new node as a learner before promoting it to a voter.
        let add_learner_resp = raft_nodes[leader_index]
            .write()
            .await
            .add_learner(
                new_node_id,
                BasicNode {
                    addr: new_node_addr.to_string(),
                },
                false,
            )
            .await?;
        wait_for_log_commit(&raft_nodes, add_learner_resp.log_id).await?;

        // Build the new membership configuration as a set of node IDs.
        // Since node 4 is already added as a learner, this promotion will convert it to a voter.
        let new_members: BTreeSet<u64> = initial_configs
            .iter()
            .map(|(node_id, _)| *node_id)
            .chain(std::iter::once(new_node_id))
            .collect();

        // Issue the membership change to promote the learner to a voter.
        let change_resp = raft_nodes[leader_index]
            .write()
            .await
            .change_membership(new_members, false)
            .await?;
        wait_for_log_commit(&raft_nodes, change_resp.log_id).await?;

        // Issue a client write after the membership change.
        let test_request = Request::Set {
            key: "test_key".to_string(),
            value: "test_value".to_string(),
        };
        let write_result = raft_nodes[leader_index]
            .write()
            .await
            .client_write(test_request)
            .await?;
        wait_for_log_commit(&raft_nodes, write_result.log_id).await?;

        // Verify that every node in the cluster, including the newly promoted voter, has applied the command.
        for stores in &state_machines {
            let sm = stores.state_machine.read().await;
            assert_eq!(sm.data.get("test_key").unwrap(), "test_value");
        }

        Ok(())
    }
}
