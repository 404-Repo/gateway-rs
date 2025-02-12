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
use std::time::Duration;
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

    // Add the node.
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

    let log_timeout = config.raft.election_timeout_max;

    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(log_timeout)).await;
        let binding = raft.read().await.metrics();
        let metrics = binding.borrow();
        if let Some(leader) = metrics.current_leader {
            info!("Node {} sees current leader: {}", node_id, leader);
        } else {
            info!("Node {}: No leader elected yet", node_id);
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
    use openraft::{BasicNode, LogId, Membership};
    use std::{collections::BTreeMap, sync::Once};
    use tokio::{sync::RwLock, time::Duration};

    static TRACING: Once = Once::new();

    // Initializes the tracing subscriber once.
    pub fn init_tracing() {
        TRACING.call_once(|| {
            tracing_subscriber::FmtSubscriber::builder()
                .with_max_level(tracing::Level::INFO)
                .with_test_writer()
                .init();
        });
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
    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn test_three_node_cluster() -> Result<()> {
        init_tracing();

        let node_configs = vec![
            (1, "127.0.0.1:21001"),
            (2, "127.0.0.1:21002"),
            (3, "127.0.0.1:21003"),
        ];

        let all_clients = DashMap::new();
        let node_clients = Arc::new(all_clients);

        let mut server_handles = Vec::new();
        let mut raft_nodes = Vec::new();
        let mut state_machines = Vec::new();

        for (node_id, addr) in &node_configs {
            let (network, log_store, state_machine_store) = (
                Network::new(node_clients.clone()),
                LogStore::default(),
                Arc::new(StateMachineStore::default()),
            );
            state_machines.push(state_machine_store.clone());

            let config = Arc::new(
                openraft::Config {
                    heartbeat_interval: 500,
                    election_timeout_min: 10000,
                    election_timeout_max: 20000,
                    install_snapshot_timeout: 500,
                    ..Default::default()
                }
                .validate()?,
            );

            let raft = Arc::new(RwLock::new(
                openraft::Raft::new(
                    *node_id,
                    config.clone(),
                    network,
                    log_store.clone(),
                    state_machine_store,
                )
                .await?,
            ));

            raft_nodes.push(raft.clone());

            let server = RServer::new(addr, None, raft).await?;
            server_handles.push(server);
        }

        tokio::time::sleep(Duration::from_secs(2)).await;

        for (i, (_, server_addr)) in node_configs.iter().enumerate() {
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

        let mut server_handles = Vec::new();
        let mut raft_nodes = Vec::new();
        let mut state_machines = Vec::new();

        for (node_id, addr) in &node_configs {
            let network = Network::new(node_clients.clone());
            let log_store = LogStore::default();
            let state_machine_store = Arc::new(StateMachineStore::default());
            state_machines.push(state_machine_store.clone());

            let config = Arc::new(
                openraft::Config {
                    heartbeat_interval: 500,
                    election_timeout_min: 10000,
                    election_timeout_max: 20000,
                    install_snapshot_timeout: 500,
                    ..Default::default()
                }
                .validate()?,
            );

            let raft = Arc::new(RwLock::new(
                Raft::new(
                    *node_id,
                    config.clone(),
                    network,
                    log_store.clone(),
                    state_machine_store,
                )
                .await?,
            ));
            raft_nodes.push(raft.clone());

            let server = RServer::new(addr, None, raft).await?;
            server_handles.push(server);
        }

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
        let leader_id = wait_for_leader(&raft_nodes, Duration::from_secs(10)).await?;

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
}
