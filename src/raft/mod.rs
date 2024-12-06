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
use openraft::Config;
use std::io::Cursor;
use std::sync::Arc;

use crate::raft::client::RClient;
use crate::raft::store::Request;
use crate::raft::store::Response;

pub type NodeId = u64;
pub type LogStore = store::LogStore;
pub type StateMachineStore = store::StateMachineStore;
pub type Raft = openraft::Raft<TypeConfig>;

pub struct App {
    pub id: NodeId,
    pub addr: String,
    pub raft: Raft,
    pub log_store: LogStore,
    pub state_machine_store: Arc<StateMachineStore>,
    pub config: Arc<openraft::Config>,
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

pub async fn start_raft_node(node_id: NodeId, bind_addr: String, nodes: Vec<String>) -> Result<()> {
    let config = Config {
        heartbeat_interval: 500,
        election_timeout_min: 1500,
        election_timeout_max: 3000,
        ..Default::default()
    };

    let config = Arc::new(config.validate().unwrap());

    let log_store = LogStore::default();
    let state_machine_store = Arc::new(StateMachineStore::default());

    let clients = DashMap::new();
    for node_addr in nodes {
        let client = RClient::new(&node_addr, "127.0.0.1:0", true).await?;
        clients.insert(node_addr, client);
    }

    let network = Network {
        clients: Arc::new(clients),
    };

    let raft = openraft::Raft::new(
        node_id,
        config.clone(),
        network,
        log_store.clone(),
        state_machine_store.clone(),
    )
    .await?;

    let _app = App {
        id: node_id,
        addr: bind_addr.clone(),
        raft,
        log_store,
        state_machine_store,
        config,
    };

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::server::RServer;
    use anyhow::{bail, Result};
    use core::panic;
    use openraft::{BasicNode, LogId};
    use std::collections::BTreeMap;
    use tokio::{sync::RwLock, time::Duration};

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

    async fn wait_for_leader(nodes: &[Arc<RwLock<Raft>>]) -> Result<()> {
        let timeout = Duration::from_secs(15);
        let start = std::time::Instant::now();

        loop {
            for node in nodes {
                let node = node.read().await;
                match node.metrics().borrow().current_leader {
                    Some(_) => return Ok(()),
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
        let _subscriber = tracing_subscriber::FmtSubscriber::builder()
            .with_max_level(tracing::Level::INFO)
            .with_test_writer()
            .init();

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
                Config {
                    heartbeat_interval: 1000,
                    election_timeout_min: 3000,
                    election_timeout_max: 8000,
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

            let mut server = RServer::new(None, raft)?;
            let addr = addr.to_string();
            let server_handle = tokio::spawn(async move {
                server.start(&addr).await.unwrap();
            });
            server_handles.push(server_handle);
        }

        tokio::time::sleep(Duration::from_secs(2)).await;

        for (i, (_, server_addr)) in node_configs.iter().enumerate() {
            let client_port = 22001 + i;
            let client_addr = format!("127.0.0.1:{}", client_port);
            let client = RClient::new(server_addr, &client_addr, true).await?;
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

        wait_for_leader(&raft_nodes).await?;

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

        for (_i, state_machine) in state_machines.iter().enumerate() {
            let sm = state_machine.state_machine.read().await;
            assert_eq!(sm.data.get("test_key").unwrap(), "test_value");
        }

        for handle in server_handles {
            handle.abort();
        }

        Ok(())
    }
}
