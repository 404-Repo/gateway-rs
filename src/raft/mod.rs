pub mod client;
pub mod gateway_state;
pub mod memstore;
pub mod network;
pub mod server;
pub mod store;

use anyhow::Result;
use bytes::Bytes;
use foldhash::quality::RandomState;
use gateway_state::GatewayState;
use network::Network;
use openraft::BasicNode;
use rustls::crypto::CryptoProvider;
use salvo::conn::rustls::Keycert;
use salvo::conn::rustls::RustlsConfig;
use server::RServer;
use std::collections::BTreeMap;
use std::io::Cursor;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::api::request::GatewayInfoExt;
use crate::api::response::GatewayInfo;
use crate::api::Task;
use crate::common::cert::generate_and_create_keycert;
use crate::common::cert::load_certificate;
use crate::common::cert::load_private_key;
use crate::common::queue::DupQueue;
use crate::config::NodeConfig;
use crate::http3::client::Http3Client;
use crate::http3::server::Http3Server;
use crate::raft::client::RClient;
use crate::raft::store::Request;
use crate::raft::store::Response;

pub type NodeId = u64;
pub type LogStore = store::LogStore;
pub type StateMachineStore = store::StateMachineStore;
pub type Raft = openraft::Raft<TypeConfig>;

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

pub struct Gateway {
    pub _id: NodeId,
    pub _config: Arc<NodeConfig>,
    pub server: RServer,
    pub _gateway_state: GatewayState,
    pub gateway_info_updater: JoinHandle<()>,
    pub gateway_leader_change: JoinHandle<()>,
    pub _log_store: LogStore,
    pub http_server: Http3Server,
    pub _task_queue: Arc<DupQueue<Task>>,
}

impl Gateway {
    pub async fn gateway_info_updater(
        config: Arc<NodeConfig>,
        gateway_state: GatewayState,
        task_queue: Arc<DupQueue<Task>>,
    ) {
        loop {
            sleep(Duration::from_millis(config.basic.update_gateway_info_ms)).await;

            let leader = match gateway_state.leader().await {
                Some(leader) => leader,
                None => {
                    warn!("No leader elected!");
                    continue;
                }
            };

            let info = GatewayInfo {
                node_id: config.network.node_id,
                domain: config.network.domain.clone(),
                ip: config.network.ip.clone(),
                name: config.network.name.clone(),
                http_port: config.http.port,
                available_tasks: task_queue.len(),
            };

            if leader == config.network.node_id {
                if let Err(e) = gateway_state.set_gateway_info(info).await {
                    error!("set_gateway_info update error: {:?}", e);
                }
                continue;
            }

            let leader_info = match gateway_state.gateway(leader).await {
                Ok(info) => info,
                Err(e) => {
                    warn!("Failed to get leader info: {:?}", e);
                    continue;
                }
            };

            let server_ip = format!("{}:{}", leader_info.ip, leader_info.http_port);
            let mut client = match Http3Client::new(
                &server_ip,
                &leader_info.domain,
                config.cert.dangerous_skip_verification,
            )
            .await
            {
                Ok(client) => client,
                Err(e) => {
                    error!("Failed to create HTTP3 client: {:?}", e);
                    continue;
                }
            };

            let info = GatewayInfoExt {
                node_id: info.node_id,
                domain: info.domain,
                ip: info.ip,
                name: info.name,
                http_port: info.http_port,
                available_tasks: info.available_tasks,
                cluster_name: config.raft.cluster_name.clone(),
            };

            let payload = match serde_json::to_vec(&info) {
                Ok(p) => p,
                Err(e) => {
                    error!("Serialization error: {:?}", e);
                    continue;
                }
            };

            // Send request to leader
            let url = format!(
                "https://{}:{}/write",
                leader_info.domain, leader_info.http_port
            );

            match client.post(&url, Bytes::from(payload)).await {
                Ok((status, response)) => {
                    if !status.is_success() {
                        error!(
                            "Gateway info update failed with status: {}, response: {:?}",
                            status,
                            String::from_utf8_lossy(&response)
                        );
                    }
                }
                Err(e) => {
                    error!("Gateway info update failed: {:?}", e);
                }
            }
        }
    }

    pub async fn gateway_leader_change(gateway_state: GatewayState) {
        let mut metrics_rx = gateway_state.metrics().await;
        let mut last_leader = None;
        loop {
            metrics_rx
                .changed()
                .await
                .expect("Failed to listen for metric changes");
            let current_leader = metrics_rx.borrow().current_leader;
            if current_leader != last_leader {
                if let Some(leader_id) = current_leader {
                    info!("Leader changed to node {}", leader_id);
                } else {
                    info!("Leadership changed, but no leader is elected yet");
                }
                last_leader = current_leader;
            }
        }
    }
}

impl Drop for Gateway {
    fn drop(&mut self) {
        self.http_server.abort();
        self.gateway_info_updater.abort();
        self.gateway_leader_change.abort();
        self.server.abort();
    }
}

#[derive(Debug, PartialEq)]
pub enum GatewayMode {
    Bootstrap,
    Vote,
    Single,
}

pub async fn start_gateway(mode: GatewayMode, config: Arc<NodeConfig>) -> Result<Gateway> {
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .or_else(|_| {
            CryptoProvider::get_default()
                .map(|_| ())
                .ok_or_else(|| anyhow::anyhow!("Failed to locate any crypto provider"))
        })?;

    let task_queue = Arc::new(DupQueue::new(config.basic.unique_validators_per_task));

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
    let clients_map = Arc::new(scc::HashMap::with_capacity_and_hasher(
        config.network.node_endpoints.len(),
        RandomState::default(),
    ));

    let network = Network::new(clients_map.clone());

    let raft = Arc::new(RwLock::new(
        Raft::new(
            config.network.node_id,
            raft_config.clone(),
            network,
            log_store.clone(),
            state_machine_store.clone(),
        )
        .await?,
    ));

    let server_addr = format!("{}:{}", config.network.ip, config.network.server_port);

    // Determine whether certificate files were supplied.
    let use_cert_files =
        !config.cert.cert_file_path.is_empty() && !config.cert.key_file_path.is_empty();
    let cert_tuple = if use_cert_files {
        Some((
            vec![load_certificate(&config.cert.cert_file_path).await?],
            load_private_key(&config.cert.key_file_path).await?,
        ))
    } else {
        None
    };

    let server = RServer::new(
        &server_addr,
        cert_tuple,
        raft.clone(),
        config.protocol.clone(),
    )
    .await?;

    let gateway_state = GatewayState::new(
        state_machine_store,
        raft.clone(),
        config.raft.cluster_name.clone(),
    );

    let key_cert = if use_cert_files {
        Ok(Keycert::new()
            .cert_from_path(&config.cert.cert_file_path)?
            .key_from_path(&config.cert.key_file_path)?)
    } else {
        generate_and_create_keycert(vec!["localhost".to_string()])
    };

    let http_addr: SocketAddr = format!("{}:{}", config.network.ip, config.http.port).parse()?;
    let tls_config = RustlsConfig::new(key_cert.ok());
    let http_server = Http3Server::run(
        http_addr,
        tls_config,
        &config.http,
        gateway_state.clone(),
        task_queue.clone(),
    )
    .await;

    // If not in single-node mode, create remote client connections.
    if mode != GatewayMode::Single {
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
                config.cert.dangerous_skip_verification,
                config.protocol.clone(),
            )
            .await?;
            clients_map
                .insert(endpoint.clone(), client)
                .map_err(|e| anyhow::anyhow!("{:?}", e))?;
        }
    }

    let node_id = config.network.node_id;
    info!("Starting node {} with mode {:?}", node_id, mode);

    // Initialize raft membership based on the mode.
    match mode {
        GatewayMode::Bootstrap => {
            if node_id == 1 {
                info!("Initializing the cluster with node {}", node_id);
                let mut initial_nodes = BTreeMap::new();
                initial_nodes.insert(
                    1,
                    BasicNode {
                        addr: server_addr.clone(),
                    },
                );
                raft.write().await.initialize(initial_nodes).await?;
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
                    raft.write().await.add_learner(*id, node, true).await?;
                }
            } else {
                info!("Node {} started as a non-initialized node. It will be added as a learner by the leader.", node_id);
            }
        }
        GatewayMode::Vote | GatewayMode::Single => {
            let mut membership_nodes = BTreeMap::new();
            membership_nodes.insert(
                node_id,
                BasicNode {
                    addr: server_addr.clone(),
                },
            );
            if let GatewayMode::Vote = mode {
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
            }
            info!(
                "Node {} initializing with membership configuration: {:?}",
                node_id, membership_nodes
            );
            match raft.write().await.initialize(membership_nodes).await {
                Ok(_) => info!(
                    "Node {} successfully initialized in {} mode",
                    node_id,
                    if mode == GatewayMode::Vote {
                        "vote"
                    } else {
                        "single"
                    }
                ),
                Err(e) => info!(
                    "Warning: Node {} not initialized (likely already initialized): {:?}",
                    node_id, e
                ),
            }
        }
    }

    let c = config.clone();
    let gs = gateway_state.clone();
    let tq = task_queue.clone();
    let gateway_info_updater = tokio::spawn(async move {
        Gateway::gateway_info_updater(c, gs, tq).await;
    });

    let gs = gateway_state.clone();
    let gateway_leader_change = tokio::spawn(async move {
        Gateway::gateway_leader_change(gs).await;
    });

    Ok(Gateway {
        _config: config.clone(),
        _id: node_id,
        server,
        _gateway_state: gateway_state,
        gateway_info_updater,
        gateway_leader_change,
        _log_store: log_store,
        http_server,
        _task_queue: task_queue,
    })
}

pub async fn start_gateway_bootstrap(config: Arc<NodeConfig>) -> Result<Gateway> {
    start_gateway(GatewayMode::Bootstrap, config).await
}

pub async fn start_gateway_vote(config: Arc<NodeConfig>) -> Result<Gateway> {
    start_gateway(GatewayMode::Vote, config).await
}

pub async fn start_gateway_single(config: Arc<NodeConfig>) -> Result<Gateway> {
    start_gateway(GatewayMode::Single, config).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{config::ProtocolConfig, raft::server::RServer};
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
        node_clients: Arc<scc::HashMap<String, RClient, RandomState>>,
    ) -> Result<(Arc<RwLock<Raft>>, Arc<StateMachineStore>, RServer)> {
        let network = Network::new(node_clients.clone());
        let log_store = LogStore::default();
        let state_machine_store = Arc::new(StateMachineStore::default());
        let pcfg = ProtocolConfig::default();
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
        let server = RServer::new(addr, None, raft.clone(), pcfg).await?;
        Ok((raft, state_machine_store, server))
    }

    async fn setup_cluster(
        node_configs: &[(u64, &str)],
        node_clients: Arc<scc::HashMap<String, RClient, RandomState>>,
    ) -> Result<(
        Arc<Config>,
        ProtocolConfig,
        Vec<Arc<RwLock<Raft>>>,
        Vec<Arc<StateMachineStore>>,
        Vec<RServer>,
    )> {
        let pcfg = ProtocolConfig::default();
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
        Ok((config, pcfg, raft_nodes, state_machines, server_handles))
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
        let node_clients = Arc::new(scc::HashMap::with_capacity_and_hasher(
            node_configs.len(),
            RandomState::default(),
        ));

        let (_config, pcfg, raft_nodes, state_machines, _server_handles) =
            setup_cluster(&node_configs, node_clients.clone()).await?;

        tokio::time::sleep(Duration::from_secs(2)).await;

        for (i, &(_, server_addr)) in node_configs.iter().enumerate() {
            let client_port = 22001 + i;
            let client_addr = format!("127.0.0.1:{}", client_port);
            let client =
                RClient::new(server_addr, "localhost", &client_addr, true, pcfg.clone()).await?;
            node_clients
                .insert(server_addr.to_string(), client)
                .map_err(|e| anyhow::anyhow!("{:?}", e))?;
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
        let node_clients = Arc::new(scc::HashMap::with_capacity_and_hasher(
            node_configs.len(),
            RandomState::default(),
        ));

        let (_config, pcfg, raft_nodes, state_machines, _server_handles) =
            setup_cluster(&node_configs, node_clients.clone()).await?;

        tokio::time::sleep(Duration::from_secs(2)).await;

        for (i, &(_, server_addr)) in node_configs.iter().enumerate() {
            let client_port = 25001 + i as u16;
            let client_addr = format!("127.0.0.1:{}", client_port);
            let client =
                RClient::new(server_addr, "localhost", &client_addr, true, pcfg.clone()).await?;
            node_clients
                .insert(server_addr.to_string(), client)
                .map_err(|e| anyhow::anyhow!("{:?}", e))?;
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
        let node_clients = Arc::new(scc::HashMap::with_capacity_and_hasher(
            node_configs.len(),
            RandomState::default(),
        ));

        let (_config, pcfg, mut raft_nodes, mut state_machines, mut server_handles) =
            setup_cluster(&node_configs, node_clients.clone()).await?;

        // Give servers a moment to start up.
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Connect a client to each node.
        for (i, &(_, server_addr)) in node_configs.iter().enumerate() {
            let client_port = 26001 + i as u16;
            let client_addr = format!("127.0.0.1:{}", client_port);
            let client =
                RClient::new(server_addr, "localhost", &client_addr, true, pcfg.clone()).await?;
            node_clients
                .insert(server_addr.to_string(), client)
                .map_err(|e| anyhow::anyhow!("{:?}", e))?;
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
        node.abort();
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
        let node_clients = Arc::new(scc::HashMap::with_capacity_and_hasher(
            initial_configs.len(),
            RandomState::default(),
        ));

        let (config, pcfg, mut raft_nodes, mut state_machines, mut server_handles) =
            setup_cluster(&initial_configs, node_clients.clone()).await?;

        tokio::time::sleep(Duration::from_secs(2)).await;

        for (i, &(_, server_addr)) in initial_configs.iter().enumerate() {
            let client_port = 28001 + i as u16;
            let client_addr = format!("127.0.0.1:{}", client_port);
            let client =
                RClient::new(server_addr, "localhost", &client_addr, true, pcfg.clone()).await?;
            node_clients
                .insert(server_addr.to_string(), client)
                .map_err(|e| anyhow::anyhow!("{:?}", e))?;
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
        let new_client =
            RClient::new(new_node_addr, "localhost", &new_client_addr, true, pcfg).await?;
        node_clients
            .insert(new_node_addr.to_string(), new_client)
            .map_err(|e| anyhow::anyhow!("{:?}", e))?;

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

        let node_clients = Arc::new(scc::HashMap::with_capacity_and_hasher(
            initial_configs.len(),
            RandomState::default(),
        ));

        let (config, pcfg, mut raft_nodes, mut state_machines, mut server_handles) =
            setup_cluster(&initial_configs, node_clients.clone()).await?;

        // Allow the servers time to start up.
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Establish client connections for the initial nodes.
        for (i, &(_, server_addr)) in initial_configs.iter().enumerate() {
            let client_port = 30001 + i as u16;
            let client_addr = format!("127.0.0.1:{}", client_port);
            let client =
                RClient::new(server_addr, "localhost", &client_addr, true, pcfg.clone()).await?;
            node_clients
                .insert(server_addr.to_string(), client)
                .map_err(|e| anyhow::anyhow!("{:?}", e))?;
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
        let new_client =
            RClient::new(new_node_addr, "localhost", &new_client_addr, true, pcfg).await?;
        node_clients
            .insert(new_node_addr.to_string(), new_client)
            .map_err(|e| anyhow::anyhow!("{:?}", e))?;

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
