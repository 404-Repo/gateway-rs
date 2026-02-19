use super::*;
use crate::raft::{LogStore, StateMachineStore, network::Network};
use anyhow::Result;
use foldhash::fast::RandomState;
use openraft::Config;
use std::net::UdpSocket;
use std::sync::{Arc, Once};

static CRYPTO_INIT: Once = Once::new();

fn init_crypto_provider() {
    CRYPTO_INIT.call_once(|| {
        crate::raft::init_crypto_provider().unwrap();
    });
}

async fn create_test_raft_node(node_id: u64) -> Result<Raft> {
    init_crypto_provider();
    let log_store = LogStore::default();
    let state_machine_store = Arc::new(StateMachineStore::default());

    let node_clients = scc::HashMap::with_capacity_and_hasher(5, RandomState::default());
    let network = Network::new(Arc::new(node_clients));

    let config = Arc::new(
        Config {
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
        state_machine_store,
    )
    .await?;

    Ok(raft)
}

#[tokio::test]
async fn test_server_bind() -> Result<()> {
    let raft = create_test_raft_node(1).await?;
    let pcfg = RServerConfig::default();

    let addr = "127.0.0.1:4444";
    let _server = RServer::new(addr, None, raft, pcfg, CancellationToken::new()).await?;

    // Give the server some time to start
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Try binding to the same address with a UDP socket, it should fail
    let socket = UdpSocket::bind(addr);
    assert!(socket.is_err(), "Port should be taken by the QUIC server.");

    Ok(())
}

#[tokio::test]
async fn test_server_connection() -> Result<()> {
    let raft = create_test_raft_node(1).await?;
    let pcfg = RServerConfig::default();

    let addr = "127.0.0.1:4445";
    let _server = RServer::new(addr, None, raft, pcfg.clone(), CancellationToken::new()).await?;

    // Give the server some time to start
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let client = crate::raft::client::RClientBuilder::new()
        .remote_addr(addr)
        .server_name("localhost")
        .local_bind_addr("127.0.0.1:0")
        .dangerous_skip_verification(true)
        .protocol_cfg(pcfg)
        .build()
        .await?;

    assert!(client.stable_id() > 0);

    Ok(())
}
