use super::*;
use crate::config::RServerConfig;
use crate::protocol::RaftMessageType;
use crate::raft::server::RServer;
use crate::raft::{LogStore, Network, StateMachineStore, TypeConfig};
use anyhow::Result;
use foldhash::fast::RandomState;
use openraft::Config;
use std::sync::{Arc, Once};
use tokio_util::sync::CancellationToken;

static CRYPTO_INIT: Once = Once::new();

fn setup_crypto() {
    CRYPTO_INIT.call_once(|| {
        crate::raft::init_crypto_provider().unwrap();
    });
}

async fn create_test_raft_node(
    node_id: u64,
) -> Result<(openraft::Raft<TypeConfig>, Arc<StateMachineStore>)> {
    setup_crypto();
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
        state_machine_store.clone(),
    )
    .await?;

    Ok((raft, state_machine_store))
}

#[tokio::test]
async fn test_rclient_connection() -> Result<()> {
    let (raft, _) = create_test_raft_node(1).await?;
    let pcfg = RServerConfig::default();

    let server_addr = "127.0.0.1:4446";
    let _server = RServer::new(
        server_addr,
        None,
        raft,
        pcfg.clone(),
        CancellationToken::new(),
    )
    .await?;

    // Give the server some time to start
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let client = RClientBuilder::new()
        .remote_addr(server_addr)
        .server_name("localhost")
        .local_bind_addr("127.0.0.1:0")
        .dangerous_skip_verification(true)
        .protocol_cfg(pcfg)
        .build()
        .await?;

    assert!(client.stable_id() > 0);

    Ok(())
}

#[tokio::test]
async fn test_rclient_send_and_receive() -> Result<()> {
    let (raft, _) = create_test_raft_node(1).await?;
    let pcfg = RServerConfig::default();

    let server_addr = "127.0.0.1:4447";
    let _server = RServer::new(
        server_addr,
        None,
        raft,
        pcfg.clone(),
        CancellationToken::new(),
    )
    .await?;

    // Give the server some time to start
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let client = RClientBuilder::new()
        .remote_addr(server_addr)
        .server_name("localhost")
        .local_bind_addr("127.0.0.1:0")
        .dangerous_skip_verification(true)
        .protocol_cfg(pcfg)
        .build()
        .await?;

    let message_to_send = RaftMessageType::VoteRequest(openraft::raft::VoteRequest {
        vote: openraft::Vote::new(1, 1),
        last_log_id: None,
    });

    let response = client.send(message_to_send).await?;

    assert!(matches!(response, RaftMessageType::VoteResponse(..)));

    Ok(())
}

#[tokio::test]
async fn test_rclient_multiple_streams() -> Result<()> {
    let (raft, _) = create_test_raft_node(1).await?;
    let pcfg = RServerConfig::default();

    let server_addr = "127.0.0.1:4448";
    let _server = RServer::new(
        server_addr,
        None,
        raft,
        pcfg.clone(),
        CancellationToken::new(),
    )
    .await?;

    // Give the server some time to start
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let client = RClientBuilder::new()
        .remote_addr(server_addr)
        .server_name("localhost")
        .local_bind_addr("127.0.0.1:0")
        .dangerous_skip_verification(true)
        .protocol_cfg(pcfg)
        .build()
        .await?;

    let mut handles = Vec::new();

    for i in 0..10 {
        let client_clone = client.clone();
        let handle = tokio::spawn(async move {
            let message_to_send = RaftMessageType::VoteRequest(openraft::raft::VoteRequest {
                vote: openraft::Vote::new(i, i),
                last_log_id: None,
            });
            client_clone.send(message_to_send).await
        });
        handles.push(handle);
    }

    let mut results = Vec::new();
    for handle in handles {
        results.push(handle.await?);
    }

    assert_eq!(results.len(), 10);
    for result in results {
        assert!(result.is_ok());
        assert!(matches!(result.unwrap(), RaftMessageType::VoteResponse(..)));
    }

    Ok(())
}
