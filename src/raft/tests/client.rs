use crate::config::RServerConfig;
use crate::protocol::RaftMessageType;
use crate::raft::test_utils::{
    create_default_local_rclient, setup_standalone_raft_node, start_server_on_reserved_addr,
};
use anyhow::Result;

#[tokio::test]
async fn test_rclient_connection() -> Result<()> {
    let (raft, _) = setup_standalone_raft_node(1).await?;
    let pcfg = RServerConfig::default();

    let (server_addr, _server) = start_server_on_reserved_addr(raft, &pcfg).await?;
    let client = create_default_local_rclient(server_addr.as_str(), &pcfg).await?;

    assert!(client.stable_id() > 0);

    Ok(())
}

#[tokio::test]
async fn test_rclient_send_and_receive() -> Result<()> {
    let (raft, _) = setup_standalone_raft_node(1).await?;
    let pcfg = RServerConfig::default();

    let (server_addr, _server) = start_server_on_reserved_addr(raft, &pcfg).await?;
    let client = create_default_local_rclient(server_addr.as_str(), &pcfg).await?;

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
    let (raft, _) = setup_standalone_raft_node(1).await?;
    let pcfg = RServerConfig::default();

    let (server_addr, _server) = start_server_on_reserved_addr(raft, &pcfg).await?;
    let client = create_default_local_rclient(server_addr.as_str(), &pcfg).await?;

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
