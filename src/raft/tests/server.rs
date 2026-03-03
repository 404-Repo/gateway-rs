use crate::config::RServerConfig;
use crate::raft::test_utils::{
    create_default_local_rclient, setup_standalone_raft_node, start_server_on_reserved_addr,
    start_test_rserver,
};
use anyhow::Result;
use std::net::UdpSocket;
use std::time::Duration;

#[tokio::test]
async fn test_server_bind() -> Result<()> {
    let (raft, _) = setup_standalone_raft_node(1).await?;
    let pcfg = RServerConfig::default();

    let (addr, _server) = start_server_on_reserved_addr(raft, &pcfg).await?;

    // Try binding to the same address with a UDP socket, it should fail
    let socket = UdpSocket::bind(addr.as_str());
    assert!(socket.is_err(), "Port should be taken by the QUIC server.");

    Ok(())
}

#[tokio::test]
async fn test_server_connection() -> Result<()> {
    let (raft, _) = setup_standalone_raft_node(1).await?;
    let pcfg = RServerConfig::default();

    let (addr, _server) = start_server_on_reserved_addr(raft, &pcfg).await?;
    let client = create_default_local_rclient(addr.as_str(), &pcfg).await?;

    assert!(client.stable_id() > 0);

    Ok(())
}

#[tokio::test]
async fn test_server_start_retries_when_addr_temporarily_in_use() -> Result<()> {
    let (raft, _) = setup_standalone_raft_node(1).await?;
    let pcfg = RServerConfig::default();

    let blocker = UdpSocket::bind("127.0.0.1:0")?;
    let addr = blocker.local_addr()?.to_string();
    let release_handle = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(300));
        drop(blocker);
    });

    let _server = start_test_rserver(addr.as_str(), raft, &pcfg).await?;
    release_handle
        .join()
        .expect("blocker release thread should not panic");

    let socket = UdpSocket::bind(addr.as_str());
    assert!(socket.is_err(), "Port should be taken by the QUIC server.");

    Ok(())
}
