use anyhow::Result;
use quinn::{Connection, Endpoint};
use rustls_platform_verifier::BuilderVerifierExt;
use sdd::{AtomicOwned, Guard, Owned, Tag};
use std::net::SocketAddr;
use std::sync::atomic::Ordering::{AcqRel, Acquire};
use std::sync::Arc;
use std::time::Duration;
use tracing::info;

use crate::{
    common::cert::SkipServerVerification,
    config::ProtocolConfig,
    protocol::{Protocol, RaftMessageType},
};

#[derive(Debug, Clone)]
pub struct RClient {
    endpoint: Arc<Endpoint>,
    connection: Arc<AtomicOwned<Connection>>,
    server_name: String,
    remote_addr: SocketAddr,
    protocol_cfg: ProtocolConfig,
}

impl RClient {
    pub async fn new(
        remote_addr: &str,
        server_name: &str,
        local_bind_addr: &str,
        dangerous_skip_verification: bool,
        protocol_cfg: ProtocolConfig,
    ) -> Result<Self> {
        let remote_addr: SocketAddr = remote_addr.parse()?;
        let local_bind_addr: SocketAddr = local_bind_addr.parse()?;

        let mut crypto = if dangerous_skip_verification {
            rustls::ClientConfig::builder()
                .dangerous()
                .with_custom_certificate_verifier(SkipServerVerification::new())
                .with_no_client_auth()
        } else {
            let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
            rustls::ClientConfig::builder_with_provider(provider)
                .with_safe_default_protocol_versions()?
                .with_platform_verifier()
                .with_no_client_auth()
        };
        crypto.alpn_protocols = vec![b"h3".to_vec()];

        let mut client_cfg = quinn::ClientConfig::new(Arc::new(
            quinn::crypto::rustls::QuicClientConfig::try_from(crypto)?,
        ));
        let timeout = quinn::IdleTimeout::try_from(Duration::from_secs(10))?;
        client_cfg.transport_config(Arc::new({
            let mut tx = quinn::TransportConfig::default();
            tx.keep_alive_interval(Some(Duration::from_millis(100)));
            tx.max_idle_timeout(Some(timeout));
            tx
        }));

        let mut endpoint = Endpoint::client(local_bind_addr)?;
        endpoint.set_default_client_config(client_cfg);

        let conn = endpoint.connect(remote_addr, server_name)?.await?;
        info!(
            "Successfully established connection to {} with id: {}",
            remote_addr,
            conn.stable_id(),
        );

        Ok(Self {
            endpoint: Arc::new(endpoint),
            connection: Arc::new(AtomicOwned::new(conn)),
            server_name: server_name.to_string(),
            remote_addr,
            protocol_cfg,
        })
    }

    #[allow(dead_code)]
    pub fn stable_id(&self) -> usize {
        let guard = Guard::new();
        self.connection
            .load(Acquire, &guard)
            .as_ref()
            .map(|c| c.stable_id())
            .unwrap_or(0)
    }

    async fn get_connection(&self) -> Result<Connection> {
        let maybe_conn = {
            let guard = Guard::new();
            self.connection
                .load(Acquire, &guard)
                .as_ref()
                .map(|c| c.clone())
        };
        if let Some(conn) = maybe_conn {
            Ok(conn)
        } else {
            self.reconnect().await
        }
    }

    async fn reconnect(&self) -> Result<Connection> {
        let new_conn = self
            .endpoint
            .connect(self.remote_addr, &self.server_name)?
            .await?;
        info!(
            "Successfully reconnected to {} with id: {}",
            self.remote_addr,
            new_conn.stable_id(),
        );
        let _ = self
            .connection
            .swap((Some(Owned::new(new_conn.clone())), Tag::None), AcqRel);
        Ok(new_conn)
    }

    pub async fn send<T>(&self, data: T) -> Result<RaftMessageType>
    where
        T: Into<RaftMessageType>,
    {
        let mut conn = self.get_connection().await?;

        // Try opening the stream once, on error, reconnect and open it again
        let (send_stream, recv_stream) = match conn.open_bi().await {
            Ok(streams) => streams,
            Err(_) => {
                conn = self.reconnect().await?;
                conn.open_bi().await?
            }
        };

        let proto = Protocol::new(
            conn.clone(),
            self.protocol_cfg.max_message_size,
            Duration::from_millis(self.protocol_cfg.receive_message_timeout_ms),
        );
        let msg: RaftMessageType = data.into();
        proto.send_message(send_stream, msg).await?;
        let resp = proto.receive_message(recv_stream).await?;
        Ok(resp)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::crypto::init_crypto_provider;
    use crate::config::ProtocolConfig;
    use crate::protocol::RaftMessageType;
    use crate::raft::network::Network;
    use crate::raft::server::RServer;
    use crate::raft::{LogStore, StateMachineStore, TypeConfig};
    use foldhash::fast::RandomState;
    use openraft::raft::AppendEntriesRequest;
    use openraft::{Config, LeaderId, Vote};
    use std::time::Duration;
    use tokio::sync::RwLock;

    async fn create_test_raft_node(
        node_id: u64,
    ) -> Result<Arc<RwLock<openraft::Raft<TypeConfig>>>> {
        init_crypto_provider().unwrap();

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

        let raft =
            openraft::Raft::new(node_id, config, network, log_store, state_machine_store).await?;

        Ok(Arc::new(RwLock::new(raft)))
    }

    #[tokio::test]
    async fn test_client_connection() -> Result<()> {
        init_crypto_provider().unwrap();

        let raft = create_test_raft_node(1).await?;
        let pcfg = ProtocolConfig::default();
        let server_addr = "127.0.0.1:8888";

        let _server = RServer::new(server_addr, None, raft, pcfg.clone()).await?;

        tokio::time::sleep(Duration::from_millis(100)).await;

        let client_addr = "127.0.0.1:8889";
        let client = RClient::new(server_addr, "localhost", client_addr, true, pcfg).await?;

        assert!(client.stable_id() > 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_send_data() -> Result<()> {
        init_crypto_provider().unwrap();

        let raft = create_test_raft_node(1).await?;
        let pcfg = ProtocolConfig::default();
        let server_addr = "127.0.0.1:7777";

        let _server = RServer::new(server_addr, None, raft, pcfg.clone()).await?;

        println!("Starting server on {}", server_addr);

        tokio::time::sleep(Duration::from_millis(200)).await;

        println!("Creating client");
        let client_addr = "127.0.0.1:7778";
        let client =
            RClient::new(server_addr, "localhost", client_addr, true, pcfg.clone()).await?;

        println!("Testing initial connection");
        assert!(client.stable_id() > 0);

        let append_entries = RaftMessageType::AppendEntriesRequest(AppendEntriesRequest {
            vote: Vote {
                leader_id: LeaderId {
                    term: 1,
                    node_id: 1,
                },
                committed: true,
            },
            prev_log_id: None,
            entries: vec![],
            leader_commit: None,
        });

        println!("Sending AppendEntries request");

        let response = client.send(append_entries).await?;

        match response {
            RaftMessageType::AppendEntriesResponse(aer) => {
                println!("Received AppendEntriesResponse {}", aer);
                Ok(())
            }
            _ => {
                println!("Unexpected response type: {:?}", response);
                Err(anyhow::anyhow!("Unexpected response type: {:?}", response))
            }
        }?;

        tokio::time::sleep(Duration::from_millis(100)).await;

        Ok(())
    }

    #[tokio::test]
    async fn test_connection_failure() -> Result<()> {
        init_crypto_provider().unwrap();

        let pcfg = ProtocolConfig::default();
        let result =
            RClient::new("127.0.0.1:9999", "localhost", "127.0.0.1:9998", true, pcfg).await;
        assert!(result.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn test_reconnect() -> Result<()> {
        init_crypto_provider().unwrap();

        let raft = create_test_raft_node(1).await?;
        let pcfg = ProtocolConfig::default();
        let server_addr = "127.0.0.1:6666";
        let _server = RServer::new(server_addr, None, raft, pcfg.clone()).await?;

        tokio::time::sleep(Duration::from_millis(100)).await;

        let client_addr = "127.0.0.1:0";
        let client =
            RClient::new(server_addr, "localhost", client_addr, true, pcfg.clone()).await?;

        // Helper to build a fresh AppendEntries request each time
        fn make_append_entries() -> RaftMessageType {
            RaftMessageType::AppendEntriesRequest(AppendEntriesRequest {
                vote: Vote {
                    leader_id: LeaderId {
                        term: 1,
                        node_id: 1,
                    },
                    committed: true,
                },
                prev_log_id: None,
                entries: Vec::new(),
                leader_commit: None,
            })
        }

        // Verify initial connection
        let response = client.send(make_append_entries()).await?;
        assert!(matches!(
            response,
            RaftMessageType::AppendEntriesResponse(_)
        ));

        let initial_stable_id = client.stable_id();
        assert!(initial_stable_id > 0);

        // Close connection to force a reconnect
        {
            let guard = Guard::new();
            if let Some(conn) = client.connection.load(Acquire, &guard).as_ref() {
                conn.close(0u32.into(), b"test close");
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Send again — should reconnect under the hood
        let response = client.send(make_append_entries()).await?;
        assert!(matches!(
            response,
            RaftMessageType::AppendEntriesResponse(_)
        ));

        // Ensure we got a new connection
        let new_stable_id = client.stable_id();
        assert!(new_stable_id > 0);
        assert_ne!(initial_stable_id, new_stable_id);

        // Final sanity check on the new connection
        let response = client.send(make_append_entries()).await?;
        assert!(matches!(
            response,
            RaftMessageType::AppendEntriesResponse(_)
        ));

        Ok(())
    }
}
