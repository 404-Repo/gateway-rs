use anyhow::Result;
use quinn::{Connection, Endpoint};
use rustls::crypto::CryptoProvider;
use std::{sync::Arc, time::Duration};
use tracing::info;

use crate::{
    common::cert::SkipServerVerification,
    config::ProtocolConfig,
    protocol::{Protocol, RaftMessageType},
};

#[derive(Debug, Clone)]
pub struct RClient {
    _endpoint: Endpoint,
    pub connection: Connection,
    protocol: Protocol,
}

impl RClient {
    pub async fn new(
        remote_addr: &str,
        server_name: &str,
        local_bind_addr: &str,
        dangerous_skip_verification: bool,
        protocol_cfg: ProtocolConfig,
    ) -> Result<Self> {
        let remote_addr: std::net::SocketAddr = remote_addr.parse()?;
        let local_bind_addr: std::net::SocketAddr = local_bind_addr.parse()?;

        rustls::crypto::aws_lc_rs::default_provider()
            .install_default()
            .or_else(|_| {
                CryptoProvider::get_default()
                    .map(|_| ())
                    .ok_or_else(|| anyhow::anyhow!("Failed to locate any crypto provider"))
            })?;

        let crypto = if dangerous_skip_verification {
            rustls::ClientConfig::builder()
                .dangerous()
                .with_custom_certificate_verifier(SkipServerVerification::new())
                .with_no_client_auth()
        } else {
            rustls::ClientConfig::builder()
                .with_root_certificates(rustls::RootCertStore::empty())
                .with_no_client_auth()
        };

        let mut client_config = quinn::ClientConfig::new(Arc::new(
            quinn::crypto::rustls::QuicClientConfig::try_from(crypto)?,
        ));

        let timeout = quinn::IdleTimeout::try_from(std::time::Duration::from_secs(10))?;
        client_config.transport_config(Arc::new({
            let mut transport_config = quinn::TransportConfig::default();
            transport_config.keep_alive_interval(Some(std::time::Duration::from_millis(100)));
            transport_config.max_idle_timeout(Some(timeout));
            transport_config
        }));

        let mut endpoint = Endpoint::client(local_bind_addr)?;
        endpoint.set_default_client_config(client_config);

        let connect = endpoint.connect(remote_addr, server_name)?;
        let connection = connect.await?;

        info!(
            "Successfully established connection with id: {}",
            connection.stable_id()
        );

        let send_timeout = Duration::from_millis(protocol_cfg.send_timeout_ms);
        let receive_timeout = Duration::from_millis(protocol_cfg.receive_message_timeout_ms);

        let protocol = Protocol::new(
            connection.clone(),
            protocol_cfg.max_message_size,
            send_timeout,
            receive_timeout,
        );

        Ok(Self {
            _endpoint: endpoint,
            connection,
            protocol,
        })
    }

    pub async fn send<T>(&self, data: T) -> Result<RaftMessageType>
    where
        T: Into<RaftMessageType>,
    {
        let (send_stream, recv_stream) = self.connection.open_bi().await?;

        let message: RaftMessageType = data.into();
        self.protocol.send_message(send_stream, message).await?;
        let response = self.protocol.receive_message(recv_stream).await?;

        Ok(response)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ProtocolConfig;
    use crate::protocol::RaftMessageType;
    use crate::raft::network::Network;
    use crate::raft::server::RServer;
    use crate::raft::{LogStore, StateMachineStore, TypeConfig};
    use foldhash::quality::RandomState;
    use openraft::raft::AppendEntriesRequest;
    use openraft::{Config, LeaderId, Vote};
    use std::time::Duration;
    use tokio::sync::RwLock;

    async fn create_test_raft_node(
        node_id: u64,
    ) -> Result<Arc<RwLock<openraft::Raft<TypeConfig>>>> {
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
        let raft = create_test_raft_node(1).await?;
        let pcfg = ProtocolConfig::default();
        let server_addr = "127.0.0.1:8888";

        let _server = RServer::new(server_addr, None, raft, pcfg.clone()).await?;

        tokio::time::sleep(Duration::from_millis(100)).await;

        let client_addr = "127.0.0.1:8889";
        let client = RClient::new(server_addr, "localhost", client_addr, true, pcfg).await?;

        assert!(client.connection.stable_id() > 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_send_data() -> Result<()> {
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
        assert!(client.connection.stable_id() > 0);

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
        let pcfg = ProtocolConfig::default();
        let result =
            RClient::new("127.0.0.1:9999", "localhost", "127.0.0.1:9998", true, pcfg).await;
        assert!(result.is_err());
        Ok(())
    }
}
