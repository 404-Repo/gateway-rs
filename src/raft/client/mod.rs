use anyhow::Result;
use quinn::{Connection, Endpoint};
use rustls::crypto::CryptoProvider;
use std::sync::Arc;
use tracing::info;

use crate::protocol::{Protocol, RaftMessageType};

#[derive(Debug, Clone)]
pub struct RClient {
    endpoint: Endpoint,
    pub connection: Connection,
    protocol: Protocol,
}

#[derive(Debug)]
struct SkipServerVerification;

impl SkipServerVerification {
    fn new() -> Arc<Self> {
        Arc::new(Self)
    }
}

impl rustls::client::danger::ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &rustls::crypto::ring::default_provider().signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &rustls::crypto::ring::default_provider().signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

impl RClient {
    pub async fn new(
        remote_addr: &str,
        server_name: &str,
        local_bind_addr: &str,
        dangerous_skip_verification: bool,
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

        let protocol = Protocol::new(connection.clone());

        Ok(Self {
            endpoint,
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
        let response = Protocol::receive_message(recv_stream).await?;

        Ok(response)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::RaftMessageType;
    use crate::raft::network::Network;
    use crate::raft::server::RServer;
    use crate::raft::{LogStore, StateMachineStore, TypeConfig};
    use openraft::raft::AppendEntriesRequest;
    use openraft::{Config, LeaderId, Vote};
    use std::time::Duration;
    use tokio::sync::RwLock;

    async fn create_test_raft_node(
        node_id: u64,
    ) -> Result<Arc<RwLock<openraft::Raft<TypeConfig>>>> {
        let log_store = LogStore::default();
        let state_machine_store = Arc::new(StateMachineStore::default());

        let node_clients = scc::HashMap::new();
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
        let server_addr = "127.0.0.1:8888";

        let _server = RServer::new(server_addr, None, raft).await?;

        tokio::time::sleep(Duration::from_millis(100)).await;

        let client_addr = "127.0.0.1:8889";
        let client = RClient::new(server_addr, "localhost", client_addr, true).await?;

        assert!(client.connection.stable_id() > 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_send_data() -> Result<()> {
        let raft = create_test_raft_node(1).await?;
        let server_addr = "127.0.0.1:7777";

        let _server = RServer::new(server_addr, None, raft).await?;

        println!("Starting server on {}", server_addr);

        tokio::time::sleep(Duration::from_millis(200)).await;

        println!("Creating client");
        let client_addr = "127.0.0.1:7778";
        let client = RClient::new(server_addr, "localhost", client_addr, true).await?;

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
        let result = RClient::new("127.0.0.1:9999", "localhost", "127.0.0.1:9998", true).await;
        assert!(result.is_err());
        Ok(())
    }
}
