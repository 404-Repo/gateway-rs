// TODO: Remove this after the code is finalized
#![allow(dead_code)]

use anyhow::Result;
use quinn::crypto::rustls::QuicClientConfig;
use quinn::{ClientConfig, Connection, Endpoint, IdleTimeout};
use rustls::crypto::CryptoProvider;
use rustls::RootCertStore;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::time::Duration;
use std::{net::SocketAddr, sync::Arc};
use tracing::info;

use crate::protocol::MAX_MESSAGE_SIZE;

#[derive(Debug, Clone)]
pub struct RClient {
    endpoint: Endpoint,
    pub connection: Connection,
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
        local_bind_addr: &str,
        dangerous_skip_verification: bool,
    ) -> Result<Self> {
        info!(
            "Creating client connection to {} from {}",
            remote_addr, local_bind_addr
        );

        let remote_addr: SocketAddr = remote_addr.parse()?;
        let local_bind_addr: SocketAddr = local_bind_addr.parse()?;

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
                .with_root_certificates(RootCertStore::empty())
                .with_no_client_auth()
        };

        let mut client_config = ClientConfig::new(Arc::new(
            QuicClientConfig::try_from(crypto)
                .map_err(|e| anyhow::anyhow!("Failed to create QUIC client config: {}", e))?,
        ));

        let timeout = IdleTimeout::try_from(Duration::from_secs(10))?;
        // Configure timeouts and keep-alive
        client_config.transport_config(Arc::new({
            let mut transport_config = quinn::TransportConfig::default();
            transport_config.keep_alive_interval(Some(Duration::from_millis(100)));
            transport_config.max_idle_timeout(Some(timeout));
            transport_config
        }));

        let mut endpoint = Endpoint::client(local_bind_addr)?;
        info!("Created endpoint at {}", local_bind_addr);

        endpoint.set_default_client_config(client_config);

        let connect = endpoint.connect(remote_addr, "localhost")?;
        info!("Initiated connection to {}", remote_addr);

        let connection = connect.await?;
        info!(
            "Successfully established connection with id: {}",
            connection.stable_id()
        );

        Ok(Self {
            endpoint,
            connection,
        })
    }

    pub async fn send<T, R>(&self, data: &T) -> Result<R>
    where
        T: Serialize,
        R: DeserializeOwned,
    {
        let (mut send_stream, mut recv_stream) = self.connection.open_bi().await?;

        let mut serializer = rmp_serde::encode::Serializer::new(Vec::new()).with_struct_map();
        data.serialize(&mut serializer)?;
        let serialized = serializer.into_inner();

        send_stream.write_all(&serialized).await?;
        send_stream.finish()?;

        let mut message_data = Vec::new();
        let mut buffer = vec![0; 8192];

        let timeout_duration = tokio::time::Duration::from_secs(3);
        let read_result = tokio::time::timeout(timeout_duration, async {
            while let Ok(Some(n)) = recv_stream.read(&mut buffer).await {
                message_data.extend_from_slice(&buffer[..n]);
                if message_data.len() > MAX_MESSAGE_SIZE {
                    return Err(anyhow::anyhow!(
                        "Response exceeded maximum size limit of {} bytes",
                        MAX_MESSAGE_SIZE
                    ));
                }
            }
            Ok(message_data)
        })
        .await?;

        let message_data = match read_result {
            Ok(data) => data,
            Err(e) => return Err(e),
        };

        if message_data.is_empty() {
            return Err(anyhow::anyhow!("Received empty response"));
        }

        let mut deserializer =
            rmp_serde::decode::Deserializer::new(std::io::Cursor::new(message_data));
        let result = R::deserialize(&mut deserializer)?;

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::RaftMessageType;
    use crate::raft::network::Network;
    use crate::raft::server::RServer;
    use crate::raft::{LogStore, StateMachineStore, TypeConfig};
    use dashmap::DashMap;
    use openraft::raft::AppendEntriesRequest;
    use openraft::{Config, LeaderId, Vote};
    use std::time::Duration;
    use tokio::sync::RwLock;

    async fn create_test_raft_node(
        node_id: u64,
    ) -> Result<Arc<RwLock<openraft::Raft<TypeConfig>>>> {
        let log_store = LogStore::default();
        let state_machine_store = Arc::new(StateMachineStore::default());

        let node_clients = DashMap::new();
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
        let mut server = RServer::new(None, raft)?;
        let server_addr = "127.0.0.1:8888";

        let server_handle = tokio::spawn(async move {
            server.start(server_addr).await.unwrap();
        });

        tokio::time::sleep(Duration::from_millis(100)).await;

        let client_addr = "127.0.0.1:8889";
        let client = RClient::new(server_addr, client_addr, true).await?;

        assert!(client.connection.stable_id() > 0);

        server_handle.abort();
        Ok(())
    }

    #[tokio::test]
    async fn test_send_data() -> Result<()> {
        let raft = create_test_raft_node(1).await?;
        let mut server = RServer::new(None, raft)?;
        let server_addr = "127.0.0.1:7777";

        println!("Starting server on {}", server_addr);
        let server_handle = tokio::spawn(async move {
            if let Err(e) = server.start(server_addr).await {
                println!("Server error: {}", e);
            }
        });

        tokio::time::sleep(Duration::from_millis(200)).await;

        println!("Creating client");
        let client_addr = "127.0.0.1:7778";
        let client = RClient::new(server_addr, client_addr, true).await?;

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

        let response = client.send(&append_entries).await?;

        match response {
            RaftMessageType::AppendEntriesResponse(aer) => {
                println!("Received AppendEntriesResponse {aer}");
                Ok(())
            }
            _ => {
                println!("Unexpected response type: {:?}", response);
                Err(anyhow::anyhow!("Unexpected response type: {:?}", response))
            }
        }?;

        server_handle.abort();
        tokio::time::sleep(Duration::from_millis(100)).await;

        Ok(())
    }

    #[tokio::test]
    async fn test_connection_failure() -> Result<()> {
        let result = RClient::new("127.0.0.1:9999", "127.0.0.1:9998", true).await;
        assert!(result.is_err());
        Ok(())
    }
}
