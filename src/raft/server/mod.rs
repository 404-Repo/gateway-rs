use crate::{protocol::Protocol, raft::TypeConfig};
use anyhow::Result;
use openraft::Raft;
use quinn::{Endpoint, Incoming, RecvStream, SendStream, ServerConfig};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use std::{net::SocketAddr, sync::Arc};
use tokio::{sync::RwLock, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

pub struct RServer {
    endpoint: Endpoint,
    server_config: ServerConfig,
    raft: Arc<RwLock<Raft<TypeConfig>>>,
    accept_task: JoinHandle<()>,
    cancel_token: CancellationToken,
}

impl RServer {
    pub async fn new(
        addr: &str,
        cert: Option<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)>,
        raft: Arc<RwLock<Raft<TypeConfig>>>,
    ) -> Result<Self> {
        let server_config = match cert {
            Some((cert_chain, key)) => ServerConfig::with_single_cert(cert_chain, key)?,
            None => Self::generate_self_signed_config()?,
        };

        let bind_addr: SocketAddr = addr.parse()?;
        let endpoint = Endpoint::server(server_config.clone(), bind_addr)?;
        info!("QUIC server listening on {}", bind_addr);

        let cancel_token = CancellationToken::new();
        let endpoint_clone = endpoint.clone();
        let raft_clone = raft.clone();
        let accept_token = cancel_token.clone();

        let accept_task: JoinHandle<()> = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = accept_token.cancelled() => {
                        info!("Cancellation requested: stopping accept loop");
                        break;
                    }
                    maybe_incoming = endpoint_clone.accept() => {
                        match maybe_incoming {
                            Some(incoming) => {
                                let connection_token = accept_token.clone();
                                let raft_inner = raft_clone.clone();
                                tokio::spawn(async move {
                                    tokio::select! {
                                        res = Self::handle_incoming(incoming, raft_inner) => {
                                            if let Err(e) = res {
                                                error!("Connection error: {}", e);
                                            }
                                        },
                                        _ = connection_token.cancelled() => {
                                            info!("Cancellation requested: stopping connection task");
                                        }
                                    }
                                });
                            },
                            None => {
                                info!("No more incoming connections");
                                break;
                            }
                        }
                    }
                }
            }
        });

        Ok(Self {
            endpoint,
            server_config,
            raft,
            accept_task,
            cancel_token,
        })
    }

    fn generate_self_signed_config() -> Result<ServerConfig> {
        let rcgen_cert = rcgen::generate_simple_self_signed(vec!["localhost".into()])?;
        let key = rcgen_cert.key_pair.serialize_der();
        let cert_der: CertificateDer<'static> = rcgen_cert.cert.der().to_vec().into();

        Ok(ServerConfig::with_single_cert(
            vec![cert_der],
            PrivateKeyDer::Pkcs8(key.into()),
        )?)
    }

    async fn handle_incoming(
        incoming: Incoming,
        raft: Arc<RwLock<Raft<TypeConfig>>>,
    ) -> Result<()> {
        let connection = incoming.await?;
        let remote_addr = connection.remote_address();
        info!("Incoming connection from {}", remote_addr);

        loop {
            match connection.accept_bi().await {
                Ok((send, recv)) => {
                    let protocol = Protocol::new(connection.clone());
                    if let Err(e) = Self::handle_request(protocol, send, recv, raft.clone()).await {
                        error!("Error handling request: {}", e);
                    }
                }
                Err(e) => {
                    error!("Connection error: {}", e);
                    break;
                }
            }
        }
        info!("Connection closed from {}", remote_addr);
        Ok(())
    }

    async fn handle_request(
        protocol: Protocol,
        send: SendStream,
        recv: RecvStream,
        raft: Arc<RwLock<Raft<TypeConfig>>>,
    ) -> Result<()> {
        let message = Protocol::receive_message(recv).await?;
        debug!("==> Received message: {:?}", message);

        let response = match Protocol::handle_message(message, raft).await {
            Ok(resp) => {
                debug!("==> Client got response: {:?}", resp);
                resp
            }
            Err(e) => {
                error!("==> Error handling message: {}", e);
                return Err(e);
            }
        };

        let result = protocol.send_message(send, response).await;
        debug!("==> Send result: {:?}", result);

        Ok(())
    }

    pub async fn abort(&self) {
        self.cancel_token.cancel();
        self.accept_task.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::{network::Network, LogStore, StateMachineStore, TypeConfig};
    use anyhow::Result;
    use dashmap::DashMap;
    use openraft::Config;
    use std::net::UdpSocket;
    use std::sync::Arc;
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

        let raft = openraft::Raft::new(
            node_id,
            config.clone(),
            network,
            log_store,
            state_machine_store,
        )
        .await?;

        Ok(Arc::new(RwLock::new(raft)))
    }

    #[tokio::test]
    async fn test_server_bind() -> Result<()> {
        let raft = create_test_raft_node(1).await?;

        let addr = "127.0.0.1:4444";
        let _server = RServer::new(addr, None, raft).await?;

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

        let addr = "127.0.0.1:4445";
        let _server = RServer::new(addr, None, raft).await?;

        // Give the server some time to start
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let client =
            crate::raft::client::RClient::new(addr, "localhost", "127.0.0.1:0", true).await?;
        assert!(client.connection.stable_id() > 0);

        Ok(())
    }
}
