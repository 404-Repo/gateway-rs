use crate::{common::cert::generate_self_signed_config, config::RServerConfig, protocol::Protocol};
use anyhow::Result;
use quinn::{
    crypto::rustls::QuicServerConfig, Endpoint, IdleTimeout, Incoming, RecvStream, SendStream,
    ServerConfig, TransportConfig, WriteError,
};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use super::Raft;

pub struct RServer {
    _endpoint: Endpoint,
    _server_config: ServerConfig,
    accept_task: JoinHandle<()>,
    cancel_token: CancellationToken,
}

impl RServer {
    pub async fn new(
        bind_addr: &str,
        cert: Option<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)>,
        raft: Raft,
        rserver_cfg: RServerConfig,
    ) -> Result<Self> {
        let mut server_config = match cert {
            Some((cert_chain, key)) => {
                let mut rustls_config = rustls::ServerConfig::builder_with_protocol_versions(&[
                    &rustls::version::TLS13,
                ])
                .with_no_client_auth()
                .with_single_cert(cert_chain, key)?;
                rustls_config.alpn_protocols = vec![b"h3".to_vec()];
                let quic_server_config = QuicServerConfig::try_from(rustls_config)?;
                quinn::ServerConfig::with_crypto(Arc::new(quic_server_config))
            }
            None => generate_self_signed_config()?,
        };

        let mut transport_cfg = TransportConfig::default();
        let idle_timeout =
            IdleTimeout::try_from(Duration::from_secs(rserver_cfg.max_idle_timeout_sec))?;
        transport_cfg.max_idle_timeout(Some(idle_timeout));
        transport_cfg.keep_alive_interval(Some(Duration::from_secs(
            rserver_cfg.keep_alive_interval_sec,
        )));
        server_config.transport_config(Arc::new(transport_cfg));

        let endpoint = Endpoint::server(server_config.clone(), bind_addr.parse::<SocketAddr>()?)?;

        info!("RServer QUIC server listening on {}", bind_addr);

        let cancel_token = CancellationToken::new();
        let accept_task =
            Self::spawn_accept_task(endpoint.clone(), raft, rserver_cfg, cancel_token.clone());

        Ok(Self {
            _endpoint: endpoint,
            _server_config: server_config,
            accept_task,
            cancel_token,
        })
    }

    fn spawn_accept_task(
        endpoint: Endpoint,
        raft: Raft,
        rserver_cfg: RServerConfig,
        cancel_token: CancellationToken,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancel_token.cancelled() => {
                        info!("Cancellation requested: stopping accept loop");
                        break;
                    }
                    maybe_incoming = endpoint.accept() => {
                        match maybe_incoming {
                            Some(incoming) => {
                                let connection_token = cancel_token.clone();
                                let raft_inner = raft.clone();
                                let p_cfg = rserver_cfg.clone();
                                tokio::spawn(async move {
                                    tokio::select! {
                                        res = RServer::handle_incoming(incoming, raft_inner, p_cfg.clone()) => {
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
        })
    }

    async fn handle_incoming(
        incoming: Incoming,
        raft: Raft,
        rserver_cfg: RServerConfig,
    ) -> Result<()> {
        let connection = incoming.await?;
        let remote_addr = connection.remote_address();
        info!("Incoming connection from {}", remote_addr);

        loop {
            match connection.accept_bi().await {
                Ok((send, recv)) => {
                    let protocol = Protocol::new(
                        connection.clone(),
                        rserver_cfg.max_message_size,
                        rserver_cfg.max_recv_buffer_size,
                        Duration::from_millis(rserver_cfg.receive_message_timeout_ms),
                    );
                    if let Err(e) = Self::handle_request(protocol, send, recv, raft.clone()).await {
                        match e.downcast_ref::<WriteError>() {
                            Some(WriteError::Stopped(_)) => {}
                            _ => {
                                error!("Error handling request: {}", e);
                            }
                        }
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
        raft: Raft,
    ) -> Result<()> {
        let message = protocol.receive_message(recv).await?;

        let response = match protocol.handle_message(message, raft).await {
            Ok(resp) => resp,
            Err(e) => {
                error!("==> Error handling message: {}", e);
                return Err(e);
            }
        };

        protocol.send_message(send, response).await?;

        Ok(())
    }

    pub fn abort(&self) {
        self.cancel_token.cancel();
        self.accept_task.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::{network::Network, LogStore, StateMachineStore};
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
        let _server = RServer::new(addr, None, raft, pcfg).await?;

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
        let _server = RServer::new(addr, None, raft, pcfg.clone()).await?;

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
}
