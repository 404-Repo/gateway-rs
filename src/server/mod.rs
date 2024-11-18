// TODO: Remove this after the code is finalized
#![allow(dead_code)]

use crate::protocol::Protocol;
use anyhow::Result;
use quinn::{Endpoint, ServerConfig};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use std::net::SocketAddr;
use tracing::{error, info};

pub struct QServer {
    endpoint: Endpoint,
    server_config: ServerConfig,
}

impl QServer {
    pub fn new(
        cert: Option<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)>,
    ) -> Result<Self> {
        let server_config = match cert {
            Some((cert_chain, key)) => ServerConfig::with_single_cert(cert_chain, key)?,
            None => Self::generate_self_signed_config()?,
        };

        let endpoint = Endpoint::server(server_config.clone(), "0.0.0.0:0".parse()?)?;
        Ok(Self {
            endpoint,
            server_config,
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

    pub async fn start(&mut self, addr: SocketAddr) -> Result<()> {
        self.endpoint = Endpoint::server(self.server_config.clone(), addr)?;
        info!("QUIC server listening on {}", addr);

        while let Some(conn) = self.endpoint.accept().await {
            tokio::spawn(async move {
                match conn.await {
                    Ok(connection) => {
                        let remote_addr = connection.remote_address();
                        info!("Incoming connection from {}", remote_addr);

                        let _protocol = Protocol::new(connection.clone());

                        while let Ok(stream) = connection.accept_uni().await {
                            let remote = remote_addr;

                            tokio::spawn(async move {
                                match Protocol::receive_message(stream).await {
                                    Ok(_message) => {}
                                    Err(e) => {
                                        error!("Failed to receive message from {}: {}", remote, e);
                                    }
                                }
                            });
                        }
                    }
                    Err(e) => {
                        error!("Connection failed: {}", e);
                    }
                }
            });
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr, UdpSocket};

    #[tokio::test]
    async fn test_server_bind() -> Result<()> {
        let mut server = QServer::new(None)?;
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 4444);

        let server_handle = tokio::spawn(async move {
            server.start(addr).await.unwrap();
        });

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Should fail if QUIC server is running
        let socket = UdpSocket::bind(addr);
        assert!(socket.is_err(), "Port should be taken by QUIC server");

        server_handle.abort();
        Ok(())
    }
}
