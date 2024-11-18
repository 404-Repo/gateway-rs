// TODO: Remove this after the code is finalized
#![allow(dead_code)]

use anyhow::Result;
use quinn::crypto::rustls::QuicClientConfig;
use quinn::{ClientConfig, Endpoint};
use rustls::crypto::CryptoProvider;
use rustls::RootCertStore;
use std::{net::SocketAddr, sync::Arc};

pub struct QClient {
    endpoint: Endpoint,
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
impl QClient {
    pub fn new(bind_addr: SocketAddr, dangerous_skip_verification: bool) -> Result<Self> {
        rustls::crypto::aws_lc_rs::default_provider()
            .install_default()
            .or_else(|_| {
                // If it fails, fall back to using existing default provider
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

        let client_config = ClientConfig::new(Arc::new(
            QuicClientConfig::try_from(crypto)
                .map_err(|e| anyhow::anyhow!("Failed to create QUIC client config: {}", e))?,
        ));

        let mut endpoint = Endpoint::client(bind_addr)
            .map_err(|e| anyhow::anyhow!("Failed to create endpoint: {}", e))?;

        endpoint.set_default_client_config(client_config);

        Ok(Self { endpoint })
    }

    pub async fn connect(&self, server_addr: SocketAddr) -> Result<quinn::Connection> {
        self.endpoint
            .connect(server_addr, "localhost")
            .map_err(|e| anyhow::anyhow!("Failed to initiate connection: {}", e))?
            .await
            .map_err(|e| anyhow::anyhow!("Failed to establish connection: {}", e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::QServer;
    use std::net::{IpAddr, Ipv4Addr};

    #[tokio::test]
    async fn test_client_connection() -> Result<()> {
        let mut server = QServer::new(None)?;
        let server_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8888);

        let server_handle = tokio::spawn(async move {
            server.start(server_addr).await.unwrap();
        });

        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let client_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8889);
        let client = QClient::new(client_addr, true)?;
        let connection = client.connect(server_addr).await?;

        assert!(connection.stable_id() > 0);

        server_handle.abort();
        Ok(())
    }
}
