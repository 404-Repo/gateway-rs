use anyhow::Result;
use quinn::{crypto::rustls::QuicServerConfig, ServerConfig};
use rustls::pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer};
use salvo::conn::rustls::Keycert;
use std::{path::Path, sync::Arc};

#[derive(Debug)]
pub struct SkipServerVerification;

impl SkipServerVerification {
    pub fn new() -> Arc<Self> {
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

pub async fn load_certificates<P>(path: P) -> Result<Vec<CertificateDer<'static>>>
where
    P: AsRef<Path>,
{
    let pem_bytes = tokio::fs::read(path).await?;
    let mut reader = pem_bytes.as_slice();

    let cert_der_list = rustls_pemfile::certs(&mut reader).collect::<Result<Vec<_>, _>>()?;

    if cert_der_list.is_empty() {
        return Err(anyhow::anyhow!("no certificates found in PEM file"));
    }

    Ok(cert_der_list.into_iter().collect())
}

pub async fn load_private_key<P>(path: P) -> Result<PrivateKeyDer<'static>>
where
    P: AsRef<Path>,
{
    let pem_bytes = tokio::fs::read(path).await?;
    let key = PrivateKeyDer::from_pem_slice(&pem_bytes)?;

    Ok(key.clone_key())
}

pub fn generate_self_signed_config() -> Result<ServerConfig> {
    let rcgen_cert = rcgen::generate_simple_self_signed(vec!["localhost".into()])?;
    let key = rcgen_cert.key_pair.serialize_der();
    let cert_der: CertificateDer<'static> = rcgen_cert.cert.der().to_vec().into();

    let mut rustls_config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der.clone()], PrivateKeyDer::Pkcs8(key.into()))?;

    rustls_config.alpn_protocols = vec![b"h3".to_vec()];

    let quic_server_config = QuicServerConfig::try_from(rustls_config)?;
    Ok(ServerConfig::with_crypto(std::sync::Arc::new(
        quic_server_config,
    )))
}

pub fn generate_and_create_keycert(
    domain_names: Vec<String>,
) -> Result<Keycert, Box<dyn std::error::Error>> {
    let certified_key = rcgen::generate_simple_self_signed(domain_names)
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;

    let cert_pem = certified_key.cert.pem();
    let cert_bytes: Vec<u8> = cert_pem.as_bytes().to_vec();

    let key_pem = certified_key.key_pair.serialize_pem();
    let key_bytes: Vec<u8> = key_pem.as_bytes().to_vec();

    let keycert = Keycert::new().cert(cert_bytes).key(key_bytes);

    Ok(keycert)
}
