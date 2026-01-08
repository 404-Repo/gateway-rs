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
    parse_certificates_from_pem_bytes(&pem_bytes)
}

pub async fn load_private_key<P>(path: P) -> Result<PrivateKeyDer<'static>>
where
    P: AsRef<Path>,
{
    let pem_bytes = tokio::fs::read(path).await?;
    let key = PrivateKeyDer::from_pem_slice(&pem_bytes)?;

    Ok(key)
}

pub fn parse_certificates_from_pem_bytes(pem_bytes: &[u8]) -> Result<Vec<CertificateDer<'static>>> {
    let cert_der_list = CertificateDer::pem_slice_iter(pem_bytes).collect::<Result<Vec<_>, _>>()?;

    if cert_der_list.is_empty() {
        return Err(anyhow::anyhow!("no certificates found in PEM data"));
    }

    Ok(cert_der_list.into_iter().collect())
}

pub fn generate_self_signed_config() -> Result<ServerConfig> {
    let rcgen_cert = rcgen::generate_simple_self_signed(vec!["localhost".into()])?;
    let key = rcgen_cert.signing_key.serialize_der();
    let cert_der: CertificateDer<'static> = rcgen_cert.cert.der().to_vec().into();

    let mut rustls_config =
        rustls::ServerConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
            .with_no_client_auth()
            .with_single_cert(vec![cert_der], PrivateKeyDer::Pkcs8(key.into()))?;

    rustls_config.alpn_protocols = vec![b"h3".to_vec()];

    let quic_server_config = QuicServerConfig::try_from(rustls_config)?;
    Ok(ServerConfig::with_crypto(std::sync::Arc::new(
        quic_server_config,
    )))
}

pub fn generate_and_create_keycert(domain_names: Vec<String>) -> Result<Keycert> {
    let certified_key = rcgen::generate_simple_self_signed(domain_names)?;

    let cert_pem = certified_key.cert.pem();
    let key_pem = certified_key.signing_key.serialize_pem();

    let keycert = Keycert::new().cert(cert_pem).key(key_pem);

    Ok(keycert)
}
