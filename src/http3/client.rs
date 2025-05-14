use anyhow::Result;
use bytes::{Buf, Bytes, BytesMut};
use futures::future;
use h3;
use h3::error::ConnectionError;
use h3_quinn;
use http::{self, StatusCode};
use quinn;
use rustls;
use rustls_platform_verifier::BuilderVerifierExt;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;
use tracing::error;

use crate::common::cert::SkipServerVerification;

pub struct Http3Client {
    send_request: h3::client::SendRequest<h3_quinn::OpenStreams, Bytes>,
}

impl Http3Client {
    pub async fn new(
        server_domain: &str,
        server_ip: &str,
        dangerous_skip_verification: bool,
    ) -> Result<Http3Client> {
        let mut endpoint = quinn::Endpoint::client("0.0.0.0:0".parse()?)?;

        let mut rustls_config = if dangerous_skip_verification {
            rustls::ClientConfig::builder()
                .dangerous()
                .with_custom_certificate_verifier(SkipServerVerification::new())
                .with_no_client_auth()
        } else {
            let crypto_provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
            rustls::ClientConfig::builder_with_provider(crypto_provider)
                .with_safe_default_protocol_versions()?
                .with_platform_verifier()
                .with_no_client_auth()
        };

        rustls_config.alpn_protocols = vec![b"h3".to_vec()];

        let quinn_client_config =
            quinn::crypto::rustls::QuicClientConfig::try_from(Arc::new(rustls_config))?;
        let client_config = quinn::ClientConfig::new(Arc::new(quinn_client_config));
        endpoint.set_default_client_config(client_config);

        let addr = server_ip.parse()?;
        let conn = endpoint.connect(addr, server_domain)?.await?;
        let quinn_conn = h3_quinn::Connection::new(conn);

        let (mut driver, send_request) = h3::client::new(quinn_conn).await?;
        tokio::spawn(async move {
            let err: ConnectionError = future::poll_fn(|cx| driver.poll_close(cx)).await;
            if !err.is_h3_no_error() {
                error!("Http3 client driver error: {:?}", err);
            }
        });

        Ok(Self { send_request })
    }

    pub async fn get(
        &mut self,
        url: &str,
        timeout_duration: Option<Duration>,
    ) -> Result<(StatusCode, Bytes)> {
        let uri = url.parse::<http::uri::Uri>()?;
        let request = http::Request::builder().method("GET").uri(&uri).body(())?;

        let fut = async {
            let mut stream = self.send_request.send_request(request).await?;
            stream.finish().await?;
            let response = stream.recv_response().await?;

            let mut buf = BytesMut::new();
            while let Some(mut chunk) = stream.recv_data().await? {
                let bytes = chunk.copy_to_bytes(chunk.remaining());
                buf.extend_from_slice(&bytes);
            }

            Ok((response.status(), buf.freeze()))
        };

        match timeout_duration {
            Some(duration) => timeout(duration, fut)
                .await
                .map_err(|_| anyhow::anyhow!("GET request timed out"))?,
            None => fut.await,
        }
    }

    pub async fn post(
        &mut self,
        url: &str,
        data: Bytes,
        timeout_duration: Option<Duration>,
    ) -> Result<(StatusCode, Bytes)> {
        let uri = url.parse::<http::uri::Uri>()?;
        let request = http::Request::builder()
            .method("POST")
            .uri(&uri)
            .header("Content-Type", "application/json")
            .body(())?;

        let fut = async {
            let mut stream = self.send_request.send_request(request).await?;
            stream.send_data(data).await?;
            stream.finish().await?;
            let response = stream.recv_response().await?;

            let mut buf = BytesMut::new();
            while let Some(mut chunk) = stream.recv_data().await? {
                let bytes = chunk.copy_to_bytes(chunk.remaining());
                buf.extend_from_slice(&bytes);
            }

            Ok((response.status(), buf.freeze()))
        };

        match timeout_duration {
            Some(duration) => timeout(duration, fut)
                .await
                .map_err(|_| anyhow::anyhow!("POST request timed out"))?,
            None => fut.await,
        }
    }
}
