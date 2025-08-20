use anyhow::Result;
use bytes::{Buf, Bytes, BytesMut};
use futures::future;
use h3::error::ConnectionError;
use h3_quinn::{Connection as H3QuinnConnection, OpenStreams};
use http::{self, StatusCode};
use quinn::{self, Connection as QuinnConnection, Endpoint, IdleTimeout, TransportConfig};
use rustls_platform_verifier::BuilderVerifierExt;
use sdd::{AtomicOwned, Guard, Owned, Tag};
use std::sync::atomic::Ordering::{AcqRel, Acquire};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;
use tracing::{error, info};

use crate::common::cert::SkipServerVerification;

const DEFAULT_MAX_IDLE_TIMEOUT_SEC: u64 = 4;
const DEFAULT_KEEP_ALIVE_INTERVAL_SEC: u64 = 1;

pub struct Http3ClientBuilder {
    server_domain: Option<String>,
    server_ip: Option<String>,
    max_idle_timeout_sec: Option<u64>,
    keep_alive_interval: Option<u64>,
    dangerous_skip_verification: bool,
}

impl Http3ClientBuilder {
    pub fn new() -> Self {
        Self {
            server_domain: None,
            server_ip: None,
            max_idle_timeout_sec: None,
            keep_alive_interval: None,
            dangerous_skip_verification: false,
        }
    }

    pub fn server_domain(mut self, domain: impl Into<String>) -> Self {
        self.server_domain = Some(domain.into());
        self
    }

    pub fn server_ip(mut self, ip: impl Into<String>) -> Self {
        self.server_ip = Some(ip.into());
        self
    }

    pub fn max_idle_timeout_sec(mut self, secs: u64) -> Self {
        self.max_idle_timeout_sec = Some(secs);
        self
    }

    pub fn keep_alive_interval(mut self, secs: u64) -> Self {
        self.keep_alive_interval = Some(secs);
        self
    }

    pub fn dangerous_skip_verification(mut self, skip: bool) -> Self {
        self.dangerous_skip_verification = skip;
        self
    }

    pub async fn build(self) -> Result<Http3Client> {
        let server_domain = self
            .server_domain
            .ok_or_else(|| anyhow::anyhow!("`server_domain` must be set in Http3ClientBuilder"))?;
        let server_ip = self
            .server_ip
            .ok_or_else(|| anyhow::anyhow!("`server_ip` must be set in Http3ClientBuilder"))?;
        Http3Client::new_inner(
            &server_domain,
            &server_ip,
            self.max_idle_timeout_sec,
            self.keep_alive_interval,
            self.dangerous_skip_verification,
        )
        .await
    }
}

struct Inner {
    endpoint: Endpoint,
    server_domain: String,
    server_ip: String,
    quinn_conn_handle: AtomicOwned<QuinnConnection>,
    connection_handle: AtomicOwned<h3::client::SendRequest<OpenStreams, Bytes>>,
}

#[derive(Clone)]
pub struct Http3Client {
    inner: Arc<Inner>,
}

impl Http3Client {
    async fn new_inner(
        server_domain: &str,
        server_ip: &str,
        max_idle_timeout_sec: Option<u64>,
        keep_alive_interval: Option<u64>,
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
                .with_protocol_versions(&[&rustls::version::TLS13])?
                .with_platform_verifier()?
                .with_no_client_auth()
        };
        rustls_config.alpn_protocols = vec![b"h3".to_vec()];

        let client_cfg =
            quinn::crypto::rustls::QuicClientConfig::try_from(Arc::new(rustls_config))?;
        let mut qc = quinn::ClientConfig::new(Arc::new(client_cfg));
        let mut transport_config = TransportConfig::default();
        transport_config.max_idle_timeout(Some(IdleTimeout::try_from(Duration::from_secs(
            max_idle_timeout_sec.unwrap_or(DEFAULT_MAX_IDLE_TIMEOUT_SEC),
        ))?));
        transport_config.keep_alive_interval(Some(Duration::from_secs(
            keep_alive_interval.unwrap_or(DEFAULT_KEEP_ALIVE_INTERVAL_SEC),
        )));
        qc.transport_config(Arc::new(transport_config));
        endpoint.set_default_client_config(qc);

        let remote_addr = server_ip.parse()?;
        let conn = endpoint.connect(remote_addr, server_domain)?.await?;

        let h3_conn = H3QuinnConnection::new(conn.clone());
        let (mut driver, send_request) = h3::client::new(h3_conn).await?;

        let quinn_handle = AtomicOwned::new(conn.clone());
        let handle = AtomicOwned::new(send_request.clone());
        let inner = Arc::new(Inner {
            endpoint,
            server_domain: server_domain.to_string(),
            server_ip: server_ip.to_string(),
            quinn_conn_handle: quinn_handle,
            connection_handle: handle,
        });

        let inner_clone = inner.clone();
        tokio::spawn(async move {
            let err: ConnectionError = future::poll_fn(|cx| driver.poll_close(cx)).await;
            if !err.is_h3_no_error() {
                error!("Http3 client driver error: {:?}", err);
            }
            let _ = inner_clone
                .connection_handle
                .swap((None, Tag::None), AcqRel);
        });

        Ok(Http3Client { inner })
    }

    async fn get_connection_handle(&self) -> Result<h3::client::SendRequest<OpenStreams, Bytes>> {
        let (opt_handle, opt_quic_conn) = {
            let guard = Guard::new();
            let req_handle_opt = self
                .inner
                .connection_handle
                .load(Acquire, &guard)
                .as_ref()
                .map(|h| h.clone());
            let quic_conn_opt = self
                .inner
                .quinn_conn_handle
                .load(Acquire, &guard)
                .as_ref()
                .map(|c| c.clone());
            (req_handle_opt, quic_conn_opt)
        };

        let need_reconnect = match (&opt_handle, &opt_quic_conn) {
            (Some(_), Some(quic_conn)) => quic_conn.close_reason().is_some(),
            _ => true,
        };

        if need_reconnect {
            info!("HTTP/3 client is trying to reconnect...");
            let remote_addr = self.inner.server_ip.parse()?;
            let new_quinn_conn = self
                .inner
                .endpoint
                .connect(remote_addr, &self.inner.server_domain)?
                .await?;

            let _ = self.inner.quinn_conn_handle.swap(
                (Some(Owned::new(new_quinn_conn.clone())), Tag::None),
                AcqRel,
            );

            let h3_conn = H3QuinnConnection::new(new_quinn_conn.clone());
            let (mut driver, new_handle) = h3::client::new(h3_conn).await?;
            info!(
                "HTTP/3 client reconnected, new stable_id = {}",
                new_quinn_conn.stable_id()
            );

            let inner_clone = self.inner.clone();
            tokio::spawn(async move {
                let err: ConnectionError = future::poll_fn(|cx| driver.poll_close(cx)).await;
                if !err.is_h3_no_error() {
                    error!("Http3 client driver error: {:?}", err);
                }
                let _ = inner_clone
                    .connection_handle
                    .swap((None, Tag::None), AcqRel);
            });

            let _ = self
                .inner
                .connection_handle
                .swap((Some(Owned::new(new_handle.clone())), Tag::None), AcqRel);

            Ok(new_handle)
        } else {
            let handle = opt_handle
                .ok_or_else(|| anyhow::anyhow!("No existing request handle available"))?;
            Ok(handle)
        }
    }

    pub async fn get(
        &self,
        url: &str,
        timeout_duration: Option<Duration>,
    ) -> Result<(StatusCode, Bytes)> {
        let mut send_req = self.get_connection_handle().await?;
        let uri = url.parse::<http::uri::Uri>()?;
        let request = http::Request::builder().method("GET").uri(&uri).body(())?;

        let fut = async {
            let mut stream = send_req.send_request(request).await?;
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
        &self,
        url: &str,
        data: Bytes,
        timeout_duration: Option<Duration>,
    ) -> Result<(StatusCode, Bytes)> {
        self.post_with_headers(url, data, None::<&[(String, String)]>, timeout_duration)
            .await
    }

    pub async fn post_with_headers(
        &self,
        url: &str,
        data: Bytes,
        extra_headers: Option<&[(String, String)]>,
        timeout_duration: Option<Duration>,
    ) -> Result<(StatusCode, Bytes)> {
        let mut send_req = self.get_connection_handle().await?;
        let uri = url.parse::<http::uri::Uri>()?;
        let mut builder = http::Request::builder().method("POST").uri(&uri);

        builder = builder.header("content-type", "application/json");
        if let Some(headers) = extra_headers {
            for (name, value) in headers.iter() {
                builder = builder.header(name.as_str(), value.as_str());
            }
        }
        builder = builder.header("content-length", data.len());

        let request = builder.body(())?;

        let fut = async {
            let mut stream = send_req.send_request(request).await?;
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
