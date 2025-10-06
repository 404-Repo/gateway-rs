use anyhow::{anyhow, Result};
use backon::{BackoffBuilder, ConstantBuilder, Retryable};
use quinn::{Connection, Endpoint, IdleTimeout};
use rustls_platform_verifier::BuilderVerifierExt;
use sdd::{AtomicOwned, Guard, Owned, Tag};
use std::net::SocketAddr;
use std::sync::atomic::Ordering::{AcqRel, Acquire};
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info};

use crate::{
    common::cert::SkipServerVerification,
    config::RServerConfig,
    protocol::{Protocol, RaftMessageType},
};

const DEFAULT_MAX_IDLE_TIMEOUT_SEC: u64 = 4;
const DEFAULT_KEEP_ALIVE_INTERVAL_SEC: u64 = 1;
const CONNECT_RETRY_ATTEMPTS: usize = 3;
const CONNECT_RETRY_DELAY_MS: u64 = 3;

#[derive(Debug, Clone)]
pub struct RClientBuilder {
    remote_addr: Option<String>,
    server_name: Option<String>,
    local_bind_addr: Option<String>,
    dangerous_skip_verification: bool,
    max_idle_timeout_sec: Option<u64>,
    keep_alive_interval: Option<u64>,
    protocol_cfg: Option<RServerConfig>,
}

async fn connect_with_retry(
    endpoint: &Endpoint,
    remote_addr: SocketAddr,
    server_name: &str,
) -> Result<Connection> {
    let backoff = ConstantBuilder::new()
        .with_delay(Duration::from_millis(CONNECT_RETRY_DELAY_MS))
        .with_max_times(CONNECT_RETRY_ATTEMPTS)
        .build();

    (|| async {
        let connecting = endpoint
            .connect(remote_addr, server_name)
            .map_err(|e| anyhow!("Failed to start connection to {remote_addr}: {e}"))?;
        connecting
            .await
            .map_err(|e| anyhow!("Failed to establish connection to {remote_addr}: {e}"))
    })
    .retry(backoff)
    .await
}

impl RClientBuilder {
    pub fn new() -> Self {
        Self {
            remote_addr: None,
            server_name: None,
            local_bind_addr: None,
            dangerous_skip_verification: false,
            max_idle_timeout_sec: None,
            keep_alive_interval: None,
            protocol_cfg: None,
        }
    }

    pub fn remote_addr(mut self, addr: impl Into<String>) -> Self {
        self.remote_addr = Some(addr.into());
        self
    }

    pub fn server_name(mut self, name: impl Into<String>) -> Self {
        self.server_name = Some(name.into());
        self
    }

    pub fn local_bind_addr(mut self, addr: impl Into<String>) -> Self {
        self.local_bind_addr = Some(addr.into());
        self
    }

    pub fn dangerous_skip_verification(mut self, skip: bool) -> Self {
        self.dangerous_skip_verification = skip;
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

    pub fn protocol_cfg(mut self, cfg: RServerConfig) -> Self {
        self.protocol_cfg = Some(cfg);
        self
    }

    pub async fn build(self) -> Result<RClient> {
        let remote_addr = self
            .remote_addr
            .ok_or_else(|| anyhow!("`remote_addr` must be set in RClientBuilder"))?;
        let server_name = self
            .server_name
            .ok_or_else(|| anyhow!("`server_name` must be set in RClientBuilder"))?;
        let local_bind_addr = self
            .local_bind_addr
            .ok_or_else(|| anyhow!("`local_bind_addr` must be set in RClientBuilder"))?;
        let protocol_cfg = self
            .protocol_cfg
            .ok_or_else(|| anyhow!("`protocol_cfg` must be set in RClientBuilder"))?;

        RClient::new_inner(
            &remote_addr,
            &server_name,
            &local_bind_addr,
            self.dangerous_skip_verification,
            self.max_idle_timeout_sec,
            self.keep_alive_interval,
            protocol_cfg,
        )
        .await
    }
}

#[derive(Debug)]
struct RClientInner {
    endpoint: AtomicOwned<Endpoint>,
    connection: AtomicOwned<Connection>,
}

#[derive(Debug, Clone)]
pub struct RClient {
    inner: Arc<RClientInner>,
    server_name: String,
    remote_addr: SocketAddr,
    protocol_cfg: RServerConfig,
    local_bind_addr: SocketAddr,
    client_cfg: quinn::ClientConfig,
}

impl RClient {
    async fn new_inner(
        remote_addr: &str,
        server_name: &str,
        local_bind_addr: &str,
        dangerous_skip_verification: bool,
        max_idle_timeout_sec: Option<u64>,
        keep_alive_interval: Option<u64>,
        protocol_cfg: RServerConfig,
    ) -> Result<Self> {
        let remote_addr: SocketAddr = remote_addr.parse()?;
        let local_bind_addr: SocketAddr = local_bind_addr.parse()?;

        let mut crypto = if dangerous_skip_verification {
            rustls::ClientConfig::builder()
                .dangerous()
                .with_custom_certificate_verifier(SkipServerVerification::new())
                .with_no_client_auth()
        } else {
            let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
            rustls::ClientConfig::builder_with_provider(provider)
                .with_protocol_versions(&[&rustls::version::TLS13])?
                .with_platform_verifier()?
                .with_no_client_auth()
        };
        crypto.alpn_protocols = vec![b"h3".to_vec()];

        let mut client_cfg = quinn::ClientConfig::new(Arc::new(
            quinn::crypto::rustls::QuicClientConfig::try_from(crypto)?,
        ));
        let timeout = IdleTimeout::try_from(Duration::from_secs(
            max_idle_timeout_sec.unwrap_or(DEFAULT_MAX_IDLE_TIMEOUT_SEC),
        ))?;
        let mut transport = quinn::TransportConfig::default();
        transport.keep_alive_interval(Some(Duration::from_secs(
            keep_alive_interval.unwrap_or(DEFAULT_KEEP_ALIVE_INTERVAL_SEC),
        )));
        transport.max_idle_timeout(Some(timeout));
        client_cfg.transport_config(Arc::new(transport));

        let mut endpoint = Endpoint::client(local_bind_addr)?;
        endpoint.set_default_client_config(client_cfg.clone());

        let conn = connect_with_retry(&endpoint, remote_addr, server_name).await?;
        info!(
            "Successfully established connection to {} with id: {}",
            remote_addr,
            conn.stable_id(),
        );

        let inner = RClientInner {
            endpoint: AtomicOwned::new(endpoint),
            connection: AtomicOwned::new(conn),
        };

        Ok(Self {
            inner: Arc::new(inner),
            server_name: server_name.to_string(),
            remote_addr,
            protocol_cfg,
            local_bind_addr,
            client_cfg,
        })
    }

    #[allow(dead_code)]
    pub fn stable_id(&self) -> usize {
        let guard = Guard::new();
        self.inner
            .connection
            .load(Acquire, &guard)
            .as_ref()
            .map(|c| c.stable_id())
            .unwrap_or(0)
    }

    async fn get_connection(&self) -> Result<Connection> {
        let maybe_conn = {
            let guard = Guard::new();
            self.inner
                .connection
                .load(Acquire, &guard)
                .as_ref()
                .map(|c| c.clone())
        };
        if let Some(conn) = maybe_conn {
            if conn.close_reason().is_some() {
                return self.reconnect().await;
            }
            Ok(conn)
        } else {
            self.reconnect().await
        }
    }

    async fn reconnect(&self) -> Result<Connection> {
        let _ = self.inner.endpoint.swap((None, Tag::None), AcqRel);

        let mut fresh_endpoint = Endpoint::client(self.local_bind_addr)
            .map_err(|e| anyhow!("Failed to bind new endpoint: {e}"))?;
        fresh_endpoint.set_default_client_config(self.client_cfg.clone());

        let _ = self.inner.endpoint.swap(
            (Some(Owned::new(fresh_endpoint.clone())), Tag::None),
            AcqRel,
        );

        let ep_loaded = {
            let guard = Guard::new();
            self.inner
                .endpoint
                .load(Acquire, &guard)
                .as_ref()
                .cloned()
                .ok_or_else(|| anyhow!("Endpoint vanished unexpectedly after swap"))?
        };

        let connecting = ep_loaded
            .connect(self.remote_addr, &self.server_name)
            .map_err(|e| anyhow!("Failed to start connection to {}: {}", self.remote_addr, e))?;
        let new_conn = connecting.await.map_err(|e| {
            anyhow!(
                "Failed to establish connection to {}: {}",
                self.remote_addr,
                e
            )
        })?;

        info!(
            "RClient reconnected successfully to {} with id {}",
            self.remote_addr,
            new_conn.stable_id(),
        );

        let _ = self
            .inner
            .connection
            .swap((Some(Owned::new(new_conn.clone())), Tag::None), AcqRel);

        Ok(new_conn)
    }

    pub async fn send<T>(&self, data: T) -> Result<RaftMessageType>
    where
        T: Into<RaftMessageType>,
    {
        let mut conn = self.get_connection().await?;

        // Try opening the stream once, on error, reconnect and open it again
        let (send_stream, recv_stream) = match conn.open_bi().await {
            Ok(streams) => streams,
            Err(e) => {
                error!("Failed to open bidirectional stream : {e}");
                conn = self.reconnect().await?;
                conn.open_bi().await?
            }
        };

        let proto = Protocol::new(
            conn.clone(),
            self.protocol_cfg.max_message_size,
            self.protocol_cfg.max_recv_buffer_size,
            Duration::from_millis(self.protocol_cfg.receive_message_timeout_ms),
        );
        let msg: RaftMessageType = data.into();
        proto.send_message(send_stream, msg).await?;
        let resp = proto.receive_message(recv_stream).await?;
        Ok(resp)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::RServerConfig;
    use crate::protocol::RaftMessageType;
    use crate::raft::server::RServer;
    use crate::raft::{LogStore, Network, StateMachineStore, TypeConfig};
    use anyhow::Result;
    use foldhash::fast::RandomState;
    use openraft::Config;
    use std::sync::{Arc, Once};
    use tokio_util::sync::CancellationToken;

    static CRYPTO_INIT: Once = Once::new();

    fn setup_crypto() {
        CRYPTO_INIT.call_once(|| {
            crate::raft::init_crypto_provider().unwrap();
        });
    }

    async fn create_test_raft_node(
        node_id: u64,
    ) -> Result<(openraft::Raft<TypeConfig>, Arc<StateMachineStore>)> {
        setup_crypto();
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
            state_machine_store.clone(),
        )
        .await?;

        Ok((raft, state_machine_store))
    }

    #[tokio::test]
    async fn test_rclient_connection() -> Result<()> {
        let (raft, _) = create_test_raft_node(1).await?;
        let pcfg = RServerConfig::default();

        let server_addr = "127.0.0.1:4446";
        let _server = RServer::new(
            server_addr,
            None,
            raft,
            pcfg.clone(),
            CancellationToken::new(),
        )
        .await?;

        // Give the server some time to start
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let client = RClientBuilder::new()
            .remote_addr(server_addr)
            .server_name("localhost")
            .local_bind_addr("127.0.0.1:0")
            .dangerous_skip_verification(true)
            .protocol_cfg(pcfg)
            .build()
            .await?;

        assert!(client.stable_id() > 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_rclient_send_and_receive() -> Result<()> {
        let (raft, _) = create_test_raft_node(1).await?;
        let pcfg = RServerConfig::default();

        let server_addr = "127.0.0.1:4447";
        let _server = RServer::new(
            server_addr,
            None,
            raft,
            pcfg.clone(),
            CancellationToken::new(),
        )
        .await?;

        // Give the server some time to start
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let client = RClientBuilder::new()
            .remote_addr(server_addr)
            .server_name("localhost")
            .local_bind_addr("127.0.0.1:0")
            .dangerous_skip_verification(true)
            .protocol_cfg(pcfg)
            .build()
            .await?;

        let message_to_send = RaftMessageType::VoteRequest(openraft::raft::VoteRequest {
            vote: openraft::Vote::new(1, 1),
            last_log_id: None,
        });

        let response = client.send(message_to_send).await?;

        assert!(matches!(response, RaftMessageType::VoteResponse(..)));

        Ok(())
    }

    #[tokio::test]
    async fn test_rclient_multiple_streams() -> Result<()> {
        let (raft, _) = create_test_raft_node(1).await?;
        let pcfg = RServerConfig::default();

        let server_addr = "127.0.0.1:4448";
        let _server = RServer::new(
            server_addr,
            None,
            raft,
            pcfg.clone(),
            CancellationToken::new(),
        )
        .await?;

        // Give the server some time to start
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let client = RClientBuilder::new()
            .remote_addr(server_addr)
            .server_name("localhost")
            .local_bind_addr("127.0.0.1:0")
            .dangerous_skip_verification(true)
            .protocol_cfg(pcfg)
            .build()
            .await?;

        let mut handles = Vec::new();

        for i in 0..10 {
            let client_clone = client.clone();
            let handle = tokio::spawn(async move {
                let message_to_send = RaftMessageType::VoteRequest(openraft::raft::VoteRequest {
                    vote: openraft::Vote::new(i, i),
                    last_log_id: None,
                });
                client_clone.send(message_to_send).await
            });
            handles.push(handle);
        }

        let mut results = Vec::new();
        for handle in handles {
            results.push(handle.await?);
        }

        assert_eq!(results.len(), 10);
        for result in results {
            assert!(result.is_ok());
            assert!(matches!(result.unwrap(), RaftMessageType::VoteResponse(..)));
        }

        Ok(())
    }
}
