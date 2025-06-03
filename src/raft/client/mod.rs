use anyhow::{anyhow, Result};
use quinn::{Connection, Endpoint, IdleTimeout};
use rustls_platform_verifier::BuilderVerifierExt;
use sdd::{AtomicOwned, Guard, Owned, Tag};
use std::net::SocketAddr;
use std::sync::atomic::Ordering::{AcqRel, Acquire};
use std::sync::Arc;
use std::time::Duration;
use tracing::info;

use crate::{
    common::cert::SkipServerVerification,
    config::RServerConfig,
    protocol::{Protocol, RaftMessageType},
};

const DEFAULT_MAX_IDLE_TIMEOUT_SEC: u64 = 4;
const DEFAULT_KEEP_ALIVE_INTERVAL_SEC: u64 = 1;

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

        let conn = endpoint
            .connect(remote_addr, server_name)?
            .await
            .map_err(|e| anyhow!("Failed to connect: {e}"))?;
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
        {
            let old_pair = self.inner.endpoint.swap((None, Tag::None), AcqRel);
            if let Some(e) = old_pair.0 {
                drop(e)
            }
        }

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

        let new_conn = ep_loaded
            .connect(self.remote_addr, &self.server_name)
            .map_err(|e| anyhow!("Failed to connect to {}: {e}", self.remote_addr))?
            .await
            .map_err(|e| anyhow!("Async connect to {} failed: {e}", self.remote_addr))?;

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
            Err(_) => {
                conn = self.reconnect().await?;
                conn.open_bi().await?
            }
        };

        let proto = Protocol::new(
            conn.clone(),
            self.protocol_cfg.max_message_size,
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
    use crate::common::crypto_provider::init_crypto_provider;
    use crate::config::RServerConfig;
    use crate::protocol::RaftMessageType;
    use crate::raft::network::Network;
    use crate::raft::server::RServer;
    use crate::raft::{LogStore, StateMachineStore, TypeConfig};
    use foldhash::fast::RandomState;
    use openraft::raft::AppendEntriesRequest;
    use openraft::{Config, LeaderId, Vote};
    use std::time::Duration;
    use tokio::sync::RwLock;

    async fn create_test_raft_node(
        node_id: u64,
    ) -> Result<Arc<RwLock<openraft::Raft<TypeConfig>>>> {
        init_crypto_provider().unwrap();

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

        let raft =
            openraft::Raft::new(node_id, config, network, log_store, state_machine_store).await?;

        Ok(Arc::new(RwLock::new(raft)))
    }

    #[tokio::test]
    async fn test_client_connection() -> Result<()> {
        init_crypto_provider().unwrap();

        let raft = create_test_raft_node(1).await?;
        let pcfg = RServerConfig::default();
        let server_addr = "127.0.0.1:8888";

        let _server = RServer::new(server_addr, None, raft, pcfg.clone()).await?;

        tokio::time::sleep(Duration::from_millis(100)).await;

        let client_addr = "127.0.0.1:8889";
        let client = RClientBuilder::new()
            .remote_addr(server_addr)
            .server_name("localhost")
            .local_bind_addr(client_addr)
            .dangerous_skip_verification(true)
            .protocol_cfg(pcfg)
            .build()
            .await?;

        assert!(client.stable_id() > 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_send_data() -> Result<()> {
        init_crypto_provider().unwrap();

        let raft = create_test_raft_node(1).await?;
        let pcfg = RServerConfig::default();
        let server_addr = "127.0.0.1:7777";

        let _server = RServer::new(server_addr, None, raft, pcfg.clone()).await?;

        println!("Starting server on {}", server_addr);

        tokio::time::sleep(Duration::from_millis(200)).await;

        println!("Creating client");
        let client_addr = "127.0.0.1:7778";
        let client = RClientBuilder::new()
            .remote_addr(server_addr)
            .server_name("localhost")
            .local_bind_addr(client_addr)
            .dangerous_skip_verification(true)
            .protocol_cfg(pcfg.clone())
            .build()
            .await?;

        println!("Testing initial connection");
        assert!(client.stable_id() > 0);

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

        let response = client.send(append_entries).await?;

        match response {
            RaftMessageType::AppendEntriesResponse(aer) => {
                println!("Received AppendEntriesResponse {}", aer);
                Ok(())
            }
            _ => {
                println!("Unexpected response type: {:?}", response);
                Err(anyhow::anyhow!("Unexpected response type: {:?}", response))
            }
        }?;

        tokio::time::sleep(Duration::from_millis(100)).await;

        Ok(())
    }

    #[tokio::test]
    async fn test_connection_failure() -> Result<()> {
        init_crypto_provider().unwrap();

        let pcfg = RServerConfig::default();
        let result = RClientBuilder::new()
            .remote_addr("127.0.0.1:9999")
            .server_name("localhost")
            .local_bind_addr("127.0.0.1:9998")
            .dangerous_skip_verification(true)
            .protocol_cfg(pcfg)
            .build()
            .await;
        assert!(result.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn test_reconnect() -> Result<()> {
        init_crypto_provider().unwrap();

        let raft = create_test_raft_node(1).await?;
        let pcfg = RServerConfig::default();
        let server_addr = "127.0.0.1:6666";
        let _server = RServer::new(server_addr, None, raft, pcfg.clone()).await?;

        tokio::time::sleep(Duration::from_millis(100)).await;

        let client_addr = "127.0.0.1:0";
        let client = RClientBuilder::new()
            .remote_addr(server_addr)
            .server_name("localhost")
            .local_bind_addr(client_addr)
            .dangerous_skip_verification(true)
            .protocol_cfg(pcfg.clone())
            .build()
            .await?;

        // Helper to build a fresh AppendEntries request each time
        fn make_append_entries() -> RaftMessageType {
            RaftMessageType::AppendEntriesRequest(AppendEntriesRequest {
                vote: Vote {
                    leader_id: LeaderId {
                        term: 1,
                        node_id: 1,
                    },
                    committed: true,
                },
                prev_log_id: None,
                entries: Vec::new(),
                leader_commit: None,
            })
        }

        // Verify initial connection
        let response = client.send(make_append_entries()).await?;
        assert!(matches!(
            response,
            RaftMessageType::AppendEntriesResponse(_)
        ));

        let initial_stable_id = client.stable_id();
        assert!(initial_stable_id > 0);

        // Close connection to force a reconnect
        {
            let guard = Guard::new();
            if let Some(conn) = client.inner.connection.load(Acquire, &guard).as_ref() {
                conn.close(0u32.into(), b"test close");
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Send again — should reconnect under the hood
        let response = client.send(make_append_entries()).await?;
        assert!(matches!(
            response,
            RaftMessageType::AppendEntriesResponse(_)
        ));

        // Ensure we got a new connection
        let new_stable_id = client.stable_id();
        assert!(new_stable_id > 0);
        assert_ne!(initial_stable_id, new_stable_id);

        // Final sanity check on the new connection
        let response = client.send(make_append_entries()).await?;
        assert!(matches!(
            response,
            RaftMessageType::AppendEntriesResponse(_)
        ));

        Ok(())
    }
}
