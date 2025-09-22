use std::fmt;
use std::sync::Arc;

use foldhash::fast::RandomState;

use openraft::anyerror;
use openraft::error::InstallSnapshotError;
use openraft::error::NetworkError;
use openraft::error::Unreachable;
use openraft::network::RPCOption;
use openraft::network::RaftNetwork;
use openraft::network::RaftNetworkFactory;
use openraft::raft::AppendEntriesRequest;
use openraft::raft::AppendEntriesResponse;
use openraft::raft::InstallSnapshotRequest;
use openraft::raft::InstallSnapshotResponse;
use openraft::raft::VoteRequest;
use openraft::raft::VoteResponse;
use openraft::BasicNode;
use serde::de::DeserializeOwned;
use std::error::Error as StdError;

use super::NodeId;
use crate::protocol::ConversionError;
use crate::protocol::RaftMessageType;
use crate::raft::client::RClient;
use crate::raft::typ;
use crate::raft::TypeConfig;

#[derive(Debug)]
struct NetworkStringError(String);

impl fmt::Display for NetworkStringError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl StdError for NetworkStringError {}

#[derive(Debug, Clone)]
pub struct Network {
    pub clients: Arc<scc::HashMap<String, RClient, RandomState>>,
}

impl Network {
    pub fn new(clients: Arc<scc::HashMap<String, RClient, RandomState>>) -> Network {
        Network { clients }
    }

    fn get_client(&self, target_node: &BasicNode) -> Option<RClient> {
        self.clients
            .get_sync(&target_node.addr)
            .map(|ref_| (*ref_).clone())
    }

    pub async fn send_rpc<Err>(
        &self,
        _target: NodeId,
        target_node: &BasicNode,
        message: RaftMessageType,
    ) -> Result<RaftMessageType, openraft::error::RPCError<NodeId, BasicNode, Err>>
    where
        Err: StdError + DeserializeOwned,
    {
        let client = self.get_client(target_node).ok_or_else(|| {
            let err = NetworkStringError(format!(
                "No client found for target node address: {}",
                target_node.addr
            ));
            openraft::error::RPCError::Unreachable(Unreachable::new(&err))
        })?;

        let r: RaftMessageType = client.send::<RaftMessageType>(message).await.map_err(|e| {
            let err = NetworkStringError(e.to_string());
            if e.downcast_ref::<quinn::ConnectionError>().is_some() {
                openraft::error::RPCError::Unreachable(Unreachable::new(&err))
            } else {
                openraft::error::RPCError::Network(NetworkError::new(&err))
            }
        })?;
        Ok(r)
    }
}

impl RaftNetworkFactory<TypeConfig> for Network {
    type Network = NetworkConnection;

    async fn new_client(&mut self, target: NodeId, node: &BasicNode) -> Self::Network {
        NetworkConnection {
            owner: self.clone(),
            target,
            target_node: node.clone(),
        }
    }
}

#[derive(Clone)]
pub struct NetworkConnection {
    owner: Network,
    target: NodeId,
    target_node: BasicNode,
}

impl RaftNetwork<TypeConfig> for NetworkConnection {
    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, typ::RPCError> {
        let resp: RaftMessageType = self
            .owner
            .send_rpc(
                self.target,
                &self.target_node,
                RaftMessageType::AppendEntriesRequest(req),
            )
            .await?;

        let message = resp.try_into().map_err(|e: ConversionError| {
            typ::RPCError::Network(openraft::error::NetworkError::from(
                anyerror::AnyError::new(&e),
            ))
        })?;
        Ok(message)
    }

    async fn install_snapshot(
        &mut self,
        req: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<InstallSnapshotResponse<NodeId>, typ::RPCError<InstallSnapshotError>> {
        let resp: RaftMessageType = self
            .owner
            .send_rpc(
                self.target,
                &self.target_node,
                RaftMessageType::InstallSnapshotRequest(req),
            )
            .await?;

        let message = resp.try_into().map_err(|e: ConversionError| {
            typ::RPCError::Network(openraft::error::NetworkError::from(
                anyerror::AnyError::new(&e),
            ))
        })?;
        Ok(message)
    }

    async fn vote(
        &mut self,
        req: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, typ::RPCError> {
        let resp: RaftMessageType = self
            .owner
            .send_rpc(
                self.target,
                &self.target_node,
                RaftMessageType::VoteRequest(req),
            )
            .await?;

        let message = resp.try_into().map_err(|e: ConversionError| {
            typ::RPCError::Network(openraft::error::NetworkError::from(
                anyerror::AnyError::new(&e),
            ))
        })?;
        Ok(message)
    }
}
