// TODO: Remove this after the code is finalized
#![allow(dead_code)]

use std::fmt;
use std::sync::Arc;

use dashmap::DashMap;
use openraft::error::InstallSnapshotError;
use openraft::error::NetworkError;
use openraft::error::RemoteError;
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
    pub clients: Arc<DashMap<String, RClient>>,
}

impl Network {
    pub fn new(clients: Arc<DashMap<String, RClient>>) -> Network {
        Network { clients }
    }

    fn get_client(&self, target_node: &BasicNode) -> Option<RClient> {
        self.clients
            .get(&target_node.addr)
            .map(|ref_| (*ref_).clone())
    }

    pub async fn send_rpc<Resp, Err>(
        &self,
        target: NodeId,
        target_node: &BasicNode,
        message: RaftMessageType,
    ) -> Result<Resp, openraft::error::RPCError<NodeId, BasicNode, Err>>
    where
        Err: StdError + DeserializeOwned,
        Resp: DeserializeOwned,
    {
        let client = self.get_client(target_node).ok_or_else(|| {
            let err = NetworkStringError(format!(
                "No client found for target node address: {}",
                target_node.addr
            ));
            openraft::error::RPCError::Unreachable(Unreachable::new(&err))
        })?;

        client
            .send::<RaftMessageType, Result<Resp, Err>>(&message)
            .await
            .map_err(|e| {
                let err = NetworkStringError(e.to_string());
                if e.downcast_ref::<quinn::ConnectionError>().is_some() {
                    openraft::error::RPCError::Unreachable(Unreachable::new(&err))
                } else {
                    openraft::error::RPCError::Network(NetworkError::new(&err))
                }
            })?
            .map_err(|e| openraft::error::RPCError::RemoteError(RemoteError::new(target, e)))
    }
}

impl RaftNetworkFactory<TypeConfig> for Network {
    type Network = NetworkConnection;

    async fn new_client(&mut self, target: NodeId, node: &BasicNode) -> Self::Network {
        tracing::info!("CREATING A NEW Network for {node}");
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
        tracing::info!("Sending AppendEntriesRequest...");
        let resp: RaftMessageType = self
            .owner
            .send_rpc(
                self.target,
                &self.target_node,
                RaftMessageType::AppendEntriesRequest(req),
            )
            .await?;
        tracing::info!("Got response {:?}", resp);
        Ok(resp.into_append_entries_response())
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
        Ok(resp.into_install_snapshot_response())
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
        Ok(resp.into_vote_response())
    }
}
