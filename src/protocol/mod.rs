use std::convert::TryFrom;
use std::error::Error;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use quinn::{Connection, RecvStream, SendStream};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;
use tokio::sync::RwLock;

use crate::raft::NodeId;
use crate::raft::Raft;
use crate::raft::TypeConfig;

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "msg_type", content = "data")]
pub enum RaftMessageType {
    AppendEntriesRequest(AppendEntriesRequest<TypeConfig>),
    InstallSnapshotRequest(InstallSnapshotRequest<TypeConfig>),
    VoteRequest(VoteRequest<NodeId>),
    AppendEntriesResponse(AppendEntriesResponse<NodeId>),
    InstallSnapshotResponse(InstallSnapshotResponse<NodeId>),
    VoteResponse(VoteResponse<NodeId>),
}

impl fmt::Display for RaftMessageType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RaftMessageType::AppendEntriesRequest(_) => write!(f, "AppendEntriesRequest"),
            RaftMessageType::InstallSnapshotRequest(_) => write!(f, "InstallSnapshotRequest"),
            RaftMessageType::VoteRequest(_) => write!(f, "VoteRequest"),
            RaftMessageType::AppendEntriesResponse(_) => write!(f, "AppendEntriesResponse"),
            RaftMessageType::InstallSnapshotResponse(_) => write!(f, "InstallSnapshotResponse"),
            RaftMessageType::VoteResponse(_) => write!(f, "VoteResponse"),
        }
    }
}

#[derive(Debug)]
pub struct ConversionError(String);

impl fmt::Display for ConversionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Error for ConversionError {}

impl TryFrom<RaftMessageType> for AppendEntriesResponse<u64> {
    type Error = ConversionError;

    fn try_from(message: RaftMessageType) -> Result<Self, Self::Error> {
        match message {
            RaftMessageType::AppendEntriesResponse(resp) => Ok(resp),
            other => Err(ConversionError(format!(
                "Expected AppendEntriesResponse, got: {:?}",
                other
            ))),
        }
    }
}

impl TryFrom<RaftMessageType> for InstallSnapshotResponse<u64> {
    type Error = ConversionError;

    fn try_from(message: RaftMessageType) -> Result<Self, Self::Error> {
        match message {
            RaftMessageType::InstallSnapshotResponse(resp) => Ok(resp),
            other => Err(ConversionError(format!(
                "Expected InstallSnapshotResponse, got: {:?}",
                other
            ))),
        }
    }
}

impl TryFrom<RaftMessageType> for VoteResponse<u64> {
    type Error = ConversionError;

    fn try_from(message: RaftMessageType) -> Result<Self, Self::Error> {
        match message {
            RaftMessageType::VoteResponse(resp) => Ok(resp),
            other => Err(ConversionError(format!(
                "Expected VoteResponse, got: {:?}",
                other
            ))),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Protocol {
    _connection: Connection,
    max_message_size: usize,
    send_timeout_ms: Duration,
    receive_message_timeout_ms: Duration,
}

impl Protocol {
    pub fn new(
        connection: Connection,
        max_message_size: usize,
        send_timeout_ms: Duration,
        receive_message_timeout_ms: Duration,
    ) -> Self {
        Self {
            _connection: connection,
            max_message_size,
            send_timeout_ms,
            receive_message_timeout_ms,
        }
    }

    pub async fn send_message(&self, mut send: SendStream, message: RaftMessageType) -> Result<()> {
        let serialized = rmp_serde::to_vec_named(&message)?;

        if serialized.len() > self.max_message_size {
            return Err(anyhow::anyhow!(
                "Message size {} exceeds maximum allowed size of {}",
                serialized.len(),
                self.max_message_size
            ));
        }

        send.write_all(&serialized).await?;
        send.flush().await?;
        send.finish()?;

        let _ = tokio::time::timeout(self.send_timeout_ms, send.stopped()).await;
        Ok(())
    }

    pub async fn receive_message(&self, mut recv_stream: RecvStream) -> Result<RaftMessageType> {
        let mut message_data = Vec::new();
        let mut buffer = vec![0; 8192];

        let read_result = tokio::time::timeout(self.receive_message_timeout_ms, async {
            while let Some(n) = recv_stream.read(&mut buffer).await? {
                message_data.extend_from_slice(&buffer[..n]);
                if message_data.len() > self.max_message_size {
                    return Err(anyhow::anyhow!(
                        "Message exceeded maximum size limit of {} bytes",
                        self.max_message_size
                    ));
                }
            }
            Ok(message_data)
        })
        .await?;

        let message_data = read_result?;
        if message_data.is_empty() {
            return Err(anyhow::anyhow!("Received empty message"));
        }

        let message: RaftMessageType = rmp_serde::from_slice(&message_data)
            .map_err(|e| anyhow::anyhow!("Deserialization error: {:?}", e))?;

        Ok(message)
    }

    pub async fn handle_message(
        &self,
        message: RaftMessageType,
        raft: Arc<RwLock<Raft>>,
    ) -> Result<RaftMessageType> {
        match message {
            RaftMessageType::AppendEntriesRequest(req) => Ok(raft
                .write()
                .await
                .append_entries(req)
                .await
                .map(RaftMessageType::AppendEntriesResponse)?),
            RaftMessageType::InstallSnapshotRequest(req) => Ok(raft
                .write()
                .await
                .install_snapshot(req)
                .await
                .map(RaftMessageType::InstallSnapshotResponse)?),
            RaftMessageType::VoteRequest(req) => Ok(raft
                .write()
                .await
                .vote(req)
                .await
                .map(RaftMessageType::VoteResponse)?),
            _ => Err(anyhow::anyhow!("Unexpected message type")),
        }
    }
}
