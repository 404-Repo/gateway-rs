// TODO: Remove this after the code is finalized
#![allow(dead_code)]

use std::fmt;
use std::sync::Arc;

use anyhow::Result;
use openraft::raft::{AppendEntriesRequest, InstallSnapshotRequest, VoteRequest};
use openraft::raft::{AppendEntriesResponse, InstallSnapshotResponse, VoteResponse};
use quinn::{Connection, RecvStream, SendStream};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;
use tokio::sync::RwLock;

use crate::raft::NodeId;
use crate::raft::TypeConfig;

pub const MAX_MESSAGE_SIZE: usize = 64 * 1024; // 64KB default limit

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "msg_type", content = "data")]
pub enum RaftMessageType {
    AppendEntriesRequest(AppendEntriesRequest<TypeConfig>),
    InstallSnapshotRequest(InstallSnapshotRequest<TypeConfig>),
    VoteRequest(VoteRequest<NodeId>),
    AppendEntriesResponse(AppendEntriesResponse<u64>),
    InstallSnapshotResponse(InstallSnapshotResponse<u64>),
    VoteResponse(VoteResponse<u64>),
}

impl fmt::Display for RaftMessageType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RaftMessageType::AppendEntriesRequest(_) => {
                write!(f, "AppendEntriesRequest")
            }
            RaftMessageType::InstallSnapshotRequest(_) => {
                write!(f, "InstallSnapshotRequest")
            }
            RaftMessageType::VoteRequest(_) => {
                write!(f, "VoteRequest")
            }
            RaftMessageType::AppendEntriesResponse(_) => {
                write!(f, "AppendEntriesResponse")
            }
            RaftMessageType::InstallSnapshotResponse(_) => {
                write!(f, "InstallSnapshotResponse")
            }
            RaftMessageType::VoteResponse(_) => {
                write!(f, "VoteResponse")
            }
        }
    }
}

impl RaftMessageType {
    pub fn into_append_entries_response(self) -> AppendEntriesResponse<u64> {
        match self {
            RaftMessageType::AppendEntriesResponse(resp) => resp,
            other => panic!("Expected AppendEntriesResponse, got: {:?}", other),
        }
    }

    pub fn into_install_snapshot_response(self) -> InstallSnapshotResponse<u64> {
        match self {
            RaftMessageType::InstallSnapshotResponse(resp) => resp,
            other => panic!("Expected InstallSnapshotResponse, got: {:?}", other),
        }
    }

    pub fn into_vote_response(self) -> VoteResponse<u64> {
        match self {
            RaftMessageType::VoteResponse(resp) => resp,
            other => panic!("Expected VoteResponse, got: {:?}", other),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Protocol {
    connection: Connection,
}

impl Protocol {
    pub fn new(connection: Connection) -> Self {
        Self { connection }
    }

    pub async fn send_message(&self, mut send: SendStream, message: RaftMessageType) -> Result<()> {
        let serialized = rmp_serde::to_vec_named(&message)?;

        if serialized.len() > MAX_MESSAGE_SIZE {
            return Err(anyhow::anyhow!(
                "Message size {} exceeds maximum allowed size of {}",
                serialized.len(),
                MAX_MESSAGE_SIZE
            ));
        }

        send.write_all(&serialized).await?;
        send.flush().await?;
        send.finish()?;
        let _ = tokio::time::timeout(tokio::time::Duration::from_millis(300), send.stopped()).await;

        Ok(())
    }

    pub async fn receive_message(mut recv_stream: RecvStream) -> Result<RaftMessageType> {
        let mut message_data = Vec::new();
        let mut buffer = vec![0; 8192];

        let timeout_duration = tokio::time::Duration::from_secs(3);
        let read_result = tokio::time::timeout(timeout_duration, async {
            while let Some(n) = recv_stream.read(&mut buffer).await? {
                message_data.extend_from_slice(&buffer[..n]);
                if message_data.len() > MAX_MESSAGE_SIZE {
                    return Err(anyhow::anyhow!(
                        "Message exceeded maximum size limit of {} bytes",
                        MAX_MESSAGE_SIZE
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
        message: RaftMessageType,
        raft: Arc<RwLock<openraft::Raft<TypeConfig>>>,
    ) -> Result<RaftMessageType> {
        let raft = raft.write().await;
        match message {
            RaftMessageType::AppendEntriesRequest(req) => {
                let resp = raft.append_entries(req).await?;
                Ok(RaftMessageType::AppendEntriesResponse(resp))
            }
            RaftMessageType::InstallSnapshotRequest(req) => {
                let resp = raft.install_snapshot(req).await?;
                Ok(RaftMessageType::InstallSnapshotResponse(resp))
            }
            RaftMessageType::VoteRequest(req) => {
                let resp = raft.vote(req).await?;
                Ok(RaftMessageType::VoteResponse(resp))
            }
            _ => Err(anyhow::anyhow!("Unexpected message type")),
        }
    }
}
