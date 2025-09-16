use anyhow::{anyhow, Result};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use quinn::{Connection, RecvStream, SendStream};
use serde::{Deserialize, Serialize};
use std::{fmt, time::Duration};
use tokio::io::AsyncWriteExt;

use crate::raft::{NodeId, Raft, TypeConfig};

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
        write!(
            f,
            "{}",
            match self {
                Self::AppendEntriesRequest(_) => "AppendEntriesRequest",
                Self::InstallSnapshotRequest(_) => "InstallSnapshotRequest",
                Self::VoteRequest(_) => "VoteRequest",
                Self::AppendEntriesResponse(_) => "AppendEntriesResponse",
                Self::InstallSnapshotResponse(_) => "InstallSnapshotResponse",
                Self::VoteResponse(_) => "VoteResponse",
            }
        )
    }
}

#[derive(Debug)]
pub struct ConversionError(&'static str);

impl fmt::Display for ConversionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for ConversionError {}

macro_rules! impl_try_from {
    ($variant:ident, $ty:ty) => {
        impl TryFrom<RaftMessageType> for $ty {
            type Error = ConversionError;
            fn try_from(msg: RaftMessageType) -> Result<Self, Self::Error> {
                if let RaftMessageType::$variant(v) = msg {
                    Ok(v)
                } else {
                    Err(ConversionError(concat!("Expected ", stringify!($variant))))
                }
            }
        }
    };
}

impl_try_from!(AppendEntriesResponse, AppendEntriesResponse<NodeId>);
impl_try_from!(InstallSnapshotResponse, InstallSnapshotResponse<NodeId>);
impl_try_from!(VoteResponse, VoteResponse<NodeId>);

#[derive(Debug, Clone)]
pub struct Protocol {
    _connection: Connection,
    max_message_size: usize,
    max_recv_buffer_size: usize,
    receive_message_timeout_ms: Duration,
}

impl Protocol {
    pub fn new(
        conn: Connection,
        max_message_size: usize,
        max_recv_buffer_size: usize,
        receive_message_timeout_ms: Duration,
    ) -> Self {
        Self {
            _connection: conn,
            max_message_size,
            max_recv_buffer_size,
            receive_message_timeout_ms,
        }
    }

    pub async fn send_message(&self, mut send: SendStream, message: RaftMessageType) -> Result<()> {
        let data = rmp_serde::to_vec_named(&message)?;
        if data.len() > self.max_message_size {
            return Err(anyhow!(
                "Message size {} exceeds max {}",
                data.len(),
                self.max_message_size
            ));
        }
        send.write_all(&data).await?;
        send.flush().await?;
        send.finish()?;
        let _ = send.stopped().await;
        Ok(())
    }

    pub async fn receive_message(&self, mut recv_stream: RecvStream) -> Result<RaftMessageType> {
        let mut message_data = Vec::with_capacity(self.max_message_size);
        let mut buffer = vec![0; self.max_recv_buffer_size];

        let data = tokio::time::timeout(self.receive_message_timeout_ms, async {
            while let Some(n) = recv_stream.read(&mut buffer).await? {
                message_data.extend_from_slice(&buffer[..n]);
                if message_data.len() > self.max_message_size {
                    return Err(anyhow!(
                        "Message exceeded maximum size of {} bytes",
                        self.max_message_size
                    ));
                }
            }
            Ok(message_data)
        })
        .await??;

        if data.is_empty() {
            return Err(anyhow!("Received empty message"));
        }
        rmp_serde::from_slice(&data).map_err(|e| anyhow!("Deserialization error: {:?}", e))
    }

    pub async fn handle_message(
        &self,
        message: RaftMessageType,
        raft: Raft,
    ) -> Result<RaftMessageType> {
        Ok(match message {
            RaftMessageType::AppendEntriesRequest(req) => {
                RaftMessageType::AppendEntriesResponse(raft.append_entries(req).await?)
            }
            RaftMessageType::InstallSnapshotRequest(req) => {
                RaftMessageType::InstallSnapshotResponse(raft.install_snapshot(req).await?)
            }
            RaftMessageType::VoteRequest(req) => {
                RaftMessageType::VoteResponse(raft.vote(req).await?)
            }
            _ => return Err(anyhow!("Unexpected message type")),
        })
    }
}
