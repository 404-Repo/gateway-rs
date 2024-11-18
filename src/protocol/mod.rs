// TODO: Remove this after the code is finalized
#![allow(dead_code)]

use anyhow::Result;
use bytes::BytesMut;
use quinn::{Connection, RecvStream};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "type", content = "data")]
pub enum MessageType {
    Write(WriteMessage),
    Read(ReadMessage),
    ChangeMembership(MembershipMessage),
    AddLearner(LearnerMessage),
    RaftVote(VoteMessage),
    RaftAppend(AppendMessage),
    RaftSnapshot(SnapshotMessage),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct WriteMessage {
    key: Vec<u8>,
    value: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ReadMessage {
    key: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MembershipMessage {
    members: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LearnerMessage {
    learner_id: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct VoteMessage {
    term: u64,
    candidate_id: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AppendMessage {
    term: u64,
    entries: Vec<LogEntry>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SnapshotMessage {
    term: u64,
    data: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LogEntry {
    term: u64,
    data: Vec<u8>,
}

pub struct Protocol {
    connection: Connection,
}

impl Protocol {
    pub fn new(connection: Connection) -> Self {
        Self { connection }
    }

    pub async fn send_message(&self, message: MessageType) -> Result<()> {
        let mut send_stream = self.connection.open_uni().await?;

        let serialized = rmp_serde::to_vec(&message)?;
        let len = serialized.len() as u32;
        send_stream.write_all(&len.to_be_bytes()).await?;

        send_stream.write_all(&serialized).await?;
        send_stream.finish()?;

        Ok(())
    }

    pub async fn receive_message(mut recv_stream: RecvStream) -> Result<MessageType> {
        let mut buf = BytesMut::new();

        let mut len_bytes = [0u8; 4];
        recv_stream.read_exact(&mut len_bytes).await?;
        let len = u32::from_be_bytes(len_bytes) as usize;

        buf.resize(len, 0);
        recv_stream.read_exact(&mut buf).await?;
        let message = rmp_serde::from_slice(&buf)?;
        Ok(message)
    }
}

impl MessageType {
    pub fn new_write(key: Vec<u8>, value: Vec<u8>) -> Self {
        MessageType::Write(WriteMessage { key, value })
    }

    pub fn new_read(key: Vec<u8>) -> Self {
        MessageType::Read(ReadMessage { key })
    }

    pub fn new_membership(members: Vec<String>) -> Self {
        MessageType::ChangeMembership(MembershipMessage { members })
    }

    pub fn new_learner(learner_id: String) -> Self {
        MessageType::AddLearner(LearnerMessage { learner_id })
    }

    pub fn new_vote(term: u64, candidate_id: String) -> Self {
        MessageType::RaftVote(VoteMessage { term, candidate_id })
    }

    pub fn new_append(term: u64, entries: Vec<LogEntry>) -> Self {
        MessageType::RaftAppend(AppendMessage { term, entries })
    }

    pub fn new_snapshot(term: u64, data: Vec<u8>) -> Self {
        MessageType::RaftSnapshot(SnapshotMessage { term, data })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use quinn::crypto::rustls::QuicClientConfig;
    use quinn::{ClientConfig, Endpoint, ServerConfig};
    use rcgen::generate_simple_self_signed;
    use rcgen::CertifiedKey;
    use rustls::pki_types::pem::PemObject;
    use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer, ServerName, UnixTime};
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::sync::Arc;

    #[derive(Debug)]
    struct SkipServerVerification(Arc<rustls::crypto::CryptoProvider>);

    impl SkipServerVerification {
        fn new() -> Arc<Self> {
            Arc::new(Self(Arc::new(rustls::crypto::ring::default_provider())))
        }
    }

    impl rustls::client::danger::ServerCertVerifier for SkipServerVerification {
        fn verify_server_cert(
            &self,
            _end_entity: &CertificateDer<'_>,
            _intermediates: &[CertificateDer<'_>],
            _server_name: &ServerName<'_>,
            _ocsp: &[u8],
            _now: UnixTime,
        ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
            Ok(rustls::client::danger::ServerCertVerified::assertion())
        }

        fn verify_tls12_signature(
            &self,
            message: &[u8],
            cert: &CertificateDer<'_>,
            dss: &rustls::DigitallySignedStruct,
        ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
            rustls::crypto::verify_tls12_signature(
                message,
                cert,
                dss,
                &self.0.signature_verification_algorithms,
            )
        }

        fn verify_tls13_signature(
            &self,
            message: &[u8],
            cert: &CertificateDer<'_>,
            dss: &rustls::DigitallySignedStruct,
        ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
            rustls::crypto::verify_tls13_signature(
                message,
                cert,
                dss,
                &self.0.signature_verification_algorithms,
            )
        }

        fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
            self.0.signature_verification_algorithms.supported_schemes()
        }
    }

    async fn setup_server() -> Result<(Endpoint, SocketAddr)> {
        // Generate self-signed cert for testing
        let CertifiedKey { cert, key_pair } =
            generate_simple_self_signed(vec!["localhost".into()])?;
        let key = PrivatePkcs8KeyDer::from_pem_slice(key_pair.serialize_pem().as_bytes())
            .map_err(anyhow::Error::msg)?;
        let cert_der = cert.der().to_owned();

        let server_config = ServerConfig::with_single_cert(
            vec![cert_der],
            rustls::pki_types::PrivateKeyDer::Pkcs8(key),
        )?;

        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080);
        let endpoint = Endpoint::server(server_config, addr)?;

        Ok((endpoint, addr.clone()))
    }

    async fn setup_client() -> Result<Endpoint> {
        _ = rustls::crypto::ring::default_provider().install_default();
        let client_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8081);
        let mut endpoint = Endpoint::client(client_addr)?;

        endpoint.set_default_client_config(ClientConfig::new(Arc::new(
            QuicClientConfig::try_from(
                rustls::ClientConfig::builder()
                    .dangerous()
                    .with_custom_certificate_verifier(SkipServerVerification::new())
                    .with_no_client_auth(),
            )?,
        )));

        Ok(endpoint)
    }

    #[tokio::test]
    async fn test_message_transmission() -> Result<()> {
        // Setup server and client
        let (server_endpoint, server_addr) = setup_server().await?;
        let client_endpoint = setup_client().await?;

        // Server accept connection task
        let _server_task = tokio::spawn(async move {
            let incoming_conn = server_endpoint.accept().await.unwrap().await.unwrap();
            let server_protocol = Protocol::new(incoming_conn);

            // Receive messages
            while let Ok(stream) = server_protocol.connection.accept_uni().await {
                let received_msg = Protocol::receive_message(stream).await.unwrap();
                // Echo back the message
                server_protocol
                    .send_message(received_msg.clone())
                    .await
                    .unwrap();
            }
        });

        // Connect client
        let client_conn = client_endpoint.connect(server_addr, "localhost")?.await?;
        let client_protocol = Protocol::new(client_conn);

        // Test cases for each message type
        let test_messages = vec![
            // Write messages with different key-value pairs
            MessageType::new_write(b"key1".to_vec(), b"value1".to_vec()),
            MessageType::new_write(b"empty_key".to_vec(), b"".to_vec()),
            MessageType::new_write(b"".to_vec(), b"empty_value".to_vec()),
            MessageType::new_write(b"binary\0data".to_vec(), b"binary\0value".to_vec()),
            // Read messages
            MessageType::new_read(b"existing_key".to_vec()),
            MessageType::new_read(b"".to_vec()),
            MessageType::new_read(b"binary\0key".to_vec()),
            // Membership change messages
            MessageType::new_membership(vec![]),
            MessageType::new_membership(vec!["node1".into()]),
            MessageType::new_membership(vec!["node1".into(), "node2".into(), "node3".into()]),
            // Learner messages
            MessageType::new_learner("new_learner".into()),
            MessageType::new_learner("learner_with_special_chars@#$".into()),
            MessageType::new_learner("".into()),
            // Raft Vote messages
            MessageType::new_vote(0, "first_candidate".into()),
            MessageType::new_vote(1, "second_candidate".into()),
            MessageType::new_vote(u64::MAX, "max_term_candidate".into()),
            // Raft Append messages
            MessageType::new_append(1, vec![]),
            MessageType::new_append(
                1,
                vec![LogEntry {
                    term: 1,
                    data: b"single_entry".to_vec(),
                }],
            ),
            MessageType::new_append(
                2,
                vec![
                    LogEntry {
                        term: 1,
                        data: b"first_entry".to_vec(),
                    },
                    LogEntry {
                        term: 2,
                        data: b"second_entry".to_vec(),
                    },
                ],
            ),
            MessageType::new_snapshot(1, b"".to_vec()),
            MessageType::new_snapshot(1, b"small_snapshot".to_vec()),
            MessageType::new_snapshot(u64::MAX, vec![0; 1024]),
        ];

        for (i, original_msg) in test_messages.iter().enumerate() {
            client_protocol.send_message(original_msg.clone()).await?;

            let received_stream = client_protocol.connection.accept_uni().await?;
            let received_msg = Protocol::receive_message(received_stream).await?;

            assert_eq!(
                format!("{:?}", original_msg),
                format!("{:?}", received_msg),
                "Message mismatch for test case {}: {:?}",
                i,
                original_msg
            );
        }

        let message_type_counts =
            test_messages
                .iter()
                .fold(std::collections::HashMap::new(), |mut acc, msg| {
                    *acc.entry(match msg {
                        MessageType::Write(_) => "Write",
                        MessageType::Read(_) => "Read",
                        MessageType::ChangeMembership(_) => "ChangeMembership",
                        MessageType::AddLearner(_) => "AddLearner",
                        MessageType::RaftVote(_) => "RaftVote",
                        MessageType::RaftAppend(_) => "RaftAppend",
                        MessageType::RaftSnapshot(_) => "RaftSnapshot",
                    })
                    .or_insert(0) += 1;
                    acc
                });

        println!("\nMessage type coverage:");
        for (msg_type, count) in message_type_counts {
            println!("- {}: {} test cases", msg_type, count);
        }

        Ok(())
    }
}
