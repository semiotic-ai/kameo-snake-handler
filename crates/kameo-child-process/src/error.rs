use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};
use std::io;
use thiserror::Error;
use tracing::error;

#[derive(Debug, Error)]
pub enum SubprocessActorError {
    #[error("IPC error: {0}")]
    Ipc(#[from] io::Error),
    #[error("Serialization error: {0}")]
    Serialization(#[from] bincode::error::EncodeError),
    #[error("Deserialization error: {0}")]
    Deserialization(#[from] bincode::error::DecodeError),
    #[error("Actor panicked: {reason}")]
    Panicked { reason: String },
    #[error("Protocol error: {0}")]
    Protocol(String),
    #[error("Handshake failed: {0}")]
    HandshakeFailed(String),
    #[error("Connection closed")]
    ConnectionClosed,
    #[error("Unknown actor type: {actor_name}")]
    UnknownActorType { actor_name: String },
}

#[derive(Debug, Serialize, Deserialize, Encode, Decode)]
pub enum SubprocessActorIpcError {
    Protocol(String),
    HandshakeFailed(String),
    ConnectionClosed,
    UnknownActorType { actor_name: String },
}

impl From<SubprocessActorError> for SubprocessActorIpcError {
    fn from(e: SubprocessActorError) -> Self {
        match e {
            SubprocessActorError::Protocol(s) => Self::Protocol(s),
            SubprocessActorError::HandshakeFailed(s) => Self::HandshakeFailed(s),
            SubprocessActorError::ConnectionClosed => Self::ConnectionClosed,
            SubprocessActorError::UnknownActorType { actor_name } => {
                Self::UnknownActorType { actor_name }
            }
            other => Self::Protocol(format!("Non-serializable error: {other}")),
        }
    }
}

#[tracing::instrument(level = "error", fields(actor_name))]
pub fn handle_unknown_actor_error(actor_name: &str) -> SubprocessActorError {
    error!(
        status = "error",
        actor_type = actor_name,
        message = "Unknown actor type encountered"
    );
    SubprocessActorError::UnknownActorType {
        actor_name: actor_name.to_string(),
    }
}

pub trait ProtocolError: std::fmt::Debug + Send + Sync + 'static {
    fn protocol(msg: String) -> Self;
    fn handshake_failed(msg: String) -> Self;
    fn connection_closed() -> Self;
}

impl ProtocolError for SubprocessActorIpcError {
    fn protocol(msg: String) -> Self {
        Self::Protocol(msg)
    }
    fn handshake_failed(msg: String) -> Self {
        Self::HandshakeFailed(msg)
    }
    fn connection_closed() -> Self {
        Self::ConnectionClosed
    }
}
