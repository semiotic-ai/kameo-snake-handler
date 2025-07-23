use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};
use std::io;
use thiserror::Error;
use tracing::error;
#[cfg(feature = "python")]
use pyo3::exceptions::{
    PyAttributeError, PyImportError, PyModuleNotFoundError, PyRuntimeError, PyTypeError, PyValueError
};
#[cfg(feature = "python")]
use pyo3::prelude::*;

#[derive(Debug, Error)]
pub enum SubprocessIpcBackendError {
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
    #[error("Shutdown")]
    Shutdown,
}

#[derive(Debug, Serialize, Deserialize, Encode, Decode)]
pub enum SubprocessIpcBackendIpcError {
    Protocol(String),
    HandshakeFailed(String),
    ConnectionClosed,
    UnknownActorType { actor_name: String },
}

impl From<SubprocessIpcBackendError> for SubprocessIpcBackendIpcError {
    fn from(e: SubprocessIpcBackendError) -> Self {
        match e {
            SubprocessIpcBackendError::Protocol(s) => Self::Protocol(s),
            SubprocessIpcBackendError::HandshakeFailed(s) => Self::HandshakeFailed(s),
            SubprocessIpcBackendError::ConnectionClosed => Self::ConnectionClosed,
            SubprocessIpcBackendError::UnknownActorType { actor_name } => {
                Self::UnknownActorType { actor_name }
            },
            SubprocessIpcBackendError::Ipc(err) => Self::Protocol(format!("IPC error: {err}")),
            SubprocessIpcBackendError::Serialization(err) => Self::Protocol(format!("Serialization error: {err}")),
            SubprocessIpcBackendError::Deserialization(err) => Self::Protocol(format!("Deserialization error: {err}")),
            SubprocessIpcBackendError::Shutdown => Self::Protocol("Shutdown".to_string()),
            SubprocessIpcBackendError::Panicked { reason } => Self::Protocol(format!("Panicked: {reason}")),
        }
    }
}

#[tracing::instrument(level = "error", fields(actor_name))]
pub fn handle_unknown_actor_error(actor_name: &str) -> SubprocessIpcBackendError {
    error!(
        status = "error",
        actor_type = actor_name,
        message = "Unknown actor type encountered"
    );
    SubprocessIpcBackendError::UnknownActorType {
        actor_name: actor_name.to_string(),
    }
}

#[derive(Debug, thiserror::Error, Serialize, Deserialize, Encode, Decode, Clone)]
pub enum PythonExecutionError {
    #[error("Python module '{module}' not found: {message}")]
    ModuleNotFound { module: String, message: String },
    #[error("Python function '{function}' not found in module '{module}': {message}")]
    FunctionNotFound {
        module: String,
        function: String,
        message: String,
    },
    #[error("Python execution error: {message}")]
    ExecutionError { message: String },
    #[error("Python value error: {message}")]
    ValueError { message: String },
    #[error("Python type error: {message}")]
    TypeError { message: String },
    #[error("Python import error for module '{module}': {message}")]
    ImportError { module: String, message: String },
    #[error("Python attribute error: {message}")]
    AttributeError { message: String },
    #[error("Python runtime error: {message}")]
    RuntimeError { message: String },
    #[error("Failed to serialize Rust value to Python: {message}")]
    SerializationError { message: String },
    #[error("Failed to deserialize Python value to Rust: {message}")]
    DeserializationError { message: String },
    #[error("Failed to call Python function '{function}': {message}")]
    CallError { function: String, message: String },
    #[error("Failed to convert between Python and Rust types: {message}")]
    ConversionError { message: String },
    #[error("Child process terminated unexpectedly")]
    ChildProcessTerminated,
}

#[cfg(feature = "python")]
impl PythonExecutionError {
    pub fn from_pyerr(err: PyErr, py: Python) -> Self {
        if err.is_instance_of::<PyModuleNotFoundError>(py) {
            let msg = err.to_string();
            let module = msg.split("'").nth(1).unwrap_or("unknown").to_string();
            PythonExecutionError::ModuleNotFound {
                module,
                message: msg,
            }
        } else if err.is_instance_of::<PyAttributeError>(py) {
            PythonExecutionError::AttributeError {
                message: err.to_string(),
            }
        } else if err.is_instance_of::<PyValueError>(py) {
            PythonExecutionError::ValueError {
                message: err.to_string(),
            }
        } else if err.is_instance_of::<PyTypeError>(py) {
            PythonExecutionError::TypeError {
                message: err.to_string(),
            }
        } else if err.is_instance_of::<PyImportError>(py) {
            let msg = err.to_string();
            let module = msg.split("'").nth(1).unwrap_or("unknown").to_string();
            PythonExecutionError::ImportError {
                module,
                message: msg,
            }
        } else if err.is_instance_of::<PyRuntimeError>(py) {
            PythonExecutionError::RuntimeError {
                message: err.to_string(),
            }
        } else {
            PythonExecutionError::ExecutionError {
                message: err.to_string(),
            }
        }
    }
}

#[cfg(feature = "python")]
impl From<PyErr> for PythonExecutionError {
    fn from(err: PyErr) -> Self {
        Python::with_gil(|py| Self::from_pyerr(err, py))
    }
}

impl From<serde_json::Error> for PythonExecutionError {
    fn from(err: serde_json::Error) -> Self {
        PythonExecutionError::SerializationError {
            message: err.to_string(),
        }
    }
}
