// PythonExecutionError and error handling traits for kameo-snake-handler
use bincode::{Decode, Encode};
use kameo_child_process;
use pyo3::exceptions::{
    PyAttributeError, PyImportError, PyModuleNotFoundError, PyRuntimeError, PyTypeError,
    PyValueError,
};
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Trait for creating a reply from a Python execution error
pub trait ErrorReply: Sized {
    fn from_error(err: PythonExecutionError) -> Self;
}

/// Error type for Python execution
#[derive(Debug, Error, Serialize, Deserialize, Encode, Decode, Clone)]
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
}

impl PythonExecutionError {
    pub fn from_pyerr(err: PyErr, py: Python) -> Self {
        if err.is_instance_of::<PyModuleNotFoundError>(py) {
            let msg = err.to_string();
            let module = msg.split('\'').nth(1).unwrap_or("unknown").to_string();
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
            let module = msg.split('\'').nth(1).unwrap_or("unknown").to_string();
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

impl kameo_child_process::ProtocolError for PythonExecutionError {
    fn protocol(msg: String) -> Self {
        PythonExecutionError::ExecutionError { message: msg }
    }
    fn handshake_failed(msg: String) -> Self {
        PythonExecutionError::ExecutionError {
            message: format!("Handshake failed: {msg}"),
        }
    }
    fn connection_closed() -> Self {
        PythonExecutionError::ExecutionError {
            message: "Connection closed".to_string(),
        }
    }
}
