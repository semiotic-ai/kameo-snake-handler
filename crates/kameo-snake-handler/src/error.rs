// PythonExecutionError and error handling traits for kameo-snake-handler
use bincode::{Decode, Encode};
use kameo_child_process::error::PythonExecutionError;
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
