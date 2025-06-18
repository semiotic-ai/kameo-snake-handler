use std::marker::PhantomData;

use kameo::message::{Context, Message};
use kameo::actor::{Actor, ActorRef, WeakActorRef};
use kameo::error::{PanicError, ActorStopReason};

use anyhow::Result;
use async_trait::async_trait;
use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};
use tracing::{instrument, error};
use thiserror::Error;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3::exceptions::{PyModuleNotFoundError, PyAttributeError, PyValueError, PyTypeError, PyImportError, PyRuntimeError};
use kameo_child_process::KameoChildProcessMessage;
use std::ops::ControlFlow;

/// Error type for Python execution
#[derive(Debug, Error, Serialize, Deserialize, Encode, Decode)]
pub enum PythonExecutionError {
    #[error("Python module not found: {0}")]
    ModuleNotFound(String),
    #[error("Python function not found: {0}")]
    FunctionNotFound(String),
    #[error("Python execution error: {0}")]
    ExecutionError(String),
    #[error("Python value error: {0}")]
    ValueError(String),
    #[error("Python type error: {0}")]
    TypeError(String),
    #[error("Python import error: {0}")]
    ImportError(String),
    #[error("Python attribute error: {0}")]
    AttributeError(String),
    #[error("Python runtime error: {0}")]
    RuntimeError(String),
    #[error("Python serialization error: {0}")]
    SerializationError(String),
    #[error("Python deserialization error: {0}")]
    DeserializationError(String),
}

impl From<PyErr> for PythonExecutionError {
    fn from(err: PyErr) -> Self {
        Python::with_gil(|py| {
            if err.is_instance_of::<PyModuleNotFoundError>(py) {
                PythonExecutionError::ModuleNotFound(err.to_string())
            } else if err.is_instance_of::<PyAttributeError>(py) {
                PythonExecutionError::AttributeError(err.to_string())
            } else if err.is_instance_of::<PyValueError>(py) {
                PythonExecutionError::ValueError(err.to_string())
            } else if err.is_instance_of::<PyTypeError>(py) {
                PythonExecutionError::TypeError(err.to_string())
            } else if err.is_instance_of::<PyImportError>(py) {
                PythonExecutionError::ImportError(err.to_string())
            } else if err.is_instance_of::<PyRuntimeError>(py) {
                PythonExecutionError::RuntimeError(err.to_string())
            } else {
                PythonExecutionError::ExecutionError(err.to_string())
            }
        })
    }
}

/// Configuration for Python subprocess
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct PythonConfig {
    pub python_path: Vec<String>,
    pub module_name: String,
    pub function_name: String,
    pub is_async: bool,
    pub env_vars: Vec<(String, String)>,
}

/// Python subprocess actor
#[derive(Debug)]
pub struct PythonActor<M> {
    config: PythonConfig,
    _phantom: PhantomData<M>,
}

impl<M> Default for PythonActor<M> {
    fn default() -> Self {
        Self {
            config: PythonConfig {
                python_path: vec![],
                module_name: String::new(),
                function_name: String::new(),
                is_async: false,
                env_vars: vec![],
            },
            _phantom: PhantomData,
        }
    }
}

impl<M> Clone for PythonActor<M> {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            _phantom: PhantomData,
        }
    }
}

/// Builder for Python subprocess
pub struct PythonSubprocessBuilder<M> {
    config: PythonConfig,
    _phantom: PhantomData<M>,
}

impl<M> PythonSubprocessBuilder<M> {
    pub fn new() -> Self {
        Self {
            config: PythonConfig {
                python_path: vec![],
                module_name: String::new(),
                function_name: String::new(),
                is_async: false,
                env_vars: vec![],
            },
            _phantom: PhantomData,
        }
    }

    pub fn with_config(mut self, config: PythonConfig) -> Self {
        self.config = config;
        self
    }

    pub async fn spawn(self) -> Result<PythonActor<M>> {
        Ok(PythonActor {
            config: self.config,
            _phantom: PhantomData,
        })
    }
}

#[async_trait]
impl<M> Actor for PythonActor<M>
where
    M: KameoChildProcessMessage + Send + 'static,
{
    type Error = PythonExecutionError;

    #[instrument(skip(self, _actor_ref), fields(actor_type = "PythonActor"))]
    fn on_start(&mut self, _actor_ref: ActorRef<Self>) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send {
        async move {
            // Initialize Python environment
            Python::with_gil(|py| {
                // Add Python path
                for path in &self.config.python_path {
                    py.import("sys")?
                        .getattr("path")?
                        .call_method1("append", (path,))?;
                }

                // Import module to verify it exists
                py.import(&self.config.module_name)?;

                Ok(())
            })
        }
    }

    #[instrument(skip(self, _actor_ref, reason), fields(actor_type = "PythonActor"))]
    fn on_stop(&mut self, _actor_ref: WeakActorRef<Self>, reason: ActorStopReason) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send {
        async move {
            error!("Python actor stopped: {:?}", reason);
            Ok(())
        }
    }

    #[instrument(skip(self, _actor_ref, err), fields(actor_type = "PythonActor"))]
    fn on_panic(&mut self, _actor_ref: WeakActorRef<Self>, err: PanicError) -> impl std::future::Future<Output = Result<ControlFlow<ActorStopReason>, Self::Error>> + Send {
        async move {
            error!("Python actor panicked: {:?}", err);
            Ok(ControlFlow::Break(ActorStopReason::Panicked(err)))
        }
    }
}

#[async_trait]
impl<M> Message<M> for PythonActor<M>
where
    M: KameoChildProcessMessage + Send + 'static,
{
    type Reply = Result<<M as KameoChildProcessMessage>::Reply, PythonExecutionError>;

    #[instrument(skip(self, message, _ctx), fields(actor_type = "PythonActor"))]
    fn handle(&mut self, message: M, _ctx: &mut Context<Self, Self::Reply>) -> impl std::future::Future<Output = Self::Reply> + Send {
        let config = self.config.clone();
        async move {
            if config.is_async {
                // Step 1: Create the future inside the GIL.
                let future = Python::with_gil(|py| {
                    let message_json = serde_json::to_value(&message)
                        .map_err(|e| PythonExecutionError::SerializationError(e.to_string()))?;
                    let message_str = serde_json::to_string(&message_json)
                        .map_err(|e| PythonExecutionError::SerializationError(e.to_string()))?;
                    
                    let kwargs = PyDict::new(py);
                    kwargs.set_item("message", message_str)
                        .map_err(|e| PythonExecutionError::ValueError(e.to_string()))?;

                    let module = py.import(&config.module_name)
                        .map_err(|e| PythonExecutionError::ImportError(e.to_string()))?;
                    let func = module.getattr(&config.function_name)
                        .map_err(|e| PythonExecutionError::AttributeError(e.to_string()))?;

                    let py_coro = func.call((), Some(&kwargs))
                        .map_err(|e| PythonExecutionError::ExecutionError(e.to_string()))?;

                    pyo3_async_runtimes::tokio::into_future(py_coro)
                        .map_err(|e| PythonExecutionError::ExecutionError(e.to_string()))
                })?;

                // Step 2: Await the future outside the GIL.
                let py_result = future.await
                    .map_err(|e| PythonExecutionError::ExecutionError(e.to_string()))?;

                // Step 3: Process the result inside the GIL.
                Python::with_gil(|py| {
                    let json = py.import("json")
                        .map_err(|e| PythonExecutionError::ImportError(e.to_string()))?;
                    let json_str = json.call_method1("dumps", (py_result.bind(py),))
                        .map_err(|e| PythonExecutionError::SerializationError(e.to_string()))?
                        .extract::<String>()
                        .map_err(|e| PythonExecutionError::SerializationError(e.to_string()))?;
                    serde_json::from_str(&json_str)
                        .map_err(|e| PythonExecutionError::DeserializationError(e.to_string()))
                })
            } else {
                // Handle synchronous Python function
                Python::with_gil(|py| {
                    let message_json = serde_json::to_value(&message)
                        .map_err(|e| PythonExecutionError::SerializationError(e.to_string()))?;
                    let message_str = serde_json::to_string(&message_json)
                        .map_err(|e| PythonExecutionError::SerializationError(e.to_string()))?;

                    let kwargs = PyDict::new(py);
                    kwargs.set_item("message", message_str)
                        .map_err(|e| PythonExecutionError::ValueError(e.to_string()))?;

                    let module = py.import(&config.module_name)
                        .map_err(|e| PythonExecutionError::ImportError(e.to_string()))?;
                    let func = module.getattr(&config.function_name)
                        .map_err(|e| PythonExecutionError::AttributeError(e.to_string()))?;

                    let result = func.call((), Some(&kwargs))
                        .map_err(|e| PythonExecutionError::ExecutionError(e.to_string()))?;
                    
                    let json = py.import("json")
                        .map_err(|e| PythonExecutionError::ImportError(e.to_string()))?;
                    let json_str = json.call_method1("dumps", (result,))
                        .map_err(|e| PythonExecutionError::SerializationError(e.to_string()))?
                        .extract::<String>()
                        .map_err(|e| PythonExecutionError::SerializationError(e.to_string()))?;
                    serde_json::from_str(&json_str)
                        .map_err(|e| PythonExecutionError::DeserializationError(e.to_string()))
                })
            }
        }
    }
}

pub mod prelude {
    pub use super::{PythonActor, PythonConfig, PythonExecutionError, PythonSubprocessBuilder};
}


