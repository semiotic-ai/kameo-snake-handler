use std::marker::PhantomData;
use std::future::Future;
use std::ops::ControlFlow;

use kameo::actor::{Actor, ActorRef, WeakActorRef};
use kameo::error::{ActorStopReason, PanicError};
use kameo::message::{Context, Message};
use kameo_child_process::{KameoChildProcessMessage, RuntimeAware, ChildProcessBuilder, SubprocessActor};

use anyhow::Result;
use async_trait::async_trait;
use bincode::{Decode, Encode};
use pyo3::exceptions::{
    PyAttributeError, PyImportError, PyModuleNotFoundError, PyRuntimeError, PyTypeError,
    PyValueError,
};
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{error, instrument, info, Level};

/// Error type for Python execution
#[derive(Debug, Error, Serialize, Deserialize, Encode, Decode)]
pub enum PythonExecutionError {
    #[error("Python module '{module}' not found: {message}")]
    ModuleNotFound {
        module: String,
        message: String,
    },

    #[error("Python function '{function}' not found in module '{module}': {message}")]
    FunctionNotFound {
        module: String,
        function: String,
        message: String,
    },

    #[error("Python execution error: {message}")]
    ExecutionError {
        message: String,
    },

    #[error("Python value error: {message}")]
    ValueError {
        message: String,
    },

    #[error("Python type error: {message}")]
    TypeError {
        message: String,
    },

    #[error("Python import error for module '{module}': {message}")]
    ImportError {
        module: String,
        message: String,
    },

    #[error("Python attribute error: {message}")]
    AttributeError {
        message: String,
    },

    #[error("Python runtime error: {message}")]
    RuntimeError {
        message: String,
    },

    #[error("Failed to serialize Rust value to Python: {message}")]
    SerializationError {
        message: String,
    },

    #[error("Failed to deserialize Python value to Rust: {message}")]
    DeserializationError {
        message: String,
    },

    #[error("Failed to call Python function '{function}': {message}")]
    CallError {
        function: String,
        message: String,
    },

    #[error("Failed to convert between Python and Rust types: {message}")]
    ConversionError {
        message: String,
    },
}

impl From<PyErr> for PythonExecutionError {
    fn from(err: PyErr) -> Self {
        Python::with_gil(|py| {
            if err.is_instance_of::<PyModuleNotFoundError>(py) {
                let msg = err.to_string();
                let module = msg.split('\'')
                    .nth(1)
                    .unwrap_or("unknown")
                    .to_string();
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
                let module = msg.split('\'')
                    .nth(1)
                    .unwrap_or("unknown")
                    .to_string();
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
        })
    }
}

impl From<serde_json::Error> for PythonExecutionError {
    fn from(err: serde_json::Error) -> Self {
        PythonExecutionError::SerializationError {
            message: err.to_string(),
        }
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
    #[instrument]
    fn default() -> Self {
        info!("Initializing PythonActor from environment");
        let config = match std::env::var("KAMEO_PYTHON_CONFIG") {
            Ok(config_json) => {
                info!(config_json, "Found KAMEO_PYTHON_CONFIG");
                serde_json::from_str(&config_json).expect("Failed to deserialize python config from env")
            }
            Err(e) => {
                error!(error = ?e, "KAMEO_PYTHON_CONFIG not set");
                panic!("KAMEO_PYTHON_CONFIG must be set for PythonActor");
            }
        };

        Self {
            config,
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

/// Builder for a Python child process
#[derive(Debug)]
pub struct PythonChildProcessBuilder {
    python_config: PythonConfig,
    log_level: Level,
}

impl PythonChildProcessBuilder {
    /// Creates a new builder with the given Python configuration.
    #[instrument]
    pub fn new(python_config: PythonConfig) -> Self {
        Self {
            python_config,
            log_level: Level::INFO,
        }
    }

    /// Sets the log level for the child process.
    pub fn log_level(mut self, level: Level) -> Self {
        self.log_level = level;
        self
    }

    /// Spawns the child process.
    #[instrument(skip(self), fields(config = ?self.python_config, log_level = ?self.log_level))]
    pub async fn spawn<M>(self) -> Result<ActorRef<SubprocessActor<M>>>
    where
        M: KameoChildProcessMessage + Send + Sync + 'static,
    {
        info!("Spawning Python child process");
        let config_json = serde_json::to_string(&self.python_config)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        ChildProcessBuilder::<PythonActor<M>, M>::new()
            .log_level(self.log_level)
            .with_env_var("KAMEO_PYTHON_CONFIG", config_json)
            .spawn()
            .await
            .map_err(|e: std::io::Error| anyhow::anyhow!(e).into())
    }
}

/// Manages Python event loop lifecycle
struct EventLoopManager {
    loop_: PyObject,
}

impl EventLoopManager {
    /// Create a new event loop manager
    fn new(py: Python) -> Result<Self, PythonExecutionError> {
        let asyncio = py.import("asyncio")?;
        let loop_: PyObject = asyncio.getattr("new_event_loop")?.call0()?.into();
        asyncio.getattr("set_event_loop")?.call1((loop_.clone_ref(py),))?;
        
        Ok(Self { loop_ })
    }

    /// Run a coroutine to completion
    fn run_coroutine(&self, py: Python, coro: PyObject) -> Result<PyObject, PythonExecutionError> {
        Ok(self.loop_.call_method1(py, "run_until_complete", (coro,))?.into())
    }

    /// Clean up the event loop
    fn cleanup(&self, py: Python) {
        if let Ok(asyncio) = py.import("asyncio") {
            let _ = asyncio.getattr("set_event_loop").and_then(|f| f.call1((py.None(),)));
        }
    }
}

impl Drop for EventLoopManager {
    fn drop(&mut self) {
        Python::with_gil(|py| self.cleanup(py));
    }
}

#[async_trait]
impl<M> Actor for PythonActor<M>
where
    M: KameoChildProcessMessage + Send + 'static,
{
    type Error = PythonExecutionError;

    #[instrument(skip(self, _actor_ref), fields(actor_type = "PythonActor"))]
    fn on_start(
        &mut self,
        _actor_ref: ActorRef<Self>,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send {
        async move {
            tracing::info!("PythonActor started");
            Ok(())
        }
    }

    #[instrument(skip(self, _actor_ref, reason), fields(actor_type = "PythonActor"))]
    fn on_stop(
        &mut self,
        _actor_ref: WeakActorRef<Self>,
        reason: ActorStopReason,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send {
        async move {
            error!("Python actor stopped: {:?}", reason);
            Ok(())
        }
    }

    #[instrument(skip(self, _actor_ref, err), fields(actor_type = "PythonActor"))]
    fn on_panic(
        &mut self,
        _actor_ref: WeakActorRef<Self>,
        err: PanicError,
    ) -> impl std::future::Future<Output = Result<ControlFlow<ActorStopReason>, Self::Error>> + Send
    {
        async move {
            error!("Python actor panicked: {:?}", err);
            Ok(ControlFlow::Break(ActorStopReason::Panicked(err)))
        }
    }
}

impl<M> PythonActor<M>
where
    M: KameoChildProcessMessage + Send + 'static,
{
    /// Convert a Rust message to a Python dictionary
    fn serialize_message_to_py(&self, message: &M, py: Python) -> Result<PyObject, PythonExecutionError> {
        let json_str = serde_json::to_string(message)
            .map_err(|e| PythonExecutionError::SerializationError {
                message: e.to_string(),
            })?;
        let json_module = py.import("json")
            .map_err(|e| PythonExecutionError::ImportError {
                module: "json".to_string(),
                message: e.to_string(),
            })?;
        Ok(json_module.getattr("loads")?
            .call1((json_str,))?
            .into())
    }

    /// Convert a Python object back to our Reply type
    fn deserialize_py_to_reply(&self, py_obj: PyObject, py: Python) -> Result<<M as KameoChildProcessMessage>::Reply, PythonExecutionError> {
        let json_module = py.import("json")
            .map_err(|e| PythonExecutionError::ImportError {
                module: "json".to_string(),
                message: e.to_string(),
            })?;
        let json_str = json_module.getattr("dumps")?
            .call1((py_obj,))?
            .extract::<String>()?;

        serde_json::from_str(&json_str)
            .map_err(|e| PythonExecutionError::DeserializationError {
                message: e.to_string(),
            })
    }

    /// Handle async Python function call
    fn handle_async(&self, message: &M, py: Python) -> Result<PyObject, PythonExecutionError> {
        // Set up event loop manager
        let loop_manager = EventLoopManager::new(py)?;

        // Import module and convert message
        let module = py.import(&self.config.module_name)
            .map_err(|e| PythonExecutionError::ImportError {
                module: self.config.module_name.clone(),
                message: e.to_string(),
            })?;
        let py_dict = self.serialize_message_to_py(message, py)?;

        // Get and run coroutine
        let coro = module.getattr(&self.config.function_name)?
            .call1((py_dict,))
            .map_err(|e| PythonExecutionError::CallError {
                function: self.config.function_name.clone(),
                message: e.to_string(),
            })?;

        // Run the coroutine using the event loop manager
        loop_manager.run_coroutine(py, coro.into())
    }

    /// Handle sync Python function call
    fn handle_sync(&self, message: &M, py: Python) -> Result<PyObject, PythonExecutionError> {
        let module = py.import(&self.config.module_name)
            .map_err(|e| PythonExecutionError::ImportError {
                module: self.config.module_name.clone(),
                message: e.to_string(),
            })?;
        let py_dict = self.serialize_message_to_py(message, py)?;

        Ok(module.getattr(&self.config.function_name)?
            .call1((py_dict,))?
            .into())
    }
}

#[async_trait]
impl<M> Message<M> for PythonActor<M>
where
    M: KameoChildProcessMessage + Send + 'static,
{
    type Reply = Result<<M as KameoChildProcessMessage>::Reply, PythonExecutionError>;

    #[instrument(skip(self, message, _ctx), fields(actor_type = "PythonActor"))]
    fn handle(
        &mut self,
        message: M,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> impl std::future::Future<Output = Self::Reply> + Send {
        let config = self.config.clone();

        async move {
            Python::with_gil(|py| {
                // Handle the Python call based on sync/async configuration
                let result = if config.is_async {
                    self.handle_async(&message, py)
                } else {
                    self.handle_sync(&message, py)
                }?;

                // Convert the result back to our expected type
                self.deserialize_py_to_reply(result, py)
            })
        }
    }
}

#[async_trait]
impl<M> RuntimeAware for PythonActor<M>
where
    M: KameoChildProcessMessage + Send + 'static,
{
    #[instrument(skip(self, runtime), fields(python_paths = ?self.config.python_path, module_name = ?self.config.module_name, is_async = ?self.config.is_async))]
    fn init_with_runtime<'a>(&'a mut self, runtime: &'static tokio::runtime::Runtime) -> std::pin::Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>> {
        // Clone the data we need to move into the async block
        let python_paths = self.config.python_path.clone();
        let module_name = self.config.module_name.clone();

        Box::pin(async move {
            // 1. FIRST: Initialize Python interpreter for multi-threading
            tracing::info!("Preparing freethreaded Python");
            pyo3::prepare_freethreaded_python();

            // 2. Initialize pyo3-async-runtimes with the provided runtime
            tracing::info!("Initializing pyo3-async-runtimes with provided runtime");
            if let Err(e) = pyo3_async_runtimes::tokio::init_with_runtime(runtime) {
                error!("Failed to initialize pyo3-async-runtimes: {:?}", e);
                return Err(PythonExecutionError::RuntimeError {
                    message: format!("Failed to initialize pyo3-async-runtimes: {:?}", e),
                });
            }

            // 3. Set up Python paths and verify module
            tracing::info!("Setting up Python paths and verifying module");
            let module_result = Python::with_gil(|py| {
                // Add Python paths to sys.path
                let sys = py.import("sys")?;
                let path = sys.getattr("path")?;
                for p in python_paths {
                    tracing::debug!(path = ?p, "Adding Python path");
                    path.call_method1("append", (p,))?;
                }

                // Import the module to verify it exists
                tracing::debug!(module = ?module_name, "Importing module");
                match py.import(&module_name) {
                    Ok(_) => Ok(()),
                    Err(e) => {
                        error!(error = ?e, "Failed to import Python module");
                        // Convert PyErr to our error type and return it
                        Err(PythonExecutionError::from(e))
                    }
                }
            });

            // If module import failed, return the error which will trigger actor shutdown
            if let Err(e) = module_result {
                error!("Module initialization failed, triggering shutdown: {:?}", e);
                return Err(e);
            }

            tracing::info!("Python runtime initialization complete");
            Ok(())
        })
    }
}

pub mod prelude {
    pub use super::{
        PythonActor, PythonChildProcessBuilder, PythonConfig, PythonExecutionError,
    };
}
