use std::future::Future;
use std::marker::PhantomData;
use anyhow::Result;
use async_trait::async_trait;
use kameo::prelude::*;
use kameo_child_process::KameoChildProcessMessage;
use pyo3::prelude::*;
use serde::{Serialize, Deserialize};
use bincode::{Encode, Decode};
use tracing::{debug, info, instrument};
use thiserror::Error;
use std::process;

/// Error type for Python execution
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode, Error)]
pub enum PythonExecutionError {
    #[error("Python error: {0}")]
    Python(String),
    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),
    #[error("Execution error: {0}")]
    Execution(String),
    #[error("Serialization error: {0}")]
    SerializationError(String),
    #[error("Deserialization error: {0}")]
    DeserializationError(String),
}

impl From<PyErr> for PythonExecutionError {
    fn from(err: PyErr) -> Self {
        Self::Python(err.to_string())
    }
}

/// Configuration for Python execution environment
#[derive(Debug, Clone)]
pub struct PythonConfig {
    /// Python path to add to sys.path
    pub python_path: Vec<String>,
    /// Environment variables to set
    pub env_vars: Vec<(String, String)>,
    /// Python module to import
    pub module_name: String,
    /// Python function to call
    pub function_name: String,
}

impl Default for PythonConfig {
    fn default() -> Self {
        Self {
            python_path: vec![],
            env_vars: vec![],
            module_name: String::new(),
            function_name: String::new(),
        }
    }
}

/// An actor that executes Python code to handle messages
#[derive(Clone)]
pub struct PythonActor<M> 
where
    M: KameoChildProcessMessage + Send + Sync + Clone + 'static,
{
    config: PythonConfig,
    _phantom: PhantomData<M>,
}

impl<M> Default for PythonActor<M>
where
    M: KameoChildProcessMessage + Send + Sync + Clone + 'static,
{
    fn default() -> Self {
        Self {
            config: PythonConfig::default(),
            _phantom: PhantomData,
        }
    }
}

impl<M> PythonActor<M>
where
    M: KameoChildProcessMessage + Send + Sync + Clone + 'static,
{
    /// Create a new Python actor
    pub fn new(config: PythonConfig) -> Self {
        Self {
            config,
            _phantom: PhantomData,
        }
    }

    /// Initialize Python environment
    #[instrument(skip(self))]
    pub fn init_python(&self) -> Result<(), PythonExecutionError> {
        let pid = process::id();
        Python::with_gil(|py| {
            // Add paths to sys.path
            let sys = py.import("sys")?;
            let sys_path = sys.getattr("path")?;
            
            // Print current sys.path for debugging
            let path_list = sys_path.extract::<Vec<String>>()?;
            debug!(pid=%pid, "Python sys.path: {:?}", path_list);
            
            for path in &self.config.python_path {
                debug!(pid=%pid, "Adding path to sys.path: {}", path);
                sys_path.call_method1("append", (path,))?;
            }

            // Set environment variables
            if !self.config.env_vars.is_empty() {
                let os = py.import("os")?;
                let environ = os.getattr("environ")?;
                for (key, value) in &self.config.env_vars {
                    debug!(pid=%pid, "Setting env var: {}={}", key, value);
                    environ.set_item(key, value)?;
                }
            }

            // Redirect Python stdout to tracing
            let io = py.import("io")?;
            let string_io = io.getattr("StringIO")?.call0()?;
            sys.setattr("stdout", string_io)?;

            Ok(())
        })
    }

    /// Handle a message by calling the configured Python function
    #[instrument(skip(self, msg))]
    async fn handle_message(&self, msg: M) -> Result<M::Reply, PythonExecutionError> {
        let pid = process::id();
        Python::with_gil(|py| {
            // Import the configured module
            let module = py.import(&self.config.module_name)?;
            
            // Convert message to Python dict
            let msg_json = serde_json::to_string(&msg)
                .map_err(|e| PythonExecutionError::Execution(format!("Failed to serialize message: {}", e)))?;
            debug!(pid=%pid, "Sending message to Python: {}", msg_json);
            
            // Create a dict with type and data
            let json = py.import("json")?;
            let msg_dict = json.getattr("loads")?.call1((msg_json,))?;
            
            // Call the configured function with the dict
            let result = module.getattr(&self.config.function_name)?
                .call1((msg_dict,))?;

            // Convert result back to Rust
            let json_dumps = json.getattr("dumps")?.call1((result,))?;
            let result_str = json_dumps.extract::<String>()?;
            debug!(pid=%pid, "Received response from Python: {}", result_str);
            let reply = M::Reply::from(serde_json::from_str(&result_str)
                .map_err(|e| PythonExecutionError::Execution(format!("Failed to deserialize response: {}", e)))?);
            
            Ok(reply)
        })
    }
}

impl<T> Actor for PythonActor<T>
where
    T: KameoChildProcessMessage + Send + Sync + Clone + 'static,
{
    type Error = PythonExecutionError;

    fn name() -> &'static str {
        "PythonActor"
    }

    #[instrument(skip(self, _actor_ref))]
    fn on_start(&mut self, _actor_ref: ActorRef<Self>) -> impl Future<Output = Result<(), Self::Error>> + Send {
        let pid = process::id();
        async move {
            info!(pid=%pid, "Starting Python actor");
            self.init_python()?;
            Ok(())
        }
    }

    #[instrument(skip(self, _actor_ref, _reason))]
    fn on_stop(&mut self, _actor_ref: WeakActorRef<Self>, _reason: ActorStopReason) -> impl Future<Output = Result<(), Self::Error>> + Send {
        let pid = process::id();
        async move {
            info!(pid=%pid, "Stopping Python actor");
            Ok(())
        }
    }
}

impl<T> Message<T> for PythonActor<T>
where
    T: KameoChildProcessMessage + Send + Sync + Clone + 'static,
{
    type Reply = Result<T::Reply, PythonExecutionError>;

    fn handle(&mut self, msg: T, _ctx: &mut Context<Self, Self::Reply>) -> impl Future<Output = Self::Reply> + Send {
        let pid = process::id();
        let config = self.config.clone();
        debug!(pid=%pid, "Handling message in Python");
        async move {
            Python::with_gil(|py| {
                // Import the configured module
                let module = py.import(&config.module_name)?;
                
                // Convert message to Python dict
                let msg_json = serde_json::to_string(&msg)
                    .map_err(|e| PythonExecutionError::SerializationError(e.to_string()))?;
                debug!(pid=%pid, "Sending message to Python: {}", msg_json);
                
                // Create a dict with type and data
                let json = py.import("json")?;
                let msg_dict = json.getattr("loads")?.call1((msg_json,))?;
                
                // Call the configured function with the dict
                let result = module.getattr(&config.function_name)?
                    .call1((msg_dict,))?;

                // Convert result back to Rust
                let json_dumps = json.getattr("dumps")?.call1((result,))?;
                let result_str = json_dumps.extract::<String>()?;
                debug!(pid=%pid, "Received response from Python: {}", result_str);
                
                let reply: T::Reply = serde_json::from_str(&result_str)
                    .map_err(|e| PythonExecutionError::DeserializationError(e.to_string()))?;
                
                Ok(reply)
            })
        }
    }
}

/// Builder for creating Python subprocess actors
pub struct PythonSubprocessBuilder<M>
where
    M: KameoChildProcessMessage + Send + Sync + Clone + 'static,
{
    config: PythonConfig,
    _phantom: PhantomData<M>,
}

impl<M> Default for PythonSubprocessBuilder<M>
where
    M: KameoChildProcessMessage + Send + Sync + Clone + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<M> PythonSubprocessBuilder<M>
where
    M: KameoChildProcessMessage + Send + Sync + Clone + 'static,
{
    pub fn new() -> Self {
        Self {
            config: PythonConfig::default(),
            _phantom: PhantomData,
        }
    }

    pub fn with_config(mut self, config: PythonConfig) -> Self {
        self.config = config;
        self
    }

    pub async fn spawn(self) -> std::io::Result<ActorRef<kameo_child_process::SubprocessActor<M>>> {
        let builder = kameo_child_process::ChildProcessBuilder::<PythonActor<M>, M>::new()
            .log_level(tracing::Level::DEBUG);
        
        // Spawn the subprocess actor
        builder.spawn().await
    }
}

// Export the prelude for easy access to common types
pub mod prelude {
    pub use super::{
        PythonActor,
        PythonConfig,
        PythonExecutionError,
        PythonSubprocessBuilder,
    };
    pub use kameo_child_process::KameoChildProcessMessage;
}

