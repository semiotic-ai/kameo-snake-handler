use std::future::Future;
use std::marker::PhantomData;
use std::ops::ControlFlow;
use std::thread;
use std::process;
use std::fs;
use std::ffi::CString;

use anyhow::Result;
use async_trait::async_trait;
use bincode::{Decode, Encode};
use kameo::actor::{Actor, ActorRef, WeakActorRef};
use kameo::error::{ActorStopReason, PanicError};
use kameo::message::{Context, Message};
use kameo_child_process::{
    CallbackHandle, CallbackHandler, ChildCallbackMessage, ChildProcessBuilder, KameoChildProcessMessage, SubprocessActor, RuntimeConfig, RuntimeFlavor, RuntimeAware
};
use pyo3::exceptions::{
    PyAttributeError, PyImportError, PyModuleNotFoundError, PyRuntimeError, PyTypeError,
    PyValueError,
};
use pyo3::prelude::*;
use pyo3::pyclass;
use pyo3::pymethods;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{error, info, instrument, Level};
use pyo3::Python;
use kameo_child_process::ChildProcessMessageHandler;
use kameo_child_process::run_child_actor_loop;

pub mod serde_py;
pub use serde_py::{from_pyobject, to_pyobject, FromPyAny};

/// Trait for creating a reply from a Python execution error
pub trait ErrorReply: Sized {
    fn from_error(err: PythonExecutionError) -> Self;
}

/// Error type for Python execution
#[derive(Debug, Error, Serialize, Deserialize, Encode, Decode, Clone)]
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

impl PythonExecutionError {
    pub fn from_pyerr(err: PyErr, py: Python) -> Self {
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
    }
}

impl From<PyErr> for PythonExecutionError {
    fn from(err: PyErr) -> Self {
        Python::with_gil(|py| {
            Self::from_pyerr(err, py)
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
    pub env_vars: Vec<(String, String)>,
    pub is_async: bool,
    pub module_path: String,
}

/// Python subprocess actor
#[derive(Debug)]
pub struct PythonActor<M, C: ChildCallbackMessage> {
    config: PythonConfig,
    py_function: PyObject,
    callback_handle: Option<CallbackHandle<C>>,
    _phantom: PhantomData<M>,
}

impl<M, C: ChildCallbackMessage> PythonActor<M, C> {
    pub fn new(config: PythonConfig, py_function: PyObject) -> Self {
        Self {
            config,
            py_function,
            callback_handle: None,
            _phantom: PhantomData,
        }
    }
}

impl<M, C: ChildCallbackMessage> Default for PythonActor<M, C> {
    fn default() -> Self {
        panic!("PythonActor<M, C> should not be constructed with Default in production; use new() with required fields.")
    }
}

/// Builder for a Python child process
/// NOTE: For PythonActor, use the macro-based entrypoint (setup_python_subprocess_system!). This builder is not supported for PythonActor.
#[derive(Debug)]
pub struct PythonChildProcessBuilder<C: ChildCallbackMessage + Sync> {
    python_config: PythonConfig,
    log_level: Level,
    _phantom: PhantomData<C>,
}

impl<C: ChildCallbackMessage + Sync> PythonChildProcessBuilder<C> {
    /// Creates a new builder with the given Python configuration.
    #[instrument]
    pub fn new(mut python_config: PythonConfig) -> Self {
        // Always set PYTHONPATH from python_path
        let joined_path = python_config.python_path.join(":");
        // Only add if not already present in env_vars
        if !python_config.env_vars.iter().any(|(k, _)| k == "PYTHONPATH") {
            python_config.env_vars.push(("PYTHONPATH".to_string(), joined_path));
        }
        Self {
            python_config,
            log_level: Level::INFO,
            _phantom: PhantomData,
        }
    }

    /// Sets the log level for the child process.
    pub fn log_level(mut self, level: Level) -> Self {
        self.log_level = level;
        self
    }

    /// Spawns a Python child process actor and returns an ActorRef for messaging.
    /// This is the only supported way to spawn a Python child process actor from the parent.
    pub async fn spawn<M>(&self) -> std::io::Result<kameo::actor::ActorRef<kameo_child_process::SubprocessActor<M, C, PythonExecutionError>>>
    where
        M: KameoChildProcessMessage + Send + Sync + 'static,
        <M as KameoChildProcessMessage>::Reply:
            serde::Serialize + for<'de> serde::Deserialize<'de> + bincode::Encode + bincode::Decode<()> + std::fmt::Debug + Send + Sync + 'static,
    {
        use kameo_child_process::ChildProcessBuilder;
        // Serialize the PythonConfig as JSON for the child
        let config_json = serde_json::to_string(&self.python_config)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("Failed to serialize PythonConfig: {e}")))?;

        // Set the actor name to the message type name
        let mut builder = ChildProcessBuilder::<kameo_child_process::SubprocessActor<M, C, PythonExecutionError>, M, C, PythonExecutionError>::new()
            .with_actor_name(std::any::type_name::<crate::PythonActor<M, C>>())
            .log_level(self.log_level.clone())
            .with_env_var("KAMEO_PYTHON_CONFIG", config_json);
        let (actor_ref, _callback_receiver) = builder.spawn(NoopCallbackHandler).await?;
        Ok(actor_ref)
    }
}

#[async_trait]
impl<M, C: ChildCallbackMessage> Actor for PythonActor<M, C>
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
            tracing::info!(status = "started", actor_type = "PythonActor");
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
            tracing::error!(status = "stopped", actor_type = "PythonActor", ?reason);
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
            tracing::error!(status = "panicked", actor_type = "PythonActor", ?err);
            Ok(ControlFlow::Break(ActorStopReason::Panicked(err)))
        }
    }
}

#[async_trait]
impl<M, C: ChildCallbackMessage> Message<M> for PythonActor<M, C>
where
    M: KameoChildProcessMessage + Send + 'static,
{
    type Reply = Result<<M as KameoChildProcessMessage>::Reply, PythonExecutionError>;
    fn handle(&mut self, message: M, _ctx: &mut kameo::message::Context<Self, Self::Reply>) -> impl std::future::Future<Output = Self::Reply> + Send {
        self.handle_child_message(message)
    }
}

use kameo_child_process::CallbackSender;
#[async_trait]
impl<M, C: ChildCallbackMessage> CallbackSender<C> for PythonActor<M, C>
where
    M: KameoChildProcessMessage + Send + 'static,
{
    fn set_callback_handle(&mut self, handle: CallbackHandle<C>) {
        self.callback_handle = Some(handle);
    }
}

// Re-export for macro hygiene
pub use kameo_child_process;
// NOTE: Reply trait is private in kameo_child_process, so use fully qualified path in trait bounds.

#[macro_export]
macro_rules! setup_python_subprocess_system {
    (
        actors = { $(($actor:ty, $msg:ty, $callback:ty)),* $(,)? },
        child_init = $child_init:block,
        parent_init = $parent_init:block $(,)?
    ) => {
        /// Entrypoint macro for setting up a Python subprocess actor system.
        /// - `child_init` **must return a `kameo_child_process::RuntimeConfig`**.
        ///   Only this config is supported for runtime setup.
        ///   Use `RuntimeFlavor::CurrentThread` or `MultiThread` and set `worker_threads` as needed.
        fn main() -> Result<(), Box<dyn std::error::Error>> {
            let handlers: &[(&'static str, fn() -> Result<(), Box<dyn std::error::Error>>)] = &[
                $(
                    (
                        std::any::type_name::<$actor>(),
                        || {
                            use tracing::{info, debug, error, instrument};
                            #[instrument(name = "python_subprocess_entry", skip_all)]
                            fn python_entry() -> (kameo_snake_handler::PythonConfig, pyo3::PyObject) {
                                Python::with_gil(|py| {
                                    let config_json = std::env::var("KAMEO_PYTHON_CONFIG").expect("KAMEO_PYTHON_CONFIG must be set in child");
                                    let config: kameo_snake_handler::PythonConfig = serde_json::from_str(&config_json).expect("Failed to parse KAMEO_PYTHON_CONFIG");
                                    debug!(module_name = %config.module_name, function_name = %config.function_name, python_path = ?config.python_path, "Deserialized PythonConfig");
                                    let sys_path = py.import("sys").expect("import sys").getattr("path").expect("get sys.path");
                                    for path in &config.python_path {
                                        sys_path.call_method1("append", (path,)).expect("append python_path");
                                        debug!(added_path = %path, "Appended to sys.path");
                                    }
                                    let module = py.import(&config.module_name).expect("import module");
                                    debug!(module = %config.module_name, "Imported Python module");
                                    let function = module.getattr(&config.function_name).expect("getattr function");
                                    debug!(function = %config.function_name, "Located Python function");
                                    (config, function.into())
                                })
                            }
                            let gil_result = std::panic::catch_unwind(|| python_entry());
                            match gil_result {
                                Ok((config, py_function)) => {
                                    let runtime_config = { $child_init };
                                    let mut builder = match runtime_config.flavor {
                                        kameo_child_process::RuntimeFlavor::CurrentThread => {
                                            let mut b = tokio::runtime::Builder::new_current_thread();
                                            b.enable_all();
                                            b
                                        }
                                        kameo_child_process::RuntimeFlavor::MultiThread => {
                                            let mut b = tokio::runtime::Builder::new_multi_thread();
                                            b.enable_all();
                                            if let Some(threads) = runtime_config.worker_threads {
                                                b.worker_threads(threads);
                                            }
                                            b
                                        }
                                    };
                                    kameo_snake_handler::setup_python_runtime(builder);
                                    pyo3::Python::with_gil(|py| {
                                        pyo3_async_runtimes::tokio::run(py, async move {
                                            info!("Entered async block: Tokio runtime should be alive");
                                            let rust_thread_id = std::thread::current().id();
                                            let py_thread_id = Python::with_gil(|py| {
                                                let threading = py.import("threading").unwrap();
                                                threading.call_method0("get_ident").unwrap().extract::<u64>().unwrap()
                                            });
                                            debug!(?rust_thread_id, py_thread_id, "Thread IDs at start of async block");
                                            let actor = <$actor>::new(config, py_function);
                                            $crate::child_process_main_with_python_actor::<$msg, $callback>(actor).await
                                                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("{e}")))
                                        })
                                        .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)
                                    })
                                }
                                Err(e) => {
                                    error!(?e, "Panic after GIL closure");
                                    std::process::exit(1);
                                }
                            }
                        }
                    ),
                )*
            ];
            if let Ok(actor_name) = std::env::var("KAMEO_CHILD_ACTOR") {
                for (name, handler) in handlers {
                    if actor_name == *name {
                        return handler();
                    }
                }
                return Err(format!("Unknown actor type: {}", actor_name).into());
            }
            $parent_init
        }
    };
}

#[async_trait]
impl<M, C: ChildCallbackMessage> RuntimeAware for PythonActor<M, C>
where
    M: KameoChildProcessMessage + Send + 'static,
{
    async fn init_with_runtime(self) -> Result<Self, Self::Error> {
        // Any actor-specific setup can go here
        Ok(self)
    }
}

pub fn setup_python_runtime(builder: tokio::runtime::Builder) {
    tracing::info!(event = "python_subprocess", step = "init_pyo3_async_runtime");
    pyo3::prepare_freethreaded_python();
    pyo3_async_runtimes::tokio::init(builder);
    tracing::info!(event = "python_subprocess", step = "init_done");
}

pub mod prelude {
    pub use super::{
        PythonActor, PythonChildProcessBuilder, PythonConfig, PythonExecutionError, setup_python_runtime
    };
}

/// Python-specific child process main entrypoint. Does handshake, sets callback, calls init_with_runtime, and runs the actor loop.
// NOTE: We do NOT bound Reply here, as it's private and PythonExecutionError is already fully serializable and debuggable.
#[instrument(skip(actor), name = "child_process_main_with_python_actor")]
pub async fn child_process_main_with_python_actor<M, C>(mut actor: PythonActor<M, C>) -> Result<(), Box<dyn std::error::Error>>
where
    M: KameoChildProcessMessage + Send + 'static,
    C: kameo_child_process::ChildCallbackMessage + Send + 'static,
    <PythonActor<M, C> as ChildProcessMessageHandler<M>>::Reply:
        Serialize
        + for<'de> Deserialize<'de>
        + Encode
        + Decode<()> 
        + Send
        + std::fmt::Debug
        + 'static,
{
    use kameo_child_process::{handshake, CallbackHandle, run_child_actor_loop};
    tracing::info!("child_process_main_with_python_actor: about to handshake");
    let request_conn = handshake::child_request().await?;
    tracing::info!("Child about to connect callback socket");
    let callback_conn = handshake::child_callback().await?;
    let handle = CallbackHandle::new(callback_conn);
    actor.set_callback_handle(handle);
    let mut actor = actor.init_with_runtime().await.map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;
    tracing::info!("running child actor loop");
    run_child_actor_loop::<PythonActor<M, C>, M>(&mut actor, request_conn).await?;
    Ok(())
}

#[async_trait]
impl<M, C: ChildCallbackMessage> ChildProcessMessageHandler<M> for PythonActor<M, C>
where
    M: KameoChildProcessMessage + Send + 'static,
{
    type Reply = Result<<M as KameoChildProcessMessage>::Reply, PythonExecutionError>;
    async fn handle_child_message(&mut self, msg: M) -> Self::Reply {
        use crate::serde_py::to_pyobject;
        use crate::serde_py::from_pyobject;
        use pyo3::prelude::*;
        use pyo3_async_runtimes::tokio::into_future;

        tracing::debug!(event = "python_call", step = "serialize", "Serializing Rust message to Python");
        let is_async = self.config.is_async;
        let function_name = self.config.function_name.clone();
        let py_function = &self.py_function;
        let py_msg = Python::with_gil(|py| to_pyobject(py, &msg));
        let py_msg = match py_msg {
            Ok(obj) => obj,
            Err(e) => return Err(PythonExecutionError::SerializationError { message: e.to_string() }),
        };
        if is_async {
            // Async Python function: create coroutine and future inside GIL
            let fut_result = Python::with_gil(|py| {
                let py_func = py_function.bind(py);
                let coro = match py_func.call1((py_msg,)) {
                    Ok(coro) => coro,
                    Err(e) => return Err(PythonExecutionError::CallError { function: function_name.clone(), message: e.to_string() }),
                };
                match into_future(coro) {
                    Ok(fut) => Ok(fut),
                    Err(e) => Err(PythonExecutionError::from(e)),
                }
            });
            let fut = match fut_result {
                Ok(fut) => fut,
                Err(e) => return Err(e),
            };
            let py_output = match fut.await {
                Ok(obj) => obj,
                Err(e) => return Err(PythonExecutionError::from(e)),
            };
            // Deserialize Python result to Rust
            let rust_result = Python::with_gil(|py| {
                let bound = py_output.bind(py);
                from_pyobject(&bound)
                    .map_err(|e| PythonExecutionError::DeserializationError { message: e.to_string() })
            });
            match rust_result {
                Ok(reply) => Ok(reply),
                Err(e) => Err(e),
            }
        } else {
            // Sync Python function: call and get result
            let rust_result = Python::with_gil(|py| {
                let py_func = py_function.bind(py);
                match py_func.call1((py_msg,)) {
                    Ok(result) => from_pyobject(&result)
                        .map_err(|e| PythonExecutionError::DeserializationError { message: e.to_string() }),
                    Err(e) => Err(PythonExecutionError::CallError { function: function_name.clone(), message: e.to_string() }),
                }
            });
            match rust_result {
                Ok(reply) => Ok(reply),
                Err(e) => Err(e),
            }
        }
    }
}

// Add a NoopCallbackHandler for callback IPC
pub struct NoopCallbackHandler;

#[async_trait::async_trait]
impl<C: ChildCallbackMessage + Sync> CallbackHandler<C> for NoopCallbackHandler
where
    C::Reply: Send,
{
    async fn handle(&mut self, callback: C) -> C::Reply {
        tracing::info!(event = "noop_callback", ?callback, "NoopCallbackHandler received callback");
        panic!("NoopCallbackHandler called but no Default for callback reply; implement your own handler if you need a real reply");
    }
}

impl kameo_child_process::ProtocolError for PythonExecutionError {
    fn Protocol(msg: String) -> Self {
        PythonExecutionError::ExecutionError { message: msg }
    }
    fn HandshakeFailed(msg: String) -> Self {
        PythonExecutionError::ExecutionError { message: format!("Handshake failed: {msg}") }
    }
    fn ConnectionClosed() -> Self {
        PythonExecutionError::ExecutionError { message: "Connection closed".to_string() }
    }
}

