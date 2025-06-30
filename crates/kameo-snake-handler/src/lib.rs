use std::future::Future;
use std::marker::PhantomData;
use std::ops::ControlFlow;
use std::thread;
use std::process;
use std::fs;
use std::ffi::CString;
use std::any::TypeId;

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
use once_cell::sync::OnceCell;
#[macro_export]
macro_rules! declare_callback_glue {
    ($type:ty) => {
        use kameo_snake_handler::serde_py::{from_pyobject, to_pyobject};
        static CALLBACK_HANDLE: once_cell::sync::OnceCell<std::sync::Arc<kameo_child_process::CallbackHandle<$type>>> = once_cell::sync::OnceCell::new();

        pub fn register_callback_glue(py: pyo3::Python<'_>) -> pyo3::PyResult<()> {
            use kameo_snake_handler::serde_py::{from_pyobject, to_pyobject};
            #[pyo3::pyfunction]
            fn callback_handle(py: pyo3::Python<'_>, py_msg: &pyo3::Bound<'_, pyo3::PyAny>) -> pyo3::PyResult<pyo3::Py<pyo3::PyAny>> {
                let handle = CALLBACK_HANDLE.get().cloned().ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("Callback handle not initialized yet"))?;
                let msg = match from_pyobject::<$type>(py_msg.as_ref()) {
                    Ok(m) => m,
                    Err(e) => return Err(pyo3::exceptions::PyValueError::new_err(format!("Failed to parse callback: {e}"))),
                };
                pyo3_async_runtimes::tokio::future_into_py(py, async move {
                    tracing::trace!(event = "child_callback_send", ?msg, "About to send callback message from child to parent");
                    let reply = handle.ask(msg).await;
                    tracing::trace!(event = "child_callback_send", ?reply, "Received reply from parent callback handler");
                    match reply {
                        Ok(val) => pyo3::Python::with_gil(|py| to_pyobject(py, &val)
                            .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("Failed to serialize reply: {e}")))),
                        Err(e) => Err(pyo3::exceptions::PyRuntimeError::new_err(format!("Callback error: {e:?}"))),
                    }
                }).map(|bound| bound.unbind())
            }
            let py_func = pyo3::wrap_pyfunction!(callback_handle, py)?;
            let sys = py.import("sys")?;
            let modules = sys.getattr("modules")?;
            let kameo_mod = match modules.get_item("kameo") {
                Ok(ref m) => {
                    let py_mod = m.downcast::<pyo3::types::PyModule>().unwrap();
                    py_mod.clone().unbind().into_any().into_bound(py)
                }
                Err(_) => {
                    let m = pyo3::types::PyModule::new(py, "kameo")?;
                    modules.set_item("kameo", &m)?;
                    m.unbind().into_any().into_bound(py)
                }
            };
            kameo_mod.setattr("callback_handle", py_func)?;
            Ok(())
        }

        pub fn set_callback_handle(handle: std::sync::Arc<kameo_child_process::CallbackHandle<$type>>) {
            let _ = CALLBACK_HANDLE.set(handle);
        }
    };
}

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
pub struct PythonChildProcessBuilder<C: ChildCallbackMessage + Sync, H = NoopCallbackHandler>
where
    H: CallbackHandler<C> + Send + Sync + 'static,
{
    python_config: PythonConfig,
    log_level: Level,
    _phantom: PhantomData<C>,
    callback_handler: H,
}

impl<C: ChildCallbackMessage + Sync> PythonChildProcessBuilder<C, NoopCallbackHandler> {
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
            callback_handler: NoopCallbackHandler,
        }
    }
}

impl<C: ChildCallbackMessage + Sync, H> PythonChildProcessBuilder<C, H>
where
    H: CallbackHandler<C> + Send + Sync + 'static,
{
    /// Sets the log level for the child process.
    pub fn log_level(mut self, level: Level) -> Self {
        self.log_level = level;
        self
    }

    /// Inject a custom callback handler for callback IPC.
    pub fn with_callback_handler<NH>(self, handler: NH) -> PythonChildProcessBuilder<C, NH>
    where
        NH: CallbackHandler<C> + Send + Sync + 'static,
    {
        PythonChildProcessBuilder {
            python_config: self.python_config,
            log_level: self.log_level,
            _phantom: PhantomData,
            callback_handler: handler,
        }
    }

    /// Spawns a Python child process actor and returns an ActorRef for messaging.
    /// This is the only supported way to spawn a Python child process actor from the parent.
    pub async fn spawn<M>(self) -> std::io::Result<kameo::actor::ActorRef<kameo_child_process::SubprocessActor<M, C, PythonExecutionError>>>
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
        let builder = ChildProcessBuilder::<kameo_child_process::SubprocessActor<M, C, PythonExecutionError>, M, C, PythonExecutionError>::new()
            .with_actor_name(std::any::type_name::<crate::PythonActor<M, C>>())
            .log_level(self.log_level.clone())
            .with_env_var("KAMEO_PYTHON_CONFIG", config_json);
        tracing::trace!(event = "py_spawn", step = "before_builder_spawn", "About to call builder.spawn for Python child process");
        let (actor_ref, callback_receiver) = builder.spawn(self.callback_handler).await?;
        tracing::trace!(event = "py_spawn", step = "after_builder_spawn", "Returned from builder.spawn, about to spawn callback_receiver.run()");
        tokio::spawn(callback_receiver.run());
        tracing::trace!(event = "py_spawn", step = "after_callback_spawn", "Spawned callback_receiver.run(), about to return actor_ref");
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
    #[tracing::instrument(skip(self, _ctx, message), fields(actor_type = "PythonActor", message_type = std::any::type_name::<M>()))]
    fn handle(&mut self, message: M, _ctx: &mut kameo::message::Context<Self, Self::Reply>) -> impl std::future::Future<Output = Self::Reply> + Send {
        tracing::trace!(event = "actor_recv", step = "handle_child_message", message_type = std::any::type_name::<M>(), "PythonActor received message");
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
        actors = { $(($actor:ty, $msg:ty, $callback:ty, $callback_handler:ty)),* $(,)? },
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
                            declare_callback_glue!($callback);
                            use tracing::{info, debug, error, instrument};
                            use std::sync::Arc;
                            use pyo3::prelude::*;
                            use pyo3_async_runtimes::tokio::future_into_py;
                            let runtime_config = { $child_init };
                            let mut builder = match runtime_config.flavor {
                                kameo_child_process::RuntimeFlavor::MultiThread => {
                                    let mut b = tokio::runtime::Builder::new_multi_thread();
                                    b.enable_all();
                                    if let Some(threads) = runtime_config.worker_threads {
                                        b.worker_threads(threads);
                                    }
                                    b
                                }
                                kameo_child_process::RuntimeFlavor::CurrentThread => {
                                    let mut b = tokio::runtime::Builder::new_current_thread();
                                    b.enable_all();
                                    b
                                }
                            };
                            kameo_snake_handler::setup_python_runtime(builder);
                            tracing::trace!(event = "child_entry", step = "before_gil", "Child about to enter GIL and Python setup");
                            pyo3::Python::with_gil(|py| {
                                tracing::trace!(event = "child_entry", step = "in_gil", "Child inside GIL, about to import module and function");
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
                                let function: Py<PyAny> = module.getattr(&config.function_name).expect("getattr function").unbind();
                                debug!(function = %config.function_name, "Located Python function");
                                register_callback_glue(py).expect("register_callback_glue failed");
                                let rust_thread_id = std::thread::current().id();
                                let py_thread_id = Python::with_gil(|py| {
                                    let threading = py.import("threading").unwrap();
                                    threading.call_method0("get_ident").unwrap().extract::<u64>().unwrap()
                                });
                                debug!(?rust_thread_id, py_thread_id, "Thread IDs at start of async block");
                                tracing::trace!(event = "child_entry", step = "before_async_block", "Child about to enter async block");
                                pyo3_async_runtimes::tokio::run(py, async move {
                                    tracing::trace!(event = "child_entry", step = "in_async_block", "Child inside async block, about to connect sockets");
                                    let request_socket_path = std::env::var("KAMEO_REQUEST_SOCKET").unwrap_or_else(|_| "<unset>".to_string());
                                    tracing::info!(event = "child_handshake", step = "before_child_request", socket = %request_socket_path, "Child about to connect to request socket");
                                    let request_conn = match kameo_child_process::handshake::child_request().await {
                                        Ok(conn) => {
                                            tracing::info!(event = "child_handshake", step = "after_child_request", socket = %request_socket_path, "Child connected to request socket");
                                            conn
                                        },
                                        Err(e) => {
                                            tracing::error!(event = "child_handshake", step = "child_request_failed", socket = %request_socket_path, error = %e, "Child failed to connect to request socket");
                                            return Err(pyo3::exceptions::PyRuntimeError::new_err(format!("child_request handshake failed: {e}")));
                                        }
                                    };
                                    let callback_conn = kameo_child_process::handshake::child_callback().await.expect("child_callback handshake failed");
                                    let handle = kameo_child_process::CallbackHandle::new(callback_conn);
                                    set_callback_handle(std::sync::Arc::new(handle));
                                    info!("Child connected to both sockets and set callback handle");
                                    let actor = <$actor>::new(config, function);
                                    tracing::trace!(event = "child_entry", step = "before_main_actor", "Child about to call child_process_main_with_python_actor");
                                    let result = $crate::child_process_main_with_python_actor::<$msg, $callback>(actor, request_conn).await;
                                    tracing::trace!(event = "child_entry", step = "after_main_actor", ?result, "Child returned from child_process_main_with_python_actor");
                                    result.map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("{e}")))
                                })
                                .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)
                            })
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
            tracing::trace!(event = "parent_entry", step = "before_parent_init", "Parent about to enter parent_init block");
            $parent_init
            tracing::trace!(event = "parent_entry", step = "after_parent_init", "Parent returned from parent_init block");
            Ok(())
        }
    };
    // Fallback for 3-tuple: default to NoopCallbackHandler
    (
        actors = { $(($actor:ty, $msg:ty, $callback:ty)),* $(,)? },
        child_init = $child_init:block,
        parent_init = $parent_init:block $(,)?
    ) => {
        $crate::setup_python_subprocess_system! {
            actors = { $(($actor, $msg, $callback, $crate::NoopCallbackHandler)),* },
            child_init = $child_init,
            parent_init = $parent_init,
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

#[tracing::instrument(skip(builder), name = "setup_python_runtime")]
pub fn setup_python_runtime(builder: tokio::runtime::Builder) {
    pyo3::prepare_freethreaded_python();
    pyo3_async_runtimes::tokio::init(builder);
}

pub mod prelude {
    pub use super::{
        PythonActor, PythonChildProcessBuilder, PythonConfig, PythonExecutionError, setup_python_runtime
    };
}

/// Python-specific child process main entrypoint. Does handshake, sets callback, calls init_with_runtime, and runs the actor loop.
// NOTE: We do NOT bound Reply here, as it's private and PythonExecutionError is already fully serializable and debuggable.
#[instrument(skip(actor, request_conn), name = "child_process_main_with_python_actor")]
pub async fn child_process_main_with_python_actor<M, C>(mut actor: PythonActor<M, C>, mut request_conn: Box<dyn kameo_child_process::AsyncReadWrite>) -> Result<(), Box<dyn std::error::Error>>
where
    M: KameoChildProcessMessage + Send + Sync + 'static,
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
    use kameo_child_process::{run_child_actor_loop, perform_handshake, ProtocolError};
    tracing::info!("child_process_main_with_python_actor: about to handshake");
    // Perform handshake as child
    perform_handshake::<M, crate::PythonExecutionError>(&mut request_conn, false).await?;
    // Callback handle is set and injected in the macro branch for the child process
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
                serde_py::from_pyobject(&bound)
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
                    Ok(result) => serde_py::from_pyobject(&result)
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
impl<C: ChildCallbackMessage + Sync + 'static> CallbackHandler<C> for NoopCallbackHandler
where
    C::Reply: Send,
{
    async fn handle(&mut self, callback: C) -> C::Reply {
        tracing::trace!(event = "callback_handler", ?callback, "NoopCallbackHandler received callback");
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

