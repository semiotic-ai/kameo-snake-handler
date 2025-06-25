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

/// A default implementation of a callback message.
/// Users can define their own types that implement `ChildCallbackMessage`.
#[derive(Serialize, Deserialize, Encode, Decode, Debug, Clone)]
pub enum DefaultCallbackMessage {
    Event {
        name: String,
        properties: std::collections::HashMap<String, String>,
    },
    Log {
        level: String,
        message: String,
    },
}

impl ChildCallbackMessage for DefaultCallbackMessage {
    type Reply = ();
}

#[pyclass(unsendable)]
struct KameoCallbackHandle {
    handle: CallbackHandle<DefaultCallbackMessage>,
}

#[pymethods]
impl KameoCallbackHandle {
    #[pyo3(name = "ask")]
    fn ask_py<'py>(&self, py: Python<'py>, message: PyObject) -> PyResult<Bound<'py, PyAny>> {
        let handle = self.handle.clone();
        let bound_message = message.bind(py);
        let callback_message: DefaultCallbackMessage = from_pyobject(bound_message)
            .map_err(|e| PyValueError::new_err(format!("Failed to deserialize callback message: {}", e)))?;

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            match handle.ask(callback_message).await {
                Ok(reply) => Python::with_gil(|py| {
                    to_pyobject(py, &reply).map_err(|e| PyRuntimeError::new_err(e.to_string()))
                }),
                Err(e) => Err(PyRuntimeError::new_err(format!("Callback failed: {}", e))),
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
    pub env_vars: Vec<(String, String)>,
    pub is_async: bool,
    pub module_path: String,
}

/// Python subprocess actor
#[derive(Debug)]
pub struct PythonActor<M> {
    config: PythonConfig,
    callback_handle: Option<CallbackHandle<DefaultCallbackMessage>>,
    _phantom: PhantomData<M>,
}

impl<M> Default for PythonActor<M> {
    #[instrument]
    fn default() -> Self {
        info!("Initializing PythonActor from environment");
        let config = match std::env::var("KAMEO_PYTHON_CONFIG") {
            Ok(config_json) => {
                info!(config_json, "Found KAMEO_PYTHON_CONFIG");
                match serde_json::from_str(&config_json) {
                    Ok(cfg) => cfg,
                    Err(e) => {
                        // This is a fatal error: process cannot continue without config
                        panic!("Failed to deserialize python config from env: {e}");
                    }
                }
            }
            Err(e) => {
                error!(error = ?e, "KAMEO_PYTHON_CONFIG not set");
                panic!("KAMEO_PYTHON_CONFIG must be set for PythonActor");
            }
        };

        Self {
            config,
            callback_handle: None,
            _phantom: PhantomData,
        }
    }
}

impl<M> Clone for PythonActor<M> {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            callback_handle: self.callback_handle.clone(),
            _phantom: PhantomData,
        }
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
        M: KameoChildProcessMessage + Send + Sync + 'static + std::panic::UnwindSafe,
        <M as KameoChildProcessMessage>::Reply: ErrorReply,
    {
        info!("Spawning Python child process");
        let config_json = serde_json::to_string(&self.python_config)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        struct NoOpCallbackHandler;

        #[async_trait]
        impl CallbackHandler<DefaultCallbackMessage> for NoOpCallbackHandler {
            async fn handle(&mut self, _callback: DefaultCallbackMessage) -> () {
                // This handler does nothing.
            }
        }

        let actor_builder = ChildProcessBuilder::<PythonActor<M>, M, DefaultCallbackMessage>::new()
            .log_level(self.log_level)
            .with_env_var("KAMEO_PYTHON_CONFIG", config_json);

        let (actor_ref, callback_receiver) = actor_builder
            .spawn(NoOpCallbackHandler)
            .await
            .map_err(|e: std::io::Error| anyhow::anyhow!(e))?;

        tokio::spawn(async move {
            if let Err(e) = callback_receiver.run().await {
                error!("Callback receiver failed: {:?}", e);
            }
        });

        Ok(actor_ref)
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
            tracing::info!(event = "lifecycle", status = "started", actor_type = "PythonActor");
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
            tracing::error!(event = "lifecycle", status = "stopped", actor_type = "PythonActor", reason = ?reason);
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
            tracing::error!(event = "lifecycle", status = "panicked", actor_type = "PythonActor", error = ?err);
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
                // Step 1: GIL - setup, import, call, get coroutine, get Rust future
                let fut = Python::with_gil(|py| {
                    let sys = py.import("sys")?;
                    let sys_path_binding = sys.getattr("path")?;
                    let sys_path = sys_path_binding.downcast::<pyo3::types::PyList>().map_err(pyo3::PyErr::from)?;
                    for path in &config.python_path {
                        sys_path.call_method1("append", (path,))?;
                    }
                    let os = py.import("os")?;
                    let cwd: String = os.call_method0("getcwd")?.extract()?;
                    let sys_path_vec: Vec<String> = sys_path.extract()?;
                    let executable: String = sys.getattr("executable")?.extract()?;
                    tracing::info!(cwd, sys_path=?sys_path_vec, executable, "ORKY DEBUG: Python import context");
                    let module = py.import(&config.module_name)?;
                    let func = module.getattr(&config.function_name)?;
                    let py_message = match crate::serde_py::to_pyobject(py, &message) {
                        Ok(obj) => obj,
                        Err(e) => return Err(PythonExecutionError::SerializationError { message: e.to_string() }),
                    };
                    let py_coro = func.call1((py_message,))?;
                    let type_name = py_coro.get_type().name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_else(|_| "<unknown>".to_string());
                    tracing::info!(function = ?config.function_name, type_name = ?type_name, "ORKY DEBUG: Python function return type");
                    pyo3_async_runtimes::tokio::into_future(py_coro)
                        .map_err(|e| PythonExecutionError::ExecutionError { message: e.to_string() })
                }).map_err(|e| PythonExecutionError::ExecutionError { message: e.to_string() })?;

                // Step 2: Await the Rust future
                let py_result = fut.await.map_err(|e| PythonExecutionError::ExecutionError { message: e.to_string() })?;

                // Step 3: GIL - convert result
                let py_obj = Python::with_gil(|py| {
                    let bound = py_result.bind(py);
                    let ref_bound = &bound;
                    crate::serde_py::from_pyobject(ref_bound)
                });
                py_obj.map_err(|e| PythonExecutionError::DeserializationError { message: e.to_string() })
            } else {
                // Sync path
                let py_obj = Python::with_gil(|py| {
                    let sys = py.import("sys")?;
                    let sys_path_binding = sys.getattr("path")?;
                    let sys_path = sys_path_binding.downcast::<pyo3::types::PyList>().map_err(pyo3::PyErr::from)?;
                    for path in &config.python_path {
                        sys_path.call_method1("append", (path,))?;
                    }
                    let os = py.import("os")?;
                    let cwd: String = os.call_method0("getcwd")?.extract()?;
                    let sys_path_vec: Vec<String> = sys_path.extract()?;
                    let executable: String = sys.getattr("executable")?.extract()?;
                    tracing::info!(cwd, sys_path=?sys_path_vec, executable, "ORKY DEBUG: Python import context");
                    let module = py.import(&config.module_name)?;
                    let func = module.getattr(&config.function_name)?;
                    let py_message = match crate::serde_py::to_pyobject(py, &message) {
                        Ok(obj) => obj,
                        Err(e) => return Err(PythonExecutionError::SerializationError { message: e.to_string() }),
                    };
                    let result = func.call1((py_message,))?;
                    let type_name = result.get_type().name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_else(|_| "<unknown>".to_string());
                    tracing::info!(function = ?config.function_name, type_name = ?type_name, "ORKY DEBUG: Python function return type (sync)");
                    let py_obj = result.unbind();
                    Ok(py_obj)
                }).map_err(|e| PythonExecutionError::DeserializationError { message: e.to_string() })?;
                let value = Python::with_gil(|py| {
                    let bound = py_obj.bind(py);
                    crate::serde_py::from_pyobject(&bound)
                });
                value.map_err(|e| PythonExecutionError::DeserializationError { message: e.to_string() })
            }
        }
    }
}

use kameo_child_process::CallbackSender;
#[async_trait]
impl<M> CallbackSender<DefaultCallbackMessage> for PythonActor<M>
where
    M: KameoChildProcessMessage + Send + 'static,
{
    fn set_callback_handle(&mut self, handle: kameo_child_process::CallbackHandle<DefaultCallbackMessage>) {
        self.callback_handle = Some(handle);
    }
}

/// Import the Python module and function on the main thread, after entering the runtime and acquiring the GIL.
#[tracing::instrument(
    name = "import_python_function_on_main_thread",
    skip(config),
    fields(
        module = %config.module_name,
        python_path = ?std::env::var("PYTHONPATH").ok(),
        cwd = ?std::env::current_dir().ok(),
    )
)]
pub fn import_python_function_on_main_thread(config: &PythonConfig) -> Result<PyObject, PythonExecutionError> {
    Python::with_gil(|py| {
        tracing::info!(event = "python_import", step = "with_gil", thread_id = ?std::thread::current().id(), "Importing Python module on main thread (import)");
        let sys = py.import("sys").map_err(|e| PythonExecutionError::ImportError { module: "sys".to_string(), message: e.to_string() })?;
        let sys_path_binding = sys.getattr("path")?;
        let sys_path = sys_path_binding.downcast::<pyo3::types::PyList>().map_err(|e| PythonExecutionError::ImportError { module: "sys".to_string(), message: format!("failed to downcast sys.path: {e}") })?;
        for path in &config.python_path {
            sys_path.call_method1("append", (path,)).map_err(|e| PythonExecutionError::ImportError { module: "sys".to_string(), message: format!("failed to append path: {e}") })?;
        }
        let os = py.import("os").map_err(|e| PythonExecutionError::ImportError { module: "os".to_string(), message: e.to_string() })?;
        let cwd = os.call_method0("getcwd").map_err(|e| PythonExecutionError::ImportError { module: "os".to_string(), message: e.to_string() })?.extract::<String>().map_err(|e| PythonExecutionError::ImportError { module: "os".to_string(), message: e.to_string() })?;
        let sys_path_vec: Vec<String> = sys_path.extract().map_err(|e| PythonExecutionError::ImportError { module: "sys".to_string(), message: e.to_string() })?;
        let executable: String = sys.getattr("executable").map_err(|e| PythonExecutionError::ImportError { module: "sys".to_string(), message: e.to_string() })?.extract().map_err(|e| PythonExecutionError::ImportError { module: "sys".to_string(), message: e.to_string() })?;
        let files: Vec<String> = os.call_method1("listdir", ("/Users/ryan/code/kameo-snake-handler/crates/kameo-snake-testing/python",)).map_err(|e| PythonExecutionError::ImportError { module: "os".to_string(), message: e.to_string() })?.extract().map_err(|e| PythonExecutionError::ImportError { module: "os".to_string(), message: e.to_string() })?;
        tracing::error!(cwd, sys_path=?sys_path_vec, executable, files=?files, "ORKY DEBUG: Python import context");
        let module = py.import(&config.module_name)
            .map_err(|e| PythonExecutionError::ImportError { module: config.module_name.clone(), message: e.to_string() })?;
        let function = module.getattr(&config.function_name).map_err(|e| {
            PythonExecutionError::FunctionNotFound {
                module: config.module_name.clone(),
                function: config.function_name.clone(),
                message: e.to_string(),
            }
        })?;
        let function_type_name = function.get_type().name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_else(|_| "<unknown>".to_string());
        tracing::info!(event = "python_import", step = "getattr", function_type = function_type_name.as_str(), "Loaded function from module");
        Ok(function.into())
    })
}

// Re-export for macro hygiene
pub use kameo_child_process;

#[macro_export]
macro_rules! setup_python_subprocess_system {
    (
        actors = { $(($actor:ty, $msg:ty, $callback:ty)),* $(,)? },
        child_init = $child_init:block,
        parent_init = $parent_init:block $(,)?
    ) => {
        fn main() -> Result<(), Box<dyn std::error::Error>> {
            let handlers: &[(&'static str, fn() -> Result<(), Box<dyn std::error::Error>>)] = &[
                $(
                    (
                        std::any::type_name::<$actor>(),
                        || {
                            let config = $child_init;
                            tracing::info!(event = "python_subprocess", step = "child_init", config = ?config);
                            let flavor = config.flavor;
                            match flavor {
                                kameo_child_process::RuntimeFlavor::CurrentThread => {
                                    let mut builder = tokio::runtime::Builder::new_current_thread();
                                    builder.enable_all();
                                    kameo_snake_handler::setup_python_runtime(builder);
                                    Python::with_gil(|py| {
                                        pyo3_async_runtimes::tokio::get_runtime().block_on(async {
                                            tracing::info!(event = "orky_async_entry", flavor = "CurrentThread", thread_id = ?std::thread::current().id(), "ORKY: Entered async block (CurrentThread)");
                                            let rust_thread_id = std::thread::current().id();
                                            let py_thread_id = Python::with_gil(|py| {
                                                let threading = py.import("threading").unwrap();
                                                threading.call_method0("get_ident").unwrap().extract::<u64>().unwrap()
                                            });
                                            tracing::info!(event = "thread_check", where_ = "tokio::run block", rust_thread_id = ?rust_thread_id, py_thread_id, "ORKY DEBUG: Thread IDs at start of async block");
                                            kameo_child_process::child_process_main_with_runtime::<$actor, $msg, $callback>().await
                                                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("{e}")))
                                        })
                                    })
                                    .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)
                                },
                                kameo_child_process::RuntimeFlavor::MultiThread => {
                                    let mut builder = tokio::runtime::Builder::new_multi_thread();
                                    if let Some(threads) = config.worker_threads {
                                        builder.worker_threads(threads);
                                    }
                                    builder.enable_all();
                                    kameo_snake_handler::setup_python_runtime(builder);
                                    Python::with_gil(|py| {
                                        pyo3_async_runtimes::tokio::run(py, async {
                                            tracing::info!(event = "orky_async_entry", flavor = "MultiThread", thread_id = ?std::thread::current().id(), "ORKY: Entered async block (MultiThread)");
                                            let rust_thread_id = std::thread::current().id();
                                            let py_thread_id = Python::with_gil(|py| {
                                                let threading = py.import("threading").unwrap();
                                                threading.call_method0("get_ident").unwrap().extract::<u64>().unwrap()
                                            });
                                            tracing::info!(event = "thread_check", where_ = "tokio::run block", rust_thread_id = ?rust_thread_id, py_thread_id, "ORKY DEBUG: Thread IDs at start of async block");
                                            kameo_child_process::child_process_main_with_runtime::<$actor, $msg, $callback>().await
                                                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("{e}")))
                                        })
                                    })
                                    .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)
                                },
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
impl<M> RuntimeAware for PythonActor<M>
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

