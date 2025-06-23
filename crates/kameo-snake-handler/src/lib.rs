use std::future::Future;
use std::marker::PhantomData;
use std::ops::ControlFlow;
use std::panic::AssertUnwindSafe;

use anyhow::Result;
use async_trait::async_trait;
use bincode::{Decode, Encode};
use either::Either;
use futures::FutureExt;
use kameo::actor::{Actor, ActorRef, WeakActorRef};
use kameo::error::{ActorStopReason, PanicError};
use kameo::message::{Context, Message};
use kameo_child_process::{
    CallbackHandle, CallbackHandler, ChildCallbackMessage, ChildProcessBuilder, KameoChildProcessMessage, RuntimeAware, SubprocessActor
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
}

/// Python subprocess actor
#[derive(Debug)]
pub struct PythonActor<M> {
    config: PythonConfig,
    py_function: Option<PyObject>,
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
                serde_json::from_str(&config_json).expect("Failed to deserialize python config from env")
            }
            Err(e) => {
                error!(error = ?e, "KAMEO_PYTHON_CONFIG not set");
                panic!("KAMEO_PYTHON_CONFIG must be set for PythonActor");
            }
        };

        Self {
            config,
            py_function: None,
            callback_handle: None,
            _phantom: PhantomData,
        }
    }
}

impl<M> Clone for PythonActor<M> {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            py_function: self.py_function.as_ref().map(|obj| Python::with_gil(|py| obj.clone_ref(py))),
            callback_handle: self.callback_handle.clone(),
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
            py_function: None,
            callback_handle: None,
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

        let (actor_ref, callback_receiver) = ChildProcessBuilder::<PythonActor<M>, M, DefaultCallbackMessage>::new()
            .log_level(self.log_level)
            .with_env_var("KAMEO_PYTHON_CONFIG", config_json)
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
        to_pyobject(py, message)
            .map_err(|e| PythonExecutionError::SerializationError {
                message: e.to_string(),
            })
    }

    /// Convert a Python object back to our Reply type
    fn deserialize_py_to_reply(&self, py_obj: &Bound<PyAny>) -> Result<<M as KameoChildProcessMessage>::Reply, PythonExecutionError> {
        from_pyobject(py_obj)
            .map_err(|e| PythonExecutionError::DeserializationError {
                message: e.to_string(),
            })
    }
}

#[async_trait]
impl<M> Message<M> for PythonActor<M>
where
    M: KameoChildProcessMessage + Send + 'static + std::panic::UnwindSafe,
    <M as KameoChildProcessMessage>::Reply: ErrorReply,
{
    type Reply = <M as KameoChildProcessMessage>::Reply;

    #[instrument(skip(self, message, _ctx), fields(actor_type = "PythonActor"))]
    fn handle(
        &mut self,
        message: M,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> impl std::future::Future<Output = Self::Reply> + Send {
        let actor = self.clone();

        AssertUnwindSafe(async move {
            let result: Result<Self::Reply, PythonExecutionError> = match actor.py_function.as_ref() {
                Some(py_function) => {
                    let py_result_or_coro: Result<Either<PyObject, PyObject>, PyErr> = Python::with_gil(|py| {
                        let func = py_function.bind(py);
                        let py_dict = actor.serialize_message_to_py(&message, py)
                            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
                        let result_bound = func.call1((py_dict,))?;

                        let asyncio = py.import("asyncio")?;
                        if asyncio.call_method1("iscoroutine", (result_bound.clone(),))?.extract::<bool>()? {
                            Ok(Either::Left(result_bound.unbind()))
                        } else {
                            Ok(Either::Right(result_bound.unbind()))
                        }
                    });

                    match py_result_or_coro {
                        Ok(Either::Left(coro)) => {
                            let future_res = Python::with_gil(|py| {
                                // The `.clone()` here is crucial. It creates a new reference-counted handle
                                // to the Python coroutine object. This allows us to move the handle into
                                // the Rust future, ensuring the Python object remains alive even after
                                // the current GIL-bound scope is exited. Without this, we'd face
                                // lifetime errors as the object would be dropped too soon.
                                pyo3_async_runtimes::tokio::into_future(coro.bind(py).clone().into_any())
                            });

                            let awaited_res = match future_res {
                                Ok(future) => future.await,
                                Err(e) => Err(e),
                            };

                            match awaited_res {
                                Ok(final_py_obj) => Python::with_gil(|py| {
                                    actor.deserialize_py_to_reply(&final_py_obj.bind(py))
                                }),
                                Err(e) => Err(Python::with_gil(|py| PythonExecutionError::from_pyerr(e, py))),
                            }
                        }
                        Ok(Either::Right(sync_result)) => {
                            Python::with_gil(|py| actor.deserialize_py_to_reply(&sync_result.bind(py)))
                        }
                        Err(e) => Err(Python::with_gil(|py| PythonExecutionError::from_pyerr(e, py))),
                    }
                }
                None => Err(PythonExecutionError::RuntimeError {
                    message: "Python function not initialized. This is a bug.".to_string(),
                }),
            };

            match result {
                Ok(reply) => reply,
                Err(e) => {
                    error!(event = "error", error = ?e, "Python error in handler, sending error reply (not panicking or hanging)");
                    <Self::Reply>::from_error(e)
                }
            }
        })
        .catch_unwind()
        .map(|res| {
            match res {
                Ok(reply) => reply,
                Err(panic) => {
                    let message = if let Some(s) = panic.downcast_ref::<&str>() {
                        s.to_string()
                    } else if let Some(s) = panic.downcast_ref::<String>() {
                        s.clone()
                    } else {
                        "Undescribable panic".to_string()
                    };
                    error!(event = "error", error = %message, "Panic caught in PythonActor handle");
                    <Self::Reply>::from_error(PythonExecutionError::RuntimeError {
                        message: format!("Actor panicked: {}", message),
                    })
                }
            }
        })
    }
}

#[async_trait]
impl<M> RuntimeAware for PythonActor<M>
where
    M: KameoChildProcessMessage + Send + 'static,
{
    #[instrument(skip(self, runtime), fields(python_paths = ?self.config.python_path, module_name = ?self.config.module_name))]
    fn init_with_runtime<'a>(&'a mut self, runtime: &'static tokio::runtime::Runtime) -> std::pin::Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>> {
        let config = self.config.clone();
        let callback_handle = self.callback_handle.clone();

        Box::pin(async move {
            if let Err(e) = pyo3_async_runtimes::tokio::init_with_runtime(runtime) {
                error!("Failed to initialize pyo3-async-runtimes: {:?}", e);
                return Err(PythonExecutionError::RuntimeError {
                    message: format!("Failed to initialize pyo3-async-runtimes: {:?}", e),
                });
            }

            let module_result: Result<PyObject, PythonExecutionError> = Python::with_gil(|py| {
                let sys = py.import("sys")?;
                let path = sys.getattr("path")?;
                for p in &config.python_path {
                    path.call_method1("append", (p,))?;
                }

                if let Some(handle) = callback_handle.clone() {
                    let kameo_module = PyModule::new(py, "kameo")?;
                    let py_callback_handle = KameoCallbackHandle { handle };
                    let handle_obj = Py::new(py, py_callback_handle)?;
                    kameo_module.add("callback_handle", handle_obj)?;
                    sys.getattr("modules")?.set_item("kameo", kameo_module)?;
                }

                let module = py.import(&config.module_name)?;
                let function = module.getattr(&config.function_name).map_err(|e| {
                    PythonExecutionError::FunctionNotFound {
                        module: config.module_name.clone(),
                        function: config.function_name.clone(),
                        message: e.to_string(),
                    }
                })?;

                Ok(function.into())
            });

            match module_result {
                Ok(py_function) => {
                    self.py_function = Some(py_function);
                    tracing::info!("Python runtime initialization complete");
                    Ok(())
                }
                Err(e) => {
                    error!("Failed to initialize Python function: {:?}", e);
                    tracing::error!(error = ?e, "Non-resumable error: shutting down child process");
                    // Exit the process with error code 101 (conventional for fatal error)
                    std::process::exit(101);
                }
            }
        })
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

pub mod prelude {
    pub use super::{
        PythonActor, PythonChildProcessBuilder, PythonConfig, PythonExecutionError,
    };
}
