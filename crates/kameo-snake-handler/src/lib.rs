use std::marker::PhantomData;
use std::future::Future;
use std::ops::ControlFlow;

use kameo::actor::{Actor, ActorRef, WeakActorRef};
use kameo::error::{ActorStopReason, PanicError};
use kameo::message::{Context, Message};
use kameo_child_process::{KameoChildProcessMessage, RuntimeAware, ChildProcessBuilder, SubprocessActor, ChildCallbackMessage, CallbackHandler};

use anyhow::Result;
use async_trait::async_trait;
use bincode::{Decode, Encode};
use opentelemetry::global;
use opentelemetry::Context as OTelContext;
use pyo3::exceptions::{
    PyAttributeError, PyImportError, PyModuleNotFoundError, PyRuntimeError, PyTypeError,
    PyValueError,
};
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{error, instrument, info, Level};

pub mod serde_py;
pub use serde_py::{FromPyAny, to_pyobject, from_pyobject};

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
    py_function: Option<Result<PyObject, PythonExecutionError>>,
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
            _phantom: PhantomData,
        }
    }
}

impl<M> Clone for PythonActor<M> {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            py_function: self.py_function.as_ref().map(|res| match res {
                Ok(obj) => Ok(Python::with_gil(|py| obj.clone_ref(py))),
                Err(e) => Err(e.clone()),
            }),
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
            py_function: None,
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
    fn deserialize_py_to_reply(&self, py_obj: PyObject, py: Python) -> Result<<M as KameoChildProcessMessage>::Reply, PythonExecutionError> {
        from_pyobject(&py_obj.bind(py).as_borrowed())
            .map_err(|e| PythonExecutionError::DeserializationError {
                message: e.to_string(),
            })
    }

    /// Handle sync Python function call
    fn handle_sync(&self, message: &M, py: Python, func: &Bound<PyAny>) -> Result<PyObject, PythonExecutionError> {
        let py_dict = self.serialize_message_to_py(message, py)?;
        let result = func.call1((py_dict,))
            .map_err(|e| PythonExecutionError::from_pyerr(e, py))?;
        Ok(result.into())
    }
}

#[async_trait]
impl<M> Message<M> for PythonActor<M>
where
    M: KameoChildProcessMessage + Send + 'static,
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

        async move {
            let py_function_res = actor.py_function.as_ref();

            let result = match py_function_res {
                Some(Ok(py_function)) => {
                    if actor.config.is_async {
                        let mut trace_context_map = std::collections::HashMap::new();
                        global::get_text_map_propagator(|propagator| {
                            propagator.inject_context(&OTelContext::current(), &mut trace_context_map);
                        });
                        let future_res = Python::with_gil(|py| {
                            let func = py_function.bind(py);
                            
                            let py_dict = actor.serialize_message_to_py(&message, py)
                                .map_err(|e| PyErr::new::<PyRuntimeError, _>(e.to_string()))?;
                            let coro = func.call1((py_dict,))?;

                            pyo3_async_runtimes::tokio::into_future(coro.into())
                        });

                        match future_res {
                            Ok(future) => match future.await {
                                Ok(result_obj) => Python::with_gil(|py| actor.deserialize_py_to_reply(result_obj, py)),
                                Err(py_err) => Err(Python::with_gil(|py| PythonExecutionError::from_pyerr(py_err, py))),
                            },
                            Err(py_err) => Err(Python::with_gil(|py| PythonExecutionError::from_pyerr(py_err, py))),
                        }
                    } else {
                        Python::with_gil(|py| {
                            let func = py_function.bind(py);
                            let result_obj = actor.handle_sync(&message, py, func)?;
                            actor.deserialize_py_to_reply(result_obj, py)
                        })
                    }
                },
                Some(Err(e)) => Err(e.clone()),
                None => Err(PythonExecutionError::RuntimeError {
                    message: "Python function not initialized. This is a bug.".to_string(),
                }),
            };

            match result {
                Ok(reply) => reply,
                Err(e) => <Self as Message<M>>::Reply::from_error(e),
            }
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
        let config = self.config.clone();

        Box::pin(async move {
            pyo3::prepare_freethreaded_python();

            if let Err(e) = pyo3_async_runtimes::tokio::init_with_runtime(runtime) {
                error!("Failed to initialize pyo3-async-runtimes: {:?}", e);
                let err = PythonExecutionError::RuntimeError {
                    message: format!("Failed to initialize pyo3-async-runtimes: {:?}", e),
                };
                self.py_function = Some(Err(err));
                return Ok(());
            }

            let module_result: Result<PyObject, PythonExecutionError> = Python::with_gil(|py| {
                let sys = py.import("sys").map_err(|e| PythonExecutionError::from_pyerr(e, py))?;
                let path = sys.getattr("path").map_err(|e| PythonExecutionError::from_pyerr(e, py))?;
                for p in &config.python_path {
                    path.call_method1("append", (p,)).map_err(|e| PythonExecutionError::from_pyerr(e, py))?;
                }

                let module = py.import(&config.module_name).map_err(|e| PythonExecutionError::from_pyerr(e, py))?;
                let function = module.getattr(&config.function_name).map_err(|e| {
                    PythonExecutionError::FunctionNotFound {
                        module: config.module_name.clone(),
                        function: config.function_name.clone(),
                        message: e.to_string(),
                    }
                })?;

                Ok(function.into())
            });

            self.py_function = Some(module_result);
            tracing::info!("Python runtime initialization complete");
            Ok(())
        })
    }
}

use kameo_child_process::CallbackSender;
#[async_trait]
impl<M> CallbackSender<DefaultCallbackMessage> for PythonActor<M>
where
    M: KameoChildProcessMessage + Send + 'static,
{
    fn set_callback_handle(&mut self, _handle: kameo_child_process::CallbackHandle<DefaultCallbackMessage>) {
        // In this example, the PythonActor does not need to send callbacks.
        // If it did, we would store the handle here.
    }
}

pub mod prelude {
    pub use super::{
        PythonActor, PythonChildProcessBuilder, PythonConfig, PythonExecutionError,
    };
}
