use std::marker::PhantomData;
use std::ops::ControlFlow;
use anyhow::Result;
use async_trait::async_trait;
use bincode::{Decode, Encode};
use kameo::actor::{Actor, ActorRef, WeakActorRef};
use kameo::error::{ActorStopReason, PanicError};
use kameo::message::Message;
use kameo_child_process::{CallbackHandle, ChildCallbackMessage, KameoChildProcessMessage, RuntimeAware};
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use tracing::instrument;
use kameo_child_process::ChildProcessMessageHandler;
use crate::error::PythonExecutionError;

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

#[async_trait]
impl<M, C: ChildCallbackMessage> ChildProcessMessageHandler<M> for PythonActor<M, C>
where
    M: KameoChildProcessMessage + Send + 'static,
{
    type Reply = Result<<M as KameoChildProcessMessage>::Reply, PythonExecutionError>;
    async fn handle_child_message(&mut self, msg: M) -> Self::Reply {
        use pyo3::prelude::*;
        use pyo3_async_runtimes::tokio::into_future;

        tracing::debug!(event = "python_call", step = "serialize", m = ?msg, "Serializing Rust message to Python");
        let is_async = self.config.is_async;
        let function_name = self.config.function_name.clone();
        let py_function = &self.py_function;
        let py_msg = Python::with_gil(|py| crate::serde_py::to_pyobject(py, &msg));
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
                crate::serde_py::from_pyobject(&bound)
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
                    Ok(result) => crate::serde_py::from_pyobject(&result)
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

/// Python-specific child process main entrypoint. Does handshake, sets callback, calls init_with_runtime, and runs the actor loop.
#[instrument(skip(actor, request_conn), name = "child_process_main_with_python_actor")]
pub async fn child_process_main_with_python_actor<M, C>(actor: PythonActor<M, C>, mut request_conn: Box<dyn kameo_child_process::AsyncReadWrite>) -> Result<(), Box<dyn std::error::Error>>
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
    use kameo_child_process::{run_child_actor_loop, perform_handshake};
    tracing::info!("child_process_main_with_python_actor: about to handshake");
    // Perform handshake as child
    perform_handshake::<M, crate::error::PythonExecutionError>(&mut request_conn, false).await?;
    // Callback handle is set and injected in the macro branch for the child process
    let mut actor = actor.init_with_runtime().await.map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;
    tracing::info!("running child actor loop");
    match run_child_actor_loop(&mut actor, request_conn).await {
        Ok(()) => {},
        Err(kameo_child_process::ChildProcessLoopError::ChildProcessClosedCleanly) => {
            tracing::info!("Child process exited cleanly.");
        },
        Err(kameo_child_process::ChildProcessLoopError::Io(e)) => {
            tracing::error!(error = ?e, "Child process IO error");
            return Err(Box::new(e));
        }
    }
    Ok(())
} 