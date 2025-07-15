use kameo_child_process::error::PythonExecutionError;
use anyhow::Result;
use async_trait::async_trait;
use bincode::{Decode, Encode};
use kameo::actor::{Actor, ActorRef, WeakActorRef};
use kameo::error::{ActorStopReason, PanicError};
use kameo::message::Message;
use kameo_child_process::ChildProcessMessageHandler;
use kameo_child_process::{
    KameoChildProcessMessage, RuntimeAware,
};
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use std::ops::ControlFlow;
use tracing::instrument;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

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
pub struct PythonActor<M, E>
where
    E: std::fmt::Debug + Send + Sync + 'static + bincode::Encode + bincode::Decode<()>,
{
    handler: PythonMessageHandler,
    concurrent_tasks: Arc<AtomicUsize>,
    _phantom: std::marker::PhantomData<(M, E)>,
}

#[derive(Debug)]
pub struct PythonMessageHandler {
    pub py_function: Py<PyAny>,
    pub config: PythonConfig,
}

impl PythonMessageHandler {
    pub fn clone_with_gil(&self) -> Self {
        Python::with_gil(|py| Self {
            py_function: self.py_function.clone_ref(py),
            config: self.config.clone(),
        })
    }
}

impl Clone for PythonMessageHandler {
    fn clone(&self) -> Self {
        Python::with_gil(|py| Self {
            py_function: self.py_function.clone_ref(py),
            config: self.config.clone(),
        })
    }
}

impl<M, E> PythonActor<M, E>
where
    E: std::fmt::Debug + Send + Sync + 'static + bincode::Encode + bincode::Decode<()>,
{
    pub fn new(config: PythonConfig, py_function: Py<PyAny>) -> Self {
        tracing::debug!("Storing reference to Python function in handler: {:?}", py_function);
        let handler = PythonMessageHandler {
            py_function,
            config,
        };
        Self {
            handler,
            concurrent_tasks: Arc::new(AtomicUsize::new(0)),
            _phantom: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<M, E> Actor for PythonActor<M, E>
where
    M: KameoChildProcessMessage + Send + Sync + 'static,
    E: std::fmt::Debug + Send + Sync + 'static + bincode::Encode + bincode::Decode<()>,
{
    type Error = PythonExecutionError;

    #[instrument(skip(self, _actor_ref), fields(actor_type = "PythonActor"), parent = tracing::Span::current())]
    fn on_start(
        &mut self,
        _actor_ref: ActorRef<Self>,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send {
        async move {
            tracing::info!(status = "started", actor_type = "PythonActor");
            Ok(())
        }
    }

    #[instrument(skip(self, _actor_ref, reason), fields(actor_type = "PythonActor"), parent = tracing::Span::current())]
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

    #[instrument(skip(self, _actor_ref, err), fields(actor_type = "PythonActor"), parent = tracing::Span::current())]
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
impl<M, E> Message<M> for PythonActor<M, E>
where
    M: KameoChildProcessMessage + Send + Sync + 'static,
    E: std::fmt::Debug + Send + Sync + 'static + bincode::Encode + bincode::Decode<()>,
{
    type Reply = kameo::reply::DelegatedReply<Result<<M as KameoChildProcessMessage>::Reply, PythonExecutionError>>;
    #[tracing::instrument(skip(self, ctx, message), fields(actor_type = "PythonActor", message_type = std::any::type_name::<M>()), parent = tracing::Span::current())]
    fn handle(
        &mut self,
        message: M,
        ctx: &mut kameo::message::Context<Self, Self::Reply>,
    ) -> impl std::future::Future<Output = Self::Reply> + Send {
        let handler = self.handler.clone();
        let (delegated, reply_sender) = ctx.reply_sender();
        let concurrent_tasks = self.concurrent_tasks.clone();
        if let Some(reply_sender) = reply_sender {
            concurrent_tasks.fetch_add(1, Ordering::SeqCst);
            tracing::trace!(event = "actor_spawn", concurrent = concurrent_tasks.load(Ordering::SeqCst), "Spawning concurrent handler task for message");
            tokio::spawn(async move {
                let result = handler.handle_child_message_impl(message).await;
                tracing::trace!(event = "actor_reply", ?result, concurrent = concurrent_tasks.load(Ordering::SeqCst), "Sending reply from concurrent handler task");
                reply_sender.send(result);
                concurrent_tasks.fetch_sub(1, Ordering::SeqCst);
                tracing::trace!(event = "actor_task_complete", concurrent = concurrent_tasks.load(Ordering::SeqCst), "Handler task complete");
            });
        } else {
            tracing::warn!("No reply sender available for message (fire-and-forget)");
        }
        async move { delegated }
    }
}

#[async_trait]
impl<M, E> RuntimeAware for PythonActor<M, E>
where
    M: KameoChildProcessMessage + Send + Sync + 'static,
    E: std::fmt::Debug + Send + Sync + 'static + bincode::Encode + bincode::Decode<()>,
{
    async fn init_with_runtime(self) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        // Any actor-specific setup can go here
        Ok(self)
    }
}

/// Python-specific child process main entrypoint. Does handshake, sets callback, calls init_with_runtime, and runs the actor loop.
#[instrument(
    skip(actor, request_conn, config),
    name = "child_process_main_with_python_actor",
    parent = tracing::Span::current()
)]
pub async fn child_process_main_with_python_actor<M, E>(
    actor: PythonActor<M, E>,
    request_conn: Box<tokio::net::UnixStream>,
    config: Option<kameo_child_process::ChildActorLoopConfig>,
) -> Result<(), Box<dyn std::error::Error>>
where
    M: KameoChildProcessMessage + Send + Sync + 'static,
    E: std::fmt::Debug + Send + Sync + 'static + bincode::Encode + bincode::Decode<()>,
{
    use kameo_child_process::{perform_handshake, run_child_actor_loop};
    tracing::info!("child_process_main_with_python_actor: about to handshake");
    let mut conn = request_conn;
    perform_handshake::<M>(&mut conn, false).await?;
    tracing::info!("running child actor loop");
    match run_child_actor_loop::<_, M>(actor.handler.clone_with_gil(), conn, config).await {
        Ok(()) => {
            tracing::info!("Child process exited cleanly.");
            Ok(())
        }
        Err(e) => {
            tracing::error!(error = ?e, "Child process IO error");
            Err(format!("{:?}", e).into())
        }
    }
}

#[async_trait]
impl<M> ChildProcessMessageHandler<M> for PythonMessageHandler
where
    M: KameoChildProcessMessage + Send + Sync + 'static,
{
    type Reply = Result<<M as KameoChildProcessMessage>::Reply, PythonExecutionError>;
    async fn handle_child_message(&mut self, msg: M) -> Self::Reply {
        self.handle_child_message_impl(msg).await
    }
}

impl PythonMessageHandler {
    pub async fn handle_child_message_impl<M>(&self, message: M) -> Result<<M as KameoChildProcessMessage>::Reply, PythonExecutionError>
    where
        M: KameoChildProcessMessage + Send + Sync + std::fmt::Debug + 'static,
    {
        tracing::debug!("Calling stored Python function for message: {:?}", message);
        use pyo3::prelude::*;
        use pyo3_async_runtimes::tokio::into_future;
        tracing::debug!(event = "python_call", step = "start", ?message, "handle_child_message entry");
        let is_async = self.config.is_async;
        let function_name = self.config.function_name.clone();
        let py_function = Python::with_gil(|py| self.py_function.clone_ref(py));
        let py_msg = Python::with_gil(|py| crate::serde_py::to_pyobject(py, &message));
        let py_msg = match py_msg {
            Ok(obj) => obj,
            Err(e) => {
                tracing::error!(event = "python_call", step = "serialize_error", error = %e, "Failed to serialize Rust message to Python");
                return Err(PythonExecutionError::SerializationError {
                    message: e.to_string(),
                })
            }
        };
        if is_async {
            tracing::debug!(event = "python_call", step = "before_async_call", function = %function_name, "About to call async Python function");
            let fut_result = Python::with_gil(|py| {
                let py_func = py_function.bind(py);
                let coro = match py_func.call1((py_msg,)) {
                    Ok(coro) => coro,
                    Err(e) => {
                        tracing::error!(event = "python_call", step = "call_error", function = %function_name, error = %e, "Failed to call async Python function");
                        return Err(PythonExecutionError::CallError {
                            function: function_name.clone(),
                            message: e.to_string(),
                        })
                    }
                };
                match into_future(coro) {
                    Ok(fut) => Ok(fut),
                    Err(e) => {
                        tracing::error!(event = "python_call", step = "into_future_error", function = %function_name, error = %e, "Failed to convert to future");
                        Err(PythonExecutionError::from(e))
                    },
                }
            });
            let fut = match fut_result {
                Ok(fut) => fut,
                Err(e) => return Err(e),
            };
            let py_output = match fut.await {
                Ok(obj) => obj,
                Err(e) => {
                    tracing::error!(event = "python_call", step = "await_error", function = %function_name, error = %e, "Async Python call failed");
                    return Err(PythonExecutionError::from(e));
                }
            };
            tracing::debug!(event = "python_call", step = "after_async_call", function = %function_name, "Async Python call succeeded");
            let rust_result = Python::with_gil(|py| {
                let bound = py_output.bind(py);
                crate::serde_py::from_pyobject(&bound).map_err(|e| {
                    PythonExecutionError::DeserializationError {
                        message: e.to_string(),
                    }
                })
            });
            match rust_result {
                Ok(reply) => {
                    tracing::debug!(event = "python_call", step = "deserialize_ok", function = %function_name, ?reply, "Deserialized Python result to Rust");
                    Ok(reply)
                },
                Err(e) => {
                    tracing::error!(event = "python_call", step = "deserialize_error", function = %function_name, error = %e, "Failed to deserialize Python result");
                    Err(e)
                },
            }
        } else {
            tracing::debug!(event = "python_call", step = "before_sync_call", function = %function_name, "About to call sync Python function");
            let rust_result = Python::with_gil(|py| {
                let py_func = py_function.bind(py);
                match py_func.call1((py_msg,)) {
                    Ok(result) => crate::serde_py::from_pyobject(&result).map_err(|e| {
                        PythonExecutionError::DeserializationError {
                            message: e.to_string(),
                        }
                    }),
                    Err(e) => {
                        tracing::error!(event = "python_call", step = "call_error", function = %function_name, error = %e, "Failed to call sync Python function");
                        Err(PythonExecutionError::CallError {
                        function: function_name.clone(),
                        message: e.to_string(),
                        })
                    },
                }
            });
            match rust_result {
                Ok(reply) => {
                    tracing::debug!(event = "python_call", step = "deserialize_ok", function = %function_name, ?reply, "Deserialized Python result to Rust");
                    Ok(reply)
                },
                Err(e) => {
                    tracing::error!(event = "python_call", step = "deserialize_error", function = %function_name, error = %e, "Failed to deserialize Python result");
                    Err(e)
                },
            }
        }
    }
}
