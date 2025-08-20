use anyhow::Result;
use async_trait::async_trait;
// Wire format: serde + postcard
use kameo::actor::Actor;
use kameo::message::Message;
use kameo_child_process::error::PythonExecutionError;
use kameo_child_process::ChildProcessMessageHandler;
use kameo_child_process::{KameoChildProcessMessage, RuntimeAware};
use opentelemetry::propagation::TextMapPropagator;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use serde::{Deserialize, Serialize};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tracing::instrument;
use tracing_futures::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// Configuration for Python subprocess execution.
///
/// This struct defines all the configuration options needed to spawn and configure
/// a Python subprocess that can handle both synchronous and streaming messages.
///
/// ## Configuration Options
///
/// - **Python Environment**: Paths, environment variables, and module configuration
/// - **Function Specification**: Module name, function name, and file path
/// - **Async Support**: Whether the Python function is async or sync
/// - **Process Management**: Automatic environment setup and path management
/// - **OpenTelemetry Integration**: Trace context propagation between Rust and Python
///
/// ## Usage Example
///
/// ```rust,ignore
/// let config = PythonConfig {
///     python_path: vec![
///         "/usr/bin/python3".to_string(),
///         "/opt/homebrew/bin/python3".to_string(),
///     ],
///     module_name: "my_module".to_string(),
///     function_name: "process_data".to_string(),
///     env_vars: vec![
///         ("PYTHONPATH".to_string(), "/path/to/modules".to_string()),
///         ("DEBUG".to_string(), "1".to_string()),
///     ],
///     is_async: true,  // For async generators
///     enable_otel_propagation: true,  // Enable trace context propagation
///     validate_otel_dependencies: true,  // Validate OTEL packages on startup
/// };
/// ```
///
/// ## Python Function Requirements
///
/// ### Sync Functions
/// ```python
/// def process_data(message: dict) -> dict:
///     return {"result": "processed", "data": message["input"]}
/// ```
///
/// ### Async Generators (Streaming)
/// ```python
/// async def process_data(message: dict) -> AsyncGenerator[dict, None]:
///     for i in range(5):
///         yield {"index": i, "data": message["input"]}
/// ```
///
/// ## OpenTelemetry Integration
///
/// When `enable_otel_propagation` is true, trace context will be automatically
/// propagated between Rust and Python processes. This requires the following
/// Python packages to be installed:
///
/// - `opentelemetry`
/// - `opentelemetry-api`
/// - `opentelemetry-sdk`
///
/// The system will validate these dependencies if `validate_otel_dependencies` is true.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PythonConfig {
    /// List of Python executable paths to try (in order)
    pub python_path: Vec<String>,
    /// Name of the Python module to import
    pub module_name: String,
    /// Name of the function to call within the module
    pub function_name: String,
    /// Environment variables to set in the Python subprocess
    pub env_vars: Vec<(String, String)>,
    /// Whether the Python function is async (for streaming support)
    pub is_async: bool,
    /// Enable OpenTelemetry trace context propagation
    ///
    /// When enabled, trace context will be automatically propagated between
    /// Rust and Python processes. This requires the `opentelemetry` Python
    /// package to be installed in the Python environment.
    pub enable_otel_propagation: bool,
}

impl Default for PythonConfig {
    fn default() -> Self {
        Self {
            python_path: vec!["python3".to_string(), "python".to_string()],
            module_name: String::new(),
            function_name: String::new(),
            env_vars: Vec::new(),
            is_async: false,
            enable_otel_propagation: false,
        }
    }
}

/// Kameo actor for Python subprocess communication with unified streaming support.
///
/// This actor manages communication with Python subprocesses, handling both synchronous
/// and streaming message patterns. It integrates with the unified streaming protocol
/// and provides seamless Python function execution.
///
/// ## Actor Features
///
/// - **Python Integration**: Direct execution of Python functions with PyO3
/// - **Streaming Support**: Native support for Python async generators
/// - **Type Conversion**: Automatic serialization between Rust and Python types
/// - **Error Handling**: Python exceptions converted to Rust errors
/// - **Concurrency Control**: Tracks concurrent task execution
///
/// ## Message Handling
///
/// - **Sync Messages**: Calls Python functions that return single values
/// - **Stream Messages**: Calls Python async generators that yield multiple values
/// - **Error Propagation**: Python exceptions properly converted and propagated
/// - **Type Safety**: Full type safety through the message system
///
/// ## Usage
///
/// ```rust,ignore
/// // Create actor with Python configuration
/// let actor = PythonActor::new(config, py_function);
///
/// // Send sync message
/// let response = actor.ask(MyMessage { data: "hello" }).await?;
///
/// // Send streaming message
/// let stream = actor.send_stream(MyMessage { data: "stream" }).await?;
/// while let Some(item) = stream.next().await {
///     println!("Python response: {:?}", item?);
/// }
/// ```
///
/// ## Python Side
///
/// The actor calls Python functions with the following signatures:
///
/// ```python
/// # Sync function
/// def my_function(message: dict) -> dict:
///     return {"result": "processed"}
///
/// # Async generator (streaming)
/// async def my_function(message: dict) -> AsyncGenerator[dict, None]:
///     for i in range(5):
///         yield {"index": i, "data": message["data"]}
/// ```
pub struct PythonActor<M, E>
where
    E: std::fmt::Debug + Send + Sync + 'static,
{
    /// Handler for Python function execution
    handler: PythonMessageHandler,
    /// Counter for tracking concurrent task execution
    concurrent_tasks: Arc<AtomicUsize>,
    /// Phantom data for message and callback types
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
    E: std::fmt::Debug + Send + Sync + 'static,
{
    pub fn new(config: PythonConfig, py_function: Py<PyAny>) -> Self {
        tracing::debug!(
            "Storing reference to Python function in handler: {:?}",
            py_function
        );
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
    E: std::fmt::Debug + Send + Sync + 'static,
{
    type Error = PythonExecutionError;
    #[allow(refining_impl_trait)]
    fn on_start(
        &mut self,
        _actor_ref: kameo::actor::ActorRef<Self>,
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send>> {
        Box::pin(async move {
            tracing::info!(status = "started");
            Ok(())
        })
    }
    #[allow(refining_impl_trait)]
    fn on_stop(
        &mut self,
        _actor_ref: kameo::actor::WeakActorRef<Self>,
        reason: kameo::error::ActorStopReason,
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send>> {
        Box::pin(async move {
            tracing::error!(status = "stopped", ?reason);
            Ok(())
        })
    }
    #[allow(refining_impl_trait)]
    fn on_panic(
        &mut self,
        _actor_ref: kameo::actor::WeakActorRef<Self>,
        err: kameo::error::PanicError,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<
                        std::ops::ControlFlow<kameo::error::ActorStopReason>,
                        Self::Error,
                    >,
                > + Send,
        >,
    > {
        Box::pin(async move {
            tracing::error!(status = "panicked", ?err);
            Ok(std::ops::ControlFlow::Continue(()))
        })
    }
}

#[async_trait]
impl<M, E> Message<M> for PythonActor<M, E>
where
    M: KameoChildProcessMessage + Send + Sync + 'static,
    E: std::fmt::Debug + Send + Sync + 'static,
{
    type Reply = kameo::reply::DelegatedReply<Result<M::Ok, PythonExecutionError>>;
    #[tracing::instrument(skip(self, ctx, message), fields(message_type = std::any::type_name::<M>()), parent = tracing::Span::current())]
    #[allow(refining_impl_trait)]
    fn handle(
        &mut self,
        message: M,
        ctx: &mut kameo::message::Context<Self, Self::Reply>,
    ) -> Pin<Box<dyn Future<Output = Self::Reply> + Send>> {
        let handler = self.handler.clone();
        let (delegated, reply_sender) = ctx.reply_sender();
        let concurrent_tasks = self.concurrent_tasks.clone();
        if let Some(reply_sender) = reply_sender {
            concurrent_tasks.fetch_add(1, Ordering::SeqCst);
            tracing::trace!(
                event = "actor_spawn",
                concurrent = concurrent_tasks.load(Ordering::SeqCst),
                "Spawning concurrent handler task for message"
            );
            tokio::spawn(async move {
                let result = handler.handle_child_message_impl(message, None).await;
                tracing::trace!(
                    event = "actor_reply",
                    ?result,
                    concurrent = concurrent_tasks.load(Ordering::SeqCst),
                    "Sending reply from concurrent handler task"
                );
                reply_sender.send(result);
                concurrent_tasks.fetch_sub(1, Ordering::SeqCst);
                tracing::trace!(
                    event = "actor_task_complete",
                    concurrent = concurrent_tasks.load(Ordering::SeqCst),
                    "Handler task complete"
                );
            });
        } else {
            tracing::warn!("No reply sender available for message (fire-and-forget)");
        }
        Box::pin(async move { delegated })
    }
}

#[async_trait]
impl<M, E> RuntimeAware for PythonActor<M, E>
where
    M: KameoChildProcessMessage + Send + Sync + 'static,
    E: std::fmt::Debug + Send + Sync + 'static,
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
    E: std::fmt::Debug + Send + Sync + 'static,
{
    use kameo_child_process::{perform_handshake, run_child_actor_loop};
    tracing::info!("child_process_main_with_python_actor: about to handshake");
    let mut conn = request_conn;
    perform_handshake::<M>(&mut conn, false).await?;
    tracing::info!("running child actor loop");

    // One-time Python OTEL SDK initialization to enable exporting Python spans
    if let Err(e) = crate::tracing_utils::setup_python_otel_context(&opentelemetry::Context::new())
    {
        tracing::warn!(error = ?e, "Failed to initialize Python OpenTelemetry SDK");
    }
    match run_child_actor_loop::<_, M>(actor.handler.clone_with_gil(), conn, config).await {
        Ok(()) => {
            tracing::info!("Child process exited cleanly (no process::exit). Returning from child_process_main_with_python_actor.");
            Ok(())
        }
        Err(e) => {
            tracing::error!(error = ?e, "Child process IO error (no process::exit). Returning error from child_process_main_with_python_actor.");
            Err(Box::new(e))
        }
    }
}

#[async_trait]
impl<M> ChildProcessMessageHandler<M> for PythonMessageHandler
where
    M: KameoChildProcessMessage + Send + Sync + 'static,
{
    async fn handle_child_message(&mut self, msg: M) -> Result<M::Ok, PythonExecutionError> {
        self.handle_child_message_impl(msg, None).await
    }

    async fn handle_child_message_with_context(
        &mut self,
        msg: M,
        context: kameo_child_process::TracingContext,
    ) -> Result<M::Ok, PythonExecutionError> {
        self.handle_child_message_impl(msg, Some(context)).await
    }

    /// Handle a streaming message and return a stream of responses.
    ///
    /// This method processes messages that should return multiple results over time.
    /// It supports both synchronous and asynchronous Python functions:
    ///
    /// ## Sync Functions
    ///
    /// For synchronous Python functions (`is_async = false`), the function is called
    /// once and its result is wrapped in a single-item stream for protocol consistency.
    ///
    /// ## Async Functions (Streaming)
    ///
    /// For asynchronous Python functions (`is_async = true`), the function is expected
    /// to be an async generator that yields multiple values. The function is called
    /// and its async generator is consumed using the `PythonAsyncGeneratorStream`.
    ///
    /// ## Python Async Generator Protocol
    ///
    /// The Python function should implement the async generator protocol:
    /// - Return an object with `__aiter__()` method
    /// - `__aiter__()` should return an async iterator
    /// - `__anext__()` should yield values or raise `StopAsyncIteration`
    ///
    /// ## Error Handling
    ///
    /// - Python exceptions are converted to `PythonExecutionError`
    /// - `StopAsyncIteration` is handled gracefully for stream completion
    /// - GIL-related errors are prevented through proper lock management
    ///
    /// ## Example Python Function
    ///
    /// ```python
    /// async def handle_message_streaming(message):
    ///     for i in range(10):
    ///         yield {"index": i, "value": i * 2}
    ///         await asyncio.sleep(0.1)
    /// ```
    async fn handle_child_message_stream(
        &mut self,
        msg: M,
    ) -> Result<
        Box<dyn futures::stream::Stream<Item = Result<M::Ok, PythonExecutionError>> + Send + Unpin>,
        PythonExecutionError,
    > {
        tracing::debug!(event = "handle_child_message_stream", function = %self.config.function_name, is_async = %self.config.is_async, "handle_child_message_stream called");

        if !self.config.is_async {
            tracing::debug!(event = "non_async_stream", "Creating non-async stream");
            let result = self.handle_child_message_impl(msg, None).await;
            let stream = futures::stream::iter(vec![result]);
            return Ok(Box::new(stream));
        }

        tracing::debug!(
            event = "creating_async_stream",
            "Creating async generator stream"
        );
        // Create a custom stream struct that handles GIL lifetimes properly
        let stream = PythonAsyncGeneratorStream::new(self.clone(), msg).await?;
        tracing::debug!(
            event = "async_stream_created",
            "Async generator stream created"
        );
        Ok(Box::new(stream))
    }
}

/// Custom stream struct for handling Python async generators with proper GIL management.
///
/// This struct implements Rust's `Stream` trait to consume Python async generators.
/// It manages the Python Global Interpreter Lock (GIL) correctly and handles the
/// async generator lifecycle including creation, iteration, and cleanup.
///
/// ## State Machine
///
/// The stream uses a state machine to manage the async generator lifecycle:
/// - `Initial`: Starting state, need to create the generator
/// - `CreatingGenerator`: Async future for creating the generator
/// - `Generator`: Generator created, have the async iterator ready
/// - `GettingNext`: Async future for getting the next item
/// - `Exhausted`: Generator has been fully consumed
///
/// ## GIL Management
///
/// All Python operations are wrapped in `Python::with_gil()` calls to ensure
/// proper GIL acquisition and release. This prevents deadlocks and ensures
/// thread safety when working with Python objects from Rust async contexts.
///
/// ## Error Handling
///
/// The stream properly handles Python exceptions including `StopAsyncIteration`
/// for normal stream completion and other exceptions for error conditions.
/// All errors are converted to `PythonExecutionError` for consistent error handling.
struct PythonAsyncGeneratorStream<M>
where
    M: KameoChildProcessMessage + Send + Sync + std::fmt::Debug + 'static,
{
    handler: PythonMessageHandler,
    msg: Option<M>,
    state: PythonAsyncGeneratorState<M>,
}

/// State machine for managing Python async generator lifecycle.
///
/// This enum tracks the current state of the async generator stream,
/// allowing the `Stream::poll_next` method to handle different phases
/// of the generator's lifecycle correctly.
enum PythonAsyncGeneratorState<M>
where
    M: KameoChildProcessMessage + Send + Sync + std::fmt::Debug + 'static,
{
    /// Initial state - need to create the generator
    Initial,
    /// Creating the generator (async future)
    CreatingGenerator {
        fut: Pin<
            Box<
                dyn Future<
                        Output = Result<
                            (pyo3::PyObject, pyo3::Py<pyo3::PyAny>),
                            PythonExecutionError,
                        >,
                    > + Send,
            >,
        >,
    },
    /// Generator created, have the async iterator
    Generator { async_iter: pyo3::Py<pyo3::PyAny> },
    /// Getting next item (async future)
    GettingNext {
        async_iter: pyo3::Py<pyo3::PyAny>,
        fut: Pin<Box<dyn Future<Output = Result<Option<M::Ok>, PythonExecutionError>> + Send>>,
    },
    /// Generator exhausted
    Exhausted,
}

impl<M> std::fmt::Debug for PythonAsyncGeneratorState<M>
where
    M: KameoChildProcessMessage + Send + Sync + std::fmt::Debug + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Initial => write!(f, "Initial"),
            Self::CreatingGenerator { .. } => write!(f, "CreatingGenerator"),
            Self::Generator { .. } => write!(f, "Generator"),
            Self::GettingNext { .. } => write!(f, "GettingNext"),
            Self::Exhausted => write!(f, "Exhausted"),
        }
    }
}

impl<M> PythonAsyncGeneratorStream<M>
where
    M: KameoChildProcessMessage + Send + Sync + std::fmt::Debug + 'static,
{
    async fn new(handler: PythonMessageHandler, msg: M) -> Result<Self, PythonExecutionError> {
        Ok(Self {
            handler,
            msg: Some(msg),
            state: PythonAsyncGeneratorState::Initial,
        })
    }
}

impl<M> futures::stream::Stream for PythonAsyncGeneratorStream<M>
where
    M: KameoChildProcessMessage + Send + Sync + std::fmt::Debug + 'static,
{
    type Item = Result<M::Ok, PythonExecutionError>;

    /// Poll the stream for the next item from the Python async generator.
    ///
    /// This method implements the core streaming logic using a state machine approach.
    /// It handles the transition between different states of the async generator lifecycle
    /// and ensures proper GIL management throughout the process.
    ///
    /// ## State Transitions
    ///
    /// 1. **Initial → CreatingGenerator**: Start creating the Python async generator
    /// 2. **CreatingGenerator → Generator**: Generator created, ready to iterate
    /// 3. **Generator → GettingNext**: Start getting the next item
    /// 4. **GettingNext → Generator**: Item received, ready for next iteration
    /// 5. **GettingNext → Exhausted**: Stream completed (StopAsyncIteration)
    ///
    /// ## GIL Safety
    ///
    /// All Python operations are wrapped in `Python::with_gil()` calls to ensure
    /// thread safety and prevent deadlocks. The GIL is acquired only when needed
    /// and released immediately after use.
    ///
    /// ## Error Handling
    ///
    /// - `StopAsyncIteration`: Normal stream completion, returns `None`
    /// - Other Python exceptions: Converted to `PythonExecutionError`
    /// - GIL errors: Prevented through proper lock management
    ///
    /// ## Performance Considerations
    ///
    /// - Uses `Pin<Box<dyn Future>>` for async operations within sync `poll_next`
    /// - Clones `Py<PyAny>` objects only when necessary
    /// - Minimizes GIL acquisition time
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        use pyo3::prelude::*;
        use pyo3_async_runtimes::tokio::into_future;

        tracing::debug!(event = "stream_poll_next", state = ?self.state, "Stream poll_next called");

        match &mut self.state {
            PythonAsyncGeneratorState::Initial => {
                tracing::debug!(
                    event = "stream_initial_state",
                    "Starting to create generator"
                );
                // Start creating the generator
                let handler = self.handler.clone();
                let msg = self
                    .msg
                    .take()
                    .expect("Message should be available in Initial state");

                let fut = async move {
                    let function_name = handler.config.function_name.clone();
                    let py_function = Python::with_gil(|py| handler.py_function.clone_ref(py));

                    // Serialize Rust message to Python object
                    let py_msg = match Python::with_gil(|py| crate::serde_py::to_pyobject(py, &msg))
                    {
                        Ok(obj) => obj,
                        Err(e) => {
                            tracing::error!(event = "serialize_error", error = %e, "Failed to serialize Rust message to Python");
                            return Err(PythonExecutionError::SerializationError {
                                message: e.to_string(),
                            });
                        }
                    };

                    // Call the Python function directly
                    let result = Python::with_gil(|py| {
                        let py_func = py_function.bind(py);
                        tracing::debug!(event = "calling_python_function", function = %function_name, "Calling Python function directly");
                        let result = match py_func.call1((py_msg,)) {
                            Ok(result) => {
                                tracing::debug!(
                                    event = "python_function_result",
                                    "Python function returned: {:?}",
                                    result
                                );
                                result
                            }
                            Err(e) => {
                                return Err(PythonExecutionError::CallError {
                                    function: function_name.clone(),
                                    message: e.to_string(),
                                });
                            }
                        };

                        // Check if the result is an async generator
                        let is_async_gen = result.hasattr("__aiter__").unwrap_or(false);
                        if !is_async_gen {
                            return Err(PythonExecutionError::CallError {
                                function: function_name.clone(),
                                message: "Python function did not return an async generator"
                                    .to_string(),
                            });
                        }

                        // Get the async iterator by calling __aiter__()
                        let async_iter = match result.call_method0("__aiter__") {
                            Ok(obj) => {
                                tracing::debug!(
                                    event = "got_aiter",
                                    "Got __aiter__ result: {:?}",
                                    obj
                                );
                                obj.unbind()
                            }
                            Err(e) => {
                                tracing::error!(event = "aiter_error", error = %e, "Failed to get __aiter__");
                                return Err(PythonExecutionError::from(e));
                            }
                        };

                        Ok((result.into(), async_iter))
                    });

                    result
                };

                self.state = PythonAsyncGeneratorState::CreatingGenerator { fut: Box::pin(fut) };

                // Poll the future immediately
                self.poll_next(cx)
            }

            PythonAsyncGeneratorState::CreatingGenerator { fut } => {
                match fut.as_mut().poll(cx) {
                    std::task::Poll::Ready(Ok((_gen, async_iter))) => {
                        self.state = PythonAsyncGeneratorState::Generator { async_iter };
                        // Start getting the first item
                        self.poll_next(cx)
                    }
                    std::task::Poll::Ready(Err(e)) => std::task::Poll::Ready(Some(Err(e))),
                    std::task::Poll::Pending => std::task::Poll::Pending,
                }
            }

            PythonAsyncGeneratorState::Generator { async_iter } => {
                // Start getting the next item
                let async_iter_clone = Python::with_gil(|py| async_iter.clone_ref(py));

                let fut = async move {
                    let next_fut = Python::with_gil(|py| {
                        let bound = async_iter_clone.bind(py);
                        bound
                            .call_method0("__anext__")
                            .map(|coro| into_future(coro).unwrap())
                            .map_err(PythonExecutionError::from)
                    });

                    let next_fut = match next_fut {
                        Ok(fut) => fut,
                        Err(e) => return Err(e),
                    };

                    let item_result = match next_fut.await {
                        Ok(item) => item,
                        Err(e) => {
                            // Check if it's a StopAsyncIteration exception
                            let is_stop_iteration = Python::with_gil(|py| {
                                e.is_instance_of::<pyo3::exceptions::PyStopAsyncIteration>(py)
                            });
                            if is_stop_iteration {
                                return Ok(None); // End of iteration
                            } else {
                                tracing::error!(event = "generator_error", error = %e, "Async generator error");
                                return Err(PythonExecutionError::from(e));
                            }
                        }
                    };

                    let rust_item = match Python::with_gil(|py| {
                        let bound = item_result.bind(py);
                        crate::serde_py::from_pyobject(bound).map_err(|e| {
                            PythonExecutionError::DeserializationError {
                                message: e.to_string(),
                            }
                        })
                    }) {
                        Ok(item) => item,
                        Err(e) => {
                            tracing::error!(event = "deserialize_error", error = %e, "Failed to deserialize stream item");
                            return Err(e);
                        }
                    };

                    Ok(Some(rust_item))
                };

                self.state = PythonAsyncGeneratorState::GettingNext {
                    async_iter: Python::with_gil(|py| async_iter.clone_ref(py)),
                    fut: Box::pin(fut),
                };

                // Poll the future immediately
                self.poll_next(cx)
            }

            PythonAsyncGeneratorState::GettingNext { async_iter, fut } => {
                match fut.as_mut().poll(cx) {
                    std::task::Poll::Ready(Ok(Some(item))) => {
                        // Store the async_iter back in the Generator state for the next iteration
                        let async_iter = Python::with_gil(|py| async_iter.clone_ref(py));
                        self.state = PythonAsyncGeneratorState::Generator { async_iter };
                        std::task::Poll::Ready(Some(Ok(item)))
                    }
                    std::task::Poll::Ready(Ok(None)) => {
                        self.state = PythonAsyncGeneratorState::Exhausted;
                        std::task::Poll::Ready(None)
                    }
                    std::task::Poll::Ready(Err(e)) => std::task::Poll::Ready(Some(Err(e))),
                    std::task::Poll::Pending => std::task::Poll::Pending,
                }
            }

            PythonAsyncGeneratorState::Exhausted => std::task::Poll::Ready(None),
        }
    }
}

impl<M> Unpin for PythonAsyncGeneratorStream<M> where
    M: KameoChildProcessMessage + Send + Sync + std::fmt::Debug + 'static
{
}

impl PythonMessageHandler {
    pub async fn handle_child_message_impl<M>(
        &self,
        message: M,
        _tracing_context: Option<kameo_child_process::TracingContext>,
    ) -> Result<M::Ok, PythonExecutionError>
    where
        M: KameoChildProcessMessage + Send + Sync + std::fmt::Debug + 'static,
    {
        use pyo3::prelude::*;
        use pyo3_async_runtimes::tokio::into_future;

        let is_async = self.config.is_async;
        let function_name = self.config.function_name.clone();
        let py_function = Python::with_gil(|py| self.py_function.clone_ref(py));

        // Serialize Rust message to Python object
        let py_msg = {
            let serialize_span =
                crate::tracing_utils::create_python_serialize_span(std::any::type_name::<M>());
            async { Python::with_gil(|py| crate::serde_py::to_pyobject(py, &message)) }
                .instrument(serialize_span)
                .await
        };

        let py_msg = match py_msg {
            Ok(obj) => obj,
            Err(e) => {
                tracing::error!(event = "serialize_error", error = %e, "Failed to serialize Rust message to Python");
                return Err(PythonExecutionError::SerializationError {
                    message: e.to_string(),
                });
            }
        };

        let result = if is_async {
            // Async Python function call
            let async_call_span =
                crate::tracing_utils::create_python_async_call_span(&function_name);

            async {
                let fut_result = Python::with_gil(|py| {
                    if self.config.enable_otel_propagation {
                        // Inject the CURRENT Rust span context (python_async_call) so any Python spans
                        // become children of this particular call.
                        let context_to_inject = tracing::Span::current().context();
                        
                        // Extract trace context to carrier format
                        let mut carrier = std::collections::HashMap::new();
                        let propagator = opentelemetry_sdk::propagation::TraceContextPropagator::new();
                        propagator.inject_context(&context_to_inject, &mut carrier);
                        
                        // Create Python dict from carrier
                        let carrier_dict = PyDict::new(py);
                        for (key, value) in carrier {
                            if let Err(e) = carrier_dict.set_item(key, value) {
                                return Err(PythonExecutionError::CallError {
                                    function: function_name.clone(),
                                    message: format!("Failed to create carrier dict: {}", e),
                                });
                            }
                        }
                        
                        // Import and call the run_with_otel_context helper
                        let run_with_otel_context = match py.run(std::ffi::CString::new(crate::tracing_utils::PY_OTEL_RUNNER).unwrap().as_c_str(), None, None) {
                            Ok(_) => {
                                match py.import("__main__") {
                                    Ok(main) => match main.getattr("run_with_otel_context") {
                                        Ok(func) => func,
                                        Err(e) => {
                                            return Err(PythonExecutionError::CallError {
                                                function: function_name.clone(),
                                                message: format!("Failed to get run_with_otel_context: {}", e),
                                            });
                                        }
                                    },
                                    Err(e) => {
                                        return Err(PythonExecutionError::CallError {
                                            function: function_name.clone(),
                                            message: format!("Failed to import __main__: {}", e),
                                        });
                                    }
                                }
                            },
                            Err(e) => {
                                return Err(PythonExecutionError::CallError {
                                    function: function_name.clone(),
                                    message: format!("Failed to define run_with_otel_context: {}", e),
                                });
                            }
                        };
                        
                        // Call run_with_otel_context with carrier, user function, and message
                        let py_func = py_function.bind(py);
                        let coro = match run_with_otel_context.call1((carrier_dict, py_func, py_msg)) {
                            Ok(result) => result,
                            Err(e) => {
                                return Err(PythonExecutionError::CallError {
                                    function: function_name.clone(),
                                    message: e.to_string(),
                                });
                            }
                        };
                        
                        match into_future(coro) {
                            Ok(fut) => Ok(fut),
                            Err(e) => Err(PythonExecutionError::from(e)),
                        }
                    } else {
                        // OTEL propagation disabled - call function directly
                        let py_func = py_function.bind(py);
                        let coro = match py_func.call1((py_msg,)) {
                            Ok(result) => result,
                            Err(e) => {
                                return Err(PythonExecutionError::CallError {
                                    function: function_name.clone(),
                                    message: e.to_string(),
                                });
                            }
                        };
                        match into_future(coro) {
                            Ok(fut) => Ok(fut),
                            Err(e) => Err(PythonExecutionError::from(e)),
                        }
                    }
                });
                
                let fut = match fut_result {
                    Ok(fut) => fut,
                    Err(e) => return Err(e),
                };
                
                let result = match fut.await {
                    Ok(obj) => obj,
                    Err(e) => {
                        tracing::error!(event = "await_error", function = %function_name, error = %e, "Async Python call failed");
                        return Err(PythonExecutionError::from(e));
                    }
                };
                
                // Force flush Python spans after function execution
                Python::with_gil(|py| {
                    let flush_code = r#"
try:
    import opentelemetry.trace as trace
    provider = trace.get_tracer_provider()
    if hasattr(provider, 'force_flush'):
        provider.force_flush()
except Exception:
    pass
"#;
                    let _ = py.run(std::ffi::CString::new(flush_code).unwrap().as_c_str(), None, None);
                });
                
                Ok(result)
            }.instrument(async_call_span).await?
        } else {
            // Sync Python function call
            let sync_call_span = crate::tracing_utils::create_python_sync_call_span(&function_name);

            async {
                let result = Python::with_gil(|py| {
                    if self.config.enable_otel_propagation {
                        // Inject the CURRENT Rust span context (python_sync_call) so any Python spans
                        // become children of this particular call.
                        let context_to_inject = tracing::Span::current().context();

                        // Extract trace context to carrier format
                        let mut carrier = std::collections::HashMap::new();
                        let propagator =
                            opentelemetry_sdk::propagation::TraceContextPropagator::new();
                        propagator.inject_context(&context_to_inject, &mut carrier);

                        // Create Python dict from carrier
                        let carrier_dict = PyDict::new(py);
                        for (key, value) in carrier {
                            if let Err(e) = carrier_dict.set_item(key, value) {
                                return Err(PythonExecutionError::CallError {
                                    function: function_name.clone(),
                                    message: format!("Failed to create carrier dict: {}", e),
                                });
                            }
                        }

                        // Import and call the run_with_otel_context helper
                        let run_with_otel_context = match py.run(
                            std::ffi::CString::new(crate::tracing_utils::PY_OTEL_RUNNER)
                                .unwrap()
                                .as_c_str(),
                            None,
                            None,
                        ) {
                            Ok(_) => match py.import("__main__") {
                                Ok(main) => match main.getattr("run_with_otel_context") {
                                    Ok(func) => func,
                                    Err(e) => {
                                        return Err(PythonExecutionError::CallError {
                                            function: function_name.clone(),
                                            message: format!(
                                                "Failed to get run_with_otel_context: {}",
                                                e
                                            ),
                                        });
                                    }
                                },
                                Err(e) => {
                                    return Err(PythonExecutionError::CallError {
                                        function: function_name.clone(),
                                        message: format!("Failed to import __main__: {}", e),
                                    });
                                }
                            },
                            Err(e) => {
                                return Err(PythonExecutionError::CallError {
                                    function: function_name.clone(),
                                    message: format!(
                                        "Failed to define run_with_otel_context: {}",
                                        e
                                    ),
                                });
                            }
                        };

                        // Call run_with_otel_context with carrier, user function, and message
                        let py_func = py_function.bind(py);
                        match run_with_otel_context.call1((carrier_dict, py_func, py_msg)) {
                            Ok(result) => Ok(result.into()),
                            Err(e) => Err(PythonExecutionError::CallError {
                                function: function_name.clone(),
                                message: e.to_string(),
                            }),
                        }
                    } else {
                        // OTEL propagation disabled - call function directly
                        let py_func = py_function.bind(py);
                        match py_func.call1((py_msg,)) {
                            Ok(result) => Ok(result.into()),
                            Err(e) => Err(PythonExecutionError::CallError {
                                function: function_name.clone(),
                                message: e.to_string(),
                            }),
                        }
                    }
                });

                let result = match result {
                    Ok(obj) => obj,
                    Err(e) => return Err(e),
                };

                // Force flush Python spans after function execution
                Python::with_gil(|py| {
                    let flush_code = r#"
try:
    import opentelemetry.trace as trace
    provider = trace.get_tracer_provider()
    if hasattr(provider, 'force_flush'):
        provider.force_flush()
except Exception:
    pass
"#;
                    let _ = py.run(
                        std::ffi::CString::new(flush_code).unwrap().as_c_str(),
                        None,
                        None,
                    );
                });

                Ok(result)
            }
            .instrument(sync_call_span)
            .await?
        };

        // Deserialize Python result to Rust
        let rust_result = {
            let deserialize_span = crate::tracing_utils::create_python_deserialize_span(
                std::any::type_name::<M::Ok>(),
            );
            async {
                Python::with_gil(|py| {
                    let bound = result.bind(py);
                    crate::serde_py::from_pyobject(bound).map_err(|e| {
                        PythonExecutionError::DeserializationError {
                            message: e.to_string(),
                        }
                    })
                })
            }
            .instrument(deserialize_span)
            .await
        };

        match rust_result {
            Ok(reply) => {
                tracing::debug!(
                    event = "deserialize_success",
                    ?reply,
                    "Deserialized Python result to Rust"
                );
                Ok(reply)
            }
            Err(e) => {
                tracing::error!(event = "deserialize_error", error = %e, "Failed to deserialize Python result");
                Err(e)
            }
        }
    }
}
