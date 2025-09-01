#![forbid(unsafe_code)]

//! # Kameo Child Process IPC Library
//!
//! This library provides a robust inter-process communication (IPC) system for Rust applications
//! that need to communicate with child processes, particularly Python subprocesses. It's built on
//! top of the Kameo actor system and provides both synchronous and streaming message protocols.
//!
//! ## Architecture Overview
//!
//! The library implements a multiplexed IPC protocol over Unix domain sockets with the following
//! key components:
//!
//! ### Core Protocol
//! - **Control Messages**: Handshake, Sync (single response), Stream (streaming response), StreamEnd
//! - **Multiplexed Envelopes**: All messages are wrapped with correlation IDs and tracing context
//! - **Bidirectional Communication**: Request/response and callback channels
//!
//! ### Streaming Support
//! The library now uses a unified streaming protocol where all responses are treated as streams:
//! - **Sync Messages**: Converted to single-item streams for backward compatibility
//! - **Stream Messages**: Native streaming with multiple items and proper termination
//! - **Stream End Markers**: Explicit stream termination with optional final value
//!
//! ### Key Types
//! - `SubprocessIpcBackend`: Core IPC backend for parent processes
//! - `SubprocessIpcActor`: Kameo actor wrapper for IPC communication
//! - `ChildProcessMessageHandler`: Trait for handling messages in child processes
//! - `ReplySlot`: Unified slot for both sync and streaming responses
//!
//! ### Tracing & Observability
//! - OpenTelemetry integration for distributed tracing
//! - Metrics collection for performance monitoring
//! - Structured logging with correlation IDs
//!
//! ## Usage Example
//!
//! ```rust
//! use kameo_child_process::prelude::*;
//!
//! // Define your message types (serde only; on-wire serialization uses serde-brief)
//! #[derive(Serialize, Deserialize, Debug, Clone)]
//! struct MyMessage { data: String }
//!
//! impl KameoChildProcessMessage for MyMessage {
//!     type Ok = String;
//! }
//!
//! // Create backend and actor
//! let backend = SubprocessIpcBackend::from_duplex(stream);
//! let actor = spawn_subprocess_ipc_actor(backend);
//!
//! // Send synchronous message
//! let response = actor.ask(MyMessage { data: "hello".to_string() }).await?;
//!
//! // Send streaming message
//! let stream = actor.send_stream(MyMessage { data: "stream".to_string() }).await?;
//! while let Some(item) = stream.next().await {
//!     println!("Received: {:?}", item?);
//! }
//! ```

use futures::stream::StreamExt;
use once_cell::sync::Lazy;
use opentelemetry::propagation::TextMapPropagator;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use std::sync::Arc;

static GLOBAL_PROPAGATOR: Lazy<Arc<dyn TextMapPropagator + Send + Sync>> =
    Lazy::new(|| Arc::new(TraceContextPropagator::new()));

pub mod callback;
pub use callback::*;
pub mod handshake;
pub use handshake::*;
pub mod metrics;
pub mod tracing_utils;
pub mod callback_runtime;

use anyhow::Result;
use async_trait::async_trait;
// Wire format: serde + serde-brief
use kameo::actor::{Actor, ActorRef, WeakActorRef};
use kameo::prelude::*;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::io;
use std::marker::PhantomData;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tracing::Level;
use tracing_futures::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;

use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize};
use tokio::sync::mpsc;
use tracing::{error, trace};
pub mod error;
pub use error::PythonExecutionError;

/// A serializable representation of a tracing span's context for OTEL propagation
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct TracingContext(pub std::collections::HashMap<String, String>);

impl TracingContext {
    /// Capture the current span context for propagation
    pub fn from_current_span() -> Self {
        use opentelemetry::propagation::Injector;
        let mut context = Self::default();
        struct MapInjector<'a>(&'a mut std::collections::HashMap<String, String>);
        impl Injector for MapInjector<'_> {
            fn set(&mut self, key: &str, value: String) {
                self.0.insert(key.to_string(), value);
            }
        }

        // Get the current span context from tracing, not from OTEL
        let current_ctx = tracing::Span::current().context();

        // Debug: Log what's in the current context
        tracing::debug!(
            event = "tracing_context_debug",
            "Creating TracingContext from current span"
        );

        GLOBAL_PROPAGATOR.inject_context(&current_ctx, &mut MapInjector(&mut context.0));

        // Debug: Log what was injected
        tracing::debug!(
            event = "tracing_context_injected",
            context_keys = ?context.0.keys().collect::<Vec<_>>(),
            context_values = ?context.0.values().collect::<Vec<_>>(),
            "TracingContext created"
        );

        context
    }
    /// Extract a parent context for span creation
    pub fn extract_parent(&self) -> opentelemetry::Context {
        use opentelemetry::propagation::Extractor;
        struct MapExtractor<'a>(&'a std::collections::HashMap<String, String>);
        impl Extractor for MapExtractor<'_> {
            fn get(&self, key: &str) -> Option<&str> {
                self.0.get(key).map(|s| s.as_str())
            }
            fn keys(&self) -> Vec<&str> {
                self.0.keys().map(|k| k.as_str()).collect()
            }
        }
        GLOBAL_PROPAGATOR.extract(&MapExtractor(&self.0))
    }
}

/// A wrapper to send a message with its tracing context.
#[derive(Serialize, Deserialize, Debug)]
pub struct WithTracingContext<T> {
    pub inner: T,
    pub context: TracingContext,
}

/// Trait for actors that need access to the runtime
#[async_trait]
pub trait RuntimeAware: Actor
where
    Self::Error: std::error::Error + Send + Sync + 'static,
{
    /// Called when the runtime is available, before on_start
    async fn init_with_runtime(self) -> Result<Self, Self::Error>
    where
        Self: Sized;
}

/// Trait object for async read/write operations
pub trait AsyncReadWrite: AsyncRead + AsyncWrite + Send + Unpin + 'static {}

impl std::fmt::Debug for dyn AsyncReadWrite {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "AsyncReadWrite")
    }
}

impl<T: AsyncRead + AsyncWrite + Send + Unpin + 'static> AsyncReadWrite for T {}

// Helper methods are now part of the trait itself
impl dyn AsyncReadWrite {
    // No need for helper methods here anymore since they're in the trait
}

/// Trait for messages that can be sent to a Kameo child process actor.
pub trait KameoChildProcessMessage:
    Send + Serialize + DeserializeOwned + std::fmt::Debug + Clone + 'static
{
    type Ok: Send + Serialize + DeserializeOwned + std::fmt::Debug + Clone + 'static;
}

/// Control message for the unified IPC protocol.
///
/// This enum represents the different types of control messages that can be sent over the IPC
/// connection. The protocol has been unified to treat all responses as streams, with sync
/// messages being converted to single-item streams for backward compatibility.
///
/// ## Protocol Flow
///
/// 1. **Handshake**: Initial connection establishment
/// 2. **Sync**: Single request/response (converted to single-item stream internally)
/// 3. **Stream**: Multiple items in a stream
/// 4. **StreamEnd**: Explicit stream termination with optional final value
///
/// ## Examples
///
/// ```rust
/// // Sync message (single response)
/// Control::Sync(MultiplexEnvelope { correlation_id: 1, inner: msg, context })
///
/// // Streaming message (multiple responses)
/// Control::Stream(MultiplexEnvelope { correlation_id: 2, inner: msg, context })
///
/// // Stream termination
/// Control::StreamEnd(MultiplexEnvelope { correlation_id: 2, inner: Some(final_value), context })
/// ```
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum Control<T> {
    /// Initial handshake message for connection establishment
    Handshake,
    /// Synchronous message - converted to single-item stream internally
    Sync(MultiplexEnvelope<T>),
    /// Streaming message - part of a multi-item stream
    Stream(MultiplexEnvelope<T>),
    /// Stream end marker - indicates stream completion with optional final value
    StreamEnd(MultiplexEnvelope<Option<T>>),
}

impl<T> Control<T> {
    pub fn is_handshake(&self) -> bool {
        matches!(self, Control::Handshake)
    }
    pub fn into_sync(self) -> Option<MultiplexEnvelope<T>> {
        match self {
            Control::Sync(env) => Some(env),
            _ => None,
        }
    }
    pub fn is_stream(&self) -> bool {
        matches!(self, Control::Stream(_))
    }
    pub fn is_stream_end(&self) -> bool {
        matches!(self, Control::StreamEnd(_))
    }
}

/// Envelope for multiplexed requests
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MultiplexEnvelope<T> {
    pub correlation_id: u64,
    pub inner: T,
    pub context: TracingContext,
}

pub struct WriteRequest<M> {
    pub correlation_id: u64,
    pub control: Control<M>,
}

pub type CorrelationId = u64;
/// Unified reply slot for both synchronous and streaming responses.
///
/// This struct implements a unified approach where all responses are treated as streams.
/// Synchronous responses are converted to single-item streams internally, while streaming
/// responses can contain multiple items.
///
/// ## Design
///
/// - **Stream Sender**: Used by the child process to send response items
/// - **Stream Receiver**: Used by the parent process to receive response items
/// - **Unified Protocol**: Both sync and stream responses use the same underlying mechanism
///
/// ## Usage
///
/// ```rust
/// // Create a new reply slot
/// let mut slot = ReplySlot::new();
///
/// // For sync responses (single item)
/// slot.try_send_stream_item(Ok(result))?;
/// slot.close_stream();
///
/// // For streaming responses (multiple items)
/// slot.try_send_stream_item(Ok(item1))?;
/// slot.try_send_stream_item(Ok(item2))?;
/// slot.close_stream();
///
/// // Parent receives the stream
/// let receiver = slot.take_stream_receiver()?;
/// while let Some(item) = receiver.recv().await {
///     println!("Received: {:?}", item?);
/// }
/// ```
pub struct ReplySlot<R> {
    /// Sender for streaming response items (used by child process)
    stream_sender: Option<mpsc::UnboundedSender<Result<R, PythonExecutionError>>>,
    /// Receiver for streaming response items (used by parent process)
    stream_receiver: Option<mpsc::UnboundedReceiver<Result<R, PythonExecutionError>>>,
    /// Permit to limit max in-flight requests; dropped when slot is removed
    inflight_permit: Option<tokio::sync::OwnedSemaphorePermit>,
}

impl<R> ReplySlot<R> {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        Self {
            stream_sender: Some(tx),
            stream_receiver: Some(rx),
            inflight_permit: None,
        }
    }

    pub fn new_streaming() -> Self {
        Self::new() // Same as new() now
    }

    // Sync methods now use streaming internally
    pub async fn set_and_notify(&mut self, value: R) {
        if let Some(sender) = &self.stream_sender {
            if sender.send(Ok(value)).is_err() {
                tracing::error!(
                    event = "reply_slot",
                    error = "Failed to send reply, receiver dropped",
                    "Reply channel closed"
                );
            }
            // Close the stream after sending
            self.close_stream();
        }
    }

    pub async fn wait(mut self) -> Option<R> {
        if let Some(mut receiver) = self.stream_receiver.take() {
            // Wait for the first item and close
            match receiver.recv().await {
                Some(Ok(item)) => Some(item),
                Some(Err(_)) => None, // Error case
                None => None,         // Channel closed
            }
        } else {
            None
        }
    }

    // Streaming methods
    pub fn try_send_stream_item(&mut self, item: Result<R, PythonExecutionError>) -> bool {
        if let Some(sender) = &self.stream_sender {
            sender.send(item).is_ok()
        } else {
            false
        }
    }

    pub fn try_send_stream_error(&mut self, err: PythonExecutionError) {
        if let Some(sender) = &self.stream_sender {
            let _ = sender.send(Err(err));
        }
    }

    pub fn close_stream(&mut self) {
        self.stream_sender.take();
    }

    pub fn into_stream_receiver(
        mut self,
    ) -> Option<mpsc::UnboundedReceiver<Result<R, PythonExecutionError>>> {
        self.stream_receiver.take()
    }

    pub fn take_stream_receiver(
        &mut self,
    ) -> Option<mpsc::UnboundedReceiver<Result<R, PythonExecutionError>>> {
        self.stream_receiver.take()
    }

    pub fn set_inflight_permit(&mut self, permit: tokio::sync::OwnedSemaphorePermit) {
        self.inflight_permit = Some(permit);
    }
}

impl<R> ReplySlot<Result<R, PythonExecutionError>> {
    pub fn try_set_err(&mut self, err: PythonExecutionError) {
        if let Some(sender) = self.stream_sender.take() {
            if sender.send(Err(err)).is_err() {
                tracing::error!(
                    event = "reply_slot",
                    error = "Failed to send error reply, receiver dropped",
                    "Error reply channel closed"
                );
            }
            // Close the stream after sending
            self.close_stream();
        }
    }
}

impl<R> Default for ReplySlot<R> {
    fn default() -> Self {
        Self::new()
    }
}

pub struct InFlightMap<OkType>(pub Arc<DashMap<CorrelationId, ReplySlot<OkType>>>);

impl<OkType> Clone for InFlightMap<OkType> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<OkType> InFlightMap<OkType> {
    pub fn new() -> Self {
        Self(Arc::new(DashMap::new()))
    }
}

impl<R> Default for InFlightMap<R> {
    fn default() -> Self {
        Self::new()
    }
}

/// Encapsulates a full-duplex UnixStream for protocol artefacts, enforcing correct split/unsplit usage.
pub struct DuplexUnixStream {
    inner: tokio::net::UnixStream,
}

impl DuplexUnixStream {
    pub fn new(inner: tokio::net::UnixStream) -> Self {
        Self { inner }
    }
    pub fn into_split(
        self,
    ) -> (
        tokio::net::unix::OwnedReadHalf,
        tokio::net::unix::OwnedWriteHalf,
    ) {
        self.inner.into_split()
    }
    pub fn into_inner(self) -> tokio::net::UnixStream {
        self.inner
    }
}

impl AsRef<tokio::net::UnixStream> for DuplexUnixStream {
    fn as_ref(&self) -> &tokio::net::UnixStream {
        &self.inner
    }
}

/// Core IPC backend for parent processes implementing the unified streaming protocol.
///
/// This backend manages the communication with child processes over Unix domain sockets.
/// It implements a unified streaming protocol where all responses are treated as streams,
/// with synchronous responses converted to single-item streams for backward compatibility.
///
/// ## Architecture
///
/// - **Writer Task**: Handles sending messages to the child process
/// - **Reader Task**: Handles receiving responses from the child process
/// - **In-Flight Map**: Tracks pending requests with correlation IDs
/// - **Adaptive Throttling**: Monitors pending count to prevent overwhelming
///
/// ## Protocol Flow
///
/// 1. **Send Message**: Creates correlation ID and ReplySlot
/// 2. **Send Control**: Wraps message in appropriate Control variant
/// 3. **Wait Response**: Receives stream items from child process
/// 4. **Return Result**: For sync messages, returns first item; for streams, returns full stream
///
/// ## Key Features
///
/// - **Unified Streaming**: All responses use the same underlying stream mechanism
/// - **Correlation Tracking**: Each request gets a unique correlation ID
/// - **Error Handling**: Comprehensive error propagation and cleanup
/// - **Metrics Integration**: Built-in performance monitoring
/// - **Graceful Shutdown**: Proper cleanup on cancellation
///
/// ## Usage
///
/// ```rust
/// // Create backend from duplex stream
/// let backend = SubprocessIpcBackend::from_duplex(stream);
///
/// // Send synchronous message (returns single result)
/// let result = backend.send(message).await?;
///
/// // Send streaming message (returns stream of results)
/// let stream = backend.send_stream(message).await?;
/// while let Some(item) = stream.next().await {
///     println!("Stream item: {:?}", item?);
/// }
/// ```
pub struct SubprocessIpcBackend<M>
where
    M: KameoChildProcessMessage + Send + Sync + Clone + 'static,
{
    /// Channel for sending write requests to the writer task
    write_tx: tokio::sync::mpsc::Sender<WriteRequest<M>>,
    /// Map of in-flight requests indexed by correlation ID
    in_flight: InFlightMap<Result<M::Ok, PythonExecutionError>>,
    /// Atomic counter for generating unique correlation IDs
    next_id: AtomicU64,
    /// Token for graceful shutdown of all tasks
    cancellation_token: tokio_util::sync::CancellationToken,
    /// Track pending requests for adaptive throttling
    pending_count: AtomicUsize,
    /// Limit concurrent in-flight requests to protect IPC channel and slot map
    inflight_limit: Arc<tokio::sync::Semaphore>,
    /// Phantom data for message type
    _phantom: std::marker::PhantomData<M>,
}

impl<M> SubprocessIpcBackend<M>
where
    M: KameoChildProcessMessage + Send + Sync + Clone + 'static,
{
    /// Canonical constructor: wire up the backend from a DuplexUnixStream, splitting it internally.
    pub fn from_duplex(stream: DuplexUnixStream) -> Arc<Self> {
        let (read_half, write_half) = stream.into_split();
        Self::new(read_half, write_half)
    }

    pub fn new(
        read_half: tokio::net::unix::OwnedReadHalf,
        write_half: tokio::net::unix::OwnedWriteHalf,
    ) -> Arc<Self> {
        use crate::error::PythonExecutionError;
        use std::sync::Arc;
        use tokio::sync::mpsc::channel;
        // Bounded write queue to enforce backpressure across the IPC writer path
        let (write_tx, mut write_rx) = channel::<WriteRequest<M>>(1024);
        let in_flight: InFlightMap<Result<M::Ok, PythonExecutionError>> = InFlightMap::new();
        let in_flight_reader = in_flight.clone();
        let cancellation_token = tokio_util::sync::CancellationToken::new();
        let cancellation_token_writer = cancellation_token.clone();
        let cancellation_token_reader = cancellation_token.clone();

        // Create the result first so we can track pending counts
        let result = Arc::new(Self {
            write_tx,
            in_flight,
            next_id: AtomicU64::new(1),
            cancellation_token,
            pending_count: AtomicUsize::new(0),
            inflight_limit: Arc::new(tokio::sync::Semaphore::new(
                std::env::var("KAMEO_INFLIGHT_LIMIT")
                    .ok()
                    .and_then(|s| s.parse::<usize>().ok())
                    .unwrap_or(8192),
            )),
            _phantom: PhantomData,
        });

        let result_clone = result.clone();

        // Writer task with simpler direct writes
        tokio::spawn(async move {
            let mut writer = crate::framing::LengthPrefixedWrite::new(write_half);

            loop {
                tokio::select! {
                    _ = cancellation_token_writer.cancelled() => {
                        tracing::info!(event = "writer_task", "Writer task received shutdown signal, exiting");
                        break;
                    }

                    message = write_rx.recv() => {
                        match message {
                            Some(write_req) => {
                                // Process write directly, one at a time
                                if let Err(e) = writer.write_msg(&write_req.control).await {
                                    tracing::error!(event = "writer_task", error = ?e, "Failed to write message");
                                    // Don't break on errors - just log them and continue
                                }
                            },
                            None => {
                                tracing::info!(event = "writer_task", "Channel closed, exiting");
                                break;
                            }
                        }
                    }
                }
            }

            tracing::info!(event = "writer_task", "Writer task exiting");
        });

        // Reader task with improved error handling
        tokio::spawn(async move {
            let mut reader = crate::framing::LengthPrefixedRead::new(read_half);

            // Initialize metrics
            metrics::init_metrics();

            // Create a task to periodically log metrics
            let metrics_token = cancellation_token_reader.clone();
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(5));
                loop {
                    tokio::select! {
                        _ = metrics_token.cancelled() => {
                            break;
                        }
                        _ = interval.tick() => {
                            metrics::MetricsReporter::log_metrics_state();
                        }
                    }
                }
            });

            loop {
                tokio::select! {
                    _ = cancellation_token_reader.cancelled() => {
                        tracing::info!(event = "reader_task", "Reader task received shutdown signal, exiting");
                        break;
                    }
                    result = reader.read_msg::<Control<Result<M::Ok, PythonExecutionError>>>() => {
                        match result {
                            Ok(ctrl) => {
                                match ctrl {
                                    Control::Sync(envelope) => {
                                    let correlation_id = envelope.correlation_id;
                                        trace!(event = "parent_in_flight", action = "sync_response_received", correlation_id, "Received sync response for correlation_id");

                                        // Handle sync responses as single-item streams
                                        if let Some(mut slot) = in_flight_reader.0.get_mut(&correlation_id) {
                                            let result = envelope.inner.clone();
                                            // Send through streaming channel and close
                                            if slot.try_send_stream_item(Ok(result.clone())) {
                                                trace!(event = "parent_in_flight", action = "sync_item_sent", correlation_id, "Sent sync item through streaming channel");
                                                slot.close_stream();
                                            } else {
                                                // Receiver dropped: treat as normal cancellation and clean up
                                                trace!(event = "parent_in_flight", action = "sync_receiver_dropped", correlation_id, "Sync reply receiver dropped; cleaning up");
                                                // remove inflight entry below
                                                metrics::MetricsHandle::parent(std::any::type_name::<M>()).track_error("sync_receiver_dropped");
                                            }
                                        } else {
                                            tracing::error!(event = "parent_in_flight", correlation_id, "Received sync reply for unknown correlation id");
                                            metrics::MetricsHandle::parent(std::any::type_name::<M>()).track_error("sync_unknown_correlation_id");
                                        }

                                        // Remove from in_flight after sending sync response
                                        if in_flight_reader.0.remove(&correlation_id).is_some() {
                                            result_clone.pending_count.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                                        }
                                    }
                                    Control::Stream(envelope) => {
                                        let correlation_id = envelope.correlation_id;
                                        trace!(event = "parent_in_flight", action = "stream_item_received", correlation_id, "Received stream item for correlation_id");

                                        // Handle streaming responses
                                        if let Some(mut slot) = in_flight_reader.0.get_mut(&correlation_id) {
                                            let result = envelope.inner.clone();
                                            // Send through streaming channel
                                            if slot.try_send_stream_item(Ok(result.clone())) {
                                                trace!(event = "parent_in_flight", action = "stream_item_sent", correlation_id, "Sent stream item through streaming channel");
                                    } else {
                                                // Receiver dropped mid-stream: downgrade to trace and remove inflight entry
                                                trace!(event = "parent_in_flight", action = "stream_receiver_dropped", correlation_id, "Stream reply receiver dropped; cleaning up");
                                                // Close and remove to avoid repeated log spam
                                                if let Some(mut slot) = in_flight_reader.0.get_mut(&correlation_id) {
                                                    slot.close_stream();
                                                }
                                                if in_flight_reader.0.remove(&correlation_id).is_some() {
                                                    result_clone.pending_count.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                                                }
                                                metrics::MetricsHandle::parent(std::any::type_name::<M>()).track_error("stream_receiver_dropped");
                                            }
                                        } else {
                                            tracing::error!(event = "parent_in_flight", correlation_id, "Received stream item for unknown correlation id");
                                            metrics::MetricsHandle::parent(std::any::type_name::<M>()).track_error("stream_unknown_correlation_id");
                                        }
                                    }
                                    Control::StreamEnd(envelope) => {
                                        let correlation_id = envelope.correlation_id;
                                        trace!(event = "parent_in_flight", action = "stream_end_received", correlation_id, "Received stream end for correlation_id");

                                        // Close the streaming channel and remove from in_flight
                                        if let Some(mut slot) = in_flight_reader.0.get_mut(&correlation_id) {
                                            slot.close_stream();
                                            trace!(event = "parent_in_flight", action = "stream_complete", correlation_id, "Stream completed and closed");
                                        }
                                        // Remove from in_flight
                                        if in_flight_reader.0.remove(&correlation_id).is_some() {
                                            result_clone.pending_count.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                                        }
                                    }
                                    Control::Handshake => {
                                        // Handshake messages shouldn't be received by parent in normal operation
                                        tracing::warn!(event = "parent_in_flight", action = "unexpected_handshake", "Received unexpected handshake from child");
                                    }
                                }
                            }
                            Err(e) => {
                                use std::io::ErrorKind;
                                if e.kind() == ErrorKind::UnexpectedEof {
                                    let in_flight_len = in_flight_reader.0.len();
                                    if in_flight_len == 0 {
                                        tracing::info!(event = "reader_task", "EOF received, no in-flight requests, clean shutdown");
                                    } else {
                                        tracing::warn!(event = "reader_task", in_flight_len, "EOF received with pending in-flight requests, waking all with error");

                                        // Track error in metrics
                                        metrics::MetricsHandle::parent(std::any::type_name::<M>()).track_error("eof_with_pending");
                                    }
                                    break;
                                }
                                error!(event = "parent_read_error", error = ?e, "Parent reader task error, exiting");

                                // Track error in metrics
                                metrics::MetricsHandle::parent(std::any::type_name::<M>()).track_error("read_error");
                                break;
                            }
                        }
                    }
                }
            }
            // On exit, drain in_flight and send error to all pending
            in_flight_reader.0.iter_mut().for_each(|mut item| {
                let (_corr_id, slot) = item.pair_mut();
                if let Some(sender) = slot.stream_sender.take() {
                    let err = PythonExecutionError::ExecutionError {
                        message: "IPC backend reply loop exited".to_string(),
                    };
                    if sender.send(Err(err)).is_err() {
                        tracing::error!(
                            event = "reader_task",
                            error = "Failed to send shutdown error to waiting task",
                            "Failed to notify waiting task about shutdown"
                        );
                    }
                }
            });
            // Reset pending count to 0
            result_clone
                .pending_count
                .store(0, std::sync::atomic::Ordering::SeqCst);
            // Clear the map after notifying all waiting tasks
            in_flight_reader.0.clear();
            tracing::info!(event = "reader_task", "Reader task exiting");
        });

        result
    }

    /// Always generate a unique correlation_id using the atomic counter.
    fn next_correlation_id(&self) -> u64 {
        self.next_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    /// Triggers shutdown of the backend, waking all pending requests with error.
    pub fn shutdown(&self) {
        self.cancellation_token.cancel();
    }

    /// Returns the current number of pending requests
    pub fn pending_count(&self) -> usize {
        self.pending_count.load(std::sync::atomic::Ordering::SeqCst)
    }

    pub async fn send(&self, msg: M) -> Result<M::Ok, PythonExecutionError> {
        // Acquire inflight permit to apply backpressure when too many requests are queued
        let permit = self
            .inflight_limit
            .clone()
            .acquire_owned()
            .await
            .map_err(|e| PythonExecutionError::ExecutionError { message: format!("Failed to acquire inflight permit: {e}") })?;

        let correlation_id = self.next_correlation_id();
        let msg_type = std::any::type_name::<M>();

        // Create the root ipc-message span that will encompass the entire IPC operation
        let ipc_message_span =
            tracing_utils::create_root_ipc_message_span(correlation_id, msg_type);

        // Create the ipc-parent-send span as a child of the ipc-message span
        let _send_span =
            tracing_utils::create_ipc_parent_send_span(correlation_id, msg_type, &ipc_message_span);

        // Create a reply slot (now always streaming)
        let mut slot = ReplySlot::new();
        slot.set_inflight_permit(permit);

        // Insert into in_flight map and track pending count
        {
            self.in_flight.0.insert(correlation_id, slot);
            self.pending_count
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }

        // Create the envelope with the ipc-message span context (not ipc-parent-send)
        // This ensures ipc-child-receive will be a sibling of ipc-parent-send
        let envelope = {
            let _message_guard = ipc_message_span.enter();
            MultiplexEnvelope {
                correlation_id,
                inner: msg,
                context: TracingContext::from_current_span(),
            }
        };

        // Send the message as a sync request (but handled as single-item stream)
        let write_req = WriteRequest {
            correlation_id,
            control: Control::Sync(envelope),
        };
        if let Err(e) = self.write_tx.send(write_req).await {
            self.in_flight.0.remove(&correlation_id);
            self.pending_count
                .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
            return Err(PythonExecutionError::ExecutionError {
                message: format!("Failed to send write request: {e}"),
            });
        }

        // Wait for the response by getting the receiver from the in_flight map
        let mut receiver = {
            let mut slot = self
                .in_flight
                .0
                .get_mut(&correlation_id)
                .expect("Slot should exist");
            slot.take_stream_receiver()
                .expect("Stream receiver should be available")
        };

        // Wait for the first (and only) item from the stream
        match receiver.recv().await {
            Some(Ok(Ok(result))) => Ok(result),
            Some(Ok(Err(e))) => Err(e),
            Some(Err(e)) => Err(e),
            None => Err(PythonExecutionError::ExecutionError {
                message: "Stream closed without response".to_string(),
            }),
        }
    }

    /// Send a streaming message to the child process.
    /// Returns a stream of responses from the child.
    pub async fn send_stream(
        &self,
        msg: M,
    ) -> Result<
        Box<dyn futures::stream::Stream<Item = Result<M::Ok, PythonExecutionError>> + Send + Unpin>,
        PythonExecutionError,
    > {
        let correlation_id = self.next_correlation_id();
        let msg_type = std::any::type_name::<M>();

        // Create the root ipc-message span that will encompass the entire IPC operation
        let ipc_message_span =
            tracing_utils::create_root_ipc_message_span(correlation_id, msg_type);

        // Create the ipc-parent-send span as a child of the ipc-message span
        let _send_span =
            tracing_utils::create_ipc_parent_send_span(correlation_id, msg_type, &ipc_message_span);

        // Create a streaming reply slot
        let mut slot = ReplySlot::new_streaming();
        let stream_receiver = slot
            .take_stream_receiver()
            .expect("Stream receiver should be available");

        // Insert into in_flight map and track pending count
        {
            self.in_flight.0.insert(correlation_id, slot);
            self.pending_count
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }

        // Create the envelope with the ipc-message span context (not ipc-parent-send)
        // This ensures ipc-child-receive will be a sibling of ipc-parent-send
        let envelope = {
            let _message_guard = ipc_message_span.enter();
            MultiplexEnvelope {
                correlation_id,
                inner: msg,
                context: TracingContext::from_current_span(),
            }
        };

        // Send the message as a streaming request
        let write_req = WriteRequest {
            correlation_id,
            control: Control::Stream(envelope),
        };
        tracing::debug!(
            event = "send_stream_request",
            correlation_id = correlation_id,
            control_type = "Stream",
            "Sending streaming request"
        );
        if let Err(e) = self.write_tx.send(write_req).await {
            self.in_flight.0.remove(&correlation_id);
            self.pending_count
                .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
            return Err(PythonExecutionError::ExecutionError {
                message: format!("Failed to send streaming write request: {e}"),
            });
        }

        // Convert the receiver into a stream using tokio_stream with type conversion
        let stream =
            tokio_stream::wrappers::UnboundedReceiverStream::new(stream_receiver).map(|item| {
                match item {
                    Ok(Ok(result)) => Ok(result),
                    Ok(Err(e)) => Err(e),
                    Err(e) => Err(e),
                }
            });
        Ok(Box::new(stream))
    }
}

/// Configuration for a child process actor
#[derive(Debug)]
pub struct ChildProcessConfig {
    pub name: String,
    pub log_level: Level,
    pub env_vars: Vec<(String, String)>,
}

impl Default for ChildProcessConfig {
    fn default() -> Self {
        Self {
            name: String::new(),
            log_level: Level::DEBUG,
            env_vars: Vec::new(),
        }
    }
}

/// Trait for handling messages in child processes with unified streaming support.
///
/// This trait defines the interface for processing messages in child processes.
/// It supports both synchronous and streaming message handling, with streaming
/// being the primary mechanism and sync messages converted to single-item streams.
///
/// ## Unified Streaming Protocol
///
/// All message handling now uses streams internally:
/// - **Sync Messages**: `handle_child_message()` returns a single result, converted to single-item stream
/// - **Stream Messages**: `handle_child_message_stream()` returns a multi-item stream
/// - **Backward Compatibility**: Default implementation converts sync to stream automatically
///
/// ## Implementation Guidelines
///
/// - **Sync Handlers**: Implement `handle_child_message()` for single request/response
/// - **Stream Handlers**: Override `handle_child_message_stream()` for multi-item responses
/// - **Error Handling**: Always return `PythonExecutionError` for proper error propagation
/// - **Async Safety**: Ensure handlers are `Send + Clone + 'static`
///
/// ## Example Implementation
///
/// ```rust
/// struct MyHandler;
///
/// #[async_trait]
/// impl ChildProcessMessageHandler<MyMessage> for MyHandler {
///     // Sync handler (converted to single-item stream)
///     async fn handle_child_message(&mut self, msg: MyMessage) -> Result<MyResponse, PythonExecutionError> {
///         // Process single message
///         Ok(MyResponse { result: "done" })
///     }
///     
///     // Stream handler (optional override)
///     async fn handle_child_message_stream(&mut self, msg: MyMessage) -> Result<Box<dyn Stream<Item = Result<MyResponse, PythonExecutionError>> + Send + Unpin>, PythonExecutionError> {
///         // Process streaming message
///         let stream = futures::stream::iter(vec![
///             Ok(MyResponse { result: "item1" }),
///             Ok(MyResponse { result: "item2" }),
///         ]);
///         Ok(Box::new(stream))
///     }
/// }
/// ```
#[async_trait]
pub trait ChildProcessMessageHandler<Msg>
where
    Msg: KameoChildProcessMessage,
{
    /// Handle a synchronous message and return a single response.
    ///
    /// This method is converted to a single-item stream internally for the unified protocol.
    /// Override `handle_child_message_stream()` if you need true streaming behavior.
    async fn handle_child_message(&mut self, msg: Msg) -> Result<Msg::Ok, PythonExecutionError>;

    /// Handle a synchronous message with tracing context and return a single response.
    ///
    /// This method is called by the child process loop with the tracing context from the envelope.
    /// The default implementation calls `handle_child_message()` without the context.
    async fn handle_child_message_with_context(
        &mut self,
        msg: Msg,
        _context: TracingContext,
    ) -> Result<Msg::Ok, PythonExecutionError> {
        self.handle_child_message(msg).await
    }

    /// Handle a streaming message and return a stream of responses.
    ///
    /// Default implementation converts the single response from `handle_child_message()`
    /// to a single-item stream for backward compatibility. Override this method to
    /// implement true streaming behavior with multiple response items.
    async fn handle_child_message_stream(
        &mut self,
        msg: Msg,
    ) -> Result<
        Box<
            dyn futures::stream::Stream<Item = Result<Msg::Ok, PythonExecutionError>>
                + Send
                + Unpin,
        >,
        PythonExecutionError,
    > {
        // Default implementation: convert single response to single-item stream
        let result = self.handle_child_message(msg).await;
        let stream = futures::stream::iter(vec![result]);
        Ok(Box::new(stream))
    }
}

/// Run the IPC handler loop in the child process. No ActorRef, no Clone, no spawn, just handle messages.
#[derive(Debug, Error)]
pub enum ChildProcessLoopError {
    #[error("IO error: {0}")]
    Io(std::io::Error),
}

impl From<std::io::Error> for ChildProcessLoopError {
    fn from(e: std::io::Error) -> Self {
        ChildProcessLoopError::Io(e)
    }
}

// Replace the entire read_next_message function with the corrected version without decoding
async fn read_next_message(
    conn: &mut tokio::net::UnixStream,
) -> Result<Option<Vec<u8>>, io::Error> {
    tracing::trace!(
        event = "child_read",
        step = "before_len",
        "About to read length prefix"
    );
    let mut len_buf = [0u8; 4];
    match conn.read_exact(&mut len_buf).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
            tracing::trace!(
                event = "child_read",
                step = "clean_eof",
                "Clean EOF detected on length read"
            );
            return Ok(None);
        }
        Err(e) => {
            tracing::trace!(event = "child_read", step = "error", error = ?e);
            return Err(e);
        }
    }
    let msg_len = u32::from_le_bytes(len_buf) as usize;
    tracing::trace!(
        event = "child_read",
        step = "after_len",
        ?len_buf,
        msg_len,
        "Read length prefix"
    );
    tracing::trace!(
        event = "child_read",
        step = "before_msg",
        msg_len,
        "About to read message of len {}",
        msg_len
    );
    let mut msg_buf = vec![0u8; msg_len];
    conn.read_exact(&mut msg_buf).await?;
    tracing::trace!(
        event = "child_read",
        step = "after_msg",
        len = msg_buf.len(),
        "Read message"
    );
    Ok(Some(msg_buf))
}

/// Configuration for the child actor loop concurrency
pub struct ChildActorLoopConfig {
    pub max_concurrency: usize,
}

impl Default for ChildActorLoopConfig {
    fn default() -> Self {
        Self {
            max_concurrency: 10_000,
        }
    }
}

pub async fn run_child_actor_loop<H, M>(
    handler: H,
    mut conn: Box<tokio::net::UnixStream>,
    config: Option<ChildActorLoopConfig>,
) -> Result<(), ChildProcessLoopError>
where
    H: ChildProcessMessageHandler<M> + Send + Clone + 'static,
    M: KameoChildProcessMessage + Send + 'static,
    M::Ok: serde::Serialize + std::fmt::Debug + 'static,
{
    tracing::debug!(
        event = "run_child_actor_loop",
        step = "start",
        "run_child_actor_loop started"
    );
    use futures::stream::{FuturesUnordered, StreamExt};
    let _config = config.unwrap_or_default();
    let mut in_flight =
        FuturesUnordered::<Box<dyn futures::Future<Output = ()> + Send + Unpin>>::new();

    // Make reply_tx Option and drop it on shutdown
    let (reply_tx_inner, mut reply_rx) = tokio::sync::mpsc::unbounded_channel::<(u64, Vec<u8>)>();
    let mut reply_tx = Some(reply_tx_inner);
    let mut shutdown = false;
    loop {
        tracing::trace!(
            event = "child_loop",
            step = "enter",
            shutdown = shutdown,
            "Entering child actor loop select"
        );
        if !shutdown {
            tokio::select! {
                biased;
                Some(_) = in_flight.next() => {
                    tracing::trace!(event = "child_in_flight", action = "complete", in_flight_len = in_flight.len(), "Handler future completed in child in_flight");
                }
                read_res = read_next_message(&mut conn) => {
                    match read_res {
                        Ok(Some(msg)) => {
                            tracing::trace!(event = "child_ipc", step = "read", len = msg.len(), raw = ?&msg[..std::cmp::min(100, msg.len())], "Read message from parent");
                            let ctrl: Control<M> = match serde_brief::from_slice(&msg[..]) {
                                Ok(ctrl) => {
                                    tracing::trace!(event = "serde_brief_decode", type_deserialized = std::any::type_name::<Control<M>>(), len = msg.len(), "Decoding Control envelope");
                                    tracing::debug!(event = "control_received", control_type = ?ctrl, "Received control message");
                                    ctrl
                                },
                                Err(e) => {
                                    tracing::error!(event = "serde_brief_decode_error", type_deserialized = std::any::type_name::<Control<M>>(), len = msg.len(), error = ?e, "Failed to decode Control envelope");
                                    continue;
                                }
                            };
                            match ctrl {
                                Control::Handshake => {
                                    tracing::debug!(event = "child_ipc", step = "handshake", "Received handshake from parent");
                                }
                                Control::Sync(envelope) => {
                                    let correlation_id = envelope.correlation_id;

                                    // Debug: Log the context contents to see what's being extracted
                                    tracing::debug!(
                                        event = "context_debug",
                                        correlation_id = correlation_id,
                                        context_keys = ?envelope.context.0.keys().collect::<Vec<_>>(),
                                        context_values = ?envelope.context.0.values().collect::<Vec<_>>(),
                                        "Extracted parent context from envelope"
                                    );

                                    let mut handler = handler.clone();
                                    let reply_tx = reply_tx.as_ref().unwrap().clone();

                                    // Use encapsulated span lifecycle management
                                    let _msg_type = std::any::type_name::<M>();

                                    // Create the message processing future with ipc-child-receive span
                                    let process_future = async move {
                                        // Create ipc-child-receive span with proper parent context
                                        let parent_cx = envelope.context.extract_parent();
                                        let child_receive_span = crate::tracing_utils::create_ipc_child_receive_span(
                                            correlation_id,
                                            std::any::type_name::<M>(),
                                            parent_cx,
                                        );

                                        // Process the message with tracing context
                                        let result = async {
                                            handler.handle_child_message_with_context(envelope.inner, envelope.context.clone()).await
                                        }.instrument(child_receive_span).await;

                                        // Send the reply
                                        let reply_envelope = MultiplexEnvelope {
                                            correlation_id,
                                            inner: result,
                                            context: envelope.context,
                                        };
                                        let ctrl = Control::Sync(reply_envelope);

                                        // Encode the reply to bytes
                                        match serde_brief::to_vec(&ctrl) {
                                            Ok(reply_bytes) => {
                                                trace!(event = "reply_encoded", correlation_id = correlation_id, reply_size = reply_bytes.len(), "Reply encoded successfully");
                                                if let Err(e) = reply_tx.send((correlation_id, reply_bytes)) {
                                                    trace!(event = "reply_send_error", correlation_id = correlation_id, error = ?e, "Failed to send reply");
                                                } else {
                                                    trace!(event = "reply_sent", correlation_id = correlation_id, "Reply sent successfully");
                                                }
                                            }
                                            Err(e) => {
                                                trace!(event = "reply_encoding_error", correlation_id = correlation_id, error = ?e, "Failed to encode reply");
                                            }
                                        }
                                    };

                                    // Box the future without redundant span instrumentation
                                    let boxed_future = Box::pin(process_future);
                                    in_flight.push(Box::new(boxed_future));
                                    tracing::trace!(event = "child_in_flight", action = "push", in_flight_len = in_flight.len(),
                                        correlation_id = correlation_id, "Pushed message future to child in_flight");
                                }
                                Control::Stream(envelope) => {
                                    let correlation_id = envelope.correlation_id;
                                    let parent_cx = envelope.context.extract_parent();

                                    tracing::debug!(
                                        event = "context_debug",
                                        correlation_id = correlation_id,
                                        context_keys = ?envelope.context.0.keys().collect::<Vec<_>>(),
                                        context_values = ?envelope.context.0.values().collect::<Vec<_>>(),
                                        "Extracted parent context from stream envelope"
                                    );

                                    let mut handler = handler.clone();
                                    let reply_tx = reply_tx.as_ref().unwrap().clone();

                                    // Use encapsulated span lifecycle management
                                    let _msg_type = std::any::type_name::<M>();

                                    // Create the streaming message processing future with ipc-child-receive span
                                    let process_future = async move {
                                        // Create ipc-child-receive span with proper parent context
                                        let child_receive_span = crate::tracing_utils::create_ipc_child_receive_span(
                                            correlation_id,
                                            std::any::type_name::<M>(),
                                            parent_cx,
                                        );

                                        // Process the message as a stream
                                        let stream_result = async {
                                            handler.handle_child_message_stream(envelope.inner).await
                                        }.instrument(child_receive_span).await;

                                        match stream_result {
                                            Ok(mut stream) => {
                                                // Process each item in the stream
                                                while let Some(item_result) = stream.next().await {
                                                    let reply_envelope = MultiplexEnvelope {
                                                        correlation_id,
                                                        inner: item_result,
                                                        context: envelope.context.clone(),
                                                    };

                                                    // Send as stream item
                                                    let ctrl = Control::Stream(reply_envelope);

                                                    // Encode the reply to bytes
                                                    match serde_brief::to_vec(&ctrl) {
                                                        Ok(reply_bytes) => {
                                                            trace!(event = "stream_reply_encoded", correlation_id = correlation_id, reply_size = reply_bytes.len(), "Stream reply encoded successfully");
                                                            if let Err(e) = reply_tx.send((correlation_id, reply_bytes)) {
                                                                trace!(event = "stream_reply_send_error", correlation_id = correlation_id, error = ?e, "Failed to send stream reply");
                                                                break;
                                                            } else {
                                                                trace!(event = "stream_reply_sent", correlation_id = correlation_id, "Stream reply sent successfully");
                                                            }
                                                        }
                                                        Err(e) => {
                                                            trace!(event = "stream_reply_encoding_error", correlation_id = correlation_id, error = ?e, "Failed to encode stream reply");
                                                            break;
                                                        }
                                                    }
                                                }

                                                // Send stream end marker
                                                let end_envelope: MultiplexEnvelope<Option<Result<(), PythonExecutionError>>> = MultiplexEnvelope {
                                                    correlation_id,
                                                    inner: None, // Stream end marker
                                                    context: envelope.context,
                                                };
                                                let end_ctrl = Control::StreamEnd(end_envelope);

                                                match serde_brief::to_vec(&end_ctrl) {
                                                    Ok(end_bytes) => {
                                                        trace!(event = "stream_end_encoded", correlation_id = correlation_id, "Stream end encoded successfully");
                                                        if let Err(e) = reply_tx.send((correlation_id, end_bytes)) {
                                                            trace!(event = "stream_end_send_error", correlation_id = correlation_id, error = ?e, "Failed to send stream end");
                                                        } else {
                                                            trace!(event = "stream_end_sent", correlation_id = correlation_id, "Stream end sent successfully");
                                                        }
                                                    }
                                                    Err(e) => {
                                                        trace!(event = "stream_end_encoding_error", correlation_id = correlation_id, error = ?e, "Failed to encode stream end");
                                                    }
                                                }
                                            }
                                            Err(e) => {
                                                // Send error as stream end
                                                let error_envelope: MultiplexEnvelope<Option<Result<M::Ok, PythonExecutionError>>> = MultiplexEnvelope {
                                                    correlation_id,
                                                    inner: Some(Err(e)),
                                                    context: envelope.context,
                                                };
                                                let error_ctrl = Control::StreamEnd(error_envelope);

                                                match serde_brief::to_vec(&error_ctrl) {
                                                    Ok(error_bytes) => {
                                                        trace!(event = "stream_error_encoded", correlation_id = correlation_id, "Stream error encoded successfully");
                                                        if let Err(e) = reply_tx.send((correlation_id, error_bytes)) {
                                                            trace!(event = "stream_error_send_error", correlation_id = correlation_id, error = ?e, "Failed to send stream error");
                                                        } else {
                                                            trace!(event = "stream_error_sent", correlation_id = correlation_id, "Stream error sent successfully");
                                                        }
                                                    }
                                                    Err(e) => {
                                                        trace!(event = "stream_error_encoding_error", correlation_id = correlation_id, error = ?e, "Failed to encode stream error");
                                                    }
                                                }
                                            }
                                        }
                                    };

                                    // Box the future without redundant span instrumentation
                                    let boxed_future = Box::pin(process_future);
                                    in_flight.push(Box::new(boxed_future));
                                    tracing::trace!(event = "child_in_flight", action = "push", in_flight_len = in_flight.len(),
                                        correlation_id = correlation_id, "Pushed streaming message future to child in_flight");
                                }
                                Control::StreamEnd(_) => {
                                    // Stream end messages are only sent from child to parent, not received by child
                                    tracing::warn!(event = "child_ipc", step = "unexpected_stream_end", "Received unexpected stream end message from parent");
                                }
                            }
                        }
                        Ok(None) => {
                            tracing::trace!(event = "child_loop", step = "clean_shutdown", "Clean shutdown (EOF) detected, setting shutdown=true");
                            shutdown = true;
                            reply_tx.take();  // Drop the sender to close the channel
                        }
                        Err(e) => {
                            tracing::error!(event = "child_loop", step = "read_error", error=?e, "Error reading message, exiting loop");
                            return Err(ChildProcessLoopError::Io(e));
                        }
                    }
                }
                Some((correlation_id, reply_bytes)) = reply_rx.recv() => {
                    if let Err(e) = conn.write_all(&(reply_bytes.len() as u32).to_le_bytes()).await {
                        tracing::error!(event = "child_ipc", step = "write_len_error", correlation_id, error = %e, "Failed to write reply length to parent");
                        break;
                    }
                    if let Err(e) = conn.write_all(&reply_bytes).await {
                        tracing::error!(event = "child_ipc", step = "write_reply_error", correlation_id, error = %e, "Failed to write reply to parent");
                        break;
                    }
                    tracing::trace!(event = "child_ipc", step = "reply_sent", correlation_id, len = reply_bytes.len(), "Sent reply to parent");
                }
            }
        } else {
            tokio::select! {
                biased;
                Some(_) = in_flight.next() => {
                    tracing::trace!(event = "child_in_flight", action = "complete", in_flight_len = in_flight.len(), "Handler future completed in child in_flight");
                }
                maybe_reply = reply_rx.recv() => {
                    if let Some((correlation_id, reply_bytes)) = maybe_reply {
                        if let Err(e) = conn.write_all(&(reply_bytes.len() as u32).to_le_bytes()).await {
                            tracing::error!(event = "child_ipc", step = "write_len_error", correlation_id, error = %e, "Failed to write reply length to parent");
                            break;
                        }
                        if let Err(e) = conn.write_all(&reply_bytes).await {
                            tracing::error!(event = "child_ipc", step = "write_reply_error", correlation_id, error = %e, "Failed to write reply to parent");
                            break;
                        }
                        tracing::trace!(event = "child_ipc", step = "reply_sent", correlation_id, len = reply_bytes.len(), "Sent reply to parent");
                    } else {
                        tracing::trace!(event = "child_loop", step = "reply_channel_closed", "Reply channel closed");
                    }
                }
                else => {
                    break;
                }
            }
        }

        if shutdown && in_flight.is_empty() && reply_rx.is_empty() {
            break;
        }
    }
    Ok(())
}

/// Prelude module for commonly used items
pub mod prelude {
    pub use super::StreamingActor;
    pub use super::SubprocessIpcActorExt;
    pub use tokio::runtime;
}

#[derive(Debug, Clone, Copy)]
pub enum RuntimeFlavor {
    CurrentThread,
    MultiThread,
}

#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    pub flavor: RuntimeFlavor,
    pub worker_threads: Option<usize>,
}

pub async fn perform_handshake<M>(
    conn: &mut (impl AsyncRead + AsyncWrite + Unpin),
    is_parent: bool,
) -> Result<(), PythonExecutionError>
where
    M: KameoChildProcessMessage + Send + Sync + 'static,
{
    use crate::Control;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    if is_parent {
        // Parent sends handshake
        let handshake_msg = Control::<M>::Handshake;
        let handshake_bytes = serde_brief::to_vec(&handshake_msg).map_err(|e| {
            PythonExecutionError::SerializationError {
                message: format!("Failed to encode handshake: {e}"),
            }
        })?;
        conn.write_all(&handshake_bytes).await.map_err(|e| {
            PythonExecutionError::ExecutionError {
                message: format!("Failed to write handshake: {e}"),
            }
        })?;
        // Parent reads handshake response
        let mut resp_buf = vec![0u8; 1024];
        let n =
            conn.read(&mut resp_buf)
                .await
                .map_err(|e| PythonExecutionError::ExecutionError {
                    message: format!("Failed to read handshake response: {e}"),
                })?;
        if n == 0 {
            return Err(PythonExecutionError::ExecutionError {
                message: "Connection closed during handshake".into(),
            });
        }
        let resp: Control<M> = serde_brief::from_slice(&resp_buf[..n]).map_err(|e| {
            PythonExecutionError::SerializationError {
                message: format!("Failed to decode handshake response: {e}"),
            }
        })?;
        if !resp.is_handshake() {
            return Err(PythonExecutionError::ExecutionError {
                message: "Invalid handshake response".into(),
            });
        }
    } else {
        // Child reads handshake
        let mut buf = vec![0u8; 1024];
        let n = conn
            .read(&mut buf)
            .await
            .map_err(|e| PythonExecutionError::ExecutionError {
                message: format!("Failed to read handshake: {e}"),
            })?;
        if n == 0 {
            return Err(PythonExecutionError::ExecutionError {
                message: "Connection closed during handshake".into(),
            });
        }
        let handshake: Control<M> = serde_brief::from_slice(&buf[..n]).map_err(|e| {
            PythonExecutionError::SerializationError {
                message: format!("Failed to decode handshake: {e}"),
            }
        })?;
        if !handshake.is_handshake() {
            return Err(PythonExecutionError::ExecutionError {
                message: "Invalid handshake message".into(),
            });
        }
        // Child sends handshake response
        let resp = Control::<M>::Handshake;
        let resp_bytes =
            serde_brief::to_vec(&resp).map_err(|e| PythonExecutionError::SerializationError {
                message: format!("Failed to encode handshake response: {e}"),
            })?;
        conn.write_all(&resp_bytes)
            .await
            .map_err(|e| PythonExecutionError::ExecutionError {
                message: format!("Failed to write handshake response: {e}"),
            })?;
    }
    Ok(())
}

/// Kameo actor wrapper for IPC communication with child processes.
///
/// This actor provides a high-level interface for communicating with child processes
/// through the unified streaming protocol. It wraps the `SubprocessIpcBackend` and
/// integrates with the Kameo actor system for proper lifecycle management.
///
/// ## Actor Integration
///
/// - **Message Handling**: Implements `Message<M>` for synchronous requests
/// - **Streaming Support**: Implements `StreamingActor<M>` for streaming requests
/// - **Lifecycle Management**: Proper startup/shutdown with actor system
/// - **Error Handling**: Integrates with Kameo's error handling mechanisms
///
/// ## Protocol Support
///
/// - **Sync Messages**: Use `actor.ask(message)` for single request/response
/// - **Stream Messages**: Use `actor.send_stream(message)` for streaming responses
/// - **Error Propagation**: All errors properly propagated through actor system
///
/// ## Usage
///
/// ```rust
/// // Create actor from backend
/// let actor = spawn_subprocess_ipc_actor(backend);
///
/// // Send sync message
/// let response = actor.ask(MyMessage { data: "hello" }).await?;
///
/// // Send streaming message
/// let stream = actor.send_stream(MyMessage { data: "stream" }).await?;
/// while let Some(item) = stream.next().await {
///     println!("Stream item: {:?}", item?);
/// }
/// ```
pub struct SubprocessIpcActor<M>
where
    M: KameoChildProcessMessage + Send + Sync + 'static,
{
    /// The underlying IPC backend that handles the actual communication
    backend: Arc<SubprocessIpcBackend<M>>,
    /// Phantom data for message type
    _phantom: std::marker::PhantomData<M>,
}

impl<M> Actor for SubprocessIpcActor<M>
where
    M: KameoChildProcessMessage + Send + Sync + 'static,
{
    type Error = PythonExecutionError;
    async fn on_start(&mut self, _actor_ref: ActorRef<Self>) -> Result<(), Self::Error> {
        Ok(())
    }
    async fn on_stop(
        &mut self,
        _actor_ref: WeakActorRef<Self>,
        reason: ActorStopReason,
    ) -> Result<(), Self::Error> {
        tracing::error!(status = "stopped", ?reason);
        Ok(())
    }
}

impl<M> kameo::message::Message<M> for SubprocessIpcActor<M>
where
    M: KameoChildProcessMessage + Send + Sync + 'static,
{
    type Reply = Result<M::Ok, PythonExecutionError>;
    fn handle(
        &mut self,
        msg: M,
        _ctx: &mut kameo::message::Context<Self, Self::Reply>,
    ) -> impl std::future::Future<Output = Self::Reply> + Send {
        let backend = self.backend.clone();
        async move { backend.send(msg).await }
    }
}

/// Trait for actors that support streaming message responses.
///
/// This trait provides a unified interface for sending streaming messages to actors
/// that can return multiple response items. It's implemented by `SubprocessIpcActor`
/// to support the unified streaming protocol.
///
/// ## Streaming Protocol
///
/// - **Multi-Item Responses**: Actors can return streams with multiple items
/// - **Error Handling**: Each stream item can be a `Result<T, E>`
/// - **Async Stream**: Returns `Box<dyn Stream>` for flexible streaming
/// - **Send + Unpin**: Streams are safe for async contexts
///
/// ## Usage
///
/// ```rust
/// // Send streaming message
/// let stream = actor.send_stream(message).await?;
///
/// // Process stream items
/// while let Some(item) = stream.next().await {
///     match item {
///         Ok(response) => println!("Received: {:?}", response),
///         Err(e) => eprintln!("Error: {:?}", e),
///     }
/// }
/// ```
#[async_trait]
pub trait StreamingActor<M>
where
    M: KameoChildProcessMessage + Send + Sync + 'static,
{
    /// Send a streaming message and return a stream of responses.
    ///
    /// This method sends a message to the actor and returns a stream that can yield
    /// multiple response items. The stream will continue until the actor indicates
    /// completion or an error occurs.
    async fn send_stream(
        &self,
        msg: M,
    ) -> Result<
        Box<dyn futures::stream::Stream<Item = Result<M::Ok, PythonExecutionError>> + Send + Unpin>,
        PythonExecutionError,
    >;
}

#[async_trait]
impl<M> StreamingActor<M> for SubprocessIpcActor<M>
where
    M: KameoChildProcessMessage + Send + Sync + 'static,
{
    async fn send_stream(
        &self,
        msg: M,
    ) -> Result<
        Box<dyn futures::stream::Stream<Item = Result<M::Ok, PythonExecutionError>> + Send + Unpin>,
        PythonExecutionError,
    > {
        self.backend.send_stream(msg).await
    }
}

/// Extension trait to add streaming methods to `ActorRef<SubprocessIpcActor<M>>`
#[async_trait]
pub trait SubprocessIpcActorExt<M>
where
    M: KameoChildProcessMessage + Send + Sync + 'static,
{
    /// Send a streaming message and return a stream of responses
    async fn send_stream(
        &self,
        msg: M,
    ) -> Result<
        Box<dyn futures::stream::Stream<Item = Result<M::Ok, PythonExecutionError>> + Send + Unpin>,
        PythonExecutionError,
    >;
}

#[async_trait]
impl<M> SubprocessIpcActorExt<M> for kameo::actor::ActorRef<SubprocessIpcActor<M>>
where
    M: KameoChildProcessMessage + Send + Sync + 'static,
{
    async fn send_stream(
        &self,
        msg: M,
    ) -> Result<
        Box<dyn futures::stream::Stream<Item = Result<M::Ok, PythonExecutionError>> + Send + Unpin>,
        PythonExecutionError,
    > {
        // Use the new StreamingRequest message type to access the real streaming functionality
        let streaming_request = StreamingRequest::new(msg);
        match self.ask(streaming_request).await {
            Ok(stream) => Ok(stream),
            Err(_) => Err(PythonExecutionError::ExecutionError {
                message: "Actor send failed".to_string(),
            }),
        }
    }
}

// 3. Factory function to create and spawn the actor shim
pub fn spawn_subprocess_ipc_actor<M>(
    backend: Arc<SubprocessIpcBackend<M>>,
) -> ActorRef<SubprocessIpcActor<M>>
where
    M: KameoChildProcessMessage + Send + Sync + 'static,
{
    kameo::spawn(SubprocessIpcActor::<M> {
        backend,
        _phantom: std::marker::PhantomData,
    })
}

pub mod framing;
pub use framing::{LengthPrefixedRead, LengthPrefixedWrite};

/// Canonical child-side protocol artefact for parent-child IPC.
/// Owns the socket, framing, and async orchestration. Use in production and tests.
pub struct SubprocessIpcChild<M>
where
    M: KameoChildProcessMessage + Send + 'static,
{
    read_half: tokio::net::unix::OwnedReadHalf,
    write_half: tokio::net::unix::OwnedWriteHalf,
    _phantom: std::marker::PhantomData<M>,
}

impl<M> SubprocessIpcChild<M>
where
    M: KameoChildProcessMessage + Send + 'static,
{
    /// Canonical constructor: wire up the child artefact from a DuplexUnixStream, splitting it internally.
    pub fn from_duplex(stream: DuplexUnixStream) -> Self {
        let (read_half, write_half) = stream.into_split();
        Self {
            read_half,
            write_half,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Run the child protocol loop, handling messages with the provided handler.
    pub async fn run<H>(self, handler: H) -> Result<(), PythonExecutionError>
    where
        H: ChildProcessMessageHandler<M> + Send + Clone + 'static,
        M::Ok: serde::Serialize + std::fmt::Debug + Sync + Send + 'static,
    {
        use crate::{Control, MultiplexEnvelope};

        tracing::debug!(
            event = "SubprocessIpcChild_run",
            step = "start",
            "SubprocessIpcChild run started"
        );
        let writer = std::sync::Arc::new(tokio::sync::Mutex::new(
            crate::framing::LengthPrefixedWrite::new(self.write_half),
        ));
        let reader_token = tokio_util::sync::CancellationToken::new();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<MultiplexEnvelope<M>>();
        let handler_token = reader_token.clone();
        let handler_task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = handler_token.cancelled() => break,
                    maybe_envelope = rx.recv() => {
                        trace!(event = "child_ipc", step = "handler_pool_recv", got = maybe_envelope.is_some(), "Handler pool received from rx");
                        if let Some(envelope) = maybe_envelope {
                            let correlation_id = envelope.correlation_id;
                            trace!(event = "child_ipc", step = "handling", correlation_id = correlation_id, "Spawning handler task");
                            let mut handler = handler.clone();
                            let writer = writer.clone();
                            tokio::spawn(async move {
                                // Extract OTEL context from the received message
                                let parent_cx = envelope.context.extract_parent();

                                // Skip redundant ipc-child-receive span - it duplicates ipc-message

                                // Create process span
                                let process_span = tracing::info_span!(
                                    "ipc-child-process",
                                    correlation_id = correlation_id,
                                    message_type = std::any::type_name::<M>(),
                                    otel.kind = "internal"
                                );
                                process_span.set_parent(parent_cx.clone());
                                let _process_guard = process_span.enter();

                                trace!(event = "message_processing_start", correlation_id, "Starting message processing");

                                // Handle as streaming message
                                let stream_result = match handler.handle_child_message_stream(envelope.inner).await {
                                    Ok(stream) => {
                                        trace!(event = "stream_processing_success", correlation_id, "Stream processing successful");
                                        Ok(stream)
                                    },
                                    Err(e) => {
                                        error!(event = "stream_processing_error", correlation_id, error = %e, "Stream processing failed");
                                        Err(e)
                                    }
                                };

                                // Drop process guard before creating reply span
                                drop(_process_guard);

                                // Create reply span
                                let reply_span = tracing::info_span!(
                                    "ipc-child-reply",
                                    correlation_id = correlation_id,
                                    message_type = std::any::type_name::<M::Ok>(),
                                    otel.kind = "producer"
                                );
                                reply_span.set_parent(parent_cx);
                                let _reply_guard = reply_span.enter();

                                // Process the stream and send each item
                                match stream_result {
                                    Ok(mut stream) => {
                                        use futures::stream::StreamExt;

                                        // Send each stream item
                                        while let Some(item) = stream.next().await {
                                            let stream_envelope = MultiplexEnvelope {
                                    correlation_id,
                                                inner: item,
                                                context: envelope.context.clone(),
                                            };

                                            let stream_ctrl = Control::Stream(stream_envelope);
                                            let mut writer_guard = writer.lock().await;
                                            if writer_guard.write_msg(&stream_ctrl).await.is_err() {
                                                error!(event = "stream_item_send_failed", correlation_id, "Failed to send stream item");
                                                break;
                                            } else {
                                                trace!(event = "stream_item_sent", correlation_id, "Stream item sent successfully");
                                            }
                                        }

                                        // Send stream end marker
                                        let end_envelope: MultiplexEnvelope<Option<Result<M::Ok, PythonExecutionError>>> = MultiplexEnvelope {
                                            correlation_id,
                                            inner: None, // Stream end marker
                                    context: envelope.context,
                                };

                                        let end_ctrl = Control::StreamEnd(end_envelope);
                                let mut writer_guard = writer.lock().await;
                                        if writer_guard.write_msg(&end_ctrl).await.is_err() {
                                            error!(event = "stream_end_send_failed", correlation_id, "Failed to send stream end");
                                } else {
                                            trace!(event = "stream_end_sent", correlation_id, "Stream end sent successfully");
                                        }
                                    },
                                    Err(e) => {
                                        // Send error as single stream item
                                        let error_envelope: MultiplexEnvelope<Result<M::Ok, PythonExecutionError>> = MultiplexEnvelope {
                                            correlation_id,
                                            inner: Err(e),
                                            context: envelope.context.clone(),
                                        };

                                        let error_ctrl = Control::Stream(error_envelope);
                                        let mut writer_guard = writer.lock().await;
                                        if writer_guard.write_msg(&error_ctrl).await.is_err() {
                                            error!(event = "error_send_failed", correlation_id, "Failed to send error");
                                        } else {
                                            trace!(event = "error_sent", correlation_id, "Error sent successfully");
                                        }

                                        // Send stream end marker
                                        let end_envelope: MultiplexEnvelope<Option<Result<M::Ok, PythonExecutionError>>> = MultiplexEnvelope {
                                            correlation_id,
                                            inner: None, // Stream end marker
                                            context: envelope.context,
                                        };

                                        let end_ctrl = Control::StreamEnd(end_envelope);
                                        let mut writer_guard = writer.lock().await;
                                        if writer_guard.write_msg(&end_ctrl).await.is_err() {
                                            error!(event = "stream_end_send_failed", correlation_id, "Failed to send stream end");
                                        } else {
                                            trace!(event = "stream_end_sent", correlation_id, "Stream end sent successfully");
                                        }
                                    }
                                }

                                // Drop reply guard to end the span
                                drop(_reply_guard);
                            });
                        } else {
                            trace!(event = "child_ipc", step = "channel_closed", "Handler pool channel closed, exiting");
                            break;
                        }
                    }
                }
            }
            tracing::info!(
                event = "child_ipc",
                step = "reader_task",
                "Reader task exiting"
            );
        });
        let reader_task = tokio::spawn(run_reader_loop(
            self.read_half,
            tx,
            reader_token,
            std::any::type_name::<M>(),
        ));
        let (_reader_res, _handler_res) =
            tokio::try_join!(reader_task, handler_task).map_err(|e| {
                PythonExecutionError::ExecutionError {
                    message: format!("Join error: {e}"),
                }
            })?;
        Ok(())
    }
}

pub async fn run_reader_loop<M>(
    read_half: tokio::net::unix::OwnedReadHalf,
    tx: tokio::sync::mpsc::UnboundedSender<MultiplexEnvelope<M>>,
    cancellation_token: tokio_util::sync::CancellationToken,
    _message_type: &'static str,
) -> Result<(), PythonExecutionError>
where
    M: Send + KameoChildProcessMessage + 'static,
{
    let mut reader = crate::framing::LengthPrefixedRead::new(read_half);
    trace!(
        event = "child_reader",
        step = "start",
        "Reader loop started"
    );
    loop {
        tokio::select! {
            _ = cancellation_token.cancelled() => break,
            result = reader.read_msg::<Control<M>>() => {
                let control: Control<M> = match result {
                    Ok(ctrl) => {
                        trace!(event = "child_reader", step = "read_control", "Read control message");
                        ctrl
                    },
                    Err(e) => {
                        trace!(event = "child_reader", step = "read_error", error = ?e, "Reader error");
                        break;
                    }
                };

                match control {
                    Control::Stream(envelope) => {
                        let correlation_id = envelope.correlation_id;
                        tracing::debug!(event = "message_received", correlation_id = correlation_id, "Child received stream message");

                        if let Err(e) = tx.send(envelope) {
                        error!(event = "message_forward_failed", correlation_id, error = %e, "Failed to forward message to handler");
                    } else {
                        trace!(event = "message_forwarded", correlation_id, "Message forwarded to handler");
                    }
                    },
                    Control::Sync(envelope) => {
                        let correlation_id = envelope.correlation_id;
                        tracing::debug!(event = "message_received", correlation_id = correlation_id, "Child received sync message");

                        if let Err(e) = tx.send(envelope) {
                            error!(event = "message_forward_failed", correlation_id, error = %e, "Failed to forward message to handler");
                        } else {
                            trace!(event = "message_forwarded", correlation_id, "Message forwarded to handler");
                        }
                    },
                    Control::StreamEnd(_) => {
                        trace!(event = "child_reader", step = "stream_end", "Received stream end, ignoring");
                    },
                    Control::Handshake => {
                        trace!(event = "child_reader", step = "handshake", "Received handshake, ignoring");
                    }
                }
            }
        }
    }
    trace!(event = "child_reader", step = "exit", "Reader loop exiting");
    Ok(())
}

/// Message type for streaming requests
pub struct StreamingRequest<M> {
    pub inner: M,
}

impl<M> StreamingRequest<M> {
    pub fn new(inner: M) -> Self {
        Self { inner }
    }
}

impl<M> kameo::message::Message<StreamingRequest<M>> for SubprocessIpcActor<M>
where
    M: KameoChildProcessMessage + Send + Sync + 'static,
{
    type Reply = Result<
        Box<dyn futures::stream::Stream<Item = Result<M::Ok, PythonExecutionError>> + Send + Unpin>,
        PythonExecutionError,
    >;
    fn handle(
        &mut self,
        msg: StreamingRequest<M>,
        _ctx: &mut kameo::message::Context<Self, Self::Reply>,
    ) -> impl std::future::Future<Output = Self::Reply> + Send {
        let backend = self.backend.clone();
        async move { backend.send_stream(msg.inner).await }
    }
}
