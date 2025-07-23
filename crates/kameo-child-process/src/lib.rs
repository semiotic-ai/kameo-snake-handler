#![forbid(unsafe_code)]

use once_cell::sync::Lazy;
use opentelemetry::propagation::TextMapPropagator;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use std::sync::Arc;
use tokio::time::{timeout, Duration};

static GLOBAL_PROPAGATOR: Lazy<Arc<dyn TextMapPropagator + Send + Sync>> = Lazy::new(|| {
    Arc::new(TraceContextPropagator::new())
});

pub mod callback;
pub mod handshake;
pub use handshake::*;
pub mod metrics;
pub mod tracing_utils;

use anyhow::Result;
use async_trait::async_trait;
use bincode::{Decode, Encode};
use kameo::actor::{Actor, ActorRef, WeakActorRef};
use kameo::prelude::*;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use std::io;
use std::marker::PhantomData;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tracing::Level;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_futures::Instrument;
use std::sync::atomic::{AtomicU64, AtomicUsize};
use tracing::{trace, error};
use dashmap::DashMap;
use tokio::sync::oneshot;
pub mod error;
pub use error::PythonExecutionError;




/// A serializable representation of a tracing span's context for OTEL propagation
#[derive(Serialize, Deserialize, Encode, Decode, Debug, Clone, Default)]
pub struct TracingContext(pub std::collections::HashMap<String, String>);

impl TracingContext {
    /// Capture the current span context for propagation
    pub fn from_current_span() -> Self {
        use opentelemetry::propagation::Injector;
        let mut context = Self::default();
        struct MapInjector<'a>(&'a mut std::collections::HashMap<String, String>);
        impl<'a> Injector for MapInjector<'a> {
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
        impl<'a> Extractor for MapExtractor<'a> {
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
#[derive(Serialize, Deserialize, Encode, Decode, Debug)]
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
    Send + Serialize + DeserializeOwned + Encode + Decode<()> + std::fmt::Debug + Clone + 'static
{
    type Ok: Send + Serialize + DeserializeOwned + Encode + Decode<()> + std::fmt::Debug + Clone + 'static;
}

/// Control message for handshake and real messages (errors are always inside the envelope)
#[derive(Debug, Serialize, Deserialize, Encode, Decode, Clone)]
pub enum Control<T> {
    Handshake,
    Real(MultiplexEnvelope<T>),
}

impl<T> Control<T> {
    pub fn is_handshake(&self) -> bool {
        matches!(self, Control::Handshake)
    }
    pub fn into_real(self) -> Option<MultiplexEnvelope<T>> {
        match self {
            Control::Real(env) => Some(env),
            _ => None,
        }
    }
}

/// Envelope for multiplexed requests
#[derive(Serialize, Deserialize, Encode, Decode, Debug, Clone)]
pub struct MultiplexEnvelope<T> {
    pub correlation_id: u64,
    pub inner: T,
    pub context: TracingContext,
}

pub struct WriteRequest<M> {
    pub correlation_id: u64,
    pub ctrl: Control<M>,
}

pub type CorrelationId = u64;
pub struct ReplySlot<R> {
    sender: Option<oneshot::Sender<R>>,
    receiver: Option<oneshot::Receiver<R>>,
}

impl<R> ReplySlot<R> {
    pub fn new() -> Self {
        let (sender, receiver) = oneshot::channel();
        Self { sender: Some(sender), receiver: Some(receiver) }
    }
    pub async fn set_and_notify(&mut self, value: R) {
        if let Some(sender) = self.sender.take() {
            if sender.send(value).is_err() {
                tracing::error!(event = "reply_slot", error = "Failed to send reply, receiver dropped", "Reply channel closed");
            }
        }
    }
    pub async fn wait(mut self) -> Option<R> {
        if let Some(receiver) = self.receiver.take() {
            receiver.await.ok()
        } else {
            None
        }
    }
}

impl<R> ReplySlot<Result<R, PythonExecutionError>> {
    pub fn try_set_err(&mut self, err: PythonExecutionError) {
        if let Some(sender) = self.sender.take() {
            if sender.send(Err(err)).is_err() {
                tracing::error!(event = "reply_slot", error = "Failed to send error reply, receiver dropped", "Error reply channel closed");
            }
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
    pub fn into_split(self) -> (tokio::net::unix::OwnedReadHalf, tokio::net::unix::OwnedWriteHalf) {
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

pub struct SubprocessIpcBackend<M>
where
    M: KameoChildProcessMessage + Send + Sync + Clone + 'static,
{
    write_tx: tokio::sync::mpsc::UnboundedSender<WriteRequest<M>>,
    in_flight: InFlightMap<Result<M::Ok, PythonExecutionError>>,
    next_id: AtomicU64,
    cancellation_token: tokio_util::sync::CancellationToken,
    // Track pending requests for adaptive throttling
    pending_count: AtomicUsize,
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
        use tokio::sync::mpsc::unbounded_channel;
        use crate::error::PythonExecutionError;
        use std::sync::Arc;
        let (write_tx, mut write_rx) = unbounded_channel::<WriteRequest<M>>();
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
                                if let Err(e) = writer.write_msg(&write_req.ctrl).await {
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
                                if let Control::Real(envelope) = ctrl {
                                    let correlation_id = envelope.correlation_id;
                                    trace!(event = "parent_in_flight", action = "response_received", correlation_id, "Received response for correlation_id");
                                    
                                    // Extract the entry from in_flight map to avoid race conditions
                                    if let Some((_, slot)) = in_flight_reader.0.remove(&correlation_id) {
                                        // Decrement pending count as we've received a response
                                        result_clone.pending_count.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                                        
                                        // Extract the sender so we can drop the slot immediately
                                        if let Some(sender) = slot.sender {
                                            // Send the response directly to the waiting task
                                            if sender.send(envelope.inner).is_err() {
                                                tracing::error!(event = "parent_in_flight", error = "Failed to send reply, receiver dropped", correlation_id);
                                                
                                                // Track error in metrics
                                                metrics::MetricsHandle::parent().track_error("reply_channel_closed");
                                            } else {
                                                trace!(event = "parent_in_flight", action = "notify", correlation_id, "Sent reply to waiting task");
                                            }
                                        } else {
                                            tracing::error!(event = "parent_in_flight", correlation_id, "Reply slot sender missing");
                                            
                                            // Track error in metrics
                                            metrics::MetricsHandle::parent().track_error("missing_sender");
                                        }
                                    } else {
                                        tracing::error!(event = "parent_in_flight", correlation_id, "Received reply for unknown correlation id");
                                        
                                        // Track error in metrics
                                        metrics::MetricsHandle::parent().track_error("unknown_correlation_id");
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
                                        metrics::MetricsHandle::parent().track_error("eof_with_pending");
                                    }
                                    break;
                                }
                                error!(event = "parent_read_error", error = ?e, "Parent reader task error, exiting");
                                
                                // Track error in metrics
                                metrics::MetricsHandle::parent().track_error("read_error");
                                break;
                            }
                        }
                    }
                }
            }
            // On exit, drain in_flight and send error to all pending
            in_flight_reader.0.iter_mut().for_each(|mut item| {
                let (_corr_id, slot) = item.pair_mut();
                if let Some(sender) = slot.sender.take() {
                    let err = PythonExecutionError::ExecutionError { message: "IPC backend reply loop exited".to_string() };
                    if sender.send(Err(err)).is_err() {
                        tracing::error!(event = "reader_task", error = "Failed to send shutdown error to waiting task", "Failed to notify waiting task about shutdown");
                    }
                }
            });
            // Reset pending count to 0
            result_clone.pending_count.store(0, std::sync::atomic::Ordering::SeqCst);
            // Clear the map after notifying all waiting tasks
            in_flight_reader.0.clear();
            tracing::info!(event = "reader_task", "Reader task exiting");
        });
        
        result
    }

    /// Always generate a unique correlation_id using the atomic counter.
    fn next_correlation_id(&self) -> u64 {
        self.next_id.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
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
        let correlation_id = self.next_correlation_id();
        let msg_type = std::any::type_name::<M>();
        
        // Create the root ipc-message span that will encompass the entire IPC operation
        let ipc_message_span = tracing_utils::create_root_ipc_message_span(correlation_id, msg_type);
        
        // Create the ipc-parent-send span as a child of the ipc-message span
        let send_span = tracing_utils::create_ipc_parent_send_span(correlation_id, msg_type, &ipc_message_span);
        
        // Create the oneshot channel BEFORE sending the message
        let (tx, rx) = tokio::sync::oneshot::channel();
        
        // Insert into in_flight map and track pending count
        {
            let mut slot = ReplySlot::new();
            slot.sender = Some(tx);
            slot.receiver = None;
            self.in_flight.0.insert(correlation_id, slot);
            self.pending_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        
        // Create the envelope with the ipc-parent-send span context
        let envelope = {
            let _send_guard = send_span.enter();
            MultiplexEnvelope {
                correlation_id,
                inner: msg,
                context: TracingContext::from_current_span(),
            }
        };
        
        // Send the message and attach the send span to the future
        let send_future = async move {
            trace!(event = "send_start", correlation_id = correlation_id, message_type = msg_type, "Starting IPC send");
            
            let ctrl = Control::Real(envelope);
            let write_req = WriteRequest { correlation_id, ctrl };
            if let Err(e) = self.write_tx.send(write_req) {
                self.in_flight.0.remove(&correlation_id);
                self.pending_count.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                return Err(PythonExecutionError::ExecutionError { 
                    message: format!("Failed to send write request: {e}") 
                });
            }
            
            trace!(event = "message_sent", correlation_id, "Message sent successfully");
            
            // Wait for reply with timeout
            let result = match timeout(Duration::from_secs(30), rx).await {
                Ok(Ok(reply)) => {
                    trace!(event = "reply_received", correlation_id, "Reply received successfully");
                    reply
                }
                Ok(Err(_)) => {
                    error!(event = "reply_channel_closed", correlation_id, "Reply channel closed");
                    if self.in_flight.0.remove(&correlation_id).is_some() {
                        self.pending_count.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                    }
                    return Err(PythonExecutionError::ExecutionError { 
                        message: "Reply channel closed before response received".to_string() 
                    });
                }
                Err(_) => {
                    error!(event = "reply_timeout", correlation_id, "Reply timeout");
                    if self.in_flight.0.remove(&correlation_id).is_some() {
                        self.pending_count.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                    }
                    return Err(PythonExecutionError::ExecutionError { 
                        message: "Reply timeout after 30 seconds".to_string() 
                    });
                }
            };
            
            // Clean up in_flight entry
            {
                self.in_flight.0.remove(&correlation_id);
                self.pending_count.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
            }
            
            trace!(event = "send_complete", correlation_id = correlation_id, "IPC send completed");
            result
        };
        
        send_future.instrument(send_span).await
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

/// Trait for message handlers in the child process (no Context, no actor system)
#[async_trait]
pub trait ChildProcessMessageHandler<Msg> where Msg: KameoChildProcessMessage {
    async fn handle_child_message(&mut self, msg: Msg) -> Result<Msg::Ok, PythonExecutionError>;
}

/// Run the IPC handler loop in the child process. No ActorRef, no Clone, no spawn, just handle messages.
#[derive(Debug,Error)]
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
async fn read_next_message(conn: &mut tokio::net::UnixStream) -> Result<Option<Vec<u8>>, io::Error> {
    tracing::trace!(event = "child_read", step = "before_len", "About to read length prefix");
    let mut len_buf = [0u8; 4];
    match conn.read_exact(&mut len_buf).await {
        Ok(_) => {},
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
            tracing::trace!(event = "child_read", step = "clean_eof", "Clean EOF detected on length read");
            return Ok(None);
        }
        Err(e) => {
            tracing::trace!(event = "child_read", step = "error", error = ?e);
            return Err(e);
        }
    }
    let msg_len = u32::from_le_bytes(len_buf) as usize;
    tracing::trace!(event = "child_read", step = "after_len", ?len_buf, msg_len, "Read length prefix");
    tracing::trace!(event = "child_read", step = "before_msg", msg_len, "About to read message of len {}", msg_len);
    let mut msg_buf = vec![0u8; msg_len];
    conn.read_exact(&mut msg_buf).await?;
    tracing::trace!(event = "child_read", step = "after_msg", len = msg_buf.len(), "Read message");
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
    M::Ok: serde::Serialize + bincode::Encode + std::fmt::Debug + 'static,
{
    // Clear all ambient span context for the entire select loop and all futures polled within it
    let none_span = tracing::Span::none();
    let _none_guard = none_span.enter();
    tracing::debug!(event = "run_child_actor_loop", step = "start", "run_child_actor_loop started");
    use futures::stream::{FuturesUnordered, StreamExt};
    let _config = config.unwrap_or_default();
    let mut in_flight = FuturesUnordered::new();

    // Make reply_tx Option and drop it on shutdown
    let (reply_tx_inner, mut reply_rx) = tokio::sync::mpsc::unbounded_channel::<(u64, Vec<u8>)>();
    let mut reply_tx = Some(reply_tx_inner);
    let mut shutdown = false;
    loop {
        tracing::trace!(event = "child_loop", step = "enter", shutdown = shutdown, "Entering child actor loop select");
        if !shutdown {
            tokio::select! {
                biased;
                Some(_) = in_flight.next() => {
                    tracing::trace!(event = "child_in_flight", action = "complete", in_flight_len = in_flight.len(), "Handler future completed in child in_flight");
                }
                read_res = read_next_message(&mut *conn) => {
                    match read_res {
                        Ok(Some(msg)) => {
                            tracing::trace!(event = "child_ipc", step = "read", len = msg.len(), raw = ?&msg[..std::cmp::min(100, msg.len())], "Read message from parent");
                            let ctrl: Control<M> = match bincode::decode_from_slice(&msg[..], bincode::config::standard()) {
                                Ok((ctrl, _)) => {
                                    tracing::trace!(event = "bincode_decode", type_deserialized = std::any::type_name::<Control<M>>(), len = msg.len(), "Decoding Control envelope");
                                    ctrl
                                },
                                Err(e) => {
                                    tracing::error!(event = "bincode_decode_error", type_deserialized = std::any::type_name::<Control<M>>(), len = msg.len(), error = ?e, "Failed to decode Control envelope");
                                    continue;
                                }
                            };
                            match ctrl {
                                Control::Handshake => {
                                    tracing::debug!(event = "child_ipc", step = "handshake", "Received handshake from parent");
                                }
                                Control::Real(envelope) => {
                                    let correlation_id = envelope.correlation_id;
                                    let parent_cx = envelope.context.extract_parent();
                                    
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
                                    let msg_type = std::any::type_name::<M>();
                                    
                                    // Create ipc-child-receive span as a child of the root ipc-message span
                                    let receive_span = tracing_utils::create_ipc_child_receive_span(
                                        correlation_id,
                                        msg_type,
                                        parent_cx.clone(),
                                    );
                                    
                                    // Create the message processing future and instrument it with the span
                                    let process_future = async move {
                                        // Process the message
                                        let result = handler.handle_child_message(envelope.inner).await;
                                        
                                        // Send the reply
                                        let reply_envelope = MultiplexEnvelope {
                                            correlation_id,
                                            inner: result,
                                            context: envelope.context,
                                        };
                                        let ctrl = Control::Real(reply_envelope);
                                        
                                        // Encode the reply to bytes
                                        match bincode::encode_to_vec(ctrl, bincode::config::standard()) {
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
                                    
                                    // Instrument the future with the receive_span
                                    in_flight.push(process_future.instrument(receive_span));
                                    tracing::trace!(event = "child_in_flight", action = "push", in_flight_len = in_flight.len(), 
                                        correlation_id = correlation_id, "Pushed message future to child in_flight");
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

// 1. Add perform_handshake function for parent/child handshake
pub async fn perform_handshake<M>(
    conn: &mut (impl AsyncRead + AsyncWrite + Unpin),
    is_parent: bool,
) -> Result<(), PythonExecutionError>
where
    M: KameoChildProcessMessage + Send + Sync + 'static,
{
    use crate::Control;
    use bincode::{decode_from_slice, encode_to_vec};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    if is_parent {
        // Parent sends handshake
        let handshake_msg = Control::<M>::Handshake;
        let handshake_bytes = encode_to_vec(&handshake_msg, bincode::config::standard())
            .map_err(|e| PythonExecutionError::SerializationError { message: format!("Failed to encode handshake: {e}") })?;
        conn.write_all(&handshake_bytes)
            .await
            .map_err(|e| PythonExecutionError::ExecutionError { message: format!("Failed to write handshake: {e}") })?;
        // Parent reads handshake response
        let mut resp_buf = vec![0u8; 1024];
        let n = conn
            .read(&mut resp_buf)
            .await
            .map_err(|e| PythonExecutionError::ExecutionError { message: format!("Failed to read handshake response: {e}") })?;
        if n == 0 {
            return Err(PythonExecutionError::ExecutionError { message: "Connection closed during handshake".into() });
        }
        let (resp, _): (Control<M>, _) =
            decode_from_slice(&resp_buf[..n], bincode::config::standard()).map_err(|e| {
                PythonExecutionError::SerializationError { message: format!("Failed to decode handshake response: {e}") }
            })?;
        if !resp.is_handshake() {
            return Err(PythonExecutionError::ExecutionError { message: "Invalid handshake response".into() });
        }
    } else {
        // Child reads handshake
        let mut buf = vec![0u8; 1024];
        let n = conn
            .read(&mut buf)
            .await
            .map_err(|e| PythonExecutionError::ExecutionError { message: format!("Failed to read handshake: {e}") })?;
        if n == 0 {
            return Err(PythonExecutionError::ExecutionError { message: "Connection closed during handshake".into() });
        }
        let (handshake, _): (Control<M>, _) =
            decode_from_slice(&buf[..n], bincode::config::standard())
                .map_err(|e| PythonExecutionError::SerializationError { message: format!("Failed to decode handshake: {e}") })?;
        if !handshake.is_handshake() {
            return Err(PythonExecutionError::ExecutionError { message: "Invalid handshake message".into() });
        }
        // Child sends handshake response
        let resp = Control::<M>::Handshake;
        let resp_bytes = encode_to_vec(&resp, bincode::config::standard()).map_err(|e| {
            PythonExecutionError::SerializationError { message: format!("Failed to encode handshake response: {e}") }
        })?;
        conn.write_all(&resp_bytes)
            .await
            .map_err(|e| PythonExecutionError::ExecutionError { message: format!("Failed to write handshake response: {e}") })?;
    }
    Ok(())
}

// 1. Define SubprocessIpcActor<M>
pub struct SubprocessIpcActor<M>
where
    M: KameoChildProcessMessage + Send + Sync + 'static,
{
    backend: Arc<SubprocessIpcBackend<M>>,
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
        tracing::error!(status = "stopped", actor_type = "SubprocessIpcActor", ?reason);
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
        async move {
            backend.send(msg).await
        }
    }
}

// 3. Factory function to create and spawn the actor shim
pub fn spawn_subprocess_ipc_actor<M>(backend: Arc<SubprocessIpcBackend<M>>) -> ActorRef<SubprocessIpcActor<M>>
where
    M: KameoChildProcessMessage + Send + Sync + 'static,
{
    kameo::spawn(SubprocessIpcActor::<M> { backend, _phantom: std::marker::PhantomData })
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
        Self { read_half, write_half, _phantom: std::marker::PhantomData }
    }

    /// Run the child protocol loop, handling messages with the provided handler.
    pub async fn run<H>(self, handler: H) -> Result<(), PythonExecutionError>
    where
        H: ChildProcessMessageHandler<M> + Send + Clone + 'static,
        M::Ok: serde::Serialize + bincode::Encode + std::fmt::Debug + Sync + Send + 'static,
    {
        
        use crate::{MultiplexEnvelope, Control};
        
        tracing::debug!(event = "SubprocessIpcChild_run", step = "start", "SubprocessIpcChild run started");
        let writer = std::sync::Arc::new(tokio::sync::Mutex::new(crate::framing::LengthPrefixedWrite::new(self.write_half)));
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
                                
                                // Create ipc-child-receive span as a child of the root ipc-message span
                                let receive_span = tracing::info_span!(
                                    "ipc-child-receive",
                                    correlation_id = correlation_id,
                                    message_type = std::any::type_name::<M>(),
                                    messaging.system = "ipc",
                                    messaging.operation = "receive",
                                    messaging.source_kind = "queue"
                                );
                                receive_span.set_parent(parent_cx.clone());
                                let _receive_guard = receive_span.enter();
                                
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
                                
                                let reply: Result<M::Ok, PythonExecutionError> = match handler.handle_child_message(envelope.inner).await {
                                    Ok(result) => {
                                        trace!(event = "message_processing_success", correlation_id, ?result, "Message processing successful");
                                        Ok(result)
                                    },
                                    Err(e) => {
                                        error!(event = "message_processing_error", correlation_id, error = %e, "Message processing failed");
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
                                
                                // Create reply envelope with tracing context
                                let reply_envelope = MultiplexEnvelope {
                                    correlation_id,
                                    inner: reply,
                                    context: envelope.context,
                                };
                                
                                let reply_ctrl = Control::Real(reply_envelope);
                                let mut writer_guard = writer.lock().await;
                                if writer_guard.write_msg(&reply_ctrl).await.is_err() {
                                    error!(event = "reply_send_failed", correlation_id, "Failed to send reply");
                                } else {
                                    trace!(event = "reply_sent", correlation_id, "Reply sent successfully");
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
            tracing::info!(event = "child_ipc", step = "reader_task", "Reader task exiting");
        });
        let reader_task = tokio::spawn(run_reader_loop(self.read_half, tx, reader_token, std::any::type_name::<M>()));
        let (_reader_res, _handler_res) = tokio::try_join!(reader_task, handler_task)
            .map_err(|e| PythonExecutionError::ExecutionError { message: format!("Join error: {e}") })?;
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
    M: Decode<()> + Send + KameoChildProcessMessage + 'static,
{
    let mut reader = crate::framing::LengthPrefixedRead::new(read_half);
    trace!(event = "child_reader", step = "start", "Reader loop started");
    loop {
        tokio::select! {
            _ = cancellation_token.cancelled() => break,
            result = reader.read_msg::<Control<M>>() => {
                let ctrl: Control<M> = match result {
                    Ok(c) => {
                        trace!(event = "child_reader", step = "read_msg", is_handshake = c.is_handshake(), "Read control message");
                        c
                    },
                    Err(e) => {
                        trace!(event = "child_reader", step = "read_error", error = ?e, "Reader error");
                        break;
                    }
                };
                if let Control::Real(env) = ctrl {
                    let correlation_id = env.correlation_id;
                    
                    tracing::debug!(event = "message_received", correlation_id = correlation_id, "Child received message");
                    
                    if let Err(e) = tx.send(env) {
                        error!(event = "message_forward_failed", correlation_id, error = %e, "Failed to forward message to handler");
                    } else {
                        trace!(event = "message_forwarded", correlation_id, "Message forwarded to handler");
                    }
                }
            }
        }
    }
    trace!(event = "child_reader", step = "exit", "Reader loop exiting");
    Ok(())
}


