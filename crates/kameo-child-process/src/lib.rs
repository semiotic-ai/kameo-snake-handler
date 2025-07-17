#![forbid(unsafe_code)]

pub mod callback;
pub mod handshake;
pub use handshake::*;

use anyhow::Result;
use async_trait::async_trait;
use bincode::{Decode, Encode};
use kameo::actor::{Actor, ActorRef, WeakActorRef};
use kameo::prelude::*;
use opentelemetry::global;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use std::io;
use std::marker::PhantomData;
use std::process::Stdio;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{Mutex as TokioMutex, Notify};
use tokio::time::Duration;
use tracing::Level;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_futures::Instrument;
use std::sync::atomic::AtomicU64;
use tracing::{trace, error};
use std::time::Instant;
use dashmap::DashMap;
use tokio::sync::oneshot;
pub mod error;
pub use error::PythonExecutionError;

/// A serializable representation of a tracing span's context.
/// This allows us to propagate traces across the process boundary.
#[derive(Serialize, Deserialize, Encode, Decode, Debug, Clone, Default)]
pub struct TracingContext(pub std::collections::HashMap<String, String>);

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
    Send + Serialize + DeserializeOwned + Encode + Decode<()> + std::fmt::Debug + 'static
{
    type Ok: Send + Serialize + DeserializeOwned + Encode + Decode<()> + std::fmt::Debug + 'static;
}

/// Control message for handshake and real messages (errors are always inside the envelope)
#[derive(Debug, Serialize, Deserialize, Encode, Decode)]
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
#[derive(Serialize, Deserialize, Encode, Decode, Debug)]
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
    M: KameoChildProcessMessage + Send + Sync + 'static,
{
    write_tx: tokio::sync::mpsc::UnboundedSender<WriteRequest<M>>,
    in_flight: InFlightMap<Result<M::Ok, PythonExecutionError>>,
    next_id: AtomicU64,
    cancellation_token: tokio_util::sync::CancellationToken,
    _phantom: std::marker::PhantomData<M>,
}

impl<M> SubprocessIpcBackend<M>
where
    M: KameoChildProcessMessage + Send + Sync + 'static,
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
        // Writer task
        tokio::spawn(async move {
            let mut writer = crate::framing::LengthPrefixedWrite::new(write_half);
            loop {
                tokio::select! {
                    _ = cancellation_token_writer.cancelled() => {
                        tracing::info!(event = "writer_task", "Writer task received shutdown signal, exiting");
                        break;
                    }
                    Some(write_req) = write_rx.recv() => {
                        if writer.write_msg(&write_req.ctrl).await.is_err() {
                            tracing::error!(event = "writer_task", error = "Failed to write message, breaking");
                            break;
                        }
                    }
                    else => break,
                }
            }
            tracing::info!(event = "writer_task", "Writer task exiting");
        });
        // Reader task
        tokio::spawn(async move {
            let mut reader = crate::framing::LengthPrefixedRead::new(read_half);
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
                                    let lock_start = Instant::now();
                                    if let Some((_, mut slot)) = in_flight_reader.0.remove(&correlation_id) {
                                        let remove_duration = lock_start.elapsed();
                                        trace!(event = "parent_in_flight", action = "remove", duration_ms = remove_duration.as_millis(), correlation_id, in_flight_len = in_flight_reader.0.len(), "Removed from parent in_flight in reader");
                                        slot.set_and_notify(envelope.inner).await;
                                        trace!(event = "parent_in_flight", action = "notify", correlation_id, in_flight_len = in_flight_reader.0.len(), "Notified reply slot in parent in_flight");
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
                                        in_flight_reader.0.retain(|_corr_id, slot| {
                                            if let Some(sender) = slot.sender.take() {
                                                if sender.send(Err(PythonExecutionError::ExecutionError { message: "IPC backend reply loop exited (EOF)".to_string() })).is_err() {
                                                    tracing::error!(event = "reader_task", error = "Failed to send EOF error to waiting task", "Failed to notify waiting task about EOF");
                                                }
                                            }
                                            false
                                        });
                                    }
                                    break;
                                }
                                error!(event = "parent_read_error", error = ?e, "Parent reader task error, exiting");
                                break;
                            }
                        }
                    }
                }
            }
            // On exit, drain in_flight and send error to all pending
            in_flight_reader.0.retain(|_corr_id, slot| {
                if let Some(sender) = slot.sender.take() {
                    if sender.send(Err(PythonExecutionError::ExecutionError { message: "IPC backend reply loop exited".to_string() })).is_err() {
                        tracing::error!(event = "reader_task", error = "Failed to send shutdown error to waiting task", "Failed to notify waiting task about shutdown");
                    }
                }
                false
            });
            tracing::info!(event = "reader_task", "Reader task exiting and drained in_flight");
        });
        Arc::new(Self {
            write_tx,
            in_flight,
            next_id: std::sync::atomic::AtomicU64::new(1),
            cancellation_token,
            _phantom: std::marker::PhantomData,
        })
    }

    /// Always generate a unique correlation_id using the atomic counter.
    fn next_correlation_id(&self) -> u64 {
        self.next_id.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    /// Triggers shutdown of the backend, waking all pending requests with error.
    pub fn shutdown(&self) {
        self.cancellation_token.cancel();
    }

    pub async fn send(&self, msg: M) -> Result<M::Ok, PythonExecutionError> {
        use tracing::trace;
        
        // Always use atomic counter for correlation_id
        let correlation_id = self.next_correlation_id();
        let span = tracing::Span::current();
        let span_id = span.id();
        trace!(event = "parent_send", step = "correlation_id", correlation_id, ?span_id, "Generated atomic correlation_id and using current span for tracing");
        let mut trace_context_map = std::collections::HashMap::new();
        global::get_text_map_propagator(|propagator| {
            propagator.inject_context(&tracing_opentelemetry::OpenTelemetrySpanExt::context(&span), &mut trace_context_map)
        });
        let envelope = MultiplexEnvelope {
            correlation_id,
            inner: msg,
            context: TracingContext(trace_context_map),
        };
        let ctrl = Control::Real(envelope);
        let write_req = WriteRequest {
            correlation_id,
            ctrl,
        };
        let reply_slot: ReplySlot<Result<M::Ok, PythonExecutionError>> = ReplySlot::new();
        {
            let lock_start = Instant::now();
            self.in_flight.0.insert(correlation_id, reply_slot);
            let insert_duration = lock_start.elapsed(); // Since no lock, but keep for consistency
            trace!(event = "parent_in_flight", action = "insert", duration_ms = insert_duration.as_millis(), in_flight_len = self.in_flight.0.len(), correlation_id, ?span_id, "Inserted into parent in_flight");
        }
        if let Err(e) = self.write_tx.send(write_req) {
            let lock_start = Instant::now();
            if let Some((_, mut slot)) = self.in_flight.0.remove(&correlation_id) {
                if let Some(sender) = slot.sender.take() {
                    if sender.send(Err(PythonExecutionError::ExecutionError { message: format!("Failed to send write request: {e}") })).is_err() {
                        tracing::error!(event = "parent_send", step = "error_notify_failed", correlation_id, "Failed to notify waiting task about send error");
                    }
                }
            }
            let remove_duration = lock_start.elapsed();
            trace!(event = "parent_in_flight", action = "remove", duration_ms = remove_duration.as_millis(), correlation_id, "Removed from parent in_flight on send fail");
            trace!(event = "parent_send", step = "send_failed", correlation_id, error = ?e, "Failed to send write request");
            return Err(PythonExecutionError::ExecutionError { message: format!("Failed to send write request: {e}") });
        }
        trace!(event = "parent_send", step = "sent", correlation_id, ?span_id, "Sent write request to writer task");
        // Await the reply by reference, do not remove from map here
        let mut slot = self.in_flight.0.get_mut(&correlation_id)
            .expect("ReplySlot must exist in in_flight map");
        let receiver = slot.receiver.take();
        drop(slot); // Release the lock before awaiting
        match receiver {
            Some(receiver) => receiver.await.unwrap_or_else(|_| Err(PythonExecutionError::ExecutionError { message: "Recv error".to_string() })),
            None => Err(PythonExecutionError::ExecutionError { message: "Reply slot already taken".to_string() }),
        }
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
    use futures::stream::{FuturesUnordered, StreamExt};
    let config = config.unwrap_or_default();
    let mut in_flight = FuturesUnordered::new();
    // Make reply_tx Option and drop it on shutdown
    let (reply_tx_inner, mut reply_rx) = tokio::sync::mpsc::unbounded_channel::<(u64, Vec<u8>)>();
    let mut reply_tx = Some(reply_tx_inner);
    let mut shutdown = false;
    loop {
        tracing::trace!(event = "child_loop", step = "enter", in_flight = in_flight.len(), shutdown = shutdown, "Entering child actor loop select");
        let _in_flight_len = in_flight.len();
        if !shutdown {
            tokio::select! {
                biased;
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
                                    let parent_cx = opentelemetry::global::get_text_map_propagator(|propagator| {
                                        propagator.extract(&envelope.context.0)
                                    });
                                    let span = tracing::info_span!("child_message_handler", event = "message", handler = "child", process_role = "child");
                                    span.set_parent(parent_cx);
                                    let mut handler = handler.clone();
                                    let reply_tx = reply_tx.as_ref().unwrap().clone();
                                    let fut = async move {
                                        let reply: Result<M::Ok, PythonExecutionError> = handler.handle_child_message(envelope.inner).await;
                                        tracing::trace!(event = "child_ipc", step = "after_handle", ?reply, correlation_id = correlation_id, "Got reply from handle_child_message");
                                        let reply_envelope = MultiplexEnvelope {
                                            correlation_id,
                                            inner: reply,
                                            context: Default::default(),
                                        };
                                        let ctrl = Control::Real(reply_envelope);
                                        match bincode::encode_to_vec(ctrl, bincode::config::standard()) {
                                            Ok(reply_bytes) => {
                                                if reply_tx.send((correlation_id, reply_bytes)).is_err() {
                                                    tracing::error!(event = "child_ipc", step = "send_error", correlation_id, "Failed to send reply to writer task, channel closed");
                                                }
                                            },
                                            Err(e) => {
                                                tracing::error!(event = "bincode_encode_error", correlation_id = correlation_id, error = ?e, "Failed to encode reply envelope");
                                            }
                                        }
                                    };
                                    in_flight.push(fut.instrument(span));
                                    tracing::trace!(event = "child_in_flight", action = "push", in_flight_len = in_flight.len(), "Pushed to child in_flight");
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
                Some(_) = in_flight.next() => {
                    tracing::trace!(event = "child_in_flight", action = "complete", in_flight_len = in_flight.len(), "Handler future completed in child in_flight");
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
            let span = tracing::info_span!("ipc_message", message_type = std::any::type_name::<M>());
            backend.send(msg).instrument(span).await
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
        
        tracing::debug!(event = "child_ipc", step = "start", "SubprocessIpcChild run started");
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
                            trace!(event = "child_ipc", step = "handling", correlation_id = envelope.correlation_id, "Spawning handler task");
                            let correlation_id = envelope.correlation_id;
                            let mut handler = handler.clone();
                            let writer = writer.clone();
                            tokio::spawn(async move {
                                let reply: Result<M::Ok, PythonExecutionError> = handler.handle_child_message(envelope.inner).await;
                                let reply_envelope = MultiplexEnvelope {
                                    correlation_id,
                                    inner: reply,
                                    context: Default::default(),
                                };
                                let ctrl = Control::Real(reply_envelope);
                                let mut writer_guard = writer.lock().await;
                                if writer_guard.write_msg(&ctrl).await.is_err() {
                                    trace!(event = "child_ipc", step = "write_reply_failed", correlation_id, "Failed to write reply");
                                } else {
                                    trace!(event = "child_ipc", step = "reply_written", correlation_id, "Wrote reply to parent");
                                }
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
                    if tx.send(env).is_err() {
                        trace!(event = "child_reader", step = "send_failed", correlation_id, "Failed to send to handler pool");
                        break;
                    }
                    trace!(event = "child_reader", step = "sent_to_pool", correlation_id, "Sent envelope to handler pool");
                }
            }
        }
    }
    trace!(event = "child_reader", step = "exit", "Reader loop exiting");
    Ok(())
}
