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
use std::io;
use std::marker::PhantomData;
use std::process::Stdio;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{Mutex as TokioMutex, Notify};
use tokio::time::Duration;
use tracing::Level;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use std::collections::HashMap;
use tracing_futures::Instrument;
use std::sync::atomic::AtomicU64;
use tracing::{trace, error};
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
    type Reply: Reply
        + Send
        + Serialize
        + DeserializeOwned
        + Encode
        + Decode<()>
        + std::fmt::Debug
        + 'static;
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

/// A slot for a reply: an Arc containing a Mutex for the reply and a Notify for wakeup.
pub type ReplySlot<R> = Arc<(TokioMutex<Option<R>>, Notify)>;

/// Newtype for the in-flight map: an Arc<Mutex<HashMap<u64, R>>>
#[derive(Debug)]
pub struct InFlightMap<R>(pub Arc<TokioMutex<HashMap<u64, R>>>);

impl<R> Clone for InFlightMap<R> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<R> InFlightMap<R> {
    pub fn new() -> Self {
        Self(Arc::new(TokioMutex::new(HashMap::new())))
    }
    pub fn as_inner(&self) -> &Arc<TokioMutex<HashMap<u64, R>>> {
        &self.0
    }
}

// Remove ShutdownHandle struct and all uses

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
    pub fn as_ref(&self) -> &tokio::net::UnixStream {
        &self.inner
    }
    pub fn into_inner(self) -> tokio::net::UnixStream {
        self.inner
    }
}

pub struct SubprocessIpcBackend<M>
where
    M: KameoChildProcessMessage + Send + Sync + 'static,
{
    write_tx: tokio::sync::mpsc::UnboundedSender<WriteRequest<M>>,
    in_flight: InFlightMap<ReplySlot<Result<M::Reply, PythonExecutionError>>>,
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

    /// Canonical constructor: wire up the backend from the read and write halves of a UnixStream.
    /// Used in both production (builder) and tests. This is the only public constructor.
    pub fn new(
        read_half: tokio::net::unix::OwnedReadHalf,
        write_half: tokio::net::unix::OwnedWriteHalf,
    ) -> Arc<Self> {
        use tokio::sync::mpsc::unbounded_channel;
        use crate::ReplySlot;
        use crate::error::PythonExecutionError;
        use std::sync::Arc;
        let (write_tx, mut write_rx) = unbounded_channel::<WriteRequest<M>>();
        let in_flight: InFlightMap<ReplySlot<Result<M::Reply, PythonExecutionError>>> = InFlightMap::new();
        let in_flight_reader = in_flight.clone();
        let cancellation_token = tokio_util::sync::CancellationToken::new();
        let cancellation_token_writer = cancellation_token.clone();
        let cancellation_token_reader = cancellation_token.clone();
        // Writer task
        tokio::spawn(async move {
            let mut writer = crate::framing::LengthPrefixedWrite::new(write_half);
            loop {
                tokio::select! {
                    _ = cancellation_token_writer.cancelled() => break,
                    Some(write_req) = write_rx.recv() => {
                        if writer.write_msg(&write_req.ctrl).await.is_err() {
                            break;
                        }
                    }
                }
            }
        });
        // Reader task
        tokio::spawn(async move {
            let mut reader = crate::framing::LengthPrefixedRead::new(read_half);
            loop {
                tokio::select! {
                    _ = cancellation_token_reader.cancelled() => break,
                    result = reader.read_msg::<Control<Result<M::Reply, PythonExecutionError>>>() => {
                        match result {
                            Ok(ctrl) => {
                                if let Control::Real(envelope) = ctrl {
                                    let correlation_id = envelope.correlation_id;
                                    let reply = envelope.inner;
                                    let mut in_flight = in_flight_reader.as_inner().lock().await;
                                    if let Some(slot) = in_flight.remove(&correlation_id) {
                                        let (reply_mutex, notify) = &*slot;
                                        let mut guard = reply_mutex.lock().await;
                                        *guard = Some(reply);
                                        drop(guard);
                                        notify.notify_one();
                                        trace!(event = "parent_in_flight", action = "notify", correlation_id, in_flight_len = in_flight.len(), "Notified reply slot in parent in_flight");
                                    }
                                }
                            }
                            Err(e) => {
                                error!(event = "parent_read_error", error = ?e, "Parent reader task error, exiting");
                                break;
                            }
                        }
                    }
                }
            }
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

    pub async fn send(&self, msg: M) -> Result<M::Reply, PythonExecutionError> {
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
        let notify: ReplySlot<Result<M::Reply, PythonExecutionError>> = Arc::new((TokioMutex::new(None), Notify::new()));
        {
            let mut in_flight = self.in_flight.as_inner().lock().await;
            in_flight.insert(correlation_id, notify.clone());
            trace!(event = "parent_in_flight", action = "insert", in_flight_len = in_flight.len(), correlation_id, ?span_id, "Inserted into parent in_flight");
        }
        if let Err(e) = self.write_tx.send(write_req) {
            let mut in_flight = self.in_flight.as_inner().lock().await;
            in_flight.remove(&correlation_id);
            trace!(event = "parent_send", step = "send_failed", correlation_id, error = ?e, "Failed to send write request");
            return Err(PythonExecutionError::ExecutionError { message: format!("Failed to send write request: {e}") });
        }
        trace!(event = "parent_send", step = "sent", correlation_id, ?span_id, "Sent write request to writer task");
        let notify_strong = notify.clone();
        let (reply_mutex, notify) = &*notify_strong;
        trace!(event = "parent_send", step = "before_notify", correlation_id, ?span_id, "Waiting for notify");
        notify.notified().await;
        trace!(event = "parent_send", step = "after_notify", correlation_id, ?span_id, "Notified, checking reply");
        let mut guard = reply_mutex.lock().await;
        match guard.take() {
            Some(res) => {
                trace!(event = "parent_send", step = "reply_received", correlation_id, ?span_id, ?res, "Received reply via notify");
                res
            },
            None => {
                tracing::error!(event = "parent_send", step = "reply_missing", correlation_id, ?span_id, "Reply missing after notify");
                Err(PythonExecutionError::ExecutionError { message: "Reply missing after notify".to_string() })
            }
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

/// Message trait for actors, supporting both in-process and protocol (serializable) replies.
pub trait ConcurrentMessage<M> {
    type Reply;
    type ProtocolReply: serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Encode + Decode<()> + 'static;
    /// Convert in-process reply to protocol reply for IPC serialization.
    fn into_protocol_reply(reply: Self::Reply) -> Self::ProtocolReply;
}

/// A builder for creating child process actors
pub struct ChildProcessBuilder<A, M>
where
    A: kameo::message::Message<M> + ConcurrentMessage<M> + Send + Sync + Actor + 'static,
    M: KameoChildProcessMessage + Send + Sync + 'static,
    <A as ConcurrentMessage<M>>::ProtocolReply: Serialize + for<'de> Deserialize<'de> + Send + Encode + Decode<()> + 'static,
{
    config: ChildProcessConfig,
    _phantom: PhantomData<(A, M)>,
}

impl<A, M> ChildProcessBuilder<A, M>
where
    A: kameo::message::Message<M> + ConcurrentMessage<M> + Send + Sync + Actor + 'static,
    M: KameoChildProcessMessage + Send + Sync + 'static,
    <A as ConcurrentMessage<M>>::ProtocolReply: Serialize + for<'de> Deserialize<'de> + Send + Encode + Decode<()> + 'static,
{
    pub fn new() -> Self {
        Self {
            config: ChildProcessConfig {
                name: A::name().to_string(),
                log_level: Level::DEBUG,
                env_vars: Vec::new(),
            },
            _phantom: PhantomData,
        }
    }

    pub fn log_level(mut self, level: Level) -> Self {
        self.config.log_level = level;
        self
    }

    pub fn with_env_var(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        let key = key.into();
        if matches!(
            key.as_str(),
            "KAMEO_REQUEST_SOCKET" | "KAMEO_CALLBACK_SOCKET" | "KAMEO_CHILD_ACTOR"
        ) {
            tracing::warn!(event = "env_var_blocked", key = %key, "Attempted to set protocol-critical env var in with_env_var; ignored");
        } else {
            self.config.env_vars.push((key, value.into()));
        }
        self
    }

    pub fn with_env_vars(mut self, vars: Vec<(String, String)>) -> Self {
        for (k, v) in vars {
            if matches!(
                k.as_str(),
                "KAMEO_REQUEST_SOCKET" | "KAMEO_CALLBACK_SOCKET" | "KAMEO_CHILD_ACTOR"
            ) {
                tracing::warn!(event = "env_var_blocked", key = %k, "Attempted to set protocol-critical env var in with_env_vars; ignored");
            } else {
                self.config.env_vars.push((k, v));
            }
        }
        self
    }

    pub fn with_actor_name(mut self, name: impl Into<String>) -> Self {
        self.config.name = name.into();
        self
    }

    /// Spawn the child process backend and return it (not an actor)
    pub async fn spawn<H>(
        self,
        _handler: H,
    ) -> io::Result<Arc<SubprocessIpcBackend<M>>>
    where
        H: Send + Clone,
    {
        let request_socket_path =
            handshake::unique_socket_path(&format!("{}-req", self.config.name));
        let callback_socket_path =
            handshake::unique_socket_path(&format!("{}-cb", self.config.name));

        tracing::debug!(event = "handshake_setup", request_socket = %request_socket_path.to_string_lossy(), callback_socket = %callback_socket_path.to_string_lossy(), "Parent binding sockets and setting env vars");
        // Set up the Unix domain sockets
        let request_endpoint = request_socket_path.to_string_lossy().to_string();
        let request_incoming = tokio::net::UnixListener::bind(&request_endpoint)?;
        tracing::debug!(event = "handshake_setup", which = "request", socket = %request_socket_path.to_string_lossy(), "Parent bound request socket");
        let callback_endpoint = callback_socket_path.to_string_lossy().to_string();
        let callback_incoming = tokio::net::UnixListener::bind(&callback_endpoint)?;
        tracing::debug!(event = "handshake_setup", which = "callback", socket = %callback_socket_path.to_string_lossy(), "Parent bound callback socket");
        // Get current executable path
        let current_exe = std::env::current_exe()?;
        // Spawn child process with parent environment, then override/remove as needed
        let mut cmd = tokio::process::Command::new(current_exe);
        cmd.envs(std::env::vars()); // Inherit all parent env vars
        cmd.env_remove("PYTHONPATH");
        // Add custom environment variables FIRST
        let mut env_snapshot = Vec::new();
        for (key, value) in self.config.env_vars.iter() {
            cmd.env(key, value);
            env_snapshot.push((key.clone(), value.clone()));
        }
        // Always set the socket env vars LAST so they cannot be overwritten
        cmd.env("KAMEO_CHILD_ACTOR", &self.config.name);
        env_snapshot.push(("KAMEO_CHILD_ACTOR".to_string(), self.config.name.clone()));
        cmd.env(
            "KAMEO_REQUEST_SOCKET",
            request_socket_path.to_string_lossy().as_ref(),
        );
        env_snapshot.push((
            "KAMEO_REQUEST_SOCKET".to_string(),
            request_socket_path.to_string_lossy().to_string(),
        ));
        cmd.env(
            "KAMEO_CALLBACK_SOCKET",
            callback_socket_path.to_string_lossy().as_ref(),
        );
        env_snapshot.push((
            "KAMEO_CALLBACK_SOCKET".to_string(),
            callback_socket_path.to_string_lossy().to_string(),
        ));
        tracing::debug!(event = "handshake_setup", child_env_request = %request_socket_path.to_string_lossy(), child_env_callback = %callback_socket_path.to_string_lossy(), "Parent set child env vars (final override)");
        tracing::debug!(event = "child_env_snapshot", env = ?env_snapshot, "Child process environment snapshot before spawn");
        // Inherit RUST_LOG for consistent logging
        if let Ok(rust_log) = std::env::var("RUST_LOG") {
            cmd.env("RUST_LOG", rust_log);
        }

        // Spawn the child process
        cmd.stdout(Stdio::inherit());
        cmd.stderr(Stdio::inherit());
        let child = cmd.spawn()?;

        tracing::info!(
            event = "lifecycle",
            status = "spawned",
            child_pid = child.id(),
            "Child process spawned"
        );

        // Strictly ordered handshake: accept request, then callback
        tracing::debug!(event = "handshake_accept", which = "request", socket = %request_socket_path.to_string_lossy(), "Parent waiting for request connection");
        let (mut request_conn, _addr) = tokio::time::timeout(
            Duration::from_secs(30),
            request_incoming.accept(),
        ).await??;
        // Parent performs handshake using perform_handshake
        perform_handshake::<M>(&mut request_conn, true)
            .await
            .map_err(|e| {
                io::Error::new(io::ErrorKind::Other, format!("Handshake failed: {e:?}"))
            })?;
        // After accepting request connection and handshake, proceed directly
        tracing::debug!(event = "handshake_accept", which = "callback", socket = %callback_socket_path.to_string_lossy(), "Parent waiting for callback connection");
        let (_callback_conn, _addr) = tokio::time::timeout(
            Duration::from_secs(30),
            callback_incoming.accept(),
        ).await??;
        tracing::trace!(
            event = "parent_spawn",
            step = "after_callback_accept",
            "Parent accepted callback connection, about to return backend and callback receiver"
        );
        // Split the request_conn into read and write halves
        let (read_half, write_half) = request_conn.into_split();
        let (write_tx, mut write_rx) = tokio::sync::mpsc::unbounded_channel::<WriteRequest<M>>();
        let in_flight: InFlightMap<ReplySlot<Result<M::Reply, PythonExecutionError>>> = InFlightMap::new();
        let in_flight_writer = in_flight.clone();
        let in_flight_reader = in_flight.clone();
        // Create shutdown handle and notifier before spawning tasks
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
                    maybe_req = write_rx.recv() => {
                        let req = match maybe_req {
                            Some(r) => r,
                            None => break,
                        };
                        if let Err(e) = writer.write_msg(&req.ctrl).await {
                            tracing::error!(event = "writer_task", error = ?e, correlation_id = req.correlation_id, "Failed to write message, removing from in_flight and breaking");
                            let mut in_flight = in_flight_writer.as_inner().lock().await;
                            in_flight.remove(&req.correlation_id);
                            break;
                        }
                    }
                }
            }
            tracing::info!(event = "writer_task", "Writer task exiting");
        });
        // Reader task
        tokio::spawn(async move {
            let mut reader = crate::framing::LengthPrefixedRead::new(read_half);
            loop {
                tokio::select! {
                    _ = cancellation_token_reader.cancelled() => break,
                    result = reader.read_msg() => {
                        let ctrl: Control<Result<M::Reply, PythonExecutionError>> = match result {
                            Ok(msg) => msg,
                            Err(e) => {
                                tracing::error!(event = "reader_task", error = ?e, "Failed to read message, breaking");
                                break;
                            }
                        };
                        match ctrl {
                            Control::Real(envelope) => {
                                let entry = {
                                    let mut in_flight = in_flight_reader.as_inner().lock().await;
                                    in_flight.remove(&envelope.correlation_id)
                                };
                                if let Some(entry) = entry {
                                    let (reply_mutex, notify) = &*entry;
                                    let mut guard = reply_mutex.lock().await;
                                    *guard = Some(envelope.inner);
                                    notify.notify_waiters();
                                }
                            }
                            Control::Handshake => continue,
                        }
                    }
                }
            }
            // On exit, drain in_flight and send error to all pending
            let mut in_flight = in_flight_reader.as_inner().lock().await;
            for (_corr_id, slot) in in_flight.drain() {
                let (reply_mutex, notify) = &*slot;
                let mut guard = reply_mutex.lock().await;
                *guard = Some(Err(PythonExecutionError::ExecutionError { message: "IPC backend reply loop exited".to_string() }));
                notify.notify_waiters();
            }
            tracing::info!(event = "reader_task", "Reader task exiting and drained in_flight");
        });
        let backend = Arc::new(SubprocessIpcBackend::<M> {
            write_tx,
            in_flight,
            next_id: AtomicU64::new(1),
            cancellation_token,
            _phantom: std::marker::PhantomData,
        });
        Ok(backend)
    }
}

/// Trait for message handlers in the child process (no Context, no actor system)
#[async_trait]
pub trait ChildProcessMessageHandler<Msg> {
    type Reply: Send + 'static;
    async fn handle_child_message(&mut self, msg: Msg) -> Self::Reply;
}

/// Run the IPC handler loop in the child process. No ActorRef, no Clone, no spawn, just handle messages.
#[derive(Debug)]
pub enum ChildProcessLoopError {
    Io(std::io::Error),
    ChildProcessClosedCleanly,
}

impl From<std::io::Error> for ChildProcessLoopError {
    fn from(e: std::io::Error) -> Self {
        ChildProcessLoopError::Io(e)
    }
}

// Helper to read the next message from the socket
async fn read_next_message<M>(conn: &mut tokio::net::UnixStream) -> Option<(u64, Vec<u8>)>
where
    M: KameoChildProcessMessage + Send + 'static,
{
    tracing::trace!(event = "child_read", step = "before_len", "About to read length prefix");
    let mut len_buf = [0u8; 4];
    if let Err(_) = conn.read_exact(&mut len_buf).await {
        return None;
    }
    let msg_len = u32::from_le_bytes(len_buf) as usize;
    tracing::trace!(event = "child_read", step = "after_len", ?len_buf, msg_len, "Read length prefix");
    tracing::trace!(event = "child_read", step = "before_msg", msg_len, "About to read message of len {}", msg_len);
    let mut msg_buf = vec![0u8; msg_len];
    let n = match conn.read_exact(&mut msg_buf).await {
        Ok(n) => n,
        Err(_) => return None,
    };
    tracing::trace!(event = "child_read", step = "after_msg", n, "Read {} bytes for message", n);
    // Decode the Control<M> envelope to extract correlation_id
    let ctrl: Control<M> = match bincode::decode_from_slice(&msg_buf[..], bincode::config::standard()) {
        Ok((ctrl, _)) => ctrl,
        Err(_) => return None,
    };
    match ctrl {
        Control::Real(envelope) => Some((envelope.correlation_id, msg_buf)),
        Control::Handshake => None,
    }
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
    H: ChildProcessMessageHandler<M, Reply = Result<<M as KameoChildProcessMessage>::Reply, PythonExecutionError>> + Send + Clone + 'static,
    M: KameoChildProcessMessage + Send + 'static,
    M::Reply: serde::Serialize + bincode::Encode + std::fmt::Debug + 'static,
{
    use futures::stream::{FuturesUnordered, StreamExt};
    let config = config.unwrap_or_default();
    let mut in_flight = FuturesUnordered::new();
    let (reply_tx, mut reply_rx) = tokio::sync::mpsc::unbounded_channel::<(u64, Vec<u8>)>();
    let shutdown = false;
    loop {
        tracing::trace!(event = "child_loop", step = "enter", in_flight = in_flight.len(), shutdown = shutdown, "Entering child actor loop select");
        let in_flight_len = in_flight.len();
        tokio::select! {
            biased;
            // Prioritize reading new messages if under concurrency limit
            Some((correlation_id, msg)) = async {
                if !shutdown && in_flight_len < config.max_concurrency {
                    read_next_message::<M>(&mut conn).await
                } else {
                    None
                }
            } => {
                tracing::trace!(event = "child_ipc", step = "read", len = msg.len(), raw = ?&msg[..], "Read message from parent");
                let ctrl: Control<M> = match bincode::decode_from_slice(&msg[..], bincode::config::standard()) {
                    Ok((ctrl, _)) => {
                        tracing::trace!(event = "bincode_decode", type_deserialized = std::any::type_name::<Control<M>>(), len = msg.len(), "Decoding Control envelope");
                        ctrl
                    },
                    Err(e) => {
                        tracing::error!(event = "bincode_decode_error", type_deserialized = std::any::type_name::<Control<M>>(), len = msg.len(), error = ?e, "Failed to decode Control envelope");
                        return Ok(());
                    }
                };
            match ctrl {
                Control::Handshake => {
                        tracing::debug!(event = "child_ipc", step = "handshake", "Received handshake from parent");
                    }
                    Control::Real(envelope) => {
                        let parent_cx = opentelemetry::global::get_text_map_propagator(|propagator| {
                            propagator.extract(&envelope.context.0)
                        });
                        let span = tracing::info_span!("child_message_handler", event = "message", handler = "child", process_role = "child");
                        span.set_parent(parent_cx);
                        let mut handler = handler.clone();
                        let reply_tx = reply_tx.clone();
                        let fut = async move {
                            let reply = handler.handle_child_message(envelope.inner).await;
                            tracing::trace!(event = "child_ipc", step = "after_handle", ?reply, correlation_id = correlation_id, "Got reply from handle_child_message");
                            let reply_envelope = MultiplexEnvelope {
                                correlation_id,
                                inner: reply,
                                context: Default::default(),
                            };
                            let ctrl = Control::Real(reply_envelope);
                            match bincode::encode_to_vec(ctrl, bincode::config::standard()) {
                                Ok(reply_bytes) => {
                                    let _ = reply_tx.send((correlation_id, reply_bytes));
                                },
                        Err(e) => {
                                    tracing::error!(event = "bincode_encode_error", correlation_id = correlation_id, error = ?e, "Failed to encode reply envelope");
                                }
                        }
                    };
                        in_flight.push(fut);
                        tracing::trace!(event = "child_in_flight", action = "push", in_flight_len = in_flight.len(), "Pushed to child in_flight");
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
            else => {
                if shutdown && in_flight.is_empty() && reply_rx.is_empty() {
            break;
                }
            }
        }
    }
    Ok(())
}

/// Prelude module for commonly used items
pub mod prelude {
    // pub use crate::child_process_main; // REMOVED: no longer exists
    // pub use crate::child_process_main_with_runtime; // REMOVED: python/handler actors must provide their own entrypoint
    pub use crate::ChildProcessBuilder;
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
    fn on_start(&mut self, _actor_ref: ActorRef<Self>) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send {
        async move { Ok(()) }
    }
    fn on_stop(
        &mut self,
        _actor_ref: WeakActorRef<Self>,
        reason: ActorStopReason,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send {
        async move {
            tracing::error!(status = "stopped", actor_type = "SubprocessIpcActor", ?reason);
            Ok(())
        }
    }
}

impl<M> kameo::message::Message<M> for SubprocessIpcActor<M>
where
    M: KameoChildProcessMessage + Send + Sync + 'static,
{
    type Reply = Result<M::Reply, PythonExecutionError>;
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
        H: ChildProcessMessageHandler<M, Reply = Result<<M as KameoChildProcessMessage>::Reply, PythonExecutionError>> + Send + Clone + 'static,
        M::Reply: serde::Serialize + bincode::Encode + std::fmt::Debug + Sync + Send + 'static,
    {
        use crate::framing::{LengthPrefixedRead, LengthPrefixedWrite};
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
                                let reply = handler.handle_child_message(envelope.inner).await;
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
    M: Decode<()> + Send + 'static,
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
