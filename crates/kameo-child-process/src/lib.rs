#![forbid(unsafe_code)]

pub mod callback;

use anyhow::Result;
use async_trait::async_trait;
use bincode::{Decode, Encode};
use futures::StreamExt;
use futures::FutureExt;
use kameo::actor::{Actor, ActorRef, WeakActorRef};
use kameo::error::{ActorStopReason, PanicError};
use kameo::message::{Context, Message};
use kameo::prelude::*;
use opentelemetry::global;
use opentelemetry::Context as OTelContext;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::io;
use std::marker::PhantomData;
use std::ops::ControlFlow;
use std::path::PathBuf;
use std::sync::Arc;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::Mutex;
use tracing::{debug, error, instrument, warn, Instrument, Level};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use uuid::Uuid;
use once_cell::sync::OnceCell;
use pyo3;

pub use callback::{CallbackHandle, ChildCallbackMessage, CallbackSender, CallbackReceiver, CallbackHandler};

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
    Self::Error: std::error::Error + Send + Sync + 'static
{
    /// Called when the runtime is available, before on_start
    fn init_with_runtime<'a>(&'a mut self, runtime: &'static tokio::runtime::Runtime) -> std::pin::Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>>;
}

/// Trait object for async read/write operations
pub trait AsyncReadWrite: AsyncRead + AsyncWrite + Send + Unpin {}

impl std::fmt::Debug for dyn AsyncReadWrite {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "AsyncReadWrite")
    }
}

#[async_trait]
impl<T: AsyncRead + AsyncWrite + Send + Unpin> AsyncReadWrite for T {}

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

/// Control message for handshake and real messages
#[derive(Debug, Serialize, Deserialize, Encode, Decode)]
pub enum Control<T> {
    Handshake,
    Real(T, TracingContext),
}

impl<T> Control<T> {
    pub fn is_handshake(&self) -> bool {
        matches!(self, Control::Handshake)
    }
    pub fn into_real(self) -> Option<(T, TracingContext)> {
        match self {
            Control::Real(t, tc) => Some((t, tc)),
            _ => None,
        }
    }
}

/// Error types for subprocess actor
#[derive(Debug, Error)]
pub enum SubprocessActorError {
    #[error("IPC error: {0}")]
    Ipc(#[from] io::Error),

    #[error("Serialization error: {0}")]
    Serialization(#[from] bincode::error::EncodeError),

    #[error("Deserialization error: {0}")]
    Deserialization(#[from] bincode::error::DecodeError),

    #[error("Actor panicked: {reason}")]
    Panicked { reason: String },

    #[error("Protocol error: {0}")]
    Protocol(String),

    #[error("Handshake failed: {0}")]
    HandshakeFailed(String),

    #[error("Connection closed")]
    ConnectionClosed,

    #[error("Unknown actor type: {actor_name}")]
    UnknownActorType { actor_name: String },
}

impl bincode::Encode for SubprocessActorError {
    fn encode<E: bincode::enc::Encoder>(&self, encoder: &mut E) -> core::result::Result<(), bincode::error::EncodeError> {
        match self {
            SubprocessActorError::Protocol(s) => s.encode(encoder),
            _ => panic!("Tried to encode non-Protocol variant of SubprocessActorError!"),
        }
    }
}

impl bincode::Decode<()> for SubprocessActorError {
    fn decode<D: bincode::de::Decoder<Context = ()>>(decoder: &mut D) -> core::result::Result<Self, bincode::error::DecodeError> {
        let s = String::decode(decoder)?;
        Ok(SubprocessActorError::Protocol(s))
    }
}

/// Handle unknown actor type errors with proper tracing
#[instrument(level = "error", fields(actor_name))]
pub fn handle_unknown_actor_error(actor_name: &str) -> SubprocessActorError {
    error!(
        event = "lifecycle",
        status = "error",
        actor_type = actor_name,
        "Unknown actor type encountered"
    );
    SubprocessActorError::UnknownActorType {
        actor_name: actor_name.to_string(),
    }
}

/// Actor that manages a subprocess and communicates with it via IPC
#[derive(Debug)]
pub struct SubprocessActor<M> {
    connection: Arc<Mutex<Box<dyn AsyncReadWrite>>>,
    child: Option<tokio::process::Child>,
    socket_path: PathBuf,
    _phantom: PhantomData<M>,
}

#[async_trait]
impl<M> Actor for SubprocessActor<M>
where
    M: KameoChildProcessMessage + Send + 'static,
{
    type Error = SubprocessActorError;

    #[instrument(skip(self, _actor_ref), fields(actor_type = "SubprocessActor"))]
    fn on_start(
        &mut self,
        _actor_ref: ActorRef<Self>,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send {
        debug!(
            event = "lifecycle",
            status = "starting",
            "Starting subprocess actor"
        );

        async move {
            // Perform handshake
            let mut conn = self.connection.lock().await;
            let handshake = Control::<M>::Handshake;
            let handshake_bytes = bincode::encode_to_vec(&handshake, bincode::config::standard())?;

            conn.write_all(&handshake_bytes).await?;

            let mut resp_buf = vec![0u8; 1024];
            let n = conn.read(&mut resp_buf).await?;
            if n == 0 {
                return Err(SubprocessActorError::HandshakeFailed(
                    "Connection closed during handshake".into(),
                ));
            }

            let (resp, _): (Control<()>, _) =
                bincode::decode_from_slice(&resp_buf[..n], bincode::config::standard())?;
            if !resp.is_handshake() {
                return Err(SubprocessActorError::HandshakeFailed(
                    "Invalid handshake response".into(),
                ));
            }

            debug!(
                event = "lifecycle",
                status = "started",
                "Subprocess actor started successfully"
            );
            Ok(())
        }
    }

    #[instrument(skip(self, _actor_ref, reason), fields(actor_type = "SubprocessActor"))]
    fn on_stop(
        &mut self,
        _actor_ref: WeakActorRef<Self>,
        reason: ActorStopReason,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send {
        debug!(
            event = "lifecycle",
            status = "stopping",
            ?reason,
            "Stopping subprocess actor"
        );

        async move {
            // Kill child process if it exists
            if let Some(mut child) = self.child.take() {
                if let Err(e) = child.kill().await {
                    warn!(event = "lifecycle", error = ?e, "Failed to kill child process");
                }
            }

            // Clean up socket file
            if let Err(e) = tokio::fs::remove_file(&self.socket_path).await {
                warn!(event = "lifecycle", error = ?e, "Failed to remove socket file");
            }

            debug!(
                event = "lifecycle",
                status = "stopped",
                "Subprocess actor stopped"
            );
            Ok(())
        }
    }

    #[instrument(skip(self, _actor_ref, err), fields(actor_type = "SubprocessActor"))]
    fn on_panic(
        &mut self,
        _actor_ref: WeakActorRef<Self>,
        err: PanicError,
    ) -> impl std::future::Future<Output = Result<ControlFlow<ActorStopReason>, Self::Error>> + Send
    {
        error!(event = "lifecycle", status = "panicked", error = ?err, "Subprocess actor panicked");
        async move { Ok(ControlFlow::Break(ActorStopReason::Panicked(err))) }
    }
}

impl<M> SubprocessActor<M> {
    pub fn new(
        connection: Box<dyn AsyncReadWrite>,
        child: tokio::process::Child,
        socket_path: PathBuf,
    ) -> Self {
        Self {
            connection: Arc::new(Mutex::new(connection)),
            child: Some(child),
            socket_path,
            _phantom: PhantomData,
        }
    }
}

pub mod handshake {
    use super::*;
    use futures::StreamExt;
    use parity_tokio_ipc::Endpoint;
    use tokio::process::Command;

    pub fn unique_socket_path(actor_name: &str) -> PathBuf {
        let mut path = std::path::PathBuf::from("/tmp");
        let short_name = &actor_name[0..std::cmp::min(8, actor_name.len())];
        path.push(format!(
            "kameo-{}-{}.sock",
            short_name,
            Uuid::new_v4().simple()
        ));
        path
    }

    #[instrument(skip(exe), fields(actor_name))]
    pub async fn host<M, R>(
        actor_name: &str,
        exe: &str,
    ) -> std::io::Result<(Box<dyn AsyncReadWrite>, tokio::process::Child, PathBuf)>
    where
        M: Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
        R: Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        let socket_path = unique_socket_path(actor_name);
        let socket_path_str = socket_path.to_string_lossy().into_owned();

        debug!(event = "handshake", status = "starting", socket_path = %socket_path_str, "Starting host handshake");

        let mut cmd = Command::new(exe);
        cmd.env("KAMEO_CHILD_ACTOR", actor_name);
        cmd.env("KAMEO_ACTOR_SOCKET", socket_path_str.clone());
        if let Ok(rust_log) = std::env::var("RUST_LOG") {
            cmd.env("RUST_LOG", rust_log);
        }

        let endpoint = Endpoint::new(socket_path_str.clone());
        let mut incoming = endpoint.incoming()?;

        debug!(
            event = "handshake",
            status = "spawning",
            "Spawning child process"
        );
        let child = cmd.spawn()?;

        debug!(
            event = "handshake",
            status = "waiting",
            "Waiting for child connection"
        );
        let conn = incoming.next().await.transpose()?.ok_or_else(|| {
            error!(event = "handshake", "No child connection received");
            io::Error::new(io::ErrorKind::Other, "No child connection")
        })?;

        debug!(
            event = "handshake",
            status = "completed",
            "Host handshake completed successfully"
        );
        Ok((Box::new(conn), child, socket_path))
    }

    #[instrument(fields(actor_name = ?std::env::var("KAMEO_CHILD_ACTOR").ok()))]
    pub async fn child_request() -> std::io::Result<Box<dyn AsyncReadWrite>> {
        let socket_path = std::env::var("KAMEO_REQUEST_SOCKET")
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "KAMEO_REQUEST_SOCKET not set"))?;

        debug!(event = "handshake", status = "starting", socket_path = %socket_path, "Starting child request handshake");

        let conn = tokio::net::UnixStream::connect(&socket_path).await?;

        debug!(
            event = "handshake",
            status = "completed",
            "Child request handshake completed successfully"
        );
        Ok(Box::new(conn))
    }

    #[instrument(fields(actor_name = ?std::env::var("KAMEO_CHILD_ACTOR").ok()))]
    pub async fn child_callback() -> std::io::Result<Box<dyn AsyncReadWrite>> {
        let socket_path = std::env::var("KAMEO_CALLBACK_SOCKET")
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "KAMEO_CALLBACK_SOCKET not set"))?;

        debug!(event = "handshake", status = "starting", socket_path = %socket_path, "Starting child callback handshake");

        let conn = tokio::net::UnixStream::connect(&socket_path).await?;

        debug!(
            event = "handshake",
            status = "completed",
            "Child callback handshake completed successfully"
        );
        Ok(Box::new(conn))
    }
}

/// Macro to register subprocess actors
#[macro_export]
macro_rules! register_subprocess_actors {
    (
        actors = { $(($actor:ty, $msg:ty, $callback:ty)),* $(,)? }
        $(,)?
    ) => {
        pub fn maybe_run_subprocess_registry() -> Option<Result<(), Box<dyn std::error::Error>>> {
            // Check if we're a child process
            if let Ok(actor_name) = std::env::var("KAMEO_CHILD_ACTOR") {
                Some(match actor_name.as_str() {
                    $(
                        stringify!($actor) => {
                            ::kameo_child_process::child_process_main::<$actor, $msg, $callback>()
                        }
                    )*
                    _ => {
                        Err(Box::new(::kameo_child_process::handle_unknown_actor_error(&actor_name)))
                    }
                })
            } else {
                None
            }
        }
    };
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

/// A builder for creating child process actors
pub struct ChildProcessBuilder<A, M, C>
where
    A: Default + Message<M> + Send + Sync + Actor + CallbackSender<C> + 'static,
    M: KameoChildProcessMessage + Send + 'static,
    <A as Message<M>>::Reply:
        Serialize + for<'de> Deserialize<'de> + Send + Encode + Decode<()> + 'static,
    C: ChildCallbackMessage,
{
    config: ChildProcessConfig,
    _phantom: PhantomData<(A, M, C)>,
}

impl<A, M, C> Default for ChildProcessBuilder<A, M, C>
where
    A: Default + Message<M> + Send + Sync + Actor + CallbackSender<C> + 'static,
    M: KameoChildProcessMessage + Send + 'static,
    <A as Message<M>>::Reply:
        Serialize + for<'de> Deserialize<'de> + Send + Encode + Decode<()> + 'static,
    C: ChildCallbackMessage,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<A, M, C> ChildProcessBuilder<A, M, C>
where
    A: Default + Message<M> + Send + Sync + Actor + CallbackSender<C> + 'static,
    M: KameoChildProcessMessage + Send + 'static,
    <A as Message<M>>::Reply:
        Serialize + for<'de> Deserialize<'de> + Send + Encode + Decode<()> + 'static,
    C: ChildCallbackMessage,
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
        self.config.env_vars.push((key.into(), value.into()));
        self
    }

    /// Spawn the child process actor and return a reference to it
    pub async fn spawn<H>(self, handler: H) -> io::Result<(ActorRef<SubprocessActor<M>>, CallbackReceiver<C, H>)>
    where
        H: CallbackHandler<C>,
    {
        let request_socket_path = handshake::unique_socket_path(&format!("{}-req", self.config.name));
        let callback_socket_path = handshake::unique_socket_path(&format!("{}-cb", self.config.name));

        // Set up the Unix domain sockets
        let request_endpoint = parity_tokio_ipc::Endpoint::new(request_socket_path.to_string_lossy().to_string());
        let mut request_incoming = request_endpoint.incoming()?;
        let callback_endpoint = parity_tokio_ipc::Endpoint::new(callback_socket_path.to_string_lossy().to_string());
        let mut callback_incoming = callback_endpoint.incoming()?;

        // Get current executable path
        let current_exe = std::env::current_exe()?;

        // Spawn child process with clean environment
        let mut cmd = tokio::process::Command::new(current_exe);
        cmd.env_remove("PYTHONPATH");
        cmd.env("KAMEO_CHILD_ACTOR", &self.config.name);
        cmd.env("KAMEO_REQUEST_SOCKET", request_socket_path.to_string_lossy().as_ref());
        cmd.env("KAMEO_CALLBACK_SOCKET", callback_socket_path.to_string_lossy().as_ref());

        // Add custom environment variables
        for (key, value) in self.config.env_vars {
            cmd.env(key, value);
        }

        // Inherit RUST_LOG for consistent logging
        if let Ok(rust_log) = std::env::var("RUST_LOG") {
            cmd.env("RUST_LOG", rust_log);
        }

        debug!(
            event = "lifecycle",
            status = "spawning",
            actor = ?self.config.name,
            "Spawning child process"
        );

        // Spawn the child process
        let mut child = cmd.spawn()?;

        debug!(
            event = "lifecycle",
            status = "spawned",
            child_pid = child.id(),
            "Child process spawned"
        );

        // Wait for child to connect to both sockets, with timeout and liveness check
        use tokio::time::{timeout, Duration, sleep};
        let timeout_duration = Duration::from_secs(3);
        let poll_interval = Duration::from_millis(50);
        let mut request_conn = None;
        let mut callback_conn = None;
        let start = std::time::Instant::now();
        loop {
            // Try to get connections
            if request_conn.is_none() {
                match request_incoming.next().now_or_never() {
                    Some(Some(Ok(conn))) => request_conn = Some(conn),
                    Some(Some(Err(e))) => warn!("Error accepting request connection: {:?}", e),
                    Some(None) => {},
                    None => {},
                }
            }
            if callback_conn.is_none() {
                match callback_incoming.next().now_or_never() {
                    Some(Some(Ok(conn))) => callback_conn = Some(conn),
                    Some(Some(Err(e))) => warn!("Error accepting callback connection: {:?}", e),
                    Some(None) => {},
                    None => {},
                }
            }
            if request_conn.is_some() && callback_conn.is_some() {
                break;
            }
            // Check if child exited
            if let Some(status) = child.try_wait()? {
                warn!("Child exited early with status: {:?}", status);
                // Clean up sockets
                let _ = tokio::fs::remove_file(&request_socket_path).await;
                let _ = tokio::fs::remove_file(&callback_socket_path).await;
                return Err(io::Error::new(io::ErrorKind::Other, format!("Child exited early: {:?}", status)));
            }
            // Timeout
            if start.elapsed() > timeout_duration {
                warn!("Timeout waiting for child handshake, killing child process");
                let _ = child.kill().await;
                let _ = tokio::fs::remove_file(&request_socket_path).await;
                let _ = tokio::fs::remove_file(&callback_socket_path).await;
                return Err(io::Error::new(io::ErrorKind::TimedOut, "Timeout waiting for child handshake"));
            }
            sleep(poll_interval).await;
        }
        let request_conn = request_conn.unwrap();
        let callback_conn = callback_conn.unwrap();

        let actor = SubprocessActor::new(Box::new(request_conn), child, request_socket_path);
        let callback_receiver = CallbackReceiver::new(Box::new(callback_conn), handler);

        Ok((kameo::spawn(actor), callback_receiver))
    }
}

/// Run the actor loop in the child process
#[instrument(skip(actor, conn))]
pub async fn run_child_actor_loop<A, Msg>(
    actor: &mut A,
    mut conn: Box<dyn AsyncReadWrite>,
) -> io::Result<()>
where
    A: Message<Msg> + Clone + Send + 'static,
    Msg: KameoChildProcessMessage + Send + 'static,
    <A as Message<Msg>>::Reply: Reply
        + Send
        + Serialize
        + DeserializeOwned
        + Encode
        + Decode<()>
        + std::fmt::Debug
        + 'static,
    <<A as Message<Msg>>::Reply as Reply>::Error:
        std::fmt::Display + std::error::Error + Send + Sync + Encode + Decode<()> + 'static,
    <<A as Message<Msg>>::Reply as Reply>::Ok: Encode + Decode<()> + Send + 'static,
{
    debug!(
        event = "lifecycle",
        status = "running",
        "Child actor loop started"
    );

    let actor_ref = kameo::spawn(actor.clone());

    loop {
        let mut buf = vec![0u8; 4096];
        let n = match conn.read(&mut buf).await {
            Ok(0) => {
                error!(
                    event = "lifecycle",
                    status = "shutdown",
                    "Child process closed connection, shutting down parent handler"
                );
                return Err(io::Error::new(io::ErrorKind::Other, "Child process closed connection"));
            }
            Ok(n) => n,
            Err(e) => {
                error!(event = "lifecycle", error = ?e, "Read error");
                break;
            }
        };

        let (msg, trace_context) = {
            let (ctrl, _): (Control<Msg>, _) =
                match bincode::decode_from_slice(&buf[..n], bincode::config::standard()) {
                    Ok((ctrl, len)) => (ctrl, len),
                    Err(e) => {
                        error!(event = "message", error = ?e, "Failed to decode message");
                        continue;
                    }
                };

            match ctrl {
                Control::Handshake => {
                    debug!(
                        event = "handshake",
                        status = "responding",
                        "Responding to handshake"
                    );
                    let resp = Control::<()>::Handshake;
                    let resp_bytes = bincode::encode_to_vec(&resp, bincode::config::standard())
                        .expect("Failed to encode handshake response");
                    conn.write_all(&resp_bytes).await?;
                    continue;
                }
                Control::Real(msg, trace_context) => (msg, trace_context),
            }
        };

        let parent_cx =
            global::get_text_map_propagator(|propagator| propagator.extract(&trace_context.0));
        let span = tracing::info_span!("child_message_handler");
        span.set_parent(parent_cx);

        let reply_fut = async {
            debug!(event = "message", status = "handling", "Handling message");
            actor_ref.ask(msg).await
        };

        let result = reply_fut.instrument(span).await;

        let reply_to_send: Result<_, SubprocessActorError> = match result {
            Ok(reply) => Ok(reply),
            Err(e) => Err(SubprocessActorError::Protocol(format!("{e}"))),
        };
        // Only Protocol(String) is ever sent, so it's always serializable
        let reply_to_send = reply_to_send.map_err(|e| {
            match e {
                SubprocessActorError::Protocol(s) => SubprocessActorError::Protocol(s),
                _ => SubprocessActorError::Protocol(format!("Non-serializable error: {e}")),
            }
        });
        let reply_bytes =
            bincode::encode_to_vec(&Control::Real(reply_to_send, TracingContext::default()), bincode::config::standard())
                .expect("Failed to encode reply");
        if let Err(e) = conn.write_all(&reply_bytes).await {
            error!(event = "message", error = ?e, "Failed to send reply to parent");
            break;
        }
        debug!(
            event = "message",
            status = "complete",
            "Message handled successfully"
        );
    }

    Ok(())
}

/// Macro to set up the complete subprocess actor system with custom runtime initialization
#[macro_export]
macro_rules! setup_subprocess_system {
    (
        actors = { $(($actor:ty, $msg:ty, $callback:ty)),* $(,)? },
        child_init = $child_init:block,
        parent_init = $parent_init:block $(,)?
    ) => {

        #[tracing::instrument(fields(pid=std::process::id()))]
        fn main() -> Result<(), Box<dyn std::error::Error>> {
            let handlers: &[(&'static str, fn() -> Result<(), Box<dyn std::error::Error>>)] = &[
                $(
                    (
                        std::any::type_name::<$actor>(),
                        || ::kameo_child_process::child_process_main_with_config::<$actor, $msg, $callback>($child_init),
                    ),
                )*
            ];
            eprintln!("!!!!!!MAIN {:?}", std::env::var("KAMEO_CHILD_ACTOR"));
            if let Ok(actor_name) = std::env::var("KAMEO_CHILD_ACTOR") {
                for (name, handler) in handlers {
                    if actor_name == *name {
                        return handler();
                    }
                }
                Err(Box::new(::kameo_child_process::handle_unknown_actor_error(&actor_name)))
            } else {
                // Parent process branch - call user-provided initialization
                $parent_init
            }
        }
    };
}

/// Prelude module for commonly used items
pub mod prelude {
    pub use crate::child_process_main;
    pub use crate::child_process_main_with_config;
    pub use crate::handshake;
    pub use crate::setup_subprocess_system;
    pub use crate::ChildProcessBuilder;
    pub use crate::SubprocessActor;
    pub use tokio::runtime;
}

#[async_trait]
impl<M> Message<M> for SubprocessActor<M>
where
    M: KameoChildProcessMessage + Send + 'static,
{
    type Reply = Result<<M as KameoChildProcessMessage>::Reply, SubprocessActorError>;

    #[instrument(skip(self, _ctx), fields(actor_type = "SubprocessActor"))]
    fn handle(
        &mut self,
        msg: M,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> impl Future<Output = Self::Reply> + Send {
        async move {
            let mut conn = self.connection.lock().await;

            let mut trace_context_map = std::collections::HashMap::new();
            global::get_text_map_propagator(|propagator| {
                propagator.inject_context(&OTelContext::current(), &mut trace_context_map);
            });
            let trace_context = TracingContext(trace_context_map);

            // Encode and send the message
            let control = Control::Real(msg, trace_context);
            let msg_bytes = bincode::encode_to_vec(&control, bincode::config::standard())?;
            conn.write_all(&msg_bytes).await?;
            conn.flush().await?;

            // Read response
            let mut resp_buf = vec![0u8; 1024 * 64]; // 64KB buffer for responses
            let n = conn.read(&mut resp_buf).await?;

            if n == 0 {
                return Err(SubprocessActorError::ConnectionClosed);
            }

            // Decode response
            let (control, _): (Control<Self::Reply>, _) =
                bincode::decode_from_slice(&resp_buf[..n], bincode::config::standard())?;

            match control {
                Control::Handshake => Err(SubprocessActorError::Protocol(
                    "Unexpected handshake response".into(),
                )),
                Control::Real(reply, _trace_context) => reply,
            }
        }
    }
}

/// Run a child process actor with the default single-threaded runtime configuration.
/// This is the default implementation used by the register_subprocess_actors macro.
pub fn child_process_main<A, M, C>() -> Result<(), Box<dyn std::error::Error>>
where
    A: Default + Message<M> + Clone + Send + CallbackSender<C> + 'static,
    M: KameoChildProcessMessage + Send + 'static,
    <A as Message<M>>::Reply: Reply
        + Send
        + Serialize
        + DeserializeOwned
        + Encode
        + Decode<()>
        + std::fmt::Debug
        + 'static,
    <<A as Message<M>>::Reply as Reply>::Error:
        std::fmt::Display + std::error::Error + Send + Sync + Encode + Decode<()> + 'static,
    <<A as Message<M>>::Reply as Reply>::Ok: Encode + Decode<()> + Send + 'static,
    C: ChildCallbackMessage,
{
    let config = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    config.block_on(async {
        let mut actor = A::default();

        // Setup callback channel
        let callback_conn = handshake::child_callback().await?;
        let handle = CallbackHandle::new(callback_conn);
        actor.set_callback_handle(handle);

        let conn = handshake::child_request().await?;
        run_child_actor_loop(&mut actor, conn).await
    })?;

    Ok(())
}

/// Run a child process actor with a custom runtime configuration.
#[instrument(skip(runtime), fields(pid=std::process::id()))]
pub fn child_process_main_with_config<A, M, C>(
    runtime: tokio::runtime::Runtime,
) -> Result<(), Box<dyn std::error::Error>>
where
    A: Default + Message<M> + Clone + Send + RuntimeAware + CallbackSender<C> + 'static,
    M: KameoChildProcessMessage + Send + 'static,
    <A as Message<M>>::Reply: Reply
        + Send
        + Serialize
        + DeserializeOwned
        + Encode
        + Decode<()>
        + std::fmt::Debug
        + 'static,
    <<A as Message<M>>::Reply as Reply>::Error:
        std::fmt::Display + std::error::Error + Send + Sync + Encode + Decode<()> + 'static,
    <<A as Message<M>>::Reply as Reply>::Ok: Encode + Decode<()> + Send + 'static,
    <A as Actor>::Error: std::error::Error + Send + Sync + 'static,
    C: ChildCallbackMessage,
{
    pyo3::prepare_freethreaded_python();
    // Store the runtime in a static Box to ensure it lives for the entire program
    static RUNTIME: OnceCell<Box<tokio::runtime::Runtime>> = OnceCell::new();
    let runtime_ref = RUNTIME.get_or_init(|| Box::new(runtime));

    let mut actor = A::default();

    runtime_ref.block_on(async {
        // Setup callback channel
        let callback_conn = handshake::child_callback().await?;
        let handle = CallbackHandle::new(callback_conn);
        actor.set_callback_handle(handle);

        // Init actor with runtime
        actor.init_with_runtime(&**runtime_ref).await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        
        // Run main request loop
        let request_conn = handshake::child_request().await?;
        run_child_actor_loop(&mut actor, request_conn).await
    })?;

    Ok(())
}

