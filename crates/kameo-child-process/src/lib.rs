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
use tracing::trace;
use std::future::Future;
use std::io;
use std::marker::PhantomData;
use std::ops::ControlFlow;
use std::path::PathBuf;
use std::sync::Arc;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, AsyncBufReadExt, BufReader};
use tokio::sync::Mutex;
use tracing::{debug, error, instrument, warn, Instrument, Level};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use uuid::Uuid;
use tokio::time::{Duration, sleep};
use tokio::process::Command;
use parity_tokio_ipc::Endpoint;
use std::process::Stdio;

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
    async fn init_with_runtime(self) -> Result<Self, Self::Error>
    where
        Self: Sized;
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
        message = "Unknown actor type encountered"
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
            actor_type = "SubprocessActor"
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
                actor_type = "SubprocessActor"
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
            actor_type = "SubprocessActor",
            reason = ?reason
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
                actor_type = "SubprocessActor"
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
        error!(event = "lifecycle", status = "panicked", actor_type = "SubprocessActor", error = ?err);
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

        debug!(event = "handshake", status = "starting", socket_path = %socket_path_str, actor_type = actor_name);

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
            actor_type = actor_name
        );
        let child = cmd.spawn()?;

        debug!(
            event = "handshake",
            status = "waiting",
            actor_type = actor_name
        );
        let conn = incoming.next().await.transpose()?.ok_or_else(|| {
            error!(event = "handshake", actor_type = actor_name, "No child connection received");
            io::Error::new(io::ErrorKind::Other, "No child connection")
        })?;

        debug!(
            event = "handshake",
            status = "completed",
            actor_type = actor_name
        );
        Ok((Box::new(conn), child, socket_path))
    }

    #[instrument(fields(pid= std::process::id(), actor_name = ?std::env::var("KAMEO_CHILD_ACTOR").ok()))]
    pub async fn child_request() -> std::io::Result<Box<dyn AsyncReadWrite>> {
        let req_env = std::env::var("KAMEO_REQUEST_SOCKET");
        let cb_env = std::env::var("KAMEO_CALLBACK_SOCKET");
        let actor_env = std::env::var("KAMEO_CHILD_ACTOR");
        tracing::info!(event = "handshake_env", KAMEO_REQUEST_SOCKET = ?req_env, KAMEO_CALLBACK_SOCKET = ?cb_env, KAMEO_CHILD_ACTOR = ?actor_env, "Child handshake env vars");
        if req_env.is_err() || cb_env.is_err() || actor_env.is_err() {
            tracing::error!(event = "handshake_env_missing", KAMEO_REQUEST_SOCKET = ?req_env, KAMEO_CALLBACK_SOCKET = ?cb_env, KAMEO_CHILD_ACTOR = ?actor_env, "Missing required handshake env var(s), aborting child early");
            return Err(std::io::Error::new(std::io::ErrorKind::Other, format!(
                "Missing required handshake env var(s): KAMEO_REQUEST_SOCKET={:?}, KAMEO_CALLBACK_SOCKET={:?}, KAMEO_CHILD_ACTOR={:?}",
                req_env, cb_env, actor_env
            )));
        }
        let socket_path = req_env.unwrap();
        tracing::debug!(event = "handshake_child", which = "request", socket_path = %socket_path, "Child got KAMEO_REQUEST_SOCKET env var");
        tracing::debug!(event = "handshake_child", which = "request", socket_path = %socket_path, "Child attempting to connect to request socket");
        let stream = Endpoint::connect(&socket_path).await?;
        Ok(Box::new(stream) as Box<dyn AsyncReadWrite>)
    }

    #[instrument(fields(actor_name = ?std::env::var("KAMEO_CHILD_ACTOR").ok()))]
    pub async fn child_callback() -> std::io::Result<Box<dyn AsyncReadWrite>> {
        let pid = std::process::id();
        let cb_env = std::env::var("KAMEO_CALLBACK_SOCKET");
        let req_env = std::env::var("KAMEO_REQUEST_SOCKET");
        let actor_env = std::env::var("KAMEO_CHILD_ACTOR");
        tracing::info!(event = "handshake_env", pid = pid, KAMEO_REQUEST_SOCKET = ?req_env, KAMEO_CALLBACK_SOCKET = ?cb_env, KAMEO_CHILD_ACTOR = ?actor_env, "Child handshake env vars");
        if cb_env.is_err() || req_env.is_err() || actor_env.is_err() {
            tracing::error!(event = "handshake_env_missing", pid = pid, KAMEO_REQUEST_SOCKET = ?req_env, KAMEO_CALLBACK_SOCKET = ?cb_env, KAMEO_CHILD_ACTOR = ?actor_env, "Missing required handshake env var(s), aborting child early");
            return Err(std::io::Error::new(std::io::ErrorKind::Other, format!(
                "Missing required handshake env var(s): KAMEO_REQUEST_SOCKET={:?}, KAMEO_CALLBACK_SOCKET={:?}, KAMEO_CHILD_ACTOR={:?}",
                req_env, cb_env, actor_env
            )));
        }
        let socket_path = cb_env.unwrap();
        tracing::debug!(event = "handshake_child", pid = pid, which = "callback", socket_path = %socket_path, "Child got KAMEO_CALLBACK_SOCKET env var");
        tracing::debug!(event = "handshake_child", pid = pid, which = "callback", socket_path = %socket_path, "Child attempting to connect to callback socket");
        let stream = Endpoint::connect(&socket_path).await?;
        Ok(Box::new(stream) as Box<dyn AsyncReadWrite>)
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
                            ::kameo_child_process::child_process_main_with_runtime::<$actor, $msg, $callback>()
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
        let key = key.into();
        if matches!(key.as_str(), "KAMEO_REQUEST_SOCKET" | "KAMEO_CALLBACK_SOCKET" | "KAMEO_CHILD_ACTOR") {
            tracing::warn!(event = "env_var_blocked", key = %key, "Attempted to set protocol-critical env var in with_env_var; ignored");
        } else {
            self.config.env_vars.push((key, value.into()));
        }
        self
    }

    pub fn with_env_vars(mut self, vars: Vec<(String, String)>) -> Self {
        for (k, v) in vars {
            if matches!(k.as_str(), "KAMEO_REQUEST_SOCKET" | "KAMEO_CALLBACK_SOCKET" | "KAMEO_CHILD_ACTOR") {
                tracing::warn!(event = "env_var_blocked", key = %k, "Attempted to set protocol-critical env var in with_env_vars; ignored");
            } else {
                self.config.env_vars.push((k, v));
            }
        }
        self
    }

    /// Spawn the child process actor and return a reference to it
    ///
    /// This is the ONLY supported way to spawn a child process. It sets all protocol-critical env vars (KAMEO_REQUEST_SOCKET, KAMEO_CALLBACK_SOCKET, KAMEO_CHILD_ACTOR) and must be used by all wrappers and consumers.
    pub async fn spawn<H>(self, handler: H) -> io::Result<(ActorRef<SubprocessActor<M>>, CallbackReceiver<C, H>)>
    where
        H: CallbackHandler<C>,
    {
        let request_socket_path = handshake::unique_socket_path(&format!("{}-req", self.config.name));
        let callback_socket_path = handshake::unique_socket_path(&format!("{}-cb", self.config.name));

        tracing::debug!(event = "handshake_setup", request_socket = %request_socket_path.to_string_lossy(), callback_socket = %callback_socket_path.to_string_lossy(), "Parent binding sockets and setting env vars");
        // Set up the Unix domain sockets
        let request_endpoint = Endpoint::new(request_socket_path.to_string_lossy().to_string());
        let mut request_incoming = request_endpoint.incoming()?;
        tracing::debug!(event = "handshake_setup", which = "request", socket = %request_socket_path.to_string_lossy(), "Parent bound request socket");
        let callback_endpoint = Endpoint::new(callback_socket_path.to_string_lossy().to_string());
        let mut callback_incoming = callback_endpoint.incoming()?;
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
        cmd.env("KAMEO_REQUEST_SOCKET", request_socket_path.to_string_lossy().as_ref());
        env_snapshot.push(("KAMEO_REQUEST_SOCKET".to_string(), request_socket_path.to_string_lossy().to_string()));
        cmd.env("KAMEO_CALLBACK_SOCKET", callback_socket_path.to_string_lossy().as_ref());
        env_snapshot.push(("KAMEO_CALLBACK_SOCKET".to_string(), callback_socket_path.to_string_lossy().to_string()));
        tracing::debug!(event = "handshake_setup", child_env_request = %request_socket_path.to_string_lossy(), child_env_callback = %callback_socket_path.to_string_lossy(), "Parent set child env vars (final override)");
        tracing::debug!(event = "child_env_snapshot", env = ?env_snapshot, "Child process environment snapshot before spawn");
        // Inherit RUST_LOG for consistent logging
        if let Ok(rust_log) = std::env::var("RUST_LOG") {
            cmd.env("RUST_LOG", rust_log);
        }

        // Spawn the child process
        cmd.stdout(Stdio::piped());
        cmd.stderr(Stdio::piped());
        let mut child = cmd.spawn()?;

        // Capture and forward child stdout
        if let Some(stdout) = child.stdout.take() {
            let mut reader = BufReader::new(stdout).lines();
            tokio::spawn(async move {
                while let Ok(Some(line)) = reader.next_line().await {
                    tracing::info!(target: "child_stdout", "[PYTHON STDOUT] {}", line);
                }
            });
        }
        // Capture and forward child stderr
        if let Some(stderr) = child.stderr.take() {
            let mut reader = BufReader::new(stderr).lines();
            tokio::spawn(async move {
                while let Ok(Some(line)) = reader.next_line().await {
                    tracing::error!(target: "child_stderr", "[PYTHON STDERR] {}", line);
                }
            });
        }

        tracing::info!(event = "lifecycle", status = "spawned", child_pid = child.id(), "Child process spawned");

        // Strictly ordered handshake: accept request, then callback
        tracing::debug!(event = "handshake_accept", which = "request", socket = %request_socket_path.to_string_lossy(), "Parent waiting for request connection");
        let mut request_conn = match tokio::time::timeout(Duration::from_secs(3), request_incoming.next()).await {
            Ok(Some(Ok(conn))) => {
                tracing::info!(event = "handshake_accept",  which = "request", socket = %request_socket_path.to_string_lossy(), "Parent accepted request connection");
                conn
            },
            Ok(Some(Err(e))) => {
                tracing::error!(event = "handshake_accept", which = "request", socket = %request_socket_path.to_string_lossy(), error = ?e, "Parent saw error accepting request connection");
                return Err(e);
            },
            Ok(None) => {
                tracing::error!(event = "handshake_accept",  which = "request", socket = %request_socket_path.to_string_lossy(), "Parent saw end of request incoming stream");
                return Err(io::Error::new(io::ErrorKind::Other, "No request connection received"));
            },
            Err(_) => {
                tracing::error!(event = "handshake_accept", which = "request", socket = %request_socket_path.to_string_lossy(), "Timeout waiting for request connection");
                let _ = child.kill().await;
                let _ = tokio::fs::remove_file(&request_socket_path).await;
                let _ = tokio::fs::remove_file(&callback_socket_path).await;
                return Err(io::Error::new(io::ErrorKind::TimedOut, "Timeout waiting for request connection"));
            }
        };
        // Send bincode-encoded Control::Handshake to child
        tracing::debug!(event = "handshake_protocol", "Parent sending Control::Handshake to child");
        let handshake_msg = Control::<()>::Handshake;
        let handshake_bytes = bincode::encode_to_vec(&handshake_msg, bincode::config::standard())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Failed to encode handshake: {e}")))?;
        request_conn.write_all(&handshake_bytes).await?;
        tracing::debug!(event = "handshake_protocol",  "Parent sent Control::Handshake to child");
        // After accepting request connection, proceed directly
        tracing::debug!(event = "handshake_accept", which = "callback", socket = %callback_socket_path.to_string_lossy(), "Parent waiting for callback connection");
        let callback_conn = match tokio::time::timeout(Duration::from_secs(10), callback_incoming.next()).await {
            Ok(Some(Ok(conn))) => {
                tracing::info!(event = "handshake_accept", which = "callback", socket = %callback_socket_path.to_string_lossy(), "Parent accepted callback connection");
                conn
            },
            Ok(Some(Err(e))) => {
                tracing::error!(event = "handshake_accept",  which = "callback", socket = %callback_socket_path.to_string_lossy(), error = ?e, "Parent saw error accepting callback connection");
                return Err(e);
            },
            Ok(None) => {
                tracing::error!(event = "handshake_accept", which = "callback", socket = %callback_socket_path.to_string_lossy(), "Parent saw end of callback incoming stream");
                return Err(io::Error::new(io::ErrorKind::Other, "No callback connection received"));
            },
            Err(_) => {
                tracing::error!(event = "handshake_accept", which = "callback", socket = %callback_socket_path.to_string_lossy(), "Timeout waiting for callback connection");
                let _ = child.kill().await;
                let _ = tokio::fs::remove_file(&request_socket_path).await;
                let _ = tokio::fs::remove_file(&callback_socket_path).await;
                return Err(io::Error::new(io::ErrorKind::TimedOut, "Timeout waiting for callback connection"));
            }
        };
        // After accepting callback connection, proceed directly
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
                    let resp_bytes = match bincode::encode_to_vec(&resp, bincode::config::standard()) {
                        Ok(bytes) => bytes,
                        Err(e) => {
                            error!(event = "handshake", error = ?e, "Failed to encode handshake response");
                            return Err(io::Error::new(io::ErrorKind::Other, format!("Failed to encode handshake response: {e}")));
                        }
                    };
                    conn.write_all(&resp_bytes).await?;
                    continue;
                }
                Control::Real(msg, trace_context) => (msg, trace_context),
            }
        };

        let parent_cx =
            global::get_text_map_propagator(|propagator| propagator.extract(&trace_context.0));
        let span = tracing::info_span!("child_message_handler", event = "message", handler = "child");
        span.set_parent(parent_cx);

        let reply_fut = async {
            trace!(event = "message", status = "handling", handler = "child");
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
        let reply_bytes = match bincode::encode_to_vec(&Control::Real(reply_to_send, TracingContext::default()), bincode::config::standard()) {
            Ok(bytes) => bytes,
            Err(e) => {
                error!(event = "message", error = ?e, handler = "child", message = "Failed to encode reply");
                break;
            }
        };
        if let Err(e) = conn.write_all(&reply_bytes).await {
            error!(event = "message", error = ?e, handler = "child", message = "Failed to send reply to parent");
            break;
        }
        trace!(
            event = "message",
            status = "complete",
            handler = "child"
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
                        || {
                            let config = $child_init;
                            let runtime = match config.flavor {
                                $crate::RuntimeFlavor::CurrentThread => tokio::runtime::Builder::new_current_thread()
                                    .enable_all()
                                    .build()
                                    .expect("Failed to build runtime"),
                                $crate::RuntimeFlavor::MultiThread => {
                                    let mut b = tokio::runtime::Builder::new_multi_thread();
                                    b.enable_all();
                                    if let Some(threads) = config.worker_threads {
                                        b.worker_threads(threads);
                                    }
                                    b.build().expect("Failed to build runtime")
                                }
                            };
                            let _guard = runtime.enter();
                            $crate::child_process_main_with_runtime::<$actor, $msg, $callback>()
                        },
                    ),
                )*
            ];
            if let Ok(actor_name) = std::env::var("KAMEO_CHILD_ACTOR") {
                for (name, handler) in handlers {
                    if actor_name == *name {
                        return handler();
                    }
                }
                Err(Box::new($crate::handle_unknown_actor_error(&actor_name)))
            } else {
                // Parent process branch - call user-provided initialization
                $parent_init
            }
        }
    };
}

/// Prelude module for commonly used items
pub mod prelude {
    // pub use crate::child_process_main; // REMOVED: no longer exists
    pub use crate::child_process_main_with_runtime;
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

/// Run a child process actor with a provided runtime. The runtime must be built and entered by the caller before calling this function.
#[instrument(fields(pid=std::process::id()))]
pub async fn child_process_main_with_runtime<A, M, C>() -> Result<(), Box<dyn std::error::Error>>
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
    std::panic::set_hook(Box::new(|panic_info| {
        let msg = if let Some(s) = panic_info.payload().downcast_ref::<&str>() {
            *s
        } else if let Some(s) = panic_info.payload().downcast_ref::<String>() {
            s.as_str()
        } else {
            "<unknown panic payload>"
        };
        let location = panic_info.location().map(|l| l.to_string()).unwrap_or_else(|| "<unknown location>".to_string());
        tracing::error!(event = "panic", message = %msg, location = %location, "Child process panicked!");
    }));

    let actor = A::default();
    tracing::debug!(event = "start_handshake");
    tracing::info!(event = "handshake_child", which = "request", "Child about to connect request socket");
    let request_conn = handshake::child_request().await;
    match &request_conn {
        Ok(_) => tracing::info!(event = "handshake_child", which = "request", "Child finished connecting request socket"),
        Err(e) => tracing::error!(event = "handshake_child", which = "request", error = ?e, "Child failed to connect request socket"),
    }
    let request_conn = match request_conn {
        Ok(conn) => conn,
        Err(e) => return Err(Box::new(e) as Box<dyn std::error::Error>),
    };
    tracing::debug!(event = "handshake_child", which = "request", "Child ALIVE after request connect, about to connect callback socket");
    let callback_conn = handshake::child_callback().await;
    match &callback_conn {
        Ok(_) => tracing::info!(event = "handshake_child", which = "callback", "Child finished connecting callback socket"),
        Err(e) => tracing::error!(event = "handshake_child", which = "callback", error = ?e, "Child failed to connect callback socket"),
    }
    let callback_conn = match callback_conn {
        Ok(conn) => conn,
        Err(e) => {
            tracing::error!(event = "handshake_child", which = "callback", error = ?e, "Child returning early after callback handshake failure");
            return Err(Box::new(e) as Box<dyn std::error::Error>);
        }
    };
    tracing::info!(event = "handshake_child", which = "callback", "Child about to set callback handle");
    let handle = CallbackHandle::new(callback_conn);
    let mut actor = actor;
    actor.set_callback_handle(handle);
    tracing::info!(event = "handshake_child", which = "callback", "Child finished set_callback_handle, about to init_with_runtime");
    let mut actor = actor.init_with_runtime().await
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;
    tracing::info!(event = "handshake_child", which = "callback", "Child finished init_with_runtime, about to run_child_actor_loop");
    let result = run_child_actor_loop(&mut actor, request_conn).await;
    if let Err(e) = result {
        tracing::warn!(event = "child_exit", error = ?e, "Child actor loop exited with error");
        return Err(format!("{e}").into());
    } else {
        tracing::info!(event = "child_exit", "Child actor loop exited cleanly");
        Ok(())
    }
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


