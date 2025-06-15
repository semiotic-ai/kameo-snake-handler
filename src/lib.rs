#![forbid(unsafe_code)]

use serde::{Serialize, Deserialize};
use std::io;
use tracing::{debug, error, warn, info, instrument, Level};
use kameo::actor::{Actor, ActorRef, WeakActorRef};
use kameo::prelude::*;
use kameo::error::{PanicError, ActorStopReason};
use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncWrite, AsyncReadExt, AsyncWriteExt};
use std::path::PathBuf;
use uuid::Uuid;
use bincode::{Encode, Decode};
use std::ops::ControlFlow;
use std::marker::PhantomData;
use serde::de::DeserializeOwned;
use std::sync::Arc;
use tokio::sync::Mutex;
use thiserror::Error;
use tracing_subscriber;
use nix::unistd::{fork, ForkResult};
use tokio::runtime::Runtime;
use futures::StreamExt;
use std::pin::Pin;
use std::future::Future;
use ipc_channel::ipc::{IpcSender, IpcReceiver, channel};

/// Trait object for async read/write operations
#[async_trait]
pub trait AsyncReadWrite: Send + Unpin {
    async fn read(&mut self, buf: &mut [u8]) -> io::Result<usize>;
    async fn write_all(&mut self, buf: &[u8]) -> io::Result<()>;
    async fn flush(&mut self) -> io::Result<()>;
    async fn shutdown(&mut self) -> io::Result<()>;
}

impl std::fmt::Debug for dyn AsyncReadWrite {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "AsyncReadWrite")
    }
}

#[async_trait]
impl<T: AsyncRead + AsyncWrite + Send + Unpin> AsyncReadWrite for T {
    async fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        AsyncReadExt::read(self, buf).await
    }

    async fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        AsyncWriteExt::write_all(self, buf).await
    }

    async fn flush(&mut self) -> io::Result<()> {
        AsyncWriteExt::flush(self).await
    }

    async fn shutdown(&mut self) -> io::Result<()> {
        AsyncWriteExt::shutdown(self).await
    }
}

// Helper methods are now part of the trait itself
impl dyn AsyncReadWrite {
    // No need for helper methods here anymore since they're in the trait
}

/// Trait for messages that can be sent to a Kameo child process actor.
pub trait KameoChildProcessMessage: Send + Serialize + DeserializeOwned + Encode + Decode<()> + std::fmt::Debug + 'static {
    type Reply: Reply + Send + Serialize + DeserializeOwned + Encode + Decode<()> + std::fmt::Debug + 'static;
}

/// Control message for handshake and real messages
#[derive(Debug, Serialize, Deserialize, Encode, Decode)]
pub enum Control<T> {
    Handshake,
    Real(T),
}

impl<T> Control<T> {
    pub fn is_handshake(&self) -> bool {
        matches!(self, Control::Handshake)
    }
    pub fn into_real(self) -> Option<T> {
        match self {
            Control::Real(t) => Some(t),
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
    fn on_start(&mut self, _actor_ref: ActorRef<Self>) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send {
        debug!(event = "lifecycle", status = "starting", "Starting subprocess actor");
        
        async move {
            // Perform handshake
            let mut conn = self.connection.lock().await;
            let handshake = Control::<M>::Handshake;
            let handshake_bytes = bincode::encode_to_vec(&handshake, bincode::config::standard())?;
            
            conn.write_all(&handshake_bytes).await?;
            
            let mut resp_buf = vec![0u8; 1024];
            let n = conn.read(&mut resp_buf).await?;
            if n == 0 {
                return Err(SubprocessActorError::HandshakeFailed("Connection closed during handshake".into()));
            }
            
            let (resp, _): (Control<()>, _) = bincode::decode_from_slice(&resp_buf[..n], bincode::config::standard())?;
            if !resp.is_handshake() {
                return Err(SubprocessActorError::HandshakeFailed("Invalid handshake response".into()));
            }

            debug!(event = "lifecycle", status = "started", "Subprocess actor started successfully");
            Ok(())
        }
    }

    #[instrument(skip(self, _actor_ref, reason), fields(actor_type = "SubprocessActor"))]
    fn on_stop(&mut self, _actor_ref: WeakActorRef<Self>, reason: ActorStopReason) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send {
        debug!(event = "lifecycle", status = "stopping", ?reason, "Stopping subprocess actor");
        
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

            debug!(event = "lifecycle", status = "stopped", "Subprocess actor stopped");
            Ok(())
        }
    }

    #[instrument(skip(self, _actor_ref, err), fields(actor_type = "SubprocessActor"))]
    fn on_panic(&mut self, _actor_ref: WeakActorRef<Self>, err: PanicError) -> impl std::future::Future<Output = Result<ControlFlow<ActorStopReason>, Self::Error>> + Send {
        error!(event = "lifecycle", status = "panicked", error = ?err, "Subprocess actor panicked");
        async move { Ok(ControlFlow::Break(ActorStopReason::Panicked(err))) }
    }
}

impl<M> SubprocessActor<M> {
    pub fn new(connection: Box<dyn AsyncReadWrite>, child: tokio::process::Child, socket_path: PathBuf) -> Self {
        Self {
            connection: Arc::new(Mutex::new(connection)),
            child: Some(child),
            socket_path,
            _phantom: PhantomData,
        }
    }

    pub async fn run_subprocess<A, Msg>(actor: A, rx: IpcReceiver<Vec<u8>>, tx: IpcSender<Vec<u8>>) -> Result<(), Box<dyn std::error::Error>>
    where
        A: Message<Msg> + Clone + Send + 'static,
        Msg: KameoChildProcessMessage + Send + 'static,
        <A as Message<Msg>>::Reply: Reply + Send + Serialize + DeserializeOwned + Encode + Decode<()> + std::fmt::Debug + 'static,
        <<A as Message<Msg>>::Reply as Reply>::Error: std::fmt::Display + std::error::Error + Send + Sync + Encode + Decode<()> + 'static,
        <<A as Message<Msg>>::Reply as Reply>::Ok: Encode + Decode<()> + Send + 'static,
    {
        // First handle handshake
        let handshake_msg = rx.recv()?;
        let (control, _): (Control<Msg>, _) = bincode::decode_from_slice(&handshake_msg, bincode::config::standard())?;
        
        if !control.is_handshake() {
            return Err("Expected handshake message".into());
        }
        
        // Send handshake response
        let handshake_response = Control::<()>::Handshake;
        let response_bytes = bincode::encode_to_vec(&handshake_response, bincode::config::standard())?;
        tx.send(response_bytes)?;

        // Main message loop
        loop {
            let msg_bytes = match rx.recv() {
                Ok(bytes) => bytes,
                Err(e) => {
                    debug!(event = "subprocess", status = "connection_closed", error = ?e, "Connection closed");
                    break;
                }
            };

            let (control, _): (Control<Msg>, _) = match bincode::decode_from_slice(&msg_bytes, bincode::config::standard()) {
                Ok(decoded) => decoded,
                Err(e) => {
                    error!(event = "subprocess", error = ?e, "Failed to decode message");
                    continue;
                }
            };

            let msg = match control.into_real() {
                Some(m) => m,
                None => {
                    error!(event = "subprocess", "Received unexpected handshake message");
                    continue;
                }
            };

            // Create a new actor ref for this message
            let actor_ref = kameo::spawn(actor.clone());
            
            // Send the message using ask() and handle the result
            let reply = match actor_ref.ask(msg).await {
                Ok(r) => Ok(r),
                Err(e) => match e {
                    kameo::error::SendError::HandlerError(e) => Err(e),
                    _ => {
                        error!(event = "subprocess", error = ?e, "Failed to handle message");
                        continue;
                    }
                }
            };
            
            // Encode and send the response
            let response = Control::Real(reply);
            let response_bytes = bincode::encode_to_vec(&response, bincode::config::standard())?;
            
            if let Err(e) = tx.send(response_bytes) {
                error!(event = "subprocess", error = ?e, "Failed to send response");
                break;
            }
        }

        Ok(())
    }
}

pub mod handshake {
    use super::*;
    use tokio::process::Command;
    use parity_tokio_ipc::Endpoint;
    use futures::StreamExt;

    pub fn unique_socket_path(actor_name: &str) -> PathBuf {
        let mut path = std::path::PathBuf::from("/tmp");
        let short_name = &actor_name[0..std::cmp::min(8, actor_name.len())];
        path.push(format!("kameo-{}-{}.sock", short_name, Uuid::new_v4().simple().to_string()));
        path
    }

    #[instrument(skip(exe), fields(actor_name))]
    pub async fn host<M, R>(actor_name: &str, exe: &str) -> std::io::Result<(Box<dyn AsyncReadWrite>, tokio::process::Child, PathBuf)>
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
        
        debug!(event = "handshake", status = "spawning", "Spawning child process");
        let child = cmd.spawn()?;

        debug!(event = "handshake", status = "waiting", "Waiting for child connection");
        let conn = incoming.next().await.transpose()?.ok_or_else(|| {
            error!(event = "handshake", "No child connection received");
            io::Error::new(io::ErrorKind::Other, "No child connection")
        })?;

        debug!(event = "handshake", status = "completed", "Host handshake completed successfully");
        Ok((Box::new(conn), child, socket_path))
    }

    #[instrument(fields(actor_name = ?std::env::var("KAMEO_CHILD_ACTOR").ok()))]
    pub async fn child() -> std::io::Result<Box<dyn AsyncReadWrite>> {
        let socket_path = std::env::var("KAMEO_ACTOR_SOCKET")
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "KAMEO_ACTOR_SOCKET not set"))?;

        debug!(event = "handshake", status = "starting", socket_path = %socket_path, "Starting child handshake");
        
        let conn = tokio::net::UnixStream::connect(&socket_path).await?;
        
        debug!(event = "handshake", status = "completed", "Child handshake completed successfully");
        Ok(Box::new(conn))
    }
}

/// Macro to register subprocess actors and provide child process handling
#[macro_export]
macro_rules! kameo_main {
    // Main macro pattern that takes actor registrations
    (
        actors = { $(($actor:ty, $msg:ty)),* $(,)? },
        parent_runtime = { worker_threads = $worker_threads:expr $(,)? },
        parent_main = $parent_main:expr
        $(,)?
    ) => {
        #[::tokio::main(flavor = "multi_thread", worker_threads = $worker_threads)]
        async fn main() -> Result<(), Box<dyn std::error::Error>> {
            // Initialize tracing
            ::tracing_subscriber::fmt()
                .with_max_level(::tracing::Level::DEBUG)
                .init();

            // Check if we're a child process - if so, run the actor and exit
            if let Ok(actor_name) = std::env::var("KAMEO_CHILD_ACTOR") {
                let span = ::tracing::span!(::tracing::Level::INFO, "process", process_role = "child", pid = std::process::id());
                let _enter = span.enter();

                ::tracing::info!(
                    event = "runtime",
                    process = "child",
                    runtime_type = "single-threaded",
                    "Child process starting"
                );

                // Match on actor type and run the appropriate handler
                match actor_name.as_str() {
                    $(
                        stringify!($actor) => {
                            let mut actor = <$actor>::default();
                            let conn = $crate::handshake::child().await.expect("Failed to establish child connection");
                            $crate::run_child_actor_loop(&mut actor, conn).await.expect("Actor loop failed");
                            ::tracing::info!(event = "lifecycle", status = "complete", actor = actor_name, "Handler complete, exiting");
                            std::process::exit(0);
                        }
                    )*
                    _ => {
                        ::tracing::error!(event = "lifecycle", status = "error", actor = actor_name, "Unknown actor type");
                        std::process::exit(1);
                    }
                }
            }

            // Parent process main logic
            ::tracing::info!(
                event = "runtime",
                process = "parent",
                runtime_type = "multi-threaded",
                worker_threads = $worker_threads,
                "Parent process starting"
            );

            // Run the parent main function
            $parent_main.await?;

            Ok(())
        }
    };
}

/// Configuration for a child process actor
pub struct ChildProcessConfig {
    pub name: String,
    pub log_level: Level,
}

/// A builder for creating child process actors
pub struct ChildProcessBuilder<A, M> 
where 
    A: Default + Message<M> + Send + Sync + 'static,
    M: KameoChildProcessMessage + Send + 'static,
    <A as Message<M>>::Reply: Serialize + for<'de> Deserialize<'de> + Send + Encode + Decode<()> + 'static,
{
    config: ChildProcessConfig,
    _phantom: PhantomData<(A, M)>,
}

impl<A, M> ChildProcessBuilder<A, M>
where 
    A: Default + Message<M> + Send + Sync + 'static,
    M: KameoChildProcessMessage + Send + 'static,
    <A as Message<M>>::Reply: Serialize + for<'de> Deserialize<'de> + Send + Encode + Decode<()> + 'static,
{
    pub fn new() -> Self {
        // Get the type name at compile time
        let type_name = std::any::type_name::<A>();
        // Extract just the struct name (last part after ::)
        let actor_name = type_name.split("::").last().unwrap_or(type_name);
        
        Self {
            config: ChildProcessConfig {
                name: actor_name.to_string(),
                log_level: Level::DEBUG,
            },
            _phantom: PhantomData,
        }
    }

    pub fn log_level(mut self, level: Level) -> Self {
        self.config.log_level = level;
        self
    }

    /// Spawn the child process actor and return a reference to it
    pub async fn spawn(self) -> io::Result<ActorRef<SubprocessActor<M>>> {
        let socket_path = handshake::unique_socket_path(&self.config.name);
        
        // Set up the Unix domain socket
        let endpoint = parity_tokio_ipc::Endpoint::new(socket_path.to_string_lossy().to_string());
        let mut incoming = endpoint.incoming()?;

        // Get current executable path
        let current_exe = std::env::current_exe()?;

        // Spawn child process with clean environment
        let mut cmd = tokio::process::Command::new(current_exe);
        cmd.env("KAMEO_CHILD_ACTOR", &self.config.name);
        cmd.env("KAMEO_ACTOR_SOCKET", socket_path.to_string_lossy().as_ref());
        
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
        let child = cmd.spawn()?;

        debug!(
            event = "lifecycle",
            status = "spawned",
            child_pid = child.id(),
            "Child process spawned"
        );

        // Wait for child to connect
        let conn = incoming.next().await.transpose()?.ok_or_else(|| {
            io::Error::new(io::ErrorKind::Other, "No child connection received")
        })?;

        let actor = SubprocessActor::new(
            Box::new(conn),
            child,
            socket_path,
        );

        Ok(kameo::spawn(actor))
    }
}

/// Run the actor loop in the child process
#[instrument(skip(actor, conn))]
pub async fn run_child_actor_loop<A, Msg>(actor: &mut A, mut conn: Box<dyn AsyncReadWrite>) -> io::Result<()>
where
    A: Message<Msg> + Clone + Send + 'static,
    Msg: KameoChildProcessMessage + Send + 'static,
    <A as Message<Msg>>::Reply: Reply + Send + Serialize + DeserializeOwned + Encode + Decode<()> + std::fmt::Debug + 'static,
    <<A as Message<Msg>>::Reply as Reply>::Error: std::fmt::Display + std::error::Error + Send + Sync + Encode + Decode<()> + 'static,
    <<A as Message<Msg>>::Reply as Reply>::Ok: Encode + Decode<()> + Send + 'static,
{
    debug!(event = "lifecycle", status = "running", "Child actor loop started");

    let mut buf = vec![0u8; 4096];
    let actor_ref = kameo::spawn(actor.clone());
    
    loop {
        let n = match conn.read(&mut buf).await {
            Ok(0) => {
                debug!(event = "lifecycle", status = "shutdown", "Parent connection closed");
                break;
            }
            Ok(n) => n,
            Err(e) => {
                error!(event = "lifecycle", error = ?e, "Read error");
                break;
            }
        };

        let ctrl: Control<Msg> = match bincode::decode_from_slice(&buf[..n], bincode::config::standard()) {
            Ok((ctrl, _)) => ctrl,
            Err(e) => {
                error!(event = "message", error = ?e, "Failed to decode message");
                continue;
            }
        };

        match ctrl {
            Control::Handshake => {
                debug!(event = "handshake", status = "responding", "Responding to handshake");
                let resp = Control::<()>::Handshake;
                let resp_bytes = bincode::encode_to_vec(&resp, bincode::config::standard())
                    .expect("Failed to encode handshake response");
                conn.write_all(&resp_bytes).await?;
            }
            Control::Real(msg) => {
                debug!(event = "message", status = "handling", "Handling message");
                let reply = match actor_ref.ask(msg).await {
                    Ok(reply) => reply,
                    Err(e) => {
                        error!(event = "error", error = ?e, "Failed to handle message");
                        return Err(io::Error::new(io::ErrorKind::Other, format!("Failed to handle message: {:?}", e)));
                    }
                };
                let reply_bytes = bincode::encode_to_vec(&Control::Real(reply), bincode::config::standard())
                    .expect("Failed to encode reply");
                conn.write_all(&reply_bytes).await?;
                debug!(event = "message", status = "complete", "Message handled successfully");
            }
        }
    }

    Ok(())
}

/// Prelude module for commonly used items
pub mod prelude {
    pub use crate::kameo_main;
    pub use crate::ChildProcessBuilder;
    pub use crate::SubprocessActor;
    pub use crate::handshake;
}

#[async_trait]
impl<M> Message<M> for SubprocessActor<M>
where
    M: KameoChildProcessMessage + Send + 'static,
{
    type Reply = Result<<M as KameoChildProcessMessage>::Reply, SubprocessActorError>;

    #[instrument(skip(self, ctx), fields(actor_type = "SubprocessActor"))]
    fn handle(&mut self, msg: M, ctx: &mut Context<Self, Self::Reply>) -> impl Future<Output = Self::Reply> + Send {
        async move {
            let mut conn = self.connection.lock().await;
            
            // Encode and send the message
            let control = Control::Real(msg);
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
            let (control, _): (Control<<M as KameoChildProcessMessage>::Reply>, _) = 
                bincode::decode_from_slice(&resp_buf[..n], bincode::config::standard())?;
                
            match control {
                Control::Handshake => Err(SubprocessActorError::Protocol("Unexpected handshake response".into())),
                Control::Real(reply) => Ok(reply),
            }
        }
    }
}
