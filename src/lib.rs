use serde::{Serialize, Deserialize};
use std::{env, io};
use std::process::Command;
use tracing::{trace, debug, info, error, warn, Span, instrument};
use kameo::actor::{Actor, ActorRef, WeakActorRef, ActorID};
use kameo::message::{Context, Message, BoxMessage};
use kameo::reply::{BoxReplySender, ReplyError};
use kameo::error::{PanicError, ActorStopReason};
use async_trait::async_trait;
use parity_tokio_ipc::{Endpoint, Connection};
use tokio::io::{AsyncReadExt, AsyncWriteExt, AsyncRead, AsyncWrite};
use std::path::PathBuf;
use uuid::Uuid;
use bincode::{Encode, Decode};
use std::ops::ControlFlow;
use std::marker::PhantomData;
use serde::de::DeserializeOwned;
use std::sync::Arc;
use tokio::sync::Mutex;
use thiserror::Error;

/// Trait object for async read/write
pub trait AsyncReadWrite: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> AsyncReadWrite for T {}

/// Trait for messages that can be sent to a Kameo child process actor.
pub trait KameoChildProcessMessage: Send + Serialize + DeserializeOwned + Encode + Decode<()> + 'static {}

impl<T> KameoChildProcessMessage for T 
where 
    T: Send + Serialize + DeserializeOwned + Encode + Decode<()> + 'static 
{}

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

/// Trait for subprocess message handlers.
#[async_trait]
pub trait Handler<M> 
where
    M: Encode + Decode<()>
{
    type Output: Serialize + for<'de> Deserialize<'de> + Send + Sync + Encode + Decode<()> + 'static;
    async fn handle(&mut self, msg: M) -> Self::Output;
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
pub struct SubprocessActor<M> {
    connection: Arc<Mutex<Box<dyn AsyncReadWrite>>>,
    child: Option<tokio::process::Child>,
    socket_path: PathBuf,
    _phantom: PhantomData<M>,
}

#[async_trait]
impl<M> Actor for SubprocessActor<M> 
where 
    M: Message<M> + KameoChildProcessMessage + Send + 'static,
    M::Reply: Serialize + for<'de> Deserialize<'de> + Send + Encode + Decode<()> + 'static,
    Self: Send + 'static,
{
    type Error = SubprocessActorError;

    #[instrument(skip(self, actor_ref), fields(actor_type = "SubprocessActor"))]
    fn on_start(&mut self, actor_ref: ActorRef<Self>) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send {
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

    #[instrument(skip(self, actor_ref, reason), fields(actor_type = "SubprocessActor"))]
    fn on_stop(&mut self, actor_ref: WeakActorRef<Self>, reason: ActorStopReason) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send {
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

    #[instrument(skip(self, actor_ref, err), fields(actor_type = "SubprocessActor"))]
    fn on_panic(&mut self, actor_ref: WeakActorRef<Self>, err: PanicError) -> impl std::future::Future<Output = Result<ControlFlow<ActorStopReason>, Self::Error>> + Send {
        error!(event = "lifecycle", status = "panicked", error = ?err, "Subprocess actor panicked");
        async move { Ok(ControlFlow::Break(ActorStopReason::Panicked(err))) }
    }
}

impl<M> Message<M> for SubprocessActor<M>
where
    M: Message<M> + KameoChildProcessMessage + Send + 'static,
    M::Reply: Serialize + for<'de> Deserialize<'de> + Send + Encode + Decode<()> + 'static,
{
    type Reply = M::Reply;

    #[instrument(skip(self, msg, ctx), fields(actor_type = "SubprocessActor", message_type = std::any::type_name::<M>()))]
    async fn handle(&mut self, msg: M, ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        debug!(event = "message", status = "handling", "Handling message");

        let msg_bytes = match bincode::encode_to_vec(&Control::Real(msg), bincode::config::standard()) {
            Ok(bytes) => bytes,
            Err(e) => {
                error!(event = "message", error = ?e, "Failed to serialize message");
                panic!("Failed to serialize message: {}", e);
            }
        };

        let mut conn = self.connection.lock().await;
        if let Err(e) = conn.write_all(&msg_bytes).await {
            error!(event = "message", error = ?e, "Failed to send message");
            panic!("Failed to send message: {}", e);
        }

        let mut resp_buf = vec![0u8; 4096];
        let n = match conn.read(&mut resp_buf).await {
            Ok(0) => {
                error!(event = "message", "Connection closed while waiting for response");
                panic!("Connection closed while waiting for response");
            }
            Ok(n) => n,
            Err(e) => {
                error!(event = "message", error = ?e, "Failed to read response");
                panic!("Failed to read response: {}", e);
            }
        };

        let (ctrl, _): (Control<M::Reply>, _) = match bincode::decode_from_slice(&resp_buf[..n], bincode::config::standard()) {
            Ok(result) => result,
            Err(e) => {
                error!(event = "message", error = ?e, "Failed to deserialize response");
                panic!("Failed to deserialize response: {}", e);
            }
        };

        match ctrl {
            Control::Real(reply) => {
                debug!(event = "message", status = "completed", "Message handled successfully");
                reply
            }
            Control::Handshake => {
                error!(event = "message", "Received unexpected handshake response");
                panic!("Received unexpected handshake response");
            }
        }
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
}

pub mod handshake {
    use super::*;
    use tokio::process::Command;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use parity_tokio_ipc::{Endpoint, Connection};
    use futures::StreamExt;
    use tokio::io::{AsyncRead, AsyncWrite};

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

/// Macro to register actors and generate subprocess entrypoint
#[macro_export]
macro_rules! register_subprocess_actors_async {
    ($(($actor:ty, $msg:ty)),* $(,)?) => {
        pub async fn maybe_run_subprocess_registry_and_exit_async() -> bool {
            if let Ok(actor_name) = std::env::var("KAMEO_CHILD_ACTOR") {
                let pid = ::std::process::id();
                let span = ::tracing::span!(::tracing::Level::INFO, "process", process_role = "child", pid);
                let _enter = span.enter();
                ::tracing::info!(event = "lifecycle", status = "starting", actor = actor_name, "Subprocess handler starting");
                match actor_name.as_str() {
                    $(
                        stringify!($actor) => {
                            $crate::run_child_actor_async::<$actor, $msg>().await;
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
            false
        }
    };
}

/// Async actor loop for a subprocess actor
#[instrument(skip_all, fields(actor_type = std::any::type_name::<A>()))]
pub async fn run_child_actor_async<A, M>()
where
    A: Default + Send + Sync + 'static + Handler<M>,
    M: Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static + Encode + Decode<()>,
    A::Output: Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
{
    debug!(event = "lifecycle", status = "connecting", "Establishing child connection");
    let mut conn = handshake::child().await.expect("child connection failed");
    let mut actor = A::default();

    debug!(event = "lifecycle", status = "running", "Child actor loop started");
    let mut buf = vec![0u8; 4096];
    
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

        let ctrl: Control<M> = match bincode::decode_from_slice(&buf[..n], bincode::config::standard()) {
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
                if let Err(e) = conn.write_all(&resp_bytes).await {
                    error!(event = "handshake", error = ?e, "Failed to send handshake response");
                    break;
                }
            }
            Control::Real(msg) => {
                debug!(event = "message", status = "handling", "Handling message");
                let reply = actor.handle(msg).await;
                let reply_bytes = bincode::encode_to_vec(&Control::Real(reply), bincode::config::standard())
                    .expect("Failed to encode reply");
                if let Err(e) = conn.write_all(&reply_bytes).await {
                    error!(event = "message", error = ?e, "Failed to send reply");
                    break;
                }
                debug!(event = "message", status = "complete", "Message handled successfully");
            }
        }
    }
}
