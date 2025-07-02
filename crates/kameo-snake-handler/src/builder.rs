use kameo_child_process::error::PythonExecutionError;
use kameo_child_process::callback::{ChildCallbackMessage, CallbackHandler, NoopCallbackHandler};
use kameo_child_process::KameoChildProcessMessage;
use std::marker::PhantomData;
use tracing::Level;
use tracing_futures::Instrument;
use async_trait::async_trait;
use tokio::sync::{mpsc, oneshot, Mutex as TokioMutex, Notify};

/// Builder for a Python child process
/// NOTE: For PythonActor, use the macro-based entrypoint (setup_python_subprocess_system!). This builder is not supported for PythonActor.
/// NOTE: SubprocessParentActor is only valid as an in-process actor with DelegatedReply. If used as a protocol boundary, use the serializable reply type and the original actor.

// --- Actor Pool for Python Child Process ---
pub struct PythonChildProcessActorPool<M>
where
    M: kameo_child_process::KameoChildProcessMessage + Send + Sync + 'static,
    <M as kameo_child_process::KameoChildProcessMessage>::Reply: serde::Serialize
        + for<'de> serde::Deserialize<'de>
        + bincode::Encode
        + bincode::Decode<()> 
        + std::fmt::Debug
        + Send
        + Sync
        + 'static,
{
    actors: Vec<kameo::actor::ActorRef<kameo_child_process::SubprocessIpcActor<M>>>,
    next: std::sync::atomic::AtomicUsize,
    child: Option<tokio::process::Child>,
    write_tx: Option<tokio::sync::mpsc::Sender<kameo_child_process::WriteRequest>>,
}

impl<M> PythonChildProcessActorPool<M>
where
    M: kameo_child_process::KameoChildProcessMessage + Send + Sync + 'static,
    <M as kameo_child_process::KameoChildProcessMessage>::Reply: serde::Serialize
        + for<'de> serde::Deserialize<'de>
        + bincode::Encode
        + bincode::Decode<()> 
        + std::fmt::Debug
        + Send
        + Sync
        + 'static,
{
    pub fn get_actor(&self) -> kameo::actor::ActorRef<kameo_child_process::SubprocessIpcActor<M>> {
        let idx = self.next.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % self.actors.len();
        self.actors[idx].clone()
    }
    pub fn all(&self) -> &[kameo::actor::ActorRef<kameo_child_process::SubprocessIpcActor<M>>] {
        &self.actors
    }
    pub async fn shutdown(mut self) {
        if let Some(write_tx) = self.write_tx.take() {
            drop(write_tx); // Close the channel to signal writer task
        }
        if let Some(mut child) = self.child.take() {
            let _ = child.kill().await;
            let _ = child.wait().await;
        }
    }
}

pub struct PythonChildProcessBuilder<C, H = NoopCallbackHandler<C>>
where
    C: ChildCallbackMessage + Sync + Clone,
    H: CallbackHandler<C> + Clone + Send + Sync + 'static,
{
    python_config: crate::PythonConfig,
    log_level: Level,
    callback_handler: H,
    _phantom: std::marker::PhantomData<C>,
}

impl<C> PythonChildProcessBuilder<C, NoopCallbackHandler<C>>
where
    C: ChildCallbackMessage + Sync + Clone,
{
    /// Creates a new builder with the given Python configuration.
    #[tracing::instrument]
    pub fn new(python_config: crate::PythonConfig) -> Self {
        let mut python_config = python_config.clone();
        // Always set PYTHONPATH from python_path
        let joined_path = python_config.python_path.join(":");
        // Only add if not already present in env_vars
        if !python_config
            .env_vars
            .iter()
            .any(|(k, _)| k == "PYTHONPATH")
        {
            python_config
                .env_vars
                .push(("PYTHONPATH".to_string(), joined_path));
        }
        Self {
            python_config,
            log_level: Level::INFO,
            callback_handler: NoopCallbackHandler::<C>::default(),
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<C, H> PythonChildProcessBuilder<C, H>
where
    C: ChildCallbackMessage + Sync + Clone,
    H: CallbackHandler<C> + Clone + Send + Sync + 'static,
{
    /// Sets the log level for the child process.
    pub fn log_level(mut self, level: Level) -> Self {
        self.log_level = level;
        self
    }

    /// Inject a custom callback handler for callback IPC.
    pub fn with_callback_handler<NH>(self, handler: NH) -> PythonChildProcessBuilder<C, NH>
    where
        NH: CallbackHandler<C> + Clone + Send + Sync + 'static,
    {
        PythonChildProcessBuilder {
            python_config: self.python_config,
            log_level: self.log_level,
            callback_handler: handler,
            _phantom: std::marker::PhantomData,
        }
    }

    pub async fn spawn_pool<M>(
        self,
        pool_size: usize,
    ) -> std::io::Result<PythonChildProcessActorPool<M>>
    where
        M: KameoChildProcessMessage + Send + Sync + 'static,
        <M as KameoChildProcessMessage>::Reply: serde::Serialize
            + for<'de> serde::Deserialize<'de>
            + bincode::Encode
            + bincode::Decode<()> 
            + std::fmt::Debug
            + Send
            + Sync
            + 'static,
    {
        use kameo_child_process::{SubprocessIpcBackend, spawn_subprocess_ipc_actor, CallbackReceiver, WriteRequest, Control, PythonExecutionError};
        use tokio::net::UnixListener;
        use std::sync::Arc;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use std::time::Duration;
        use std::collections::HashMap;
        // Serialize the PythonConfig as JSON for the child
        let config_json = serde_json::to_string(&self.python_config).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Failed to serialize PythonConfig: {e}"),
            )
        })?;
        // Set up the Unix domain sockets
        let actor_name = std::any::type_name::<crate::PythonActor<M, C, PythonExecutionError>>();
        let request_socket_path = kameo_child_process::handshake::unique_socket_path(&format!("{}-req", actor_name));
        let callback_socket_path = kameo_child_process::handshake::unique_socket_path(&format!("{}-cb", actor_name));
        let request_endpoint = request_socket_path.to_string_lossy().to_string();
        let callback_endpoint = callback_socket_path.to_string_lossy().to_string();
        let request_incoming = UnixListener::bind(&request_endpoint)?;
        let callback_incoming = UnixListener::bind(&callback_endpoint)?;
        // Spawn child process
        let current_exe = std::env::current_exe()?;
        let mut cmd = tokio::process::Command::new(current_exe);
        cmd.envs(std::env::vars());
        cmd.env_remove("PYTHONPATH");
        for (key, value) in self.python_config.env_vars.iter() {
            cmd.env(key, value);
        }
        cmd.env("KAMEO_CHILD_ACTOR", actor_name);
        cmd.env("KAMEO_REQUEST_SOCKET", request_socket_path.to_string_lossy().as_ref());
        cmd.env("KAMEO_CALLBACK_SOCKET", callback_socket_path.to_string_lossy().as_ref());
        cmd.env("KAMEO_PYTHON_CONFIG", config_json);
        if let Ok(rust_log) = std::env::var("RUST_LOG") {
            cmd.env("RUST_LOG", rust_log);
        }
        cmd.stdout(std::process::Stdio::inherit());
        cmd.stderr(std::process::Stdio::inherit());
        let mut child = cmd.spawn()?;
        // Accept request connection and perform handshake
        let (mut request_conn, _addr) = tokio::time::timeout(
            Duration::from_secs(30),
            request_incoming.accept(),
        ).await??;
        kameo_child_process::perform_handshake::<M>(&mut request_conn, true).await.map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::Other, format!("Handshake failed: {e:?}"))
        })?;
        // Accept callback connection
        let (callback_conn, _addr) = tokio::time::timeout(
            Duration::from_secs(30),
            callback_incoming.accept(),
        ).await??;
        // Backend and callback receiver setup (copied from backend builder)
        let (read_half, write_half) = request_conn.into_split();
        let (write_tx, mut write_rx) = mpsc::channel::<WriteRequest>(128);
        let in_flight: Arc<TokioMutex<HashMap<u64, Arc<(TokioMutex<Option<Result<<M as KameoChildProcessMessage>::Reply, PythonExecutionError>>>, Notify)>>>> = Arc::new(TokioMutex::new(HashMap::new()));
        let in_flight_writer = in_flight.clone();
        let in_flight_reader = in_flight.clone();
        // Writer task
        tokio::spawn(async move {
            let mut write_conn = write_half;
            while let Some(req) = write_rx.recv().await {
                let len = (req.bytes.len() as u32).to_le_bytes();
                if let Err(e) = write_conn.write_all(&len).await {
                    let mut in_flight = in_flight_writer.lock().await;
                    if let Some(slot) = in_flight.remove(&req.correlation_id) {
                        let (reply_mutex, notify) = &*slot;
                        let mut guard = reply_mutex.lock().await;
                        *guard = Some(Err(PythonExecutionError::ExecutionError { message: format!("Failed to write length prefix: {e}") }));
                        notify.notify_waiters();
                    }
                    break;
                }
                if let Err(e) = write_conn.write_all(&req.bytes).await {
                    let mut in_flight = in_flight_writer.lock().await;
                    if let Some(slot) = in_flight.remove(&req.correlation_id) {
                        let (reply_mutex, notify) = &*slot;
                        let mut guard = reply_mutex.lock().await;
                        *guard = Some(Err(PythonExecutionError::ExecutionError { message: format!("Failed to write message: {e}") }));
                        notify.notify_waiters();
                    }
                    break;
                }
                if let Err(e) = write_conn.flush().await {
                    let mut in_flight = in_flight_writer.lock().await;
                    if let Some(slot) = in_flight.remove(&req.correlation_id) {
                        let (reply_mutex, notify) = &*slot;
                        let mut guard = reply_mutex.lock().await;
                        *guard = Some(Err(PythonExecutionError::ExecutionError { message: format!("Failed to flush: {e}") }));
                        notify.notify_waiters();
                    }
                    break;
                }
            }
        });
        // Reader task
        tokio::spawn(async move {
            let mut read_conn = read_half;
            tracing::info!(event = "parent_reader", step = "start", "Parent reader task started (span ID is used as correlation_id)");
            loop {
                let mut len_buf = [0u8; 4];
                match read_conn.read_exact(&mut len_buf).await {
                    Ok(_) => {},
                    Err(e) => {
                        tracing::error!(event = "parent_reader", step = "read_len", error = ?e, "Failed to read length prefix, exiting reader task");
                        break;
                    }
                }
                let msg_len = u32::from_le_bytes(len_buf) as usize;
                let mut msg_buf = vec![0u8; msg_len];
                match read_conn.read_exact(&mut msg_buf).await {
                    Ok(_) => {
                        tracing::trace!(event = "parent_reader", step = "read_msg", msg_len, raw = ?&msg_buf[..], "Read message from child");
                    },
                    Err(e) => {
                        tracing::error!(event = "parent_reader", step = "read_msg", error = ?e, "Failed to read message body, exiting reader task");
                        break;
                    }
                }
                let ctrl_result = bincode::decode_from_slice::<Control<Result<<M as KameoChildProcessMessage>::Reply, PythonExecutionError>>, _>(&msg_buf[..], bincode::config::standard());
                match ctrl_result {
                    Ok((ctrl, _)) => {
                        tracing::trace!(event = "parent_reader", step = "decode_ctrl", ctrl_type = ?std::any::type_name::<Control<Result<<M as KameoChildProcessMessage>::Reply, PythonExecutionError>>>(), "Decoded Control envelope");
                        match ctrl {
                            Control::Real(envelope) => {
                                let correlation_id = envelope.correlation_id;
                                let slot = {
                                    let mut in_flight = in_flight_reader.lock().await;
                                    in_flight.remove(&correlation_id)
                                };
                                if let Some(slot) = slot {
                                    tracing::trace!(event = "parent_reader", step = "match_in_flight", correlation_id, "Matched reply to in_flight request");
                                    let (reply_mutex, notify) = &*slot;
                                    let mut guard = reply_mutex.lock().await;
                                    *guard = Some(envelope.inner);
                                    notify.notify_waiters();
                                } else {
                                    tracing::warn!(event = "parent_reader", step = "no_in_flight", correlation_id, "No in_flight entry for correlation_id");
                                }
                            }
                            Control::Handshake => {
                                tracing::trace!(event = "parent_reader", step = "handshake", "Received handshake control message");
                                continue;
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!(event = "parent_reader", step = "decode_error", error = ?e, raw = ?&msg_buf[..], "Failed to decode Control envelope, skipping message");
                        continue;
                    }
                }
            }
            tracing::warn!(event = "parent_reader", step = "exit", "Reader task exiting");
        });
        let backend = Arc::new(SubprocessIpcBackend::new(write_tx.clone(), in_flight.clone()));
        let callback_receiver = CallbackReceiver::new(Box::new(callback_conn), self.callback_handler);
        let mut actors = Vec::with_capacity(pool_size);
        for _ in 0..pool_size {
            actors.push(spawn_subprocess_ipc_actor(backend.clone()));
        }
        tokio::spawn(callback_receiver.run().instrument(tracing::Span::current()));
        Ok(PythonChildProcessActorPool {
            actors,
            next: std::sync::atomic::AtomicUsize::new(0),
            child: Some(child),
            write_tx: Some(write_tx),
        })
    }
}
