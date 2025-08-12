use kameo_child_process::KameoChildProcessMessage;
use tracing::Level;
use tracing_futures::Instrument;
use kameo_child_process::callback::{NoopCallbackHandler, CallbackStreamHandler};
use std::time::Duration;

/// Builder for a Python child process
/// NOTE: For PythonActor, use the macro-based entrypoint (setup_python_subprocess_system!). This builder is not supported for PythonActor.
/// NOTE: SubprocessParentActor is only valid as an in-process actor with DelegatedReply. If used as a child process actor, it will panic.
pub struct ParentActorLoopConfig {
    pub max_concurrency: usize,
}

impl Default for ParentActorLoopConfig {
    fn default() -> Self {
        Self {
            max_concurrency: 10_000,
        }
    }
}

// --- Actor Pool for Python Child Process ---
pub struct PythonChildProcessActorPool<M>
where
    M: kameo_child_process::KameoChildProcessMessage + Send + Sync + 'static,
    <M as kameo_child_process::KameoChildProcessMessage>::Ok: serde::Serialize
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
    write_tx: Option<tokio::sync::mpsc::UnboundedSender<kameo_child_process::WriteRequest<M>>>,
    // callback_shutdown: Option<tokio::sync::Notify>,
}

impl<M> PythonChildProcessActorPool<M>
where
    M: kameo_child_process::KameoChildProcessMessage + Send + Sync + 'static,
    <M as kameo_child_process::KameoChildProcessMessage>::Ok: serde::Serialize
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

/// Builder for spawning Python child processes with unified streaming support.
/// 
/// This builder provides a fluent interface for configuring and spawning Python subprocesses
/// that can handle both synchronous and streaming messages. It manages the entire lifecycle
/// of Python subprocesses, including environment setup, process spawning, and actor pool creation.
/// 
/// ## Builder Features
/// 
/// - **Python Configuration**: Environment setup, paths, and module configuration
/// - **Callback Handling**: Configurable callback message handling
/// - **Process Management**: Automatic subprocess lifecycle management
/// - **Actor Pool Creation**: Spawn multiple actors for load balancing
/// - **Streaming Support**: Full support for the unified streaming protocol
/// 
/// ## Generic Parameters
/// 
/// - `M`: Main message type for request/response communication
/// - `C`: Callback message type for bidirectional communication
/// - `H`: Callback handler type (defaults to `NoopCallbackHandler<C>`)
/// 
/// ## Usage Example
/// 
/// ```rust
/// // Create builder with Python configuration
/// let pool = PythonChildProcessBuilder::<MyMessage, MyCallback>::new(config)
///     .with_callback_handler(MyCallbackHandler)
///     .log_level(Level::DEBUG)
///     .spawn_pool(4, None)
///     .await?;
/// 
/// // Use the actor pool
/// let actor = pool.get_actor();
/// let response = actor.ask(MyMessage { data: "hello" }).await?;
/// 
/// // Send streaming message
/// let stream = actor.send_stream(MyMessage { data: "stream" }).await?;
/// while let Some(item) = stream.next().await {
///     println!("Response: {:?}", item?);
/// }
/// ```
/// 
/// ## Process Lifecycle
/// 
/// 1. **Configuration**: Set up Python environment and module paths
/// 2. **Spawning**: Create Python subprocess with proper IPC setup
/// 3. **Handshake**: Establish communication protocol
/// 4. **Actor Pool**: Create multiple actors for concurrent processing
/// 5. **Communication**: Handle both sync and streaming messages
/// 6. **Shutdown**: Graceful cleanup of processes and resources
pub struct PythonChildProcessBuilder<M, C, H = NoopCallbackHandler<C>>
where
    M: KameoChildProcessMessage + Send + Sync + 'static,
    <M as KameoChildProcessMessage>::Ok: serde::Serialize
        + for<'de> serde::Deserialize<'de>
        + bincode::Encode
        + bincode::Decode<()> 
        + std::fmt::Debug
        + Send
        + Sync
        + 'static,
    C: Send + Sync + Clone + 'static + bincode::Encode + bincode::Decode<()> + std::fmt::Debug,
    H: CallbackStreamHandler<C> + Clone + Send + Sync + 'static,
{
    /// Python configuration for subprocess setup
    python_config: crate::PythonConfig,
    /// Logging level for the subprocess
    log_level: Level,
    /// Handler for callback messages
    callback_handler: H,
    /// Phantom data for message and callback types
    _phantom: std::marker::PhantomData<(M, C)>,
}

impl<M, C> PythonChildProcessBuilder<M, C, NoopCallbackHandler<C>>
where
    M: KameoChildProcessMessage + Send + Sync + 'static,
    <M as KameoChildProcessMessage>::Ok: serde::Serialize
        + for<'de> serde::Deserialize<'de>
        + bincode::Encode
        + bincode::Decode<()> 
        + std::fmt::Debug
        + Send
        + Sync
        + 'static,
    C: Send + Sync + Clone + 'static + bincode::Encode + bincode::Decode<()> + std::fmt::Debug,
{
    /// Creates a new builder with the given Python configuration and message types.
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

impl<M, C, H> PythonChildProcessBuilder<M, C, H>
where
    M: KameoChildProcessMessage + Send + Sync + 'static,
    <M as KameoChildProcessMessage>::Ok: serde::Serialize
        + for<'de> serde::Deserialize<'de>
        + bincode::Encode
        + bincode::Decode<()> 
        + std::fmt::Debug
        + Send
        + Sync
        + 'static,
    C: Send + Sync + Clone + 'static + bincode::Encode + bincode::Decode<()> + std::fmt::Debug,
    H: CallbackStreamHandler<C> + Clone + Send + Sync + 'static,
{
    /// Sets the callback handler.
    pub fn with_callback_handler<T>(self, handler: T) -> PythonChildProcessBuilder<M, C, T>
    where
        T: CallbackStreamHandler<C> + Clone + Send + Sync + 'static,
    {
        PythonChildProcessBuilder {
            python_config: self.python_config,
            log_level: self.log_level,
            callback_handler: handler,
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn log_level(mut self, level: Level) -> Self {
        self.log_level = level;
        self
    }

    pub async fn spawn_pool(
        self,
        pool_size: usize,
        parent_config: Option<ParentActorLoopConfig>,
    ) -> std::io::Result<PythonChildProcessActorPool<M>>
    {
        use kameo_child_process::spawn_subprocess_ipc_actor;
        use kameo_child_process::callback::CallbackReceiver;
        use tokio::net::UnixListener;
        let _parent_config = parent_config.unwrap_or_default();
        // Serialize the PythonConfig as JSON for the child
        let config_json = serde_json::to_string(&self.python_config).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Failed to serialize PythonConfig: {e}"),
            )
        })?;
        // Set up the Unix domain sockets
        let actor_name = std::any::type_name::<crate::PythonActor<M, C>>();
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
        
        // Add OTEL environment variables if propagation is enabled
        if self.python_config.enable_otel_propagation {
            // Set OTEL environment variables for Python subprocess
            cmd.env("OTEL_TRACES_EXPORTER", "otlp");
            cmd.env("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317");
            cmd.env("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc");
            // Set service name to match Rust process
            cmd.env("OTEL_SERVICE_NAME", "kameo-snake");
            cmd.env("OTEL_TRACES_SAMPLER", "always_on");
            cmd.env("OTEL_TRACES_SAMPLER_ARG", "1.0");
            
            // Set Python-specific OTEL environment variables
            cmd.env("PYTHONPATH", format!("{}:{}", 
                std::env::var("PYTHONPATH").unwrap_or_default(),
                std::env::current_dir()?.join("crates/kameo-snake-testing/python").to_string_lossy()
            ));
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
        let child = cmd.spawn()?;
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
        let backend = kameo_child_process::SubprocessIpcBackend::from_duplex(
            kameo_child_process::DuplexUnixStream::new(request_conn)
        );
        let receiver = CallbackReceiver::<C, H>::from_duplex(
            kameo_child_process::DuplexUnixStream::new(callback_conn),
            self.callback_handler.clone(),
        );
        let mut actors = Vec::with_capacity(pool_size);
        for _ in 0..pool_size {
            actors.push(spawn_subprocess_ipc_actor(backend.clone()));
        }
        tokio::spawn(receiver.run().instrument(tracing::Span::current()));
        Ok(PythonChildProcessActorPool {
            actors,
            next: std::sync::atomic::AtomicUsize::new(0),
            child: Some(child),
            write_tx: None, // No longer needed
            // callback_shutdown: None, // No longer needed
        })
    }
}
