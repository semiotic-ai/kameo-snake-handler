use kameo_child_process::callback::{DynamicCallbackModule, TypedCallbackHandler};
use kameo_child_process::KameoChildProcessMessage;
use std::env::current_dir;
use std::time::Duration;
use tracing::Level;
use tracing_futures::Instrument;

use tokio_util::sync::CancellationToken;

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
/// Pool of Python child process actors for load balancing and concurrency.
///
/// This struct manages multiple actor instances connected to the same Python subprocess,
/// enabling concurrent message processing while maintaining a single process lifecycle.
/// The pool uses round-robin selection for load balancing across actors.
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
    /// Pool of actor references for round-robin load balancing
    actors: Vec<kameo::actor::ActorRef<kameo_child_process::SubprocessIpcActor<M>>>,
    /// Atomic counter for round-robin actor selection
    next: std::sync::atomic::AtomicUsize,
    /// Handle to the child process for lifecycle management
    child: Option<tokio::process::Child>,
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
    pub fn get_actor(&self) -> &kameo::actor::ActorRef<kameo_child_process::SubprocessIpcActor<M>> {
        let index = self.next.fetch_add(1, std::sync::atomic::Ordering::SeqCst) % self.actors.len();
        &self.actors[index]
    }

    pub fn actor_count(&self) -> usize {
        self.actors.len()
    }

    pub fn shutdown(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
        }
    }
}

/// Builder for Python child processes with dynamic callback support.
///
/// This builder supports multiple strongly-typed callback handlers, each responsible
/// for a specific callback type. Handlers are registered by module and type name,
/// allowing for flexible organization and automatic routing.
///
/// ## Generic Parameters
///
/// - `M`: Main message type for request/response communication
///
/// ## Usage Example
///
/// ```rust,ignore
/// // Create builder with Python configuration
/// let mut builder = PythonChildProcessBuilder::<MyMessage>::new(config);
///
/// // Add multiple callback handlers
/// builder
///     .with_callback_handler("trading", DataFetchHandler)
///     .with_callback_handler("trading", TraderHandler)
///     .with_callback_handler("weather", WeatherHandler)
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
pub struct PythonChildProcessBuilder<M>
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
{
    /// Python configuration for subprocess setup
    python_config: crate::PythonConfig,
    /// Logging level for the subprocess
    log_level: Level,
    /// Dynamic callback module for managing multiple typed handlers
    callback_module: DynamicCallbackModule,
    /// Phantom data for message type
    _phantom: std::marker::PhantomData<M>,
}

impl<M> PythonChildProcessBuilder<M>
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
            callback_module: DynamicCallbackModule::new(),
            _phantom: std::marker::PhantomData,
        }
    }

    /// Adds a typed callback handler for a specific module and type.
    ///
    /// # Arguments
    /// * `module_name` - The module name for organizing handlers (e.g., "trading", "weather")
    /// * `handler` - The typed callback handler implementing `TypedCallbackHandler<C>`
    ///
    /// # Example
    /// ```rust,ignore
    /// builder.with_callback_handler("trading", DataFetchHandler)
    /// ```
    pub fn with_callback_handler<C, H>(mut self, module_name: &str, handler: H) -> Self
    where
        C: Send + Sync + bincode::Decode<()> + for<'de> serde::Deserialize<'de> + 'static,
        H: TypedCallbackHandler<C> + Clone + Send + Sync + 'static,
    {
        if let Err(e) = self.callback_module.register_handler(module_name, handler) {
            tracing::warn!("Failed to register callback handler: {}", e);
        }
        self
    }

    /// Adds a typed callback handler with automatic module discovery.
    ///
    /// This method uses reflection to automatically determine the module name
    /// from the handler's type information.
    ///
    /// # Arguments
    /// * `handler` - The typed callback handler implementing `TypedCallbackHandler<C>`
    ///
    /// # Example
    /// ```rust,ignore
    /// builder.auto_callback_handler(DataFetchHandler)
    /// ```
    pub fn auto_callback_handler<C, H>(mut self, handler: H) -> Self
    where
        C: Send + Sync + bincode::Decode<()> + for<'de> serde::Deserialize<'de> + 'static,
        H: TypedCallbackHandler<C> + Clone + Send + Sync + 'static,
    {
        if let Err(e) = self.callback_module.auto_register_handler(handler) {
            tracing::warn!("Failed to auto-register callback handler: {}", e);
        }
        self
    }

    pub fn log_level(mut self, level: Level) -> Self {
        self.log_level = level;
        self
    }

    pub async fn spawn_pool(
        self,
        pool_size: usize,
        parent_config: Option<ParentActorLoopConfig>,
    ) -> std::io::Result<PythonChildProcessActorPool<M>> {
        use kameo_child_process::callback::TypedCallbackReceiver;
        use kameo_child_process::spawn_subprocess_ipc_actor;
        use tokio::net::UnixListener;
        let _parent_config = parent_config.unwrap_or_default();

        // Serialize the PythonConfig as JSON for the child
        let config_json = serde_json::to_string(&self.python_config).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Failed to serialize PythonConfig: {e}"),
            )
        })?;

        // Serialize the callback registry for creating Python modules
        let callback_registry = self.callback_module.get_registry();
        tracing::info!(
            "Callback registry before serialization: {:?}",
            callback_registry
        );
        let callback_registry_json = serde_json::to_string(&callback_registry).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Failed to serialize callback registry: {e}"),
            )
        })?;
        tracing::info!(
            "Serialized callback registry JSON: {}",
            callback_registry_json
        );

        // Set up the Unix domain sockets
        let actor_name = std::any::type_name::<crate::PythonActor<M, ()>>();
        let request_socket_path =
            kameo_child_process::handshake::unique_socket_path(&format!("{}-req", actor_name));
        let callback_socket_path =
            kameo_child_process::handshake::unique_socket_path(&format!("{}-cb", actor_name));
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
            cmd.env(
                "PYTHONPATH",
                format!(
                    "{}:{}",
                    std::env::var("PYTHONPATH").unwrap_or_default(),
                    current_dir()?
                        .join("crates/kameo-snake-testing/python")
                        .to_string_lossy()
                ),
            );
        }
        cmd.env("KAMEO_CHILD_ACTOR", actor_name);
        cmd.env(
            "KAMEO_REQUEST_SOCKET",
            request_socket_path.to_string_lossy().as_ref(),
        );
        cmd.env(
            "KAMEO_CALLBACK_SOCKET",
            callback_socket_path.to_string_lossy().as_ref(),
        );
        cmd.env("KAMEO_PYTHON_CONFIG", config_json);
        cmd.env("KAMEO_CALLBACK_REGISTRY", &callback_registry_json);
        tracing::info!(
            "Set KAMEO_CALLBACK_REGISTRY environment variable with {} bytes",
            callback_registry_json.len()
        );
        if let Ok(rust_log) = std::env::var("RUST_LOG") {
            cmd.env("RUST_LOG", rust_log);
        }
        cmd.stdout(std::process::Stdio::inherit());
        cmd.stderr(std::process::Stdio::inherit());
        let child = cmd.spawn()?;

        // Accept request connection and perform handshake
        let (mut request_conn, _addr) =
            tokio::time::timeout(Duration::from_secs(30), request_incoming.accept()).await??;
        kameo_child_process::perform_handshake::<M>(&mut request_conn, true)
            .await
            .map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Handshake failed: {e:?}"),
                )
            })?;

        // Accept callback connection
        let (callback_conn, _addr) =
            tokio::time::timeout(Duration::from_secs(30), callback_incoming.accept()).await??;

        // Backend and callback receiver setup
        let backend = kameo_child_process::SubprocessIpcBackend::from_duplex(
            kameo_child_process::DuplexUnixStream::new(request_conn),
        );

        // Create the typed callback receiver with our dynamic module
        let callback_module = std::sync::Arc::new(self.callback_module);
        let (read_half, write_half) =
            kameo_child_process::DuplexUnixStream::new(callback_conn).into_split();
        let reader = kameo_child_process::framing::LengthPrefixedRead::new(read_half);
        let writer = kameo_child_process::framing::LengthPrefixedWrite::new(write_half);

        // Create a cancellation token for the callback receiver
        let cancellation_token = CancellationToken::new();
        let receiver =
            TypedCallbackReceiver::new(callback_module, reader, writer, cancellation_token.clone());

        let mut actors = Vec::with_capacity(pool_size);
        for _ in 0..pool_size {
            actors.push(spawn_subprocess_ipc_actor(backend.clone()));
        }

        // Spawn the callback receiver
        tokio::spawn(receiver.run().instrument(tracing::Span::current()));

        Ok(PythonChildProcessActorPool {
            actors,
            next: std::sync::atomic::AtomicUsize::new(0),
            child: Some(child),
        })
    }
}
