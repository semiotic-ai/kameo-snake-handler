use crate::error::PythonExecutionError;
use crate::NoopCallbackHandler;
use kameo_child_process::{CallbackHandler, ChildCallbackMessage, KameoChildProcessMessage};
use tracing::Level;
use std::marker::PhantomData;

/// Builder for a Python child process
/// NOTE: For PythonActor, use the macro-based entrypoint (setup_python_subprocess_system!). This builder is not supported for PythonActor.
pub struct PythonChildProcessBuilder<C: ChildCallbackMessage + Sync, H = NoopCallbackHandler>
where
    H: CallbackHandler<C> + Send + Sync + 'static,
{
    python_config: crate::PythonConfig,
    log_level: Level,
    _phantom: PhantomData<C>,
    callback_handler: H,
}

impl<C: ChildCallbackMessage + Sync> PythonChildProcessBuilder<C, NoopCallbackHandler> {
    /// Creates a new builder with the given Python configuration.
    #[tracing::instrument]
    pub fn new(mut python_config: crate::PythonConfig) -> Self {
        // Always set PYTHONPATH from python_path
        let joined_path = python_config.python_path.join(":");
        // Only add if not already present in env_vars
        if !python_config.env_vars.iter().any(|(k, _)| k == "PYTHONPATH") {
            python_config.env_vars.push(("PYTHONPATH".to_string(), joined_path));
        }
        Self {
            python_config,
            log_level: Level::INFO,
            _phantom: PhantomData,
            callback_handler: NoopCallbackHandler,
        }
    }
}

impl<C: ChildCallbackMessage + Sync, H> PythonChildProcessBuilder<C, H>
where
    H: CallbackHandler<C> + Send + Sync + 'static,
{
    /// Sets the log level for the child process.
    pub fn log_level(mut self, level: Level) -> Self {
        self.log_level = level;
        self
    }

    /// Inject a custom callback handler for callback IPC.
    pub fn with_callback_handler<NH>(self, handler: NH) -> PythonChildProcessBuilder<C, NH>
    where
        NH: CallbackHandler<C> + Send + Sync + 'static,
    {
        PythonChildProcessBuilder {
            python_config: self.python_config,
            log_level: self.log_level,
            _phantom: PhantomData,
            callback_handler: handler,
        }
    }

    /// Spawns a Python child process actor and returns an ActorRef for messaging.
    /// This is the only supported way to spawn a Python child process actor from the parent.
    pub async fn spawn<M>(self) -> std::io::Result<kameo::actor::ActorRef<kameo_child_process::SubprocessActor<M, C, PythonExecutionError>>>
    where
        M: KameoChildProcessMessage + Send + Sync + 'static,
        <M as KameoChildProcessMessage>::Reply:
            serde::Serialize + for<'de> serde::Deserialize<'de> + bincode::Encode + bincode::Decode<()> + std::fmt::Debug + Send + Sync + 'static,
    {
        use kameo_child_process::ChildProcessBuilder;
        // Serialize the PythonConfig as JSON for the child
        let config_json = serde_json::to_string(&self.python_config)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("Failed to serialize PythonConfig: {e}")))?;

        // Set the actor name to the message type name
        let builder = ChildProcessBuilder::<kameo_child_process::SubprocessActor<M, C, PythonExecutionError>, M, C, PythonExecutionError>::new()
            .with_actor_name(std::any::type_name::<crate::PythonActor<M, C>>())
            .log_level(self.log_level.clone())
            .with_env_var("KAMEO_PYTHON_CONFIG", config_json);
        tracing::trace!(event = "py_spawn", step = "before_builder_spawn", "About to call builder.spawn for Python child process");
        let (actor_ref, callback_receiver) = builder.spawn(self.callback_handler).await?;
        tracing::trace!(event = "py_spawn", step = "after_builder_spawn", "Returned from builder.spawn, about to spawn callback_receiver.run()");
        tokio::spawn(callback_receiver.run());
        tracing::trace!(event = "py_spawn", step = "after_callback_spawn", "Spawned callback_receiver.run(), about to return actor_ref");
        Ok(actor_ref)
    }
} 