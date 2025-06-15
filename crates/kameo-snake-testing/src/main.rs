use kameo_snake_handler::{PythonActor, PythonConfig, PythonExecutionError};
use tracing::{info, Level};
use tracing_subscriber::{fmt, prelude::*, filter::EnvFilter};
use bincode::{Encode, Decode};
use serde::{Serialize, Deserialize};
use kameo_child_process::{KameoChildProcessMessage, setup_subprocess_system, ChildProcessBuilder};
use kameo::prelude::*;
use std::future::Future;
use std::process;
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub enum OrkCommand {
    KlanBonus {
        power: i32,
        klan: String,
    },
    ScrapResult {
        boy1_power: i32,
        boy2_power: i32,
    },
    LootTeef {
        base_teef: i32,
    },
    WaaaghPower {
        boyz_count: i32,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct OrkResult {
    pub result: i32,
    pub message: String,
}

impl Reply for OrkResult {
    type Ok = OrkResult;
    type Error = PythonExecutionError;
    type Value = OrkResult;

    fn to_result(self) -> Result<Self::Ok, Self::Error> {
        Ok(self)
    }

    fn into_value(self) -> Self::Value {
        self
    }

    fn into_any_err(self) -> Option<Box<dyn ReplyError>> {
        None
    }
}

impl KameoChildProcessMessage for OrkCommand {
    type Reply = OrkResult;
}

// Our custom wrapper type for the Python actor
#[derive(Clone)]
pub struct OrkPythonActor(PythonActor<OrkCommand>);

impl Default for OrkPythonActor {
    fn default() -> Self {
        let module_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("python")
            .join("ork_logic.py");
        
        let config = PythonConfig {
            python_path: vec![module_path.parent().unwrap().to_string_lossy().into_owned()],
            env_vars: vec![],
            module_name: "ork_logic".to_string(),
            function_name: "handle_message".to_string(),
        };
        
        Self(PythonActor::new(config))
    }
}

impl Message<OrkCommand> for OrkPythonActor {
    type Reply = Result<OrkResult, PythonExecutionError>;

    fn handle(&mut self, msg: OrkCommand, _ctx: &mut Context<Self, Self::Reply>) -> impl Future<Output = Self::Reply> + Send {
        let actor = self.0.clone();
        async move {
            let actor_ref = kameo::spawn(actor);
            actor_ref.ask(msg).await.map_err(|e| match e {
                kameo::error::SendError::HandlerError(e) => e,
                _ => PythonExecutionError::Execution("Actor communication failed".to_string()),
            })
        }
    }
}

impl Actor for OrkPythonActor {
    type Error = PythonExecutionError;

    fn name() -> &'static str {
        "OrkPythonActor"
    }

    fn on_start(&mut self, _actor_ref: ActorRef<Self>) -> impl Future<Output = Result<(), Self::Error>> + Send {
        let inner = self.0.clone();
        async move {
            inner.init_python()
        }
    }

    fn on_stop(&mut self, _actor_ref: WeakActorRef<Self>, _reason: ActorStopReason) -> impl Future<Output = Result<(), Self::Error>> + Send {
        async move { Ok(()) }
    }
}

setup_subprocess_system! {
    actors = {
        (OrkPythonActor, OrkCommand),
    },
    child_init = {
        let mut builder = tokio::runtime::Builder::new_multi_thread();
        builder.enable_all();
        builder
    },
    parent_init = {
        // Set up logging
        let subscriber = tracing_subscriber::registry()
            .with(fmt::layer())
            .with(EnvFilter::from_default_env()
                .add_directive(Level::DEBUG.into()));
        
        tracing::subscriber::set_global_default(subscriber)
            .expect("Failed to set tracing subscriber");

        let parent_pid = process::id();
        info!(pid=%parent_pid, "STARTIN' UP DA PARENT PROCESS!");

        // Create and run the tokio runtime
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;

        runtime.block_on(async {
            // Create actor system and spawn Python actor
            let builder = ChildProcessBuilder::<OrkPythonActor, OrkCommand>::new()
                .log_level(Level::DEBUG);

            info!(pid=%parent_pid, "GONNA SPAWN DA PYTHON ACTOR NOW!");
            let python_ref = builder.spawn().await?;

            // Send a test message that will make the Python code panic
            let msg = OrkCommand::WaaaghPower {
                boyz_count: -1, // Negative boyz should cause a panic!
            };

            info!(pid=%parent_pid, "SENDIN' A PANIC-INDUCIN' MESSAGE!");
            match python_ref.ask(msg).await {
                Ok(result) => info!(pid=%parent_pid, "GOT RESULT (SHOULDN'T HAPPEN): {:?}", result),
                Err(e) => info!(pid=%parent_pid, "GOT EXPECTED ERROR: {:?}", e),
            }

            Ok(())
        })
    }
}
