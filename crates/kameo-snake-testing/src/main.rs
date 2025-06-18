use bincode::{Decode, Encode};
use kameo::reply::Reply;
use kameo_child_process::prelude::*;
use kameo_child_process::KameoChildProcessMessage;
use kameo_snake_handler::prelude::*;
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::Level;
use tracing::{error, info};

/// Custom error type for Ork operations
#[derive(Debug, Error, Serialize, Deserialize, Clone, Decode, Encode)]
pub enum OrkError {
    #[error("Not enough boyz for WAAAGH! (need {needed}, got {got})")]
    NotEnoughBoyz { needed: i32, got: i32 },
    #[error("Unknown klan: {0}")]
    UnknownKlan(String),
    #[error("Invalid power level: {0}")]
    InvalidPower(String),
    #[error("Python error: {0}")]
    PythonError(String),
}

/// Message types that can be sent to Python subprocess
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub enum OrkMessage {
    CalculateWaaaghPower {
        boyz_count: u32,
    },
    CalculateKlanBonus {
        klan_name: String,
        base_power: u32,
    },
    CalculateScrapResult {
        attacker_power: u32,
        defender_power: u32,
    },
    CalculateLoot {
        teef: u32,
        victory_points: u32,
    },
}

impl Default for OrkMessage {
    fn default() -> Self {
        Self::CalculateWaaaghPower { boyz_count: 0 }
    }
}

/// Response types from Python subprocess
#[derive(Debug, Serialize, Deserialize, Clone, Decode, Encode)]
pub enum OrkResponse {
    WaaaghPower { power: u32 },
    KlanBonus { bonus: u32 },
    ScrapResult { victory: bool },
    LootResult { total_teef: u32, bonus_teef: u32 },
    Error { error: String },
}

impl Reply for OrkResponse {
    type Ok = Self;
    type Error = OrkError;
    type Value = Self;

    fn to_result(self) -> Result<Self::Ok, <Self as Reply>::Error> {
        match self {
            OrkResponse::Error { error } => Err(OrkError::PythonError(error)),
            _ => Ok(self),
        }
    }

    fn into_any_err(self) -> Option<Box<dyn kameo::reply::ReplyError>> {
        match self {
            OrkResponse::Error { error } => Some(Box::new(OrkError::PythonError(error))),
            _ => None,
        }
    }

    fn into_value(self) -> Self::Value {
        self
    }
}

impl KameoChildProcessMessage for OrkMessage {
    type Reply = OrkResponse;
}

setup_subprocess_system! {
    actors = {
        (PythonActor<OrkMessage>, OrkMessage),
    },
    child_init = {
        // Prepare Python for multi-threaded use
        pyo3::prepare_freethreaded_python();

        // Get the GIL and set up the asyncio event loop
        pyo3::Python::with_gil(|py| {
            let asyncio = py.import("asyncio")?;
            let event_loop = asyncio.call_method0("new_event_loop")?;
            asyncio.call_method1("set_event_loop", (&event_loop,))?;
            Ok::<(), pyo3::PyErr>(())
        }).expect("Failed to set up python event loop");

        // Initialize tracing first
        tracing_subscriber::fmt()
            .with_max_level(Level::TRACE)
            .with_file(true)
            .with_line_number(true)
            .with_thread_ids(true)
            .with_thread_names(true)
            .init();

        let mut builder = tokio::runtime::Builder::new_current_thread();
        builder.enable_all();
        builder
    },
    parent_init = {
        // Initialize tracing first
        tracing_subscriber::fmt()
            .with_env_filter("info")
            .with_file(true)
            .with_line_number(true)
            .with_thread_ids(true)
            .with_thread_names(true)
            .init();

        // Create parent runtime
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("ork-boss")
            .enable_all()
            .build()?;

        runtime.block_on(async {
            // Set up Python subprocess
            let python_path = std::env::current_dir()?
                .join("crates")
                .join("kameo-snake-testing")
                .join("python");

            // --- SYNC FLOW ---
            info!("==== SYNC FLOW ====");
            let sync_config = PythonConfig {
                python_path: vec![python_path.to_string_lossy().to_string()],
                module_name: "ork_logic".to_string(),
                function_name: "handle_message".to_string(),
                is_async: false,
                env_vars: vec![],
            };
            let sync_actor = PythonSubprocessBuilder::new().with_config(sync_config).spawn().await?;
            let sync_ref = kameo::spawn(sync_actor);
            // Valid message
            match sync_ref.ask(OrkMessage::CalculateWaaaghPower { boyz_count: 100 }).await {
                Ok(response) => info!("SYNC OK: {:?}", response),
                Err(e) => error!("SYNC ERR: {:?}", e),
            }
            // Invalid message (unknown type)
            match sync_ref.ask(OrkMessage::CalculateKlanBonus { klan_name: "UnknownKlan".to_string(), base_power: 0 }).await {
                Ok(response) => info!("SYNC OK (should error): {:?}", response),
                Err(e) => error!("SYNC ERR (expected): {:?}", e),
            }

            // --- ASYNC FLOW ---
            info!("==== ASYNC FLOW ====");
            let async_config = PythonConfig {
                python_path: vec![python_path.to_string_lossy().to_string()],
                module_name: "ork_logic_async".to_string(),
                function_name: "handle_message_async".to_string(),
                is_async: true,
                env_vars: vec![],
            };
            let async_actor = PythonSubprocessBuilder::new().with_config(async_config).spawn().await?;
            let async_ref = kameo::spawn(async_actor);
            // Valid message
            match async_ref.ask(OrkMessage::CalculateWaaaghPower { boyz_count: 100 }).await {
                Ok(response) => info!("ASYNC OK: {:?}", response),
                Err(e) => error!("ASYNC ERR: {:?}", e),
            }
            // Invalid message (unknown type)
            match async_ref.ask(OrkMessage::CalculateKlanBonus { klan_name: "UnknownKlan".to_string(), base_power: 0 }).await {
                Ok(response) => info!("ASYNC OK (should error): {:?}", response),
                Err(e) => error!("ASYNC ERR (expected): {:?}", e),
            }

            Ok::<(), Box<dyn std::error::Error>>(())
        })
    }
}
