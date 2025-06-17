use kameo_snake_handler::prelude::*;
use kameo_child_process::prelude::*;
use kameo_child_process::KameoChildProcessMessage;
use serde::{Deserialize, Serialize};
use tracing::{info, warn, error};
use kameo::reply::Reply;
use bincode::{Decode, Encode};
use thiserror::Error;

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
    CalculateWaaaghPower { boyz_count: u32 },
    CalculateKlanBonus { klan_name: String, base_power: u32 },
    CalculateScrapResult { attacker_power: u32, defender_power: u32 },
    CalculateLoot { teef: u32, victory_points: u32 },
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
        // Initialize tracing first
        tracing_subscriber::fmt()
            .with_env_filter("info")
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

            info!("Starting Python subprocess with path: {:?}", python_path);

            // Create Python config
            let config = PythonConfig {
                python_path: vec![python_path.to_string_lossy().to_string()],
                module_name: "ork_logic".to_string(),
                function_name: "handle_message".to_string(),
                is_async: false,
                env_vars: vec![],
            };

            // Create and spawn Python actor
            let builder = PythonSubprocessBuilder::new()
                .with_config(config);

            let actor = builder.spawn().await?;
            let actor_ref = kameo::spawn(actor);

            // Test messages
            let messages = vec![
                OrkMessage::CalculateWaaaghPower { boyz_count: 100 },
                OrkMessage::CalculateKlanBonus { klan_name: "Goffs".to_string(), base_power: 50 },
                OrkMessage::CalculateScrapResult { attacker_power: 150, defender_power: 100 },
                OrkMessage::CalculateLoot { teef: 100, victory_points: 5 },
            ];

            for msg in messages {
                match actor_ref.ask(msg).await {
                    Ok(response) => info!("Got response: {:?}", response),
                    Err(e) => error!("Error: {:?}", e),
                }
            }

            Ok::<(), Box<dyn std::error::Error>>(())
        })
    }
}
