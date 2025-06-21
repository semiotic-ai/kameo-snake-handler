use bincode::{Decode, Encode};
use kameo::reply::Reply;
use kameo_child_process::prelude::*;
use kameo_child_process::KameoChildProcessMessage;
use kameo_snake_handler::prelude::*;
use kameo_snake_handler::ErrorReply;
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

impl ErrorReply for OrkResponse {
    fn from_error(err: PythonExecutionError) -> Self {
        OrkResponse::Error {
            error: err.to_string(),
        }
    }
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
                .with_max_level(Level::TRACE)
                .with_file(true)
                .with_line_number(true)
                .with_thread_ids(true)
                .with_thread_names(true)
                .init();

        // Create and return a multi-threaded runtime for better Python async support
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("python-worker")
            .enable_all()
            .build()?
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
            let sync_ref = PythonChildProcessBuilder::new(sync_config).spawn().await?;
            
            // Test 1: Valid message
            info!("Test 1: Valid message (sync)");
            match sync_ref.ask(OrkMessage::CalculateWaaaghPower { boyz_count: 100 }).await {
                Ok(response) => info!("SYNC OK: {:?}", response),
                Err(e) => error!("SYNC ERR: {:?}", e),
            }

            // Test 2: Invalid message (unknown klan)
            info!("Test 2: Invalid message - unknown klan (sync)");
            match sync_ref.ask(OrkMessage::CalculateKlanBonus { klan_name: "UnknownKlan".to_string(), base_power: 0 }).await {
                Ok(response) => error!("SYNC FAILED (should error): {:?}", response),
                Err(e) => info!("SYNC ERR (expected): {:?}", e),
            }

            // Test 3: Edge case - zero boyz
            info!("Test 3: Edge case - zero boyz (sync)");
            match sync_ref.ask(OrkMessage::CalculateWaaaghPower { boyz_count: 0 }).await {
                Ok(response) => info!("SYNC OK (zero boyz): {:?}", response),
                Err(e) => error!("SYNC ERR: {:?}", e),
            }

            // Test 4: Edge case - massive number
            info!("Test 4: Edge case - massive number (sync)");
            match sync_ref.ask(OrkMessage::CalculateWaaaghPower { boyz_count: u32::MAX }).await {
                Ok(response) => error!("SYNC FAILED (should error): {:?}", response),
                Err(e) => info!("SYNC ERR (expected): {:?}", e),
            }

            // Test 5: Scrap result test
            info!("Test 5: Scrap result test (sync)");
            match sync_ref.ask(OrkMessage::CalculateScrapResult { attacker_power: 1000, defender_power: 500 }).await {
                Ok(response) => info!("SYNC OK (scrap result): {:?}", response),
                Err(e) => error!("SYNC ERR: {:?}", e),
            }

            // Test 6: Loot calculation
            info!("Test 6: Loot calculation (sync)");
            match sync_ref.ask(OrkMessage::CalculateLoot { teef: 100, victory_points: 5 }).await {
                Ok(response) => info!("SYNC OK (loot calc): {:?}", response),
                Err(e) => error!("SYNC ERR: {:?}", e),
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
            let async_ref = PythonChildProcessBuilder::new(async_config).spawn().await?;
            
            // Test 1: Valid message
            info!("Test 1: Valid message (async)");
            match async_ref.ask(OrkMessage::CalculateWaaaghPower { boyz_count: 100 }).await {
                Ok(response) => info!("ASYNC OK: {:?}", response),
                Err(e) => error!("ASYNC ERR: {:?}", e),
            }

            // Test 2: Invalid message (unknown klan)
            info!("Test 2: Invalid message - unknown klan (async)");
            match async_ref.ask(OrkMessage::CalculateKlanBonus { klan_name: "UnknownKlan".to_string(), base_power: 0 }).await {
                Ok(response) => error!("ASYNC FAILED (should error): {:?}", response),
                Err(e) => info!("ASYNC ERR (expected): {:?}", e),
            }

            // Test 3: Edge case - zero boyz
            info!("Test 3: Edge case - zero boyz (async)");
            match async_ref.ask(OrkMessage::CalculateWaaaghPower { boyz_count: 0 }).await {
                Ok(response) => info!("ASYNC OK (zero boyz): {:?}", response),
                Err(e) => error!("ASYNC ERR: {:?}", e),
            }

            // Test 4: Edge case - massive number
            info!("Test 4: Edge case - massive number (async)");
            match async_ref.ask(OrkMessage::CalculateWaaaghPower { boyz_count: u32::MAX }).await {
                Ok(response) => error!("ASYNC FAILED (should error): {:?}", response),
                Err(e) => info!("ASYNC ERR (expected): {:?}", e),
            }

            // Test 5: Scrap result test
            info!("Test 5: Scrap result test (async)");
            match async_ref.ask(OrkMessage::CalculateScrapResult { attacker_power: 1000, defender_power: 500 }).await {
                Ok(response) => info!("ASYNC OK (scrap result): {:?}", response),
                Err(e) => error!("ASYNC ERR: {:?}", e),
            }

            // Test 6: Loot calculation
            info!("Test 6: Loot calculation (async)");
            match async_ref.ask(OrkMessage::CalculateLoot { teef: 100, victory_points: 5 }).await {
                Ok(response) => info!("ASYNC OK (loot calc): {:?}", response),
                Err(e) => error!("ASYNC ERR: {:?}", e),
            }

            // Test 7: Rapid fire messages (stress test)
            info!("Test 7: Rapid fire messages (async)");
            let mut handles = Vec::new();
            for i in 0..10 {
                let ref_clone = async_ref.clone();
                handles.push(tokio::spawn(async move {
                    match ref_clone.ask(OrkMessage::CalculateWaaaghPower { boyz_count: i * 100 }).await {
                        Ok(response) => info!("ASYNC OK (rapid fire {}): {:?}", i, response),
                        Err(e) => error!("ASYNC ERR (rapid fire {}): {:?}", i, e),
                    }
                }));
            }
            for handle in handles {
                handle.await?;
            }

            // Test 8: Invalid module test
            info!("Test 8: Invalid module test");
            let invalid_module_config = PythonConfig {
                python_path: vec![python_path.to_string_lossy().to_string()],
                module_name: "non_existent_module".to_string(),
                function_name: "handle_message".to_string(),
                is_async: false,
                env_vars: vec![],
            };
            match tokio::time::timeout(
                std::time::Duration::from_secs(5),
                PythonChildProcessBuilder::new(invalid_module_config).spawn::<OrkMessage>()
            ).await {
                Ok(spawn_result) => match spawn_result {
                    Ok(_) => error!("Expected error for invalid module, but got success"),
                    Err(e) => info!("Got expected error for invalid module: {:?}", e),
                },
                Err(_) => info!("Got expected timeout for invalid module"),
            }

            // Test 9: Invalid function test
            info!("Test 9: Invalid function test");
            let invalid_function_config = PythonConfig {
                python_path: vec![python_path.to_string_lossy().to_string()],
                module_name: "ork_logic".to_string(),
                function_name: "non_existent_function".to_string(),
                is_async: false,
                env_vars: vec![],
            };
            let invalid_function_ref = PythonChildProcessBuilder::new(invalid_function_config).spawn::<OrkMessage>().await?;
            match invalid_function_ref.ask(OrkMessage::CalculateWaaaghPower { boyz_count: 100 }).await {
                Ok(_) => error!("Expected error for invalid function, but got success"),
                Err(e) => info!("Got expected error for invalid function: {:?}", e),
            }

            // Test 10: Invalid Python path test
            info!("Test 10: Invalid Python path test");
            let invalid_path_config = PythonConfig {
                python_path: vec!["non/existent/path".to_string()],
                module_name: "ork_logic".to_string(),
                function_name: "handle_message".to_string(),
                is_async: false,
                env_vars: vec![],
            };
            match PythonChildProcessBuilder::new(invalid_path_config).spawn::<OrkMessage>().await {
                Ok(_) => error!("Expected error for invalid path, but got success"),
                Err(e) => info!("Got expected error for invalid path: {:?}", e),
            }

            Ok::<(), Box<dyn std::error::Error>>(())
        })
    }
}
