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
use pyo3::types::PyAnyMethods;
use kameo_child_process::CallbackSender;
use kameo_child_process::RuntimeAware;
use pyo3::Python;
use pyo3::exceptions::PyRuntimeError;
use kameo_child_process::ChildCallbackMessage;

/// Custom error type for Ork operations
#[derive(Debug, Error, Serialize, Deserialize, Clone, Decode, Encode)]
pub enum TestError {
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
pub enum TestMessage {
    CalculatePower {
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

impl Default for TestMessage {
    fn default() -> Self {
        Self::CalculatePower { boyz_count: 0 }
    }
}

/// Response types from Python subprocess
#[derive(Debug, Serialize, Deserialize, Clone, Decode, Encode)]
pub enum TestResponse {
    Power { power: u32 },
    KlanBonus { bonus: u32 },
    ScrapResult { victory: bool },
    LootResult { total_teef: u32, bonus_teef: u32 },
    Error { error: String },
}

impl ErrorReply for TestResponse {
    fn from_error(err: PythonExecutionError) -> Self {
        TestResponse::Error {
            error: err.to_string(),
        }
    }
}

impl Reply for TestResponse {
    type Ok = Self;
    type Error = TestError;
    type Value = Self;

    fn to_result(self) -> Result<Self::Ok, <Self as Reply>::Error> {
        match self {
            TestResponse::Error { error } => Err(TestError::PythonError(error)),
            _ => Ok(self),
        }
    }

    fn into_any_err(self) -> Option<Box<dyn kameo::reply::ReplyError>> {
        match self {
            TestResponse::Error { error } => Some(Box::new(TestError::PythonError(error))),
            _ => None,
        }
    }

    fn into_value(self) -> Self::Value {
        self
    }
}

impl KameoChildProcessMessage for TestMessage {
    type Reply = TestResponse;
}

// Add a dummy callback type for tests
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct TestCallbackMessage;

impl ChildCallbackMessage for TestCallbackMessage {
    type Reply = ();
}

kameo_snake_handler::setup_python_subprocess_system! {
    actors = {
        (PythonActor<TestMessage, TestCallbackMessage>, TestMessage, TestCallbackMessage),
    },
    child_init = {{
        tracing_subscriber::fmt()
            .with_env_filter("debug")
            .with_file(true)
            .with_line_number(true)
            .with_thread_ids(true)
            .with_thread_names(true)
            .init();
        kameo_child_process::RuntimeConfig {
            flavor: kameo_child_process::RuntimeFlavor::CurrentThread,
            worker_threads: None,
        }
    }},
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
            let site_packages = "crates/kameo-snake-testing/python/venv/lib/python3.13/site-packages";
            let python_path_vec = vec![
                site_packages.to_string(),
                python_path.to_string_lossy().to_string(),
            ];
            // SYNC TESTS
            let sync_config = PythonConfig {
                python_path: python_path_vec.clone(),
                module_name: "ork_logic".to_string(),
                function_name: "handle_message".to_string(),
                env_vars: vec![],
                is_async: false,
                module_path: "crates/kameo-snake-testing/python/ork_logic.py".to_string(),
            };
            let sync_ref = PythonChildProcessBuilder::<TestCallbackMessage>::new(sync_config).spawn::<TestMessage>().await?;
            // Test 1: Valid message
            info!("Test 1: Valid message (sync)");
            match sync_ref.ask(TestMessage::CalculatePower { boyz_count: 100 }).await {
                Ok(response) => info!("SYNC OK: {:?}", response),
                Err(e) => error!("SYNC ERR: {:?}", e),
            }
            // Test 2: Invalid message (unknown klan)
            info!("Test 2: Invalid message - unknown klan (sync)");
            match sync_ref.ask(TestMessage::CalculateKlanBonus { klan_name: "UnknownKlan".to_string(), base_power: 0 }).await {
                Ok(response) => error!("SYNC FAILED (should error): {:?}", response),
                Err(e) => info!("SYNC ERR (expected): {:?}", e),
            }
            // Test 3: Edge case - zero boyz
            info!("Test 3: Edge case - zero boyz (sync)");
            match sync_ref.ask(TestMessage::CalculatePower { boyz_count: 0 }).await {
                Ok(response) => info!("SYNC OK (zero boyz): {:?}", response),
                Err(e) => error!("SYNC ERR: {:?}", e),
            }
            // Test 4: Edge case - massive number
            info!("Test 4: Edge case - massive number (sync)");
            match sync_ref.ask(TestMessage::CalculatePower { boyz_count: u32::MAX }).await {
                Ok(response) => error!("SYNC FAILED (should error): {:?}", response),
                Err(e) => info!("SYNC ERR (expected): {:?}", e),
            }
            // Test 5: Scrap result test
            info!("Test 5: Scrap result test (sync)");
            match sync_ref.ask(TestMessage::CalculateScrapResult { attacker_power: 1000, defender_power: 500 }).await {
                Ok(response) => info!("SYNC OK (scrap result): {:?}", response),
                Err(e) => error!("SYNC ERR: {:?}", e),
            }
            // Test 6: Loot calculation
            info!("Test 6: Loot calculation (sync)");
            match sync_ref.ask(TestMessage::CalculateLoot { teef: 100, victory_points: 5 }).await {
                Ok(response) => info!("SYNC OK (loot calc): {:?}", response),
                Err(e) => error!("SYNC ERR: {:?}", e),
            }
            // ASYNC TESTS
            let async_config = PythonConfig {
                python_path: python_path_vec.clone(),
                module_name: "ork_logic_async".to_string(),
                function_name: "handle_message_async".to_string(),
                env_vars: vec![],
                is_async: true,
                module_path: "crates/kameo-snake-testing/python/ork_logic_async.py".to_string(),
            };
            let async_ref = PythonChildProcessBuilder::<TestCallbackMessage>::new(async_config).spawn::<TestMessage>().await?;
            // Test 1: Valid message (async)
            info!("Test 1: Valid message (async)");
            match async_ref.ask(TestMessage::CalculatePower { boyz_count: 100 }).await {
                Ok(response) => info!("ASYNC OK: {:?}", response),
                Err(e) => error!("ASYNC ERR: {:?}", e),
            }
            // Test 2: Invalid message (unknown klan)
            info!("Test 2: Invalid message - unknown klan (async)");
            match async_ref.ask(TestMessage::CalculateKlanBonus { klan_name: "UnknownKlan".to_string(), base_power: 0 }).await {
                Ok(response) => error!("ASYNC FAILED (should error): {:?}", response),
                Err(e) => info!("ASYNC ERR (expected): {:?}", e),
            }
            // Test 3: Edge case - zero boyz
            info!("Test 3: Edge case - zero boyz (async)");
            match async_ref.ask(TestMessage::CalculatePower { boyz_count: 0 }).await {
                Ok(response) => info!("ASYNC OK (zero boyz): {:?}", response),
                Err(e) => error!("ASYNC ERR: {:?}", e),
            }
            // Test 4: Edge case - massive number
            info!("Test 4: Edge case - massive number (async)");
            match async_ref.ask(TestMessage::CalculatePower { boyz_count: u32::MAX }).await {
                Ok(response) => error!("ASYNC FAILED (should error): {:?}", response),
                Err(e) => info!("ASYNC ERR (expected): {:?}", e),
            }
            // Test 5: Scrap result test
            info!("Test 5: Scrap result test (async)");
            match async_ref.ask(TestMessage::CalculateScrapResult { attacker_power: 1000, defender_power: 500 }).await {
                Ok(response) => info!("ASYNC OK (scrap result): {:?}", response),
                Err(e) => error!("ASYNC ERR: {:?}", e),
            }
            // Test 6: Loot calculation
            info!("Test 6: Loot calculation (async)");
            match async_ref.ask(TestMessage::CalculateLoot { teef: 100, victory_points: 5 }).await {
                Ok(response) => info!("ASYNC OK (loot calc): {:?}", response),
                Err(e) => error!("ASYNC ERR: {:?}", e),
            }
            // Test 7: Rapid fire messages (async)
            info!("Test 7: Rapid fire messages (async)");
            let mut handles = Vec::new();
            for i in 0..10 {
                let actor_ref_async = async_ref.clone();
                handles.push(tokio::spawn(async move {
                    match actor_ref_async.ask(TestMessage::CalculatePower { boyz_count: i * 100 }).await {
                        Ok(response) => info!("ASYNC OK (rapid fire {}): {:?}", i, response),
                        Err(e) => error!("ASYNC ERR (rapid fire {}): {:?}", i, e),
                    }
                }));
            }
            for handle in handles {
                handle.await?;
            }
            Ok::<(), Box<dyn std::error::Error>>(())
        })
    }
}

async fn run_sync_tests(python_path: Vec<String>) -> Result<(), Box<dyn std::error::Error>> {
    let sync_config = PythonConfig {
        python_path: python_path.clone(),
        module_name: "ork_logic".to_string(),
        function_name: "handle_message".to_string(),
        env_vars: vec![],
        is_async: false,
        module_path: "crates/kameo-snake-testing/python/ork_logic.py".to_string(),
    };
    let sync_ref = PythonChildProcessBuilder::<TestCallbackMessage>::new(sync_config).spawn::<TestMessage>().await?;
    
    // Test 1: Valid message
    info!("Test 1: Valid message (sync)");
    match sync_ref.ask(TestMessage::CalculatePower { boyz_count: 100 }).await {
        Ok(response) => info!("SYNC OK: {:?}", response),
        Err(e) => error!("SYNC ERR: {:?}", e),
    }

    // Test 2: Invalid message (unknown klan)
    info!("Test 2: Invalid message - unknown klan (sync)");
    match sync_ref.ask(TestMessage::CalculateKlanBonus { klan_name: "UnknownKlan".to_string(), base_power: 0 }).await {
        Ok(response) => error!("SYNC FAILED (should error): {:?}", response),
        Err(e) => info!("SYNC ERR (expected): {:?}", e),
    }

    // Test 3: Edge case - zero boyz
    info!("Test 3: Edge case - zero boyz (sync)");
    match sync_ref.ask(TestMessage::CalculatePower { boyz_count: 0 }).await {
        Ok(response) => info!("SYNC OK (zero boyz): {:?}", response),
        Err(e) => error!("SYNC ERR: {:?}", e),
    }

    // Test 4: Edge case - massive number
    info!("Test 4: Edge case - massive number (sync)");
    match sync_ref.ask(TestMessage::CalculatePower { boyz_count: u32::MAX }).await {
        Ok(response) => error!("SYNC FAILED (should error): {:?}", response),
        Err(e) => info!("SYNC ERR (expected): {:?}", e),
    }

    // Test 5: Scrap result test
    info!("Test 5: Scrap result test (sync)");
    match sync_ref.ask(TestMessage::CalculateScrapResult { attacker_power: 1000, defender_power: 500 }).await {
        Ok(response) => info!("SYNC OK (scrap result): {:?}", response),
        Err(e) => error!("SYNC ERR: {:?}", e),
    }

    // Test 6: Loot calculation
    info!("Test 6: Loot calculation (sync)");
    match sync_ref.ask(TestMessage::CalculateLoot { teef: 100, victory_points: 5 }).await {
        Ok(response) => info!("SYNC OK (loot calc): {:?}", response),
        Err(e) => error!("SYNC ERR: {:?}", e),
    }
    
    Ok(())
}

async fn run_async_tests(python_path: Vec<String>) -> Result<(), Box<dyn std::error::Error>> {
    info!("==== ASYNC FLOW ====");
    let async_config = PythonConfig {
        python_path: python_path.clone(),
        module_name: "ork_logic_async".to_string(),
        function_name: "handle_message_async".to_string(),
        env_vars: vec![],
        is_async: true,
        module_path: "crates/kameo-snake-testing/python/ork_logic_async.py".to_string(),
    };
    let async_ref = PythonChildProcessBuilder::<TestCallbackMessage>::new(async_config).spawn::<TestMessage>().await?;
    
    // Test 1: Valid message
    info!("Test 1: Valid message (async)");
    match async_ref.ask(TestMessage::CalculatePower { boyz_count: 100 }).await {
        Ok(response) => info!("ASYNC OK: {:?}", response),
        Err(e) => error!("ASYNC ERR: {:?}", e),
    }

    // Test 2: Invalid message (unknown klan)
    info!("Test 2: Invalid message - unknown klan (async)");
    match async_ref.ask(TestMessage::CalculateKlanBonus { klan_name: "UnknownKlan".to_string(), base_power: 0 }).await {
        Ok(response) => error!("ASYNC FAILED (should error): {:?}", response),
        Err(e) => info!("ASYNC ERR (expected): {:?}", e),
    }

    // Test 3: Edge case - zero boyz
    info!("Test 3: Edge case - zero boyz (async)");
    match async_ref.ask(TestMessage::CalculatePower { boyz_count: 0 }).await {
        Ok(response) => info!("ASYNC OK (zero boyz): {:?}", response),
        Err(e) => error!("ASYNC ERR: {:?}", e),
    }

    // Test 4: Edge case - massive number
    info!("Test 4: Edge case - massive number (async)");
    match async_ref.ask(TestMessage::CalculatePower { boyz_count: u32::MAX }).await {
        Ok(response) => error!("ASYNC FAILED (should error): {:?}", response),
        Err(e) => info!("ASYNC ERR (expected): {:?}", e),
    }

    // Test 5: Scrap result test
    info!("Test 5: Scrap result test (async)");
    match async_ref.ask(TestMessage::CalculateScrapResult { attacker_power: 1000, defender_power: 500 }).await {
        Ok(response) => info!("ASYNC OK (scrap result): {:?}", response),
        Err(e) => error!("ASYNC ERR: {:?}", e),
    }

    // Test 6: Loot calculation
    info!("Test 6: Loot calculation (async)");
    match async_ref.ask(TestMessage::CalculateLoot { teef: 100, victory_points: 5 }).await {
        Ok(response) => info!("ASYNC OK (loot calc): {:?}", response),
        Err(e) => error!("ASYNC ERR: {:?}", e),
    }

    // Test 7: Rapid fire messages (stress test)
    info!("Test 7: Rapid fire messages (async)");
    let mut handles = Vec::new();
    for i in 0..10 {
        let ref_clone = async_ref.clone();
        handles.push(tokio::spawn(async move {
            match ref_clone.ask(TestMessage::CalculatePower { boyz_count: i * 100 }).await {
                Ok(response) => info!("ASYNC OK (rapid fire {}): {:?}", i, response),
                Err(e) => error!("ASYNC ERR (rapid fire {}): {:?}", i, e),
            }
        }));
    }
    for handle in handles {
        handle.await?;
    }

    Ok(())
}

async fn run_invalid_config_tests(python_path: Vec<String>) -> Result<(), Box<dyn std::error::Error>> {
    // Test 8: Invalid module test
    info!("Test 8: Invalid module test");
    let invalid_module_config = PythonConfig {
        python_path: python_path.clone(),
        module_name: "non_existent_module".to_string(),
        function_name: "handle_message".to_string(),
        env_vars: vec![],
        is_async: false,
        module_path: "crates/kameo-snake-testing/python/non_existent_module.py".to_string(),
    };
    
    let spawn_result = PythonChildProcessBuilder::<TestCallbackMessage>::new(invalid_module_config).spawn::<TestMessage>().await;
    match spawn_result {
        Ok(_) => panic!("Spawning with invalid module should fail"),
        Err(e) => info!("Received expected error on spawn: {}", e),
    }

    // Test 9: Invalid function test
    info!("Test 9: Invalid function test");
    let invalid_function_config = PythonConfig {
        python_path: python_path.clone(),
        module_name: "ork_logic".to_string(),
        function_name: "non_existent_function".to_string(),
        env_vars: vec![],
        is_async: false,
        module_path: "crates/kameo-snake-testing/python/ork_logic.py".to_string(),
    };
    let spawn_result = PythonChildProcessBuilder::<TestCallbackMessage>::new(invalid_function_config).spawn::<TestMessage>().await;
    match spawn_result {
        Ok(_) => panic!("Spawning with invalid function should fail"),
        Err(e) => info!("Received expected error on spawn: {}", e),
    }


    // Test 10: Invalid path test
    info!("Test 10: Invalid path test");
    let invalid_path_config = PythonConfig {
        python_path: python_path.clone(),
        module_name: "ork_logic".to_string(),
        function_name: "handle_message".to_string(),
        env_vars: vec![],
        is_async: false,
        module_path: "crates/kameo-snake-testing/python/ork_logic.py".to_string(),
    };
    let spawn_result = PythonChildProcessBuilder::<TestCallbackMessage>::new(invalid_path_config).spawn::<TestMessage>().await;
    match spawn_result {
        Ok(_) => panic!("Spawning with invalid path should fail"),
        Err(e) => info!("Received expected error on spawn: {}", e),
    }

    Ok(())
}

