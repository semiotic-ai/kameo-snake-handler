use bincode::{Decode, Encode};
use kameo::reply::Reply;
use kameo_child_process::KameoChildProcessMessage;
use kameo_snake_handler::prelude::*;
use kameo_snake_handler::ErrorReply;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{error, info};
use kameo_child_process::ChildCallbackMessage;
use kameo_snake_handler::declare_callback_glue;

/// Custom error type for logic operations
#[derive(Debug, Error, Serialize, Deserialize, Clone, Decode, Encode)]
pub enum TestError {
    #[error("Not enough entities for operation (need {needed}, got {got})")]
    NotEnoughEntities { needed: i32, got: i32 },
    #[error("Unknown category: {0}")]
    UnknownCategory(String),
    #[error("Invalid power level: {0}")]
    InvalidPower(String),
    #[error("Python error: {0}")]
    PythonError(String),
}

/// Message types that can be sent to Python subprocess
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub enum TestMessage {
    CalculatePower {
        count: u32,
    },
    CalculateCategoryBonus {
        category_name: String,
        base_power: u32,
    },
    CalculateCompetitionResult {
        attacker_power: u32,
        defender_power: u32,
    },
    CalculateReward {
        currency: u32,
        points: u32,
    },
    CallbackRoundtrip { value: u32 },
}

impl Default for TestMessage {
    fn default() -> Self {
        Self::CalculatePower { count: 0 }
    }
}

/// Response types from Python subprocess
#[derive(Debug, Serialize, Deserialize, Clone, Decode, Encode)]
pub enum TestResponse {
    Power { power: u32 },
    CategoryBonus { bonus: u32 },
    CompetitionResult { victory: bool },
    RewardResult { total_currency: u32, bonus_currency: u32 },
    Error { error: String },
    CallbackRoundtripResult { value: u32 },
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

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct TestCallbackMessage {
    pub value: u32,
}

impl ChildCallbackMessage for TestCallbackMessage {
    type Reply = u32;
}
 
// Define a real callback handler for the test
pub struct TestCallbackHandler;

#[async_trait::async_trait]
impl kameo_child_process::CallbackHandler<TestCallbackMessage> for TestCallbackHandler {
    async fn handle(&mut self, callback: TestCallbackMessage) -> u32 {
        tracing::info!(event = "callback_roundtrip", value = callback.value, "Received callback in Rust");
        callback.value + 1
    }
}

// --- DSPy Trader Demo Types ---
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub enum TraderMessage {
    OrderDetails { item: String, currency: u32 },
}

#[derive(Debug, Serialize, Deserialize, Clone, Decode, Encode)]
pub enum TraderResponse {
    OrderResult { result: String },
    Error { error: String },
}

impl ErrorReply for TraderResponse {
    fn from_error(err: PythonExecutionError) -> Self {
        TraderResponse::Error {
            error: err.to_string(),
        }
    }
}

impl Reply for TraderResponse {
    type Ok = Self;
    type Error = TestError;
    type Value = Self;

    fn to_result(self) -> Result<Self::Ok, <Self as Reply>::Error> {
        match self {
            TraderResponse::Error { error } => Err(TestError::PythonError(error)),
            _ => Ok(self),
        }
    }

    fn into_any_err(self) -> Option<Box<dyn kameo::reply::ReplyError>> {
        match self {
            TraderResponse::Error { error } => Some(Box::new(TestError::PythonError(error))),
            _ => None,
        }
    }

    fn into_value(self) -> Self::Value {
        self
    }
}

impl KameoChildProcessMessage for TraderMessage {
    type Reply = TraderResponse;
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct TraderCallbackMessage {
    pub value: u32,
}

impl ChildCallbackMessage for TraderCallbackMessage {
    type Reply = u32;
}

pub struct TraderCallbackHandler;

#[async_trait::async_trait]
impl kameo_child_process::CallbackHandler<TraderCallbackMessage> for TraderCallbackHandler {
    async fn handle(&mut self, callback: TraderCallbackMessage) -> u32 {
        tracing::info!(event = "trader_callback", value = callback.value, "Received trader callback in Rust");
        callback.value + 10
    }
}

kameo_snake_handler::setup_python_subprocess_system! {
    actors = {
        (PythonActor<TestMessage, TestCallbackMessage>, TestMessage, TestCallbackMessage),
        (PythonActor<TraderMessage, TraderCallbackMessage>, TraderMessage, TraderCallbackMessage),
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
            flavor: kameo_child_process::RuntimeFlavor::MultiThread ,
            worker_threads: Some(2),
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
            .thread_name("test-main")
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
            run_sync_tests(python_path_vec.clone()).await?;
            run_async_tests(python_path_vec.clone()).await?;
            run_invalid_config_tests(python_path_vec.clone()).await?;
            run_trader_demo(python_path_vec.clone()).await?;
            Ok::<(), Box<dyn std::error::Error>>(())
        })?
    }
}

#[tracing::instrument]
async fn run_sync_tests(python_path: Vec<String>) -> Result<(), Box<dyn std::error::Error>> {
    let sync_config = PythonConfig {
        python_path: python_path.clone(),
        module_name: "logic".to_string(),
        function_name: "handle_message".to_string(),
        env_vars: vec![],
        is_async: false,
        module_path: "crates/kameo-snake-testing/python/logic.py".to_string(),
    };
    tracing::trace!(event = "test_spawn", step = "before_spawn", "About to spawn Python child process");
    let sync_ref = PythonChildProcessBuilder::<TestCallbackMessage>::new(sync_config).spawn::<TestMessage>().await?;
    tracing::trace!(event = "test_spawn", step = "after_spawn", "Returned from spawn, about to send first .ask()");
    tracing::trace!(event = "test_send", step = "before_ask", "About to send first sync .ask() to child actor");
    let resp = sync_ref.ask(TestMessage::CalculatePower { count: 100 }).await;
    tracing::trace!(event = "test_send", step = "after_ask", ?resp, "Received response from first sync .ask() to child actor");
    assert!(matches!(resp, Ok(TestResponse::Power { .. })), "SYNC Test 1 failed: got {:?}", resp);

    // Test 2: Invalid message (unknown category)
    let resp = sync_ref.ask(TestMessage::CalculateCategoryBonus { category_name: "UnknownCategory".to_string(), base_power: 0 }).await;
    assert!(resp.is_err(), "SYNC Test 2 should error, got {:?}", resp);

    // Test 3: Edge case - zero count
    let resp = sync_ref.ask(TestMessage::CalculatePower { count: 0 }).await;
    assert!(resp.is_err(), "SYNC Test 3 should error, got {:?}", resp);

    // Test 4: Edge case - massive number
    let resp = sync_ref.ask(TestMessage::CalculatePower { count: u32::MAX }).await;
    assert!(resp.is_err(), "SYNC Test 4 should error, got {:?}", resp);

    // Test 5: Competition result test
    let resp = sync_ref.ask(TestMessage::CalculateCompetitionResult { attacker_power: 1000, defender_power: 500 }).await;
    assert!(matches!(resp, Ok(TestResponse::CompetitionResult { .. })), "SYNC Test 5 failed: got {:?}", resp);

    // Test 6: Reward calculation
    let resp = sync_ref.ask(TestMessage::CalculateReward { currency: 100, points: 5 }).await;
    assert!(matches!(resp, Ok(TestResponse::RewardResult { .. })), "SYNC Test 6 failed: got {:?}", resp);
    
    Ok(())
}

async fn run_async_tests(python_path: Vec<String>) -> Result<(), Box<dyn std::error::Error>> {
    info!("==== ASYNC FLOW ====");
    let async_config = PythonConfig {
        python_path: python_path.clone(),
        module_name: "logic_async".to_string(),
        function_name: "handle_message_async".to_string(),
        env_vars: vec![],
        is_async: true,
        module_path: "crates/kameo-snake-testing/python/logic_async.py".to_string(),
    };
    let async_ref = PythonChildProcessBuilder::<TestCallbackMessage>::new(async_config)
        .with_callback_handler(TestCallbackHandler)
        .spawn::<TestMessage>().await?;
    
    // Test 1: Valid message
    let resp = async_ref.ask(TestMessage::CalculatePower { count: 100 }).await;
    assert!(matches!(resp, Ok(TestResponse::Power { .. })), "ASYNC Test 1 failed: got {:?}", resp);

    // Test 2: Invalid message (unknown category)
    let resp = async_ref.ask(TestMessage::CalculateCategoryBonus { category_name: "UnknownCategory".to_string(), base_power: 0 }).await;
    assert!(resp.is_err(), "ASYNC Test 2 should error, got {:?}", resp);

    // Test 3: Edge case - zero count
    let resp = async_ref.ask(TestMessage::CalculatePower { count: 0 }).await;
    assert!(resp.is_err(), "ASYNC Test 3 should error, got {:?}", resp);

    // Test 4: Edge case - massive number
    let resp = async_ref.ask(TestMessage::CalculatePower { count: u32::MAX }).await;
    assert!(resp.is_err(), "ASYNC Test 4 should error, got {:?}", resp);

    // Test 5: Competition result test
    let resp = async_ref.ask(TestMessage::CalculateCompetitionResult { attacker_power: 1000, defender_power: 500 }).await;
    assert!(matches!(resp, Ok(TestResponse::CompetitionResult { .. })), "ASYNC Test 5 failed: got {:?}", resp);

    // Test 6: Reward calculation
    let resp = async_ref.ask(TestMessage::CalculateReward { currency: 100, points: 5 }).await;
    assert!(matches!(resp, Ok(TestResponse::RewardResult { .. })), "ASYNC Test 6 failed: got {:?}", resp);

    // Test: Callback roundtrip
    let resp = async_ref.ask(TestMessage::CallbackRoundtrip { value: 42 }).await;
    match resp {
        Ok(TestResponse::CallbackRoundtripResult { value }) => {
            assert_eq!(value, 43, "Callback roundtrip value should be incremented");
        }
        Ok(other) => panic!("ASYNC callback roundtrip: Unexpected response: {:?}", other),
        Err(e) => panic!("ASYNC callback roundtrip failed: {:?}", e),
    }

    // Test 7: Rapid fire messages (stress test)
    let mut handles = Vec::new();
    for i in 0..10 {
        if i == 0 { continue; } // skip count=0, which is always an error
        let ref_clone = async_ref.clone();
        handles.push(tokio::spawn(async move {
            ref_clone.ask(TestMessage::CalculatePower { count: i * 100 }).await
        }));
    }
    let results = futures::future::join_all(handles).await;
    for (i, res) in results.into_iter().enumerate() {
        let resp = res.expect("Task panicked");
        assert!(matches!(resp, Ok(TestResponse::Power { .. })), "Rapid fire {} failed: {:?}", i + 1, resp);
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
        Ok(_actor_ref) => panic!("Spawning with invalid module should fail"),
        Err(e) => info!("Received expected error on spawn: {}", e),
    }

    // Test 9: Invalid function test
    info!("Test 9: Invalid function test");
    let invalid_function_config = PythonConfig {
        python_path: python_path.clone(),
        module_name: "logic".to_string(),
        function_name: "non_existent_function".to_string(),
        env_vars: vec![],
        is_async: false,
        module_path: "crates/kameo-snake-testing/python/logic.py".to_string(),
    };
    let spawn_result = PythonChildProcessBuilder::<TestCallbackMessage>::new(invalid_function_config).spawn::<TestMessage>().await;
    match spawn_result {
        Ok(_actor_ref) => panic!("Spawning with invalid function should fail"),
        Err(e) => info!("Received expected error on spawn: {}", e),
    }


    // Test 10: Invalid path test
    info!("Test 10: Invalid path test");
    let invalid_path_config = PythonConfig {
        python_path: python_path.clone(),
        module_name: "logic".to_string(),
        function_name: "handle_message".to_string(),
        env_vars: vec![],
        is_async: false,
        module_path: "crates/kameo-snake-testing/python/logic.py".to_string(),
    };
    let spawn_result = PythonChildProcessBuilder::<TestCallbackMessage>::new(invalid_path_config).spawn::<TestMessage>().await;
    match spawn_result {
        Ok(_actor_ref) => panic!("Spawning with invalid path should fail"),
        Err(e) => info!("Received expected error on spawn: {}", e),
    }

    Ok(())
}

#[tracing::instrument]
async fn run_trader_demo(python_path: Vec<String>) -> Result<(), Box<dyn std::error::Error>> {
    let trader_config = PythonConfig {
        python_path: python_path.clone(),
        module_name: "dspy_trader".to_string(),
        function_name: "handle_message".to_string(),
        env_vars: vec![],
        is_async: true,
        module_path: "crates/kameo-snake-testing/python/dspy_trader.py".to_string(),
    };
    let trader_ref = PythonChildProcessBuilder::<TraderCallbackMessage>::new(trader_config)
        .with_callback_handler(TraderCallbackHandler)
        .spawn::<TraderMessage>().await?;
    let resp = trader_ref.ask(TraderMessage::OrderDetails { item: "widget".to_string(), currency: 42 }).await;
    tracing::info!(?resp, "Trader demo response");
    assert!(matches!(resp, Ok(TraderResponse::OrderResult { .. })), "Trader demo failed: got {:?}", resp);
    Ok(())
}

