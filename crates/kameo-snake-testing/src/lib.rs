use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Stdio;
use tokio::process::Command;
use tokio::io::{BufReader, AsyncBufReadExt};
use kameo::prelude::*;
use kameo_snake_handler::{PythonActor, PythonConfig, PythonExecutionError};
use kameo_child_process::KameoChildProcessMessage;
use serde::{Deserialize, Serialize};
use tracing::{debug, error, info, warn};
use bincode::{Decode, Encode};

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
#[serde(tag = "type")]
pub enum OrkCommand {
    KlanBonus { power: i32, klan: String },
    ScrapResult { boy1_power: i32, boy2_power: i32 },
    LootTeef { base_teef: i32 },
    WaaaghPower { boyz_count: i32 },
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

    fn into_any_err(self) -> Option<Box<dyn ReplyError>> {
        Some(Box::new(PythonExecutionError::Execution(self.message.clone())))
    }

    fn into_value(self) -> Self::Value {
        self
    }
}

impl KameoChildProcessMessage for OrkCommand {
    type Reply = OrkResult;
}

impl From<Result<OrkResult, PythonExecutionError>> for OrkResult {
    fn from(result: Result<OrkResult, PythonExecutionError>) -> Self {
        match result {
            Ok(result) => result,
            Err(e) => OrkResult {
                result: 0,
                message: format!("Error: {}", e),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::runtime::Runtime;

    fn create_python_actor() -> PythonActor<OrkCommand> {
        PythonActor::new(PythonConfig {
            python_path: vec!["python".to_string()],
            env_vars: vec![],
            module_name: "ork_logic".to_string(),
            function_name: "handle_message".to_string(),
        })
    }

    #[test]
    fn test_klan_bonus() {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let actor = create_python_actor();
            let actor_ref = kameo::spawn(actor);
            
            let cmd = OrkCommand::KlanBonus {
                power: 100,
                klan: "Goffs".to_string(),
            };
            
            let result = actor_ref.ask(cmd).await.unwrap();
            assert!(result.result > 100); // Should have some bonus
            println!("Klan bonus result: {:?}", result);
        });
    }

    #[test]
    fn test_scrap_result() {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let actor = create_python_actor();
            let actor_ref = kameo::spawn(actor);
            
            let cmd = OrkCommand::ScrapResult {
                boy1_power: 100,
                boy2_power: 90,
            };
            
            let result = actor_ref.ask(cmd).await.unwrap();
            assert!(result.result == 1 || result.result == 2); // Should be either boy 1 or 2
            println!("Scrap result: {:?}", result);
        });
    }

    #[test]
    fn test_loot_teef() {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let actor = create_python_actor();
            let actor_ref = kameo::spawn(actor);
            
            let cmd = OrkCommand::LootTeef {
                base_teef: 100,
            };
            
            let result = actor_ref.ask(cmd).await.unwrap();
            println!("Loot result: {:?}", result);
        });
    }

    #[test]
    fn test_waaagh_power() {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let actor = create_python_actor();
            let actor_ref = kameo::spawn(actor);
            
            let cmd = OrkCommand::WaaaghPower {
                boyz_count: 10,
            };
            
            let result = actor_ref.ask(cmd).await.unwrap();
            assert!(result.result > 1000); // Should be significant WAAAGH! power
            println!("WAAAGH! power result: {:?}", result);
        });
    }
} 