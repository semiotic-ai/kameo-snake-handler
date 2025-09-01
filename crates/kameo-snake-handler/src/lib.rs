//! # Kameo Snake Handler - Python Integration Library
//!
//! This library provides seamless integration between Rust and Python subprocesses using
//! the Kameo actor system and the unified streaming IPC protocol. It enables Rust applications
//! to spawn Python subprocesses and communicate with them using both synchronous and streaming
//! message patterns.
//!
//! ## Architecture Overview
//!
//! The library builds on top of `kameo-child-process` and adds Python-specific functionality:
//!
//! ### Core Components
//! - **PythonActor**: Kameo actor that handles Python subprocess communication
//! - **PythonChildProcessBuilder**: Builder for spawning Python child processes
//! - **PythonMessageHandler**: Handler for Python function execution
//! - **Serde Integration**: Bidirectional serialization between Rust and Python
//!
//! ### Streaming Support
//! The library fully supports the unified streaming protocol:
//! - **Sync Python Functions**: Return single values, converted to single-item streams
//! - **Async Python Generators**: Return multiple values as native streams
//! - **Error Handling**: Python exceptions properly converted to Rust errors
//! - **Backward Compatibility**: Existing sync code continues to work
//!
//! ### Python Integration Features
//! - **Async Runtime**: Full async Python support with `pyo3-async-runtimes`
//! - **Type Conversion**: Automatic conversion between Rust and Python types
//! - **Environment Management**: Configurable Python paths and environment variables
//! - **Process Management**: Automatic lifecycle management of Python subprocesses
//!
//! ## Usage Example
//!
//! ```rust,ignore
//! use kameo_snake_handler::prelude::*;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Configure Python subprocess
//!     let config = PythonConfig {
//!         python_path: vec!["/path/to/python".to_string()],
//!         module_name: "my_module".to_string(),
//!         function_name: "my_function".to_string(),
//!         env_vars: vec![("PYTHONPATH".to_string(), "/path/to/modules".to_string())],
//!         is_async: false,
//!     };
//!
//!     // Spawn Python subprocess pool
//!     let pool = PythonChildProcessBuilder::<MyMessage, MyCallback>::new(config)
//!         .spawn_pool(4, None)
//!         .await?;
//!
//!     // Send sync message
//!     let actor = pool.get_actor();
//!     let response = actor.ask(MyMessage { data: "hello" }).await?;
//!
//!     // Send streaming message
//!     let stream = actor.send_stream(MyMessage { data: "stream" }).await?;
//!     while let Some(item) = stream.next().await {
//!         println!("Python response: {:?}", item?);
//!     }
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Python Side Implementation
//!
//! ```python
//! # my_module.py
//! import asyncio
//! from typing import AsyncGenerator, Dict, Any
//!
//! async def my_function(message: Dict[str, Any]) -> AsyncGenerator[Dict[str, Any], None]:
//!     # Streaming response
//!     for i in range(5):
//!         yield {"index": i, "data": message["data"]}
//!     
//!     # Or for sync response:
//!     # return {"result": "done"}
//! ```

pub mod serde_py;
pub use serde_py::{from_pyobject, to_pyobject, FromPyAny};

mod error;
pub use error::ErrorReply;
pub use kameo_child_process::error::PythonExecutionError;

mod builder;
pub use builder::PythonChildProcessBuilder;

mod actor;
pub use actor::{child_process_main_with_python_actor, PythonActor, PythonConfig};

#[doc(hidden)]
pub mod macros;

// Internal support modules now live under `macros::` and are not re-exported

pub mod telemetry;
pub mod tracing_utils;

pub use crate::actor::PythonMessageHandler;

// Experimental: static Python code generation (exposed for tests and tooling)
pub mod codegen_py;

#[tracing::instrument(skip(builder), name = "setup_python_runtime")]
pub fn setup_python_runtime(builder: tokio::runtime::Builder) {
    pyo3::prepare_freethreaded_python();
    pyo3_async_runtimes::tokio::init(builder);
}

pub mod prelude {
    pub use super::{PythonExecutionError};
    pub use super::{setup_python_runtime, PythonActor, PythonChildProcessBuilder, PythonConfig};
}
