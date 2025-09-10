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

/// Initialize the Python logging -> Rust tracing bridge.
///
/// Call this while holding the GIL, ideally before importing user modules that may configure logging.
#[inline]
pub fn setup_python_logging_bridge(py: pyo3::Python<'_>) -> pyo3::PyResult<()> {
    // Initialize the base bridge first
    tracing_for_pyo3_logging::setup_logging(py)?;

    // Apply a safe, idempotent monkeypatch to sanitize logging extras that collide with
    // LogRecord reserved attributes (e.g., 'message'), which otherwise cause KeyError.
    // This prevents crashes when user code logs with reserved keys, especially under concurrency.
    let patch_code = r#"
import logging

if not getattr(logging, '_kameo_makeRecord_patched', False):
    _orig_makeRecord = logging.Logger.makeRecord

    try:
        _reserved = set(vars(logging.LogRecord(__name__, logging.INFO, __file__, 0, '', (), None)))
    except Exception:
        _reserved = {
            'name','msg','args','levelname','levelno','pathname','filename','module',
            'exc_info','exc_text','stack_info','lineno','funcName','created','msecs',
            'relativeCreated','thread','threadName','process','processName'
        }

    # Ensure 'message' is considered reserved even if not present in the default dict yet
    _reserved.add('message')

    def _kameo_makeRecord(self, name, level, fn, lno, msg, args, exc_info, func=None, extra=None, sinfo=None, **kwargs):
        if extra:
            moved = {}
            sanitized = {}
            for k, v in extra.items():
                if k in _reserved:
                    moved[k] = v
                else:
                    sanitized[k] = v
            if moved:
                # Preserve conflicting keys under a namespaced field rather than dropping
                sanitized.setdefault('py_reserved', moved)
            extra = sanitized
        return _orig_makeRecord(self, name, level, fn, lno, msg, args, exc_info, func=func, extra=extra, sinfo=sinfo, **kwargs)

    logging.Logger.makeRecord = _kameo_makeRecord
    logging._kameo_makeRecord_patched = True
"#;

    py.run(std::ffi::CString::new(patch_code).unwrap().as_c_str(), None, None)?;

    // Deduplicate output: remove Python StreamHandlers so logs don't print directly to stdout/stderr
    // and ensure propagation so all library logs reach the root and the Rust bridge handler.
    let dedup_code = r#"
import logging

if not getattr(logging, '_kameo_dedup_handlers', False):
    root = logging.getLogger()
    # Ensure root level is DEBUG so the Rust bridge can capture everything and rely on Rust filtering
    try:
        root.setLevel(logging.DEBUG)
    except Exception:
        pass

    # Ensure all named loggers propagate up to root (so the bridge handler sees them)
    try:
        for name, logger in list(logging.root.manager.loggerDict.items()):
            if isinstance(logger, logging.Logger):
                logger.propagate = True
    except Exception:
        pass

    # Remove direct StreamHandlers to avoid duplicate console prints from Python side
    def _is_stream_handler(h):
        try:
            return isinstance(h, logging.StreamHandler)
        except Exception:
            return False

    try:
        # Root
        for h in list(getattr(root, 'handlers', ())):
            if _is_stream_handler(h):
                root.removeHandler(h)
        # Named loggers
        for logger in [l for l in logging.root.manager.loggerDict.values() if isinstance(l, logging.Logger)]:
            for h in list(getattr(logger, 'handlers', ())):
                if _is_stream_handler(h):
                    logger.removeHandler(h)
    except Exception:
        pass

    logging._kameo_dedup_handlers = True
"#;
    py.run(std::ffi::CString::new(dedup_code).unwrap().as_c_str(), None, None)?;
    Ok(())
}
