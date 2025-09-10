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

use pyo3::prelude::*;

#[pyfunction]
fn _kameo_emit_log(
    logger: &str,
    levelno: u32,
    message: &str,
    pathname: &str,
    lineno: u32,
) -> PyResult<()> {
    if levelno >= 50 {
        tracing::error!(target: "py", logger = %logger, pathname = %pathname, lineno = lineno, message = %message);
    } else if levelno >= 40 {
        tracing::error!(target: "py", logger = %logger, pathname = %pathname, lineno = lineno, message = %message);
    } else if levelno >= 30 {
        tracing::warn!(target: "py", logger = %logger, pathname = %pathname, lineno = lineno, message = %message);
    } else if levelno >= 20 {
        tracing::info!(target: "py", logger = %logger, pathname = %pathname, lineno = lineno, message = %message);
    } else if levelno >= 10 {
        tracing::debug!(target: "py", logger = %logger, pathname = %pathname, lineno = lineno, message = %message);
    } else {
        tracing::trace!(target: "py", logger = %logger, pathname = %pathname, lineno = lineno, message = %message);
    }
    Ok(())
}

fn install_kameo_rs_logging_module(py: Python<'_>) -> PyResult<()> {
    let m = PyModule::new(py, "kameo_rs_logging")?;
    m.add_function(pyo3::wrap_pyfunction!(_kameo_emit_log, &m)?)?;
    let sys = py.import("sys")?;
    let modules = sys.getattr("modules")?;
    modules.set_item("kameo_rs_logging", m)?;
    Ok(())
}

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
    // This prevents crashes when user code logs with reserved keys
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

    // Deduplicate output: remove Python StreamHandlers so logs don't print directly to stdout/stderr,
    // ensure propagation, and block future basicConfig/addHandler attempts from re-adding them.
    let dedup_code = r#"
import logging, sys

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

    # Mark all existing handlers for preservation (installed by the bridge/setup phase)
    try:
        for logger in [root] + [l for l in logging.root.manager.loggerDict.values() if isinstance(l, logging.Logger)]:
            for h in list(getattr(logger, 'handlers', ())) :
                try:
                    setattr(h, '_kameo_preserve', True)
                except Exception:
                    pass
    except Exception:
        pass

    # Remove direct console StreamHandlers; keep preserved and bridge handlers
    def _is_stream_handler(h):
        try:
            return isinstance(h, logging.StreamHandler)
        except Exception:
            return False

    # Prevent future basicConfig() calls from installing StreamHandlers that print to console
    try:
        if not getattr(logging, '_kameo_basicConfig_patched', False):
            _orig_basicConfig = logging.basicConfig

            def _kameo_basicConfig(*args, **kwargs):
                # Only honor level; suppress handler installation
                level = kwargs.get('level', None)
                try:
                    if level is not None:
                        logging.getLogger().setLevel(level)
                except Exception:
                    pass
                # Do NOT add handlers; users' configs must flow to Rust via the bridge
                return None

            logging.basicConfig = _kameo_basicConfig
            logging._kameo_basicConfig_patched = True
    except Exception:
        pass

    try:
        # Root
        for h in list(getattr(root, 'handlers', ())) :
            try:
                if getattr(h, '_kameo_preserve', False):
                    continue
                if _is_stream_handler(h):
                    stream = getattr(h, 'stream', None)
                    if stream in (getattr(sys, 'stdout', None), getattr(sys, 'stderr', None)):
                        root.removeHandler(h)
            except Exception:
                pass
        # Named loggers
        for logger in [l for l in logging.root.manager.loggerDict.values() if isinstance(l, logging.Logger)]:
            for h in list(getattr(logger, 'handlers', ())) :
                try:
                    if getattr(h, '_kameo_preserve', False):
                        continue
                    if _is_stream_handler(h):
                        stream = getattr(h, 'stream', None)
                        if stream in (getattr(sys, 'stdout', None), getattr(sys, 'stderr', None)):
                            logger.removeHandler(h)
                except Exception:
                    pass
    except Exception:
        pass

    # Block future additions of console StreamHandlers; preserve marked handlers
    try:
        if not getattr(logging, '_kameo_addHandler_patched', False):
            _orig_addHandler = logging.Logger.addHandler

            def _kameo_addHandler(self, h):
                try:
                    if getattr(h, '_kameo_preserve', False):
                        return _orig_addHandler(self, h)
                    is_stream = isinstance(h, logging.StreamHandler)
                except Exception:
                    is_stream = False
                if is_stream:
                    try:
                        stream = getattr(h, 'stream', None)
                        if stream in (getattr(sys, 'stdout', None), getattr(sys, 'stderr', None)):
                            return None
                    except Exception:
                        return None
                return _orig_addHandler(self, h)

            logging.Logger.addHandler = _kameo_addHandler
            logging._kameo_addHandler_patched = True
    except Exception:
        pass

    logging._kameo_dedup_handlers = True
    # One-time probe to verify INFO routing through the Rust bridge
    try:
        logging.info("kameo_python_bridge_ready")
    except Exception:
        pass
"#;
    py.run(std::ffi::CString::new(dedup_code).unwrap().as_c_str(), None, None)?;

    // Ensure non-stream handlers (e.g., the Rust tracing bridge handler) do not filter below INFO
    // by forcing their level to NOTSET, letting Rust's EnvFilter control visibility.
    let level_fix_code = r#"
import logging

def _is_stream_handler(h):
    try:
        return isinstance(h, logging.StreamHandler)
    except Exception:
        return False

try:
    loggers = [logging.getLogger()]
    for l in logging.root.manager.loggerDict.values():
        if isinstance(l, logging.Logger):
            loggers.append(l)
    for logger in loggers:
        for h in list(getattr(logger, 'handlers', ())) :
            try:
                if not _is_stream_handler(h):
                    h.setLevel(0)  # NOTSET
            except Exception:
                pass
except Exception:
    pass
"#;
    py.run(std::ffi::CString::new(level_fix_code).unwrap().as_c_str(), None, None)?;

    // Install the kameo_rs_logging module and a Python handler that forwards to Rust tracing
    install_kameo_rs_logging_module(py)?;
    let forwarder_code = r#"
import logging
import kameo_rs_logging as krl

if not getattr(logging, '_kameo_forward_handler_installed', False):
    class KameoRustHandler(logging.Handler):
        def emit(self, record):
            try:
                krl._kameo_emit_log(
                    record.name,
                    int(record.levelno),
                    record.getMessage(),
                    getattr(record, 'pathname', ''),
                    int(getattr(record, 'lineno', 0)),
                )
            except Exception:
                pass

    h = KameoRustHandler()
    h.setLevel(logging.NOTSET)
    root = logging.getLogger()
    root.addHandler(h)
    logging._kameo_forward_handler_installed = True
"#;
    py.run(std::ffi::CString::new(forwarder_code).unwrap().as_c_str(), None, None)?;

    // Re-assert the bridge handler in case any prior cleanup removed it inadvertently
    // (idempotent in the bridge crate).
    let _ = tracing_for_pyo3_logging::setup_logging(py);
    Ok(())
}
