/// Setup macro for Python subprocess systems with dynamic callback support.
///
/// This macro generates the main entry point for Python subprocess binaries. It handles
/// the complex initialization required for Python-Rust interop, callback systems, and
/// process lifecycle management.
///
/// ## Architecture Overview
///
/// The macro creates a multi-actor system where:
/// - **Parent Process**: Rust application with DynamicCallbackModule for handling callbacks
/// - **Child Process**: Python subprocess with kameo module injected for IPC communication
/// - **Callback System**: Bidirectional streaming communication over Unix sockets
///
/// ## Generated Structure
///
/// 1. **Actor Registration**: Registers actor types based on message types
/// 2. **Child Initialization**: Sets up Python runtime, injects kameo module, establishes IPC
/// 3. **Callback Infrastructure**: Creates dynamic Python modules based on registered handlers
/// 4. **Process Lifecycle**: Handles startup, communication, and graceful shutdown
///
/// ## Python Side Integration
///
/// The macro automatically creates:
/// - `kameo.callback_handle(path, data)` - Legacy string-based callback API
/// - `kameo.{module}.{HandlerType}(data)` - Elegant module-based callback API
///
/// ## Usage Example
///
/// ```rust,ignore
/// kameo_snake_handler::setup_python_subprocess_system! {
///     actor = (MyMessage),
///     child_init = {
///         // Child process initialization
///         tracing_subscriber::fmt().init();
///         kameo_child_process::RuntimeConfig {
///             flavor: kameo_child_process::RuntimeFlavor::MultiThread,
///             worker_threads: Some(2),
///         }
///     },
///     parent_init = {
///         // Parent process initialization
///         tracing_subscriber::fmt().init();
///         tokio::runtime::Builder::new_multi_thread().build()?
///             .block_on(async { Ok(()) })?
///     }
/// }
/// ```
#[macro_export]
macro_rules! setup_python_subprocess_system {
    (
        $(actor = ($msg:ty)),* ,
        child_init = $child_init:block,
        parent_init = $parent_init:block
    ) => {
        fn main() -> Result<(), Box<dyn std::error::Error>> {
            let handlers: &[(&str, fn() -> Result<(), Box<dyn std::error::Error>>)] = &[
                $((
                    std::any::type_name::<kameo_snake_handler::PythonActor<$msg, ()>>(),
                    || {
                        use tracing::{info, debug, error, instrument};
                        use pyo3::prelude::*;
                        use pyo3::wrap_pyfunction;
                        use pyo3_async_runtimes::tokio::future_into_py;
                        use kameo_snake_handler::telemetry::{build_subscriber_with_otel_and_fmt_async_with_config, TelemetryExportConfig};
                        use std::sync::Arc;
                        use tokio::sync::Mutex;
                        use kameo_snake_handler::serde_py;
                        use kameo_child_process::callback;
                        // serde-brief used for wire format
                        use serde_json;

                        // Global callback connection for this child process
                        static CALLBACK_CONNECTION: std::sync::OnceLock<std::sync::Arc<tokio::sync::Mutex<tokio::net::UnixStream>>> = std::sync::OnceLock::new();

                        // In the new dynamic callback system, callbacks are handled by the parent process
                        // through DynamicCallbackModule, so we don't need static callback handlers here

                        /// Core callback handler injected into Python as `kameo.callback_handle`.
                        ///
                        /// This function bridges Python callback requests to the Rust parent process
                        /// via IPC over Unix sockets. It implements the dynamic callback protocol
                        /// where Python specifies the handler path and the parent routes the request
                        /// to the appropriate typed handler.
                        ///
                        /// ## Protocol Flow
                        ///
                        /// 1. **Python Request**: Python calls with (callback_path, data)
                        /// 2. **Serialization**: Convert Python data to TypedCallbackEnvelope
                        /// 3. **IPC Send**: Send envelope over shared Unix socket to parent
                        /// 4. **Response Stream**: Return CallbackAsyncIterator for streaming responses
                        ///
                        /// ## Connection Management
                        ///
                        /// Uses a shared Unix socket connection stored in CALLBACK_CONNECTION.
                        /// The connection is established once during child startup and reused
                        /// for all callback requests from this child process.
                        ///
                        /// ## Arguments
                        ///
                        /// - `callback_path`: Handler path like "module.HandlerType"
                        /// - `py_msg`: Python message data to be sent to the handler
                        ///
                        /// ## Returns
                        ///
                        /// CallbackAsyncIterator that can be used with Python's `async for`
                        /// to receive streaming responses from the Rust handler.
                        #[pyo3::pyfunction]
                        fn callback_handle_inner<'py>(py: pyo3::Python<'py>, callback_path: &str, py_msg: &pyo3::Bound<'py, pyo3::PyAny>) -> pyo3::PyResult<pyo3::Py<pyo3::PyAny>> {
                            // Convert Python message to callback data using our serde integration
                            let callback_data: serde_json::Value = match kameo_snake_handler::serde_py::from_pyobject(py_msg) {
                                Ok(data) => data,
                                Err(e) => return Err(pyo3::exceptions::PyRuntimeError::new_err(format!("Failed to convert Python message: {e}"))),
                            };

                            // Generate unique correlation ID for request/response matching
                            // Using nanosecond timestamp ensures uniqueness within a process
                            let correlation_id = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_nanos() as u64;

                            let envelope = kameo_child_process::callback::TypedCallbackEnvelope {
                                callback_path: callback_path.to_string(),
                                correlation_id,
                                callback_data: serde_json::to_vec(&callback_data)
                                    .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("Failed to serialize callback data: {e}")))?,
                                context: kameo_child_process::callback::TracingContext::default(),
                            };

                            // Use the existing callback connection that's already established in the macro
                            Ok(pyo3_async_runtimes::tokio::future_into_py(py, async move {
                                // Get the stored callback connection
                                let callback_conn_mutex = CALLBACK_CONNECTION.get()
                                    .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("Callback connection not initialized"))?;
                                let mut callback_conn_guard = callback_conn_mutex.lock().await;

                                // Send the envelope using the existing connection with proper framing
                                let envelope_bytes = serde_brief::to_vec(&envelope)
                                    .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("Failed to serialize envelope: {e}")))?;

                                // Send length-prefixed data (same framing as the existing system)
                                let length = envelope_bytes.len() as u32;
                                let mut message = Vec::new();
                                message.extend_from_slice(&length.to_le_bytes());
                                message.extend_from_slice(&envelope_bytes);

                                use tokio::io::AsyncWriteExt;
                                callback_conn_guard.write_all(&message).await
                                    .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("Failed to send callback envelope: {e}")))?;
                                callback_conn_guard.flush().await
                                    .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("Failed to flush callback envelope: {e}")))?;

                                // Create a response reader to receive streaming responses
                                let callback_conn_for_reader = CALLBACK_CONNECTION.get()
                                    .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("Callback connection not initialized"))?
                                    .clone();

                                // Return the async iterator for streaming responses
                                pyo3::Python::with_gil(|py| -> pyo3::PyResult<pyo3::Py<pyo3::PyAny>> {
                                    let iterator = CallbackAsyncIterator {
                                        callback_conn: callback_conn_for_reader,
                                        correlation_id,
                                        exhausted: Arc::new(std::sync::atomic::AtomicBool::new(false)), // Not exhausted - will read responses
                                        count: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
                                    };
                                    let py_iterator = pyo3::Py::new(py, iterator)?;
                                    Ok(py_iterator.into())
                                })
                            })?.unbind())
                        }

                        /// Python async iterator for streaming callback responses.
                        ///
                        /// This struct implements Python's async iterator protocol (__aiter__, __anext__)
                        /// to provide a native Python interface for receiving streaming responses from
                        /// Rust callback handlers in the parent process.
                        ///
                        /// ## Protocol Implementation
                        ///
                        /// - **__aiter__()**: Returns self (required by Python async iterator protocol)
                        /// - **__anext__()**: Reads next response from Unix socket, handles stream termination
                        ///
                        /// ## Response Handling
                        ///
                        /// 1. Reads TypedCallbackResponse messages from the shared Unix socket
                        /// 2. Filters responses by correlation_id to handle concurrent callbacks
                        /// 3. Deserializes response data and yields to Python
                        /// 4. Handles stream termination via `is_final` flag
                        ///
                        /// ## Connection Sharing
                        ///
                        /// Uses Arc<Mutex<UnixStream>> to safely share the callback socket between
                        /// multiple concurrent callback requests from the same child process.
                        ///
                        /// ## State Management
                        ///
                        /// - `exhausted`: Atomic flag indicating stream completion
                        /// - `count`: Atomic counter for received response items
                        /// - `correlation_id`: Unique ID for filtering responses to this callback
                        #[pyo3::pyclass]
                        struct CallbackAsyncIterator {
                            /// Shared connection to parent process for reading responses
                            callback_conn: std::sync::Arc<tokio::sync::Mutex<tokio::net::UnixStream>>,
                            /// Unique ID for this callback request (filters responses)
                            correlation_id: u64,
                            /// Atomic flag indicating if the stream has been exhausted
                            exhausted: Arc<std::sync::atomic::AtomicBool>,
                            /// Atomic counter for the number of items received
                            count: Arc<std::sync::atomic::AtomicUsize>,
                        }

                        #[pyo3::pymethods]
                        impl CallbackAsyncIterator {
                            fn __aiter__(slf: pyo3::PyRef<Self>) -> pyo3::PyRef<Self> {
                                slf
                            }

                            fn __anext__<'py>(&self, py: pyo3::Python<'py>) -> pyo3::PyResult<pyo3::Py<pyo3::PyAny>> {
                                let callback_conn = self.callback_conn.clone();
                                let correlation_id = self.correlation_id;
                                let exhausted = self.exhausted.clone();
                                let count = self.count.clone();

                                Ok(pyo3_async_runtimes::tokio::future_into_py(py, async move {
                                    use tokio::io::AsyncReadExt;

                                    // Check if already exhausted
                                    if exhausted.load(std::sync::atomic::Ordering::SeqCst) {
                                        return Err(pyo3::exceptions::PyStopAsyncIteration::new_err(()));
                                    }

                                    // Read length-prefixed response from the callback socket
                                    let mut conn_guard = callback_conn.lock().await;

                                    // Read the length prefix (4 bytes)
                                    let mut length_bytes = [0u8; 4];
                                    match conn_guard.read_exact(&mut length_bytes).await {
                                        Ok(_) => {},
                                        Err(e) => {
                                            tracing::info!("Connection closed or error reading length: {}", e);
                                            exhausted.store(true, std::sync::atomic::Ordering::SeqCst);
                                            return Err(pyo3::exceptions::PyStopAsyncIteration::new_err(()));
                                        }
                                    }

                                    let length = u32::from_le_bytes(length_bytes) as usize;
                                    tracing::debug!("Reading response message of length: {}", length);

                                    // Read the message data
                                    let mut message_bytes = vec![0u8; length];
                                    match conn_guard.read_exact(&mut message_bytes).await {
                                        Ok(_) => {},
                                        Err(e) => {
                                            tracing::error!("Error reading message data: {}", e);
                                            exhausted.store(true, std::sync::atomic::Ordering::SeqCst);
                                            return Err(pyo3::exceptions::PyStopAsyncIteration::new_err(()));
                                        }
                                    }

                                    // Deserialize the TypedCallbackResponse
                                    let response: kameo_child_process::callback::TypedCallbackResponse =
                                        serde_brief::from_slice(&message_bytes)
                                            .map_err(|e| {
                                                tracing::error!("Failed to deserialize response: {}", e);
                                                pyo3::exceptions::PyRuntimeError::new_err(format!("Failed to deserialize response: {}", e))
                                            })?;

                                    tracing::debug!("Received response for correlation_id: {} (expected: {})", response.correlation_id, correlation_id);

                                    // Check if this response is for our correlation ID
                                    if response.correlation_id != correlation_id {
                                        // Not our response - this shouldn't happen with proper multiplexing
                                        // For now, just mark as exhausted
                                        tracing::warn!("Received response for different correlation_id: {} (expected: {})", response.correlation_id, correlation_id);
                                        exhausted.store(true, std::sync::atomic::Ordering::SeqCst);
                                        return Err(pyo3::exceptions::PyStopAsyncIteration::new_err(()));
                                    }

                                    // Check if this is the final response (stream termination)
                                    if response.is_final {
                                        tracing::info!("🏁 Received final response for correlation_id {}, terminating stream", correlation_id);
                                        exhausted.store(true, std::sync::atomic::Ordering::SeqCst);
                                        return Err(pyo3::exceptions::PyStopAsyncIteration::new_err(()));
                                    }

                                    // Increment count and return response
                                    let item_count = count.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                                    tracing::info!("🎯 Yielding response item {} for correlation_id {}", item_count, correlation_id);

                                    // Convert response data to Python object
                                    pyo3::Python::with_gil(|py| -> pyo3::PyResult<pyo3::Py<pyo3::PyAny>> {
                                        let json_value = serde_json::json!({
                                            "callback_path": response.callback_path,
                                            "response_type": response.response_type,
                                            "item_number": item_count,
                                            "is_final": response.is_final,
                                            "data": String::from_utf8_lossy(&response.response_data)
                                        });
                                        use kameo_snake_handler::serde_py::to_pyobject;
                                        match to_pyobject(py, &json_value) {
                                            Ok(py_obj) => Ok(py_obj.into()),
                                            Err(e) => Err(pyo3::exceptions::PyRuntimeError::new_err(format!("Failed to convert to Python object: {e}"))),
                                        }
                                    })
                                })?.unbind())
                            }
                        }

                        // runtime_config
                        let runtime_config = { $child_init };
                        // builder
                        let mut builder = match runtime_config.flavor {
                            kameo_child_process::RuntimeFlavor::MultiThread => {
                                let mut b = tokio::runtime::Builder::new_multi_thread();
                                b.enable_all();
                                if let Some(threads) = runtime_config.worker_threads {
                                    b.worker_threads(threads);
                                }
                                b
                            }
                            kameo_child_process::RuntimeFlavor::CurrentThread => {
                                let mut b = tokio::runtime::Builder::new_current_thread();
                                b.enable_all();
                                b
                            }
                        };
                        kameo_snake_handler::setup_python_runtime(builder);


                        let root_span = tracing::info_span!("child_process", process_role = "child");
                        let result = pyo3::Python::with_gil(|py| {
                            // config_json and config
                            let config_json = std::env::var("KAMEO_PYTHON_CONFIG").expect("KAMEO_PYTHON_CONFIG must be set in child");
                            let config: kameo_snake_handler::PythonConfig = serde_json::from_str(&config_json).expect("Failed to parse KAMEO_PYTHON_CONFIG");

                            // callback registry for creating Python modules
                            tracing::info!("=== DYNAMIC MODULE CREATION START ===");
                            let callback_registry_json = std::env::var("KAMEO_CALLBACK_REGISTRY").expect("KAMEO_CALLBACK_REGISTRY must be set in child");
                            tracing::info!("Raw callback registry JSON: {}", callback_registry_json);
                            let callback_registry: std::collections::HashMap<String, Vec<String>> = serde_json::from_str(&callback_registry_json).expect("Failed to parse KAMEO_CALLBACK_REGISTRY");
                            tracing::info!("Parsed callback registry: {:?}", callback_registry);

                            // Add more detailed logging for module creation
                            tracing::info!("Creating dynamic Python modules from callback registry");
                            // Also parse response type names for precise stub generation
                            let resp_types_json = std::env::var("KAMEO_CALLBACK_RESP_TYPES").unwrap_or_else(|_| "{}".to_string());
                            let resp_types: std::collections::HashMap<String, String> = serde_json::from_str(&resp_types_json).unwrap_or_default();
                            // Parse request type names for precise stub parameter annotations
                            let req_types_json = std::env::var("KAMEO_CALLBACK_REQ_TYPES").unwrap_or_else(|_| "{}".to_string());
                            let req_types: std::collections::HashMap<String, String> = serde_json::from_str(&req_types_json).unwrap_or_default();
                            // Parse callback request IR (path -> Vec<Decl>) to generate dataclasses for request types
                            let req_ir_json = std::env::var("KAMEO_CALLBACK_REQ_IR").unwrap_or_else(|_| "{}".to_string());
                            let req_ir_map: std::collections::HashMap<String, Vec<kameo_snake_handler::codegen_py::Decl>> = serde_json::from_str(&req_ir_json).unwrap_or_default();
                            tracing::debug!(target: "kameo_snake_handler::macros::codegen", req_ir_map_keys = ?req_ir_map.keys().collect::<Vec<_>>(), "Parsed callback request IR");
                            // Parse callback response IR (path -> Vec<Decl>) to generate dataclasses and match functions for response types
                            let resp_ir_json = std::env::var("KAMEO_CALLBACK_RESP_IR").unwrap_or_else(|_| "{}".to_string());
                            let resp_ir_map: std::collections::HashMap<String, Vec<kameo_snake_handler::codegen_py::Decl>> = serde_json::from_str(&resp_ir_json).unwrap_or_default();
                            tracing::debug!(target: "kameo_snake_handler::macros::codegen", resp_ir_map_keys = ?resp_ir_map.keys().collect::<Vec<_>>(), "Parsed callback response IR");
                            // sys.modules and kameo_mod
                            let sys = py.import("sys").expect("import sys");
                            let modules = sys.getattr("modules").expect("get sys.modules");
                            let kameo_mod = match modules.get_item("kameo") {
                                Ok(m) => m.downcast::<pyo3::types::PyModule>().unwrap().clone(),
                                Err(_) => {
                                    let m = pyo3::types::PyModule::new(py, "kameo").expect("create kameo module");
                                    modules.set_item("kameo", &m).expect("inject kameo into sys.modules");
                                    tracing::debug!("Injected kameo module into sys.modules BEFORE user import");
                                    m.clone()
                                }
                            };

                            // Create dynamic module structure based on callback registry
                            tracing::info!("Starting dynamic module creation for {} modules", callback_registry.len());
                            for (module_name, handler_types) in &callback_registry {
                                tracing::info!("Creating Python module: kameo.{} with {} handler types", module_name, handler_types.len());

                                // Create or get the submodule (e.g., kameo.test, kameo.basic, kameo.trader)
                                let submodule = match kameo_mod.getattr(module_name) {
                                    Ok(existing) => existing.downcast::<pyo3::types::PyModule>().unwrap().clone(),
                                    Err(_) => {
                                        let new_module = pyo3::types::PyModule::new(py, module_name)?;
                                        kameo_mod.setattr(module_name, &new_module)?;
                                        new_module.clone()
                                    }
                                };

                                // Create callback functions for each handler type in this module
                                for handler_type in handler_types {
                                    tracing::debug!("Creating callback function: kameo.{}.{}", module_name, handler_type);

                                    // Create a closure that captures the callback path
                                    let callback_path = format!("{}.{}", module_name, handler_type);
                                    let callback_path_clone = callback_path.clone();

                                    // Create a Python function that calls our callback_handle_inner with the right path
                                    let callback_fn = pyo3::types::PyCFunction::new_closure(
                                        py,
                                        None, // No name needed
                                        None, // No doc needed
                                        move |args: &pyo3::Bound<'_, pyo3::types::PyTuple>, _kwargs: Option<&pyo3::Bound<'_, pyo3::types::PyDict>>| {
                                            let py = args.py();
                                            if args.len() != 1 {
                                                return Err(pyo3::exceptions::PyTypeError::new_err("Expected exactly one argument"));
                                            }
                                            let py_msg = args.get_item(0)?;
                                            callback_handle_inner(py, &callback_path_clone, &py_msg)
                                        }
                                    )?;

                                    submodule.setattr(handler_type, callback_fn)?;
                                    tracing::info!("Created callback function: kameo.{}.{}", module_name, handler_type);
                                }
                                tracing::info!("Completed module: kameo.{}", module_name);
                            }
                            tracing::info!("Dynamic module creation completed for all {} modules", callback_registry.len());
                            // sys.path
                            let sys_path = py.import("sys").expect("import sys").getattr("path").expect("get sys.path");
                            for path in &config.python_path {
                                sys_path.call_method1("append", (path,)).expect("append python_path");
                                debug!(added_path = %path, "Appended to sys.path");
                            }
                            // Determine write directory per module: write into the module's package directory
                            let path0 = sys_path.get_item(0).expect("sys.path[0]").extract::<String>().expect("path str");
                            let default_dir = if let Some(dir) = config.python_path.last() {
                                std::path::PathBuf::from(dir)
                            } else {
                                std::path::PathBuf::from(&path0)
                            };
                            
                            // Extract package name from module_name (e.g., "agentflow_agents.a2a_agent" -> "agentflow_agents")
                            let package_name = config.module_name.split('.').next().unwrap_or(&config.module_name);
                            let module_dir = default_dir.join(package_name);
                            std::fs::create_dir_all(&module_dir).expect("mkdir module package dir");
                            
                            tracing::debug!(target: "kameo_snake_handler::macros::codegen", 
                                dir = %module_dir.display(), 
                                module = %config.module_name, 
                                package = %package_name,
                                "Ensured Python package directory for generated files");

                            // Deterministic module names so user code can import them directly
                            let types_mod = "invocation_generated_types".to_string();
                            let stubs_mod = "callback_generated_types".to_string();
                            let stubs_path = module_dir.join(format!("{}.py", stubs_mod));
                            let mut callback_stubs: Vec<kameo_snake_handler::codegen_py::CallbackStub> = Vec::new();
                            for (module_name, handler_types) in &callback_registry {
                                for handler_type in handler_types {
                                    let full_path = format!("{}.{}", module_name, handler_type);
                                    // Default request type Any if missing
                                    let request_type = req_types.get(&full_path).cloned().unwrap_or_else(|| "Any".to_string());
                                    let response_type = resp_types.get(&full_path).cloned().unwrap_or_else(|| "Any".to_string());
                                    callback_stubs.push(kameo_snake_handler::codegen_py::CallbackStub {
                                        path: full_path,
                                        request_type,
                                        response_type,
                                    });
                                }
                            }
                            let stubs_src = kameo_snake_handler::codegen_py::emit_callback_module(&stubs_mod, &types_mod, &callback_stubs);
                            std::fs::write(&stubs_path, stubs_src).expect("write stubs module");
                            // If callback IR is provided, regenerate callback_generated_types.py with both wrappers and types
                            let has_callback_ir = !req_ir_map.is_empty() || !resp_ir_map.is_empty();
                            tracing::debug!(target: "kameo_snake_handler::macros::codegen", 
                                has_callback_ir = %has_callback_ir, 
                                req_ir_map_empty = %req_ir_map.is_empty(), 
                                resp_ir_map_empty = %resp_ir_map.is_empty(), 
                                "Checking if callback_generated_types.py should include callback types");
                            
                            if has_callback_ir {
                                // Merge all callback decls (request + response)
                                let mut all_callback_decls: Vec<kameo_snake_handler::codegen_py::Decl> = Vec::new();
                                
                                // Add request types
                                for decls in req_ir_map.values() {
                                    all_callback_decls.extend_from_slice(decls);
                                }
                                
                                // Add response types
                                for decls in resp_ir_map.values() {
                                    all_callback_decls.extend_from_slice(decls);
                                }
                                
                                // Generate callback types and strip out the problematic __future__ import
                                let mut callback_types_src = kameo_snake_handler::codegen_py::emit_python_module("callback_types", &all_callback_decls);
                                
                                // Remove the __future__ import and other imports since they're already in the wrapper file
                                let lines: Vec<&str> = callback_types_src.lines().collect();
                                let mut filtered_lines: Vec<&str> = Vec::new();
                                let mut skip_next_indented = false;
                                
                                for line in lines.iter() {
                                    if line.contains("from __future__") || 
                                       line.contains("try:") || 
                                       line.contains("except ImportError:") || 
                                       line.contains("TypeGuard") || 
                                       line.contains("# package:") {
                                        skip_next_indented = true;
                                        continue;
                                    }
                                    
                                    // If we're skipping indented lines and this line is indented, skip it
                                    if skip_next_indented && line.starts_with("    ") {
                                        continue;
                                    }
                                    
                                    // Reset the flag when we hit a non-indented line
                                    if !line.starts_with("    ") {
                                        skip_next_indented = false;
                                    }
                                    
                                    filtered_lines.push(line);
                                }
                                
                                let clean_callback_types = filtered_lines.join("\n");
                                
                                // Read the existing file and append the clean types
                                let existing_content = std::fs::read_to_string(&stubs_path).expect("read existing callback_generated_types.py");
                                let enhanced_content = format!("{}\n\n# Callback type definitions\n{}", existing_content, clean_callback_types);
                                
                                std::fs::write(&stubs_path, enhanced_content).expect("write enhanced callback generated types module");
                                tracing::debug!(target: "kameo_snake_handler::macros::codegen", 
                                    path = %stubs_path.display(), 
                                    module = %stubs_mod,
                                    decl_count = all_callback_decls.len(),
                                    "Enhanced callback_generated_types.py with callback types");
                            }
                            // If callback IR is available for any paths in this module, append generated unions to the stubs file for strict typing
                            // (ir emission for callbacks can be added when IR is provided from parent)
                            // Expose the generated module names via environment (compat)
                            std::env::set_var("KAMEO_GENERATED_CALLBACK_TYPES", &stubs_mod);
                            std::env::set_var("KAMEO_GENERATED_INVOCATION_TYPES", &types_mod);

                            // Emit Python types using the tested codegen and user-provided IR
                            {
                                use kameo_snake_handler::codegen_py::emit_python_module;
                                let inv_ir_json = std::env::var("KAMEO_INVOCATION_IR").unwrap_or_else(|_| "[]".to_string());
                                let decls: Vec<kameo_snake_handler::codegen_py::Decl> = serde_json::from_str(&inv_ir_json).unwrap_or_default();
                                let types_src = emit_python_module(&types_mod, &decls);
                                let types_path = module_dir.join(format!("{}.py", types_mod));
                                std::fs::write(&types_path, types_src).expect("write types module");
                                // No test-type specific additions; depend only on IR-generated content
                            }

                            // import module and function
                            let module = match py.import(&config.module_name) {
                                Ok(m) => m,
                                Err(e) => {
                                    error!(error = %e, module = %config.module_name, "Failed to import Python module - this will cause parent actor to exit");
                                    return Err(pyo3::exceptions::PyImportError::new_err(format!("Failed to import module '{}': {}", config.module_name, e)));
                                }
                            };
                            debug!(module = %config.module_name, "Imported Python module");
                            let function: Py<PyAny> = match module.getattr(&config.function_name) {
                                Ok(f) => f.unbind(),
                                Err(e) => {
                                    error!(error = %e, function = %config.function_name, module = %config.module_name, "Failed to get function from Python module - this will cause parent actor to exit");
                                    return Err(pyo3::exceptions::PyAttributeError::new_err(format!("Failed to get function '{}' from module '{}': {}", config.function_name, config.module_name, e)));
                                }
                            };
                            debug!(function = %config.function_name, "Located Python function");
                            let actor = kameo_snake_handler::PythonActor::<$msg, ()>::new(config, function);
                            let async_block = async move {
                                let (subscriber, _guard) = build_subscriber_with_otel_and_fmt_async_with_config(
                                    TelemetryExportConfig {
                                        otlp_enabled: true,
                                        stdout_enabled: true,
                                        metrics_enabled: true,
                                    }
                                ).await;
                                tracing::subscriber::set_global_default(subscriber).expect("set global");
                                tracing::info!("Child process telemetry initialized");
                                let request_conn = match kameo_child_process::child_request().await {
                                    Ok(conn) => conn,
                                    Err(e) => {
                                        tracing::info!(error = ?e, "Parent disconnected (request connect failed), exiting cleanly");
                                        return Ok(());
                                    }
                                };
                                let callback_conn = match kameo_child_process::child_callback().await {
                                    Ok(conn) => *conn,
                                    Err(e) => {
                                        tracing::info!(error = ?e, "Parent disconnected (callback connect failed), exiting cleanly");
                                        return Ok(());
                                    }
                                };

                                // Store the callback connection for use by the callback handler
                                let connection_mutex = std::sync::Arc::new(tokio::sync::Mutex::new(callback_conn));
                                if let Err(_) = CALLBACK_CONNECTION.set(connection_mutex) {
                                    tracing::error!("Failed to store callback connection - connection already initialized");
                                    return Ok(());
                                }

                                tracing::debug!("Setting up dynamic callback system for child process");
                                // In the new dynamic callback system, we don't need to set up static callback handlers
                                // The parent process will handle callbacks through the DynamicCallbackModule
                                tracing::info!(event = "child_ready", "Child connected to both sockets and ready for dynamic callbacks");
                                kameo_snake_handler::child_process_main_with_python_actor::<$msg, ()>(actor, request_conn, None).await.map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
                            };
                            pyo3_async_runtimes::tokio::run(py, async_block.instrument(root_span))
                        });
                        result.map_err(|e| Box::new(e) as Box<dyn std::error::Error>)
                    }
                )),*
            ];
            if let Ok(actor_name) = std::env::var("KAMEO_CHILD_ACTOR") {
                for (name, handler) in handlers {
                    if actor_name == *name {
                        return handler();
                    }
                }
                return Err(format!("Unknown actor type: {}", actor_name).into());
            }
            // Parent code directly
            $parent_init
            Ok(())
        }
    };
}
