// Re-export macro from submodules. Initially keep everything in this file, parity with old macros.rs

// Internal modules used by the macro expansion
pub mod py_callback;
pub mod py_dynamic;
// old local runtime replaced by kameo-child-process runtime

/// Setup macro for Python subprocess systems with dynamic callback support.
#[macro_export]
macro_rules! setup_python_subprocess_system {
    (
        $(actor = ($msg:ty)),* ,
        child_init = $child_init:block,
        parent_init = $parent_init:block
    ) => {
        fn main() -> Result<(), Box<dyn std::error::Error>> {
            let handlers: &[(&str, fn() -> Result<(), Box<dyn std::error::Error>>)] = &[
                $(
                    (
                        std::any::type_name::<kameo_snake_handler::PythonActor<$msg, ()>>(),
                        || {
                            use tracing::{info, debug, error, instrument};
                            use pyo3::prelude::*;
                            use kameo_snake_handler::telemetry::{build_subscriber_with_otel_and_fmt_async_with_config, TelemetryExportConfig};
                            use serde_json;

                            let runtime_config = { $child_init };
                            let mut builder = match runtime_config.flavor {
                                kameo_child_process::RuntimeFlavor::MultiThread => {
                                    let mut b = tokio::runtime::Builder::new_multi_thread();
                                    b.enable_all();
                                    if let Some(threads) = runtime_config.worker_threads { b.worker_threads(threads); }
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
                                let config_json = std::env::var("KAMEO_PYTHON_CONFIG").expect("KAMEO_PYTHON_CONFIG must be set in child");
                                let config: kameo_snake_handler::PythonConfig = serde_json::from_str(&config_json).expect("Failed to parse KAMEO_PYTHON_CONFIG");

                                // Extracted: create dynamic modules and emit generated files
                                $crate::macros::py_dynamic::create_dynamic_modules(py, &config)?;

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
                                        TelemetryExportConfig { otlp_enabled: true, stdout_enabled: true, metrics_enabled: true }
                                    ).await;
                                    tracing::subscriber::set_global_default(subscriber).expect("set global");
                                    tracing::info!("Child process telemetry initialized");
                                    let request_conn = match kameo_child_process::child_request().await { Ok(conn) => conn, Err(e) => { tracing::info!(error = ?e, "Parent disconnected (request connect failed), exiting cleanly"); return Ok(()); } };
                                    let callback_conn = match kameo_child_process::child_callback().await { Ok(conn) => *conn, Err(e) => { tracing::info!(error = ?e, "Parent disconnected (callback connect failed), exiting cleanly"); return Ok(()); } };

                                    if let Err(_) = $crate::macros::py_callback::set_connection(callback_conn) {
                                        tracing::error!("Failed to store callback connection - connection already initialized");
                                        return Ok(());
                                    }

                                    tracing::debug!("Setting up dynamic callback system for child process");
                                    tracing::info!(event = "child_ready", "Child connected to both sockets and ready for dynamic callbacks");
                                    kameo_snake_handler::child_process_main_with_python_actor::<$msg, ()>(actor, request_conn, None).await.map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
                                };
                                pyo3_async_runtimes::tokio::run(py, async_block.instrument(root_span))
                            });
                            result.map_err(|e| Box::new(e) as Box<dyn std::error::Error>)
                        }
                    )
                ),*
            ];
            if let Ok(actor_name) = std::env::var("KAMEO_CHILD_ACTOR") {
                for (name, handler) in handlers { if actor_name == *name { return handler(); } }
                return Err(format!("Unknown actor type: {}", actor_name).into());
            }
            $parent_init
            Ok(())
        }
    };
}


