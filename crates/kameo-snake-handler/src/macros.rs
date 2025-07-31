#[macro_export]
macro_rules! setup_python_subprocess_system {
    (
        $(actor = ($msg:ty, $callback:ty)),* ,
        child_init = $child_init:block,
        parent_init = $parent_init:block
    ) => {
        fn main() -> Result<(), Box<dyn std::error::Error>> {
            let handlers: &[(&str, fn() -> Result<(), Box<dyn std::error::Error>>)] = &[
                $((
                    std::any::type_name::<kameo_snake_handler::PythonActor<$msg, $callback>>(),
                    || {
                        use tracing::{info, debug, error, instrument};
                        use std::sync::Arc;
                        use pyo3::prelude::*;
                        use pyo3_async_runtimes::tokio::future_into_py;
                        use kameo_child_process::callback::{CallbackIpcChild, CallbackHandler};
                        use kameo_child_process::DuplexUnixStream;
                        use kameo_snake_handler::telemetry::{build_subscriber_with_otel_and_fmt_async_with_config, TelemetryExportConfig};

                        // Inlined declare_callback_glue
                        static CALLBACK_HANDLE: once_cell::sync::OnceCell<kameo_child_process::callback::CallbackHandle<$callback>> = once_cell::sync::OnceCell::new();
                        #[allow(non_snake_case)]
                        fn set_callback_handle_glue(handle: kameo_child_process::callback::CallbackHandle<$callback>) {
                            let _ = CALLBACK_HANDLE.set(handle);
                        }
                        #[pyfunction]
                        fn callback_handle<'py>(py: pyo3::Python<'py>, py_msg: &pyo3::Bound<'py, pyo3::PyAny>) -> pyo3::PyResult<pyo3::Bound<'py, pyo3::PyAny>> {
                            use pyo3::prelude::*;
                            use kameo_snake_handler::serde_py::{from_pyobject, to_pyobject};
                            let handle = CALLBACK_HANDLE.get().cloned().ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("Callback handle not initialized yet"))?;
                            let msg = match from_pyobject::<$callback>(py_msg.as_ref()) {
                                Ok(m) => m,
                                Err(e) => return Err(pyo3::exceptions::PyValueError::new_err(format!("Failed to parse callback: {e}"))),
                            };
                            pyo3_async_runtimes::tokio::future_into_py(py, async move {
                                match handle.handle(msg).await {
                                    Ok(_) => Python::with_gil(|py| Ok(py.None())),
                                    Err(e) => Err(pyo3::exceptions::PyRuntimeError::new_err(format!("Callback handler error: {e}"))),
                                }
                            })
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
                            // callback_handle
                            let py_func = pyo3::wrap_pyfunction!(callback_handle, py)?;
                            kameo_mod.setattr("callback_handle", py_func)?;
                            tracing::debug!("Set callback_handle on kameo module");
                            // sys.path
                            let sys_path = py.import("sys").expect("import sys").getattr("path").expect("get sys.path");
                            for path in &config.python_path {
                                sys_path.call_method1("append", (path,)).expect("append python_path");
                                debug!(added_path = %path, "Appended to sys.path");
                            }
                            // import module and function
                            let module = py.import(&config.module_name).expect("import module");
                            debug!(module = %config.module_name, "Imported Python module");
                            let function: Py<PyAny> = module.getattr(&config.function_name).expect("getattr function").unbind();
                            debug!(function = %config.function_name, "Located Python function");
                            let actor = kameo_snake_handler::PythonActor::<$msg, $callback>::new(config, function);
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
                                    Ok(conn) => conn,
                                    Err(e) => {
                                        tracing::info!(error = ?e, "Parent disconnected (callback connect failed), exiting cleanly");
                                        return Ok(());
                                    }
                                };
                                tracing::debug!("Setting callback handle glue for child process: {}", stringify!($callback));
                                set_callback_handle_glue(
                                    CallbackIpcChild::<$callback>::from_duplex(DuplexUnixStream::new(*callback_conn))
                                        as Arc<dyn CallbackHandler<$callback>>
                                );
                                tracing::debug!("Set callback handle glue for {}", stringify!($callback));
                                info!("Child connected to both sockets and set callback handle");
                                kameo_snake_handler::child_process_main_with_python_actor::<$msg, $callback>(actor, request_conn, None).await.map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
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