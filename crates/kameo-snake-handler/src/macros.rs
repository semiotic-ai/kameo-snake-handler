#[macro_export]
macro_rules! declare_callback_glue {
    ($type:ty) => {
        use kameo_snake_handler::serde_py::{from_pyobject, to_pyobject};
        static CALLBACK_HANDLE: once_cell::sync::OnceCell<std::sync::Arc<kameo_child_process::CallbackHandle<$type>>> = once_cell::sync::OnceCell::new();

        pub fn register_callback_glue(py: pyo3::Python<'_>) -> pyo3::PyResult<()> {
            use kameo_snake_handler::serde_py::{from_pyobject, to_pyobject};
            #[pyo3::pyfunction]
            fn callback_handle(py: pyo3::Python<'_>, py_msg: &pyo3::Bound<'_, pyo3::PyAny>) -> pyo3::PyResult<pyo3::Py<pyo3::PyAny>> {
                let handle = CALLBACK_HANDLE.get().cloned().ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("Callback handle not initialized yet"))?;
                let msg = match from_pyobject::<$type>(py_msg.as_ref()) {
                    Ok(m) => m,
                    Err(e) => return Err(pyo3::exceptions::PyValueError::new_err(format!("Failed to parse callback: {e}"))),
                };
                pyo3_async_runtimes::tokio::future_into_py(py, async move {
                    tracing::trace!(event = "child_callback_send", ?msg, "About to send callback message from child to parent");
                    let reply = handle.ask(msg).await;
                    tracing::trace!(event = "child_callback_send", ?reply, "Received reply from parent callback handler");
                    match reply {
                        Ok(val) => pyo3::Python::with_gil(|py| to_pyobject(py, &val)
                            .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("Failed to serialize reply: {e}")))),
                        Err(e) => Err(pyo3::exceptions::PyRuntimeError::new_err(format!("Callback error: {e:?}"))),
                    }
                }).map(|bound| bound.unbind())
            }
            let py_func = pyo3::wrap_pyfunction!(callback_handle, py)?;
            let sys = py.import("sys")?;
            let modules = sys.getattr("modules")?;
            let kameo_mod = match modules.get_item("kameo") {
                Ok(ref m) => {
                    let py_mod = m.downcast::<pyo3::types::PyModule>().unwrap();
                    py_mod.clone().unbind().into_any().into_bound(py)
                }
                Err(_) => {
                    let m = pyo3::types::PyModule::new(py, "kameo")?;
                    modules.set_item("kameo", &m)?;
                    m.unbind().into_any().into_bound(py)
                }
            };
            kameo_mod.setattr("callback_handle", py_func)?;
            Ok(())
        }

        pub fn set_callback_handle(handle: std::sync::Arc<kameo_child_process::CallbackHandle<$type>>) {
            let _ = CALLBACK_HANDLE.set(handle);
        }
    };
}

#[macro_export]
macro_rules! setup_python_subprocess_system {
    (
        actors = { $(($actor:ty, $msg:ty, $callback:ty, $callback_handler:ty)),* $(,)? },
        child_init = $child_init:block,
        parent_init = $parent_init:block $(,)?
    ) => {
        fn main() -> Result<(), Box<dyn std::error::Error>> {
            let handlers: &[(&'static str, fn() -> Result<(), Box<dyn std::error::Error>>)] = &[
                $(
                    (
                        std::any::type_name::<$actor>(),
                        || {
                            declare_callback_glue!($callback);
                            use tracing::{info, debug, error, instrument};
                            use std::sync::Arc;
                            use pyo3::prelude::*;
                            use pyo3_async_runtimes::tokio::future_into_py;
                            let runtime_config = { $child_init };
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
                            tracing::trace!(event = "child_entry", step = "before_gil", "Child about to enter GIL and Python setup");
                            pyo3::Python::with_gil(|py| {
                                tracing::trace!(event = "child_entry", step = "in_gil", "Child inside GIL, about to import module and function");
                                let config_json = std::env::var("KAMEO_PYTHON_CONFIG").expect("KAMEO_PYTHON_CONFIG must be set in child");
                                let config: kameo_snake_handler::PythonConfig = serde_json::from_str(&config_json).expect("Failed to parse KAMEO_PYTHON_CONFIG");
                                debug!(module_name = %config.module_name, function_name = %config.function_name, python_path = ?config.python_path, "Deserialized PythonConfig");
                                let sys_path = py.import("sys").expect("import sys").getattr("path").expect("get sys.path");
                                for path in &config.python_path {
                                    sys_path.call_method1("append", (path,)).expect("append python_path");
                                    debug!(added_path = %path, "Appended to sys.path");
                                }
                                let module = py.import(&config.module_name).expect("import module");
                                debug!(module = %config.module_name, "Imported Python module");
                                let function: Py<PyAny> = module.getattr(&config.function_name).expect("getattr function").unbind();
                                debug!(function = %config.function_name, "Located Python function");
                                register_callback_glue(py).expect("register_callback_glue failed");
                                let rust_thread_id = std::thread::current().id();
                                let py_thread_id = Python::with_gil(|py| {
                                    let threading = py.import("threading").unwrap();
                                    threading.call_method0("get_ident").unwrap().extract::<u64>().unwrap()
                                });
                                debug!(?rust_thread_id, py_thread_id, "Thread IDs at start of async block");
                                tracing::trace!(event = "child_entry", step = "before_async_block", "Child about to enter async block");
                                pyo3_async_runtimes::tokio::run(py, async move {
                                    tracing::trace!(event = "child_entry", step = "in_async_block", "Child inside async block, about to connect sockets");
                                    let request_socket_path = std::env::var("KAMEO_REQUEST_SOCKET").unwrap_or_else(|_| "<unset>".to_string());
                                    tracing::info!(event = "child_handshake", step = "before_child_request", socket = %request_socket_path, "Child about to connect to request socket");
                                    let request_conn = match kameo_child_process::child_request().await {
                                        Ok(conn) => {
                                            tracing::info!(event = "child_handshake", step = "after_child_request", socket = %request_socket_path, "Child connected to request socket");
                                            conn
                                        },
                                        Err(e) => {
                                            tracing::error!(event = "child_handshake", step = "child_request_failed", socket = %request_socket_path, error = %e, "Child failed to connect to request socket");
                                            return Err(pyo3::exceptions::PyRuntimeError::new_err(format!("child_request handshake failed: {e}")));
                                        }
                                    };
                                    let callback_conn = kameo_child_process::child_callback().await.expect("child_callback handshake failed");
                                    let handle = kameo_child_process::CallbackHandle::new(callback_conn);
                                    set_callback_handle(std::sync::Arc::new(handle));
                                    info!("Child connected to both sockets and set callback handle");
                                    let actor = <$actor>::new(config, function);
                                    tracing::trace!(event = "child_entry", step = "before_main_actor", "Child about to call child_process_main_with_python_actor");
                                    let result = $crate::child_process_main_with_python_actor::<$msg, $callback>(actor, request_conn).await;
                                    tracing::trace!(event = "child_entry", step = "after_main_actor", ?result, "Child returned from child_process_main_with_python_actor");
                                    result.map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("{e}")))
                                })
                                .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)
                            })
                        }
                    ),
                )*
            ];
            if let Ok(actor_name) = std::env::var("KAMEO_CHILD_ACTOR") {
                for (name, handler) in handlers {
                    if actor_name == *name {
                        return handler();
                    }
                }
                return Err(format!("Unknown actor type: {}", actor_name).into());
            }
            tracing::trace!(event = "parent_entry", step = "before_parent_init", "Parent about to enter parent_init block");
            $parent_init
            tracing::trace!(event = "parent_entry", step = "after_parent_init", "Parent returned from parent_init block");
            Ok(())
        }
    };
    // Fallback for 3-tuple: default to NoopCallbackHandler
    (
        actors = { $(($actor:ty, $msg:ty, $callback:ty)),* $(,)? },
        child_init = $child_init:block,
        parent_init = $parent_init:block $(,)?
    ) => {
        $crate::setup_python_subprocess_system! {
            actors = { $(($actor, $msg, $callback, $crate::NoopCallbackHandler)),* },
            child_init = $child_init,
            parent_init = $parent_init,
        }
    };
} 