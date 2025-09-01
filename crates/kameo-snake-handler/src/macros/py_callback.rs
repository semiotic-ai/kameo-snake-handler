//! Python callback bridge: shared connection, async iterator, and entrypoint.
//!
//! Child-side support for the dynamic callback protocol. This module:
//! - Maintains a single shared Unix socket connection to the parent for all callbacks
//! - Exposes `callback_handle_inner` to Python, which serializes and sends requests
//! - Returns a Python async iterator that yields streaming responses from the parent
//!
//! Protocol overview:
//! 1) Python calls a generated function `kameo.<module>.<HandlerType>(req)`.
//! 2) That invokes `callback_handle_inner`, which packages the request into a
//!    `TypedCallbackEnvelope` and writes it to the shared callback socket.
//! 3) A `CallbackAsyncIterator` is returned to Python which reads length-prefixed
//!    `TypedCallbackResponse` messages until a final marker is received.
//!
//! Tracing is emitted for I/O operations and state transitions; a per-request
//! correlation ID enables cross-process stream attribution.

use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};

#[tracing::instrument(level = "debug", skip(conn))]
/// Set the shared callback socket used by all child-initiated callbacks.
///
/// Returns `Err("connection already initialized")` if called more than once.
pub fn set_connection(conn: tokio::net::UnixStream) -> Result<(), &'static str> {
    kameo_child_process::callback_runtime::init(conn)
}

/// Python async iterator over a single callback stream.
///
/// The iterator reads demultiplexed `TypedCallbackResponse` messages from a
/// per-correlation receiver provided by the callback runtime. When the parent
/// indicates end-of-stream, or an error occurs, `__anext__` raises
/// `StopAsyncIteration`.
#[pyo3::pyclass]
pub struct CallbackAsyncIterator {
    correlation_id: u64,
    exhausted: Arc<std::sync::atomic::AtomicBool>,
    count: Arc<std::sync::atomic::AtomicUsize>,
    rx: Arc<Mutex<mpsc::UnboundedReceiver<kameo_child_process::callback::TypedCallbackResponse>>>,
}

#[pyo3::pymethods]
impl CallbackAsyncIterator {
    /// Part of Python async iterator protocol: returns `self`.
    fn __aiter__(slf: pyo3::PyRef<Self>) -> pyo3::PyRef<Self> { slf }

    /// Read and yield the next response item or raise `StopAsyncIteration` when
    /// the stream completes or the socket is closed.
    fn __anext__<'py>(&self, py: pyo3::Python<'py>) -> pyo3::PyResult<pyo3::Py<pyo3::PyAny>> {
        let correlation_id = self.correlation_id;
        let exhausted = self.exhausted.clone();
        let count = self.count.clone();
        let rx = self.rx.clone();

        Ok(pyo3_async_runtimes::tokio::future_into_py(py, async move {

            if exhausted.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(pyo3::exceptions::PyStopAsyncIteration::new_err(()));
            }

            // Receive next demuxed response for this correlation id
            let resp_opt = {
                let mut guard = rx.lock().await;
                guard.recv().await
            };

            let response = match resp_opt {
                Some(r) => r,
                None => {
                    exhausted.store(true, std::sync::atomic::Ordering::SeqCst);
                    return Err(pyo3::exceptions::PyStopAsyncIteration::new_err(()));
                }
            };

            if response.is_final {
                tracing::info!(correlation_id, "final response, terminating stream");
                exhausted.store(true, std::sync::atomic::Ordering::SeqCst);
                return Err(pyo3::exceptions::PyStopAsyncIteration::new_err(()));
            }

            let item_count = count.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
            tracing::info!(item_count, correlation_id, "yielding response item");

            pyo3::Python::with_gil(|py| -> pyo3::PyResult<pyo3::Py<pyo3::PyAny>> {
                let json_value = serde_json::json!({
                    "callback_path": response.callback_path,
                    "response_type": response.response_type,
                    "item_number": item_count,
                    "is_final": response.is_final,
                    "data": String::from_utf8_lossy(&response.response_data)
                });
                match crate::serde_py::to_pyobject(py, &json_value) {
                    Ok(py_obj) => Ok(py_obj),
                    Err(e) => Err(pyo3::exceptions::PyRuntimeError::new_err(format!("Failed to convert to Python object: {e}"))),
                }
            })
        })?.unbind())
    }
}

/// Entry point invoked by generated Python callables (e.g., `kameo.<module>.<HandlerType>`).
///
/// Writes a `TypedCallbackEnvelope` containing the serialized `py_msg` to the
/// shared callback socket and returns a `CallbackAsyncIterator` for streaming
/// responses.
#[pyo3::pyfunction]
pub fn callback_handle_inner<'py>(
    py: pyo3::Python<'py>,
    callback_path: &str,
    py_msg: &pyo3::Bound<'py, pyo3::PyAny>
) -> pyo3::PyResult<pyo3::Py<pyo3::PyAny>> {
    let callback_data: serde_json::Value = match crate::serde_py::from_pyobject(py_msg) {
        Ok(data) => data,
        Err(e) => return Err(pyo3::exceptions::PyRuntimeError::new_err(format!("Failed to convert Python message: {e}"))),
    };

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

    Ok(pyo3_async_runtimes::tokio::future_into_py(py, async move {
        // Register a route and get the receiver for this correlation id
        let rx = kameo_child_process::callback_runtime::register_stream(correlation_id)
            .await
            .map_err(pyo3::exceptions::PyRuntimeError::new_err)?;

        // Send the envelope via the writer task
        kameo_child_process::callback_runtime::send(envelope)
            .map_err(pyo3::exceptions::PyRuntimeError::new_err)?;

        // Wrap the receiver into a PyO3 class
        pyo3::Python::with_gil(|py| -> pyo3::PyResult<pyo3::Py<pyo3::PyAny>> {
            let iterator = CallbackAsyncIterator {
                correlation_id,
                exhausted: Arc::new(std::sync::atomic::AtomicBool::new(false)),
                count: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
                rx: Arc::new(Mutex::new(rx)),
            };
            let py_iterator = pyo3::Py::new(py, iterator)?;
            Ok(py_iterator.into())
        })
    })?.unbind())
}


