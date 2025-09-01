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
use tokio::sync::Mutex;

static CALLBACK_CONNECTION: std::sync::OnceLock<Arc<Mutex<tokio::net::UnixStream>>> = std::sync::OnceLock::new();

#[tracing::instrument(level = "debug", skip(conn))]
/// Set the shared callback socket used by all child-initiated callbacks.
///
/// Returns `Err("connection already initialized")` if called more than once.
pub fn set_connection(conn: tokio::net::UnixStream) -> Result<(), &'static str> {
    let connection_mutex = Arc::new(Mutex::new(conn));
    CALLBACK_CONNECTION.set(connection_mutex).map_err(|_| "connection already initialized")
}

/// Python async iterator over a single callback stream.
///
/// The iterator reads length-prefixed `TypedCallbackResponse` messages from the
/// shared socket. When the parent indicates end-of-stream, or an error occurs,
/// `__anext__` raises `StopAsyncIteration`.
#[pyo3::pyclass]
pub struct CallbackAsyncIterator {
    callback_conn: Arc<Mutex<tokio::net::UnixStream>>, // shared reader
    correlation_id: u64,
    exhausted: Arc<std::sync::atomic::AtomicBool>,
    count: Arc<std::sync::atomic::AtomicUsize>,
}

#[pyo3::pymethods]
impl CallbackAsyncIterator {
    /// Part of Python async iterator protocol: returns `self`.
    fn __aiter__(slf: pyo3::PyRef<Self>) -> pyo3::PyRef<Self> { slf }

    /// Read and yield the next response item or raise `StopAsyncIteration` when
    /// the stream completes or the socket is closed.
    fn __anext__<'py>(&self, py: pyo3::Python<'py>) -> pyo3::PyResult<pyo3::Py<pyo3::PyAny>> {
        let callback_conn = self.callback_conn.clone();
        let correlation_id = self.correlation_id;
        let exhausted = self.exhausted.clone();
        let count = self.count.clone();

        Ok(pyo3_async_runtimes::tokio::future_into_py(py, async move {
            use tokio::io::AsyncReadExt;

            if exhausted.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(pyo3::exceptions::PyStopAsyncIteration::new_err(()));
            }

            let mut conn_guard = callback_conn.lock().await;

            let mut length_bytes = [0u8; 4];
            if let Err(e) = conn_guard.read_exact(&mut length_bytes).await {
                tracing::info!(error = %e, "callback reader length read error or closed");
                exhausted.store(true, std::sync::atomic::Ordering::SeqCst);
                return Err(pyo3::exceptions::PyStopAsyncIteration::new_err(()));
            }

            let length = u32::from_le_bytes(length_bytes) as usize;
            tracing::debug!(length, "reading response message");

            let mut message_bytes = vec![0u8; length];
            if let Err(e) = conn_guard.read_exact(&mut message_bytes).await {
                tracing::error!(error = %e, "callback reader body read error");
                exhausted.store(true, std::sync::atomic::Ordering::SeqCst);
                return Err(pyo3::exceptions::PyStopAsyncIteration::new_err(()));
            }

            let response: kameo_child_process::callback::TypedCallbackResponse =
                serde_brief::from_slice(&message_bytes).map_err(|e| {
                    tracing::error!(error = %e, "deserialize TypedCallbackResponse failed");
                    pyo3::exceptions::PyRuntimeError::new_err(format!("Failed to deserialize response: {}", e))
                })?;

            tracing::debug!(got = response.correlation_id, expected = correlation_id, "received response");

            if response.correlation_id != correlation_id {
                tracing::warn!(got = response.correlation_id, expected = correlation_id, "correlation id mismatch");
                exhausted.store(true, std::sync::atomic::Ordering::SeqCst);
                return Err(pyo3::exceptions::PyStopAsyncIteration::new_err(()));
            }

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
        let callback_conn_mutex = CALLBACK_CONNECTION
            .get()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("Callback connection not initialized"))?;
        let mut callback_conn_guard = callback_conn_mutex.lock().await;

        let envelope_bytes = serde_brief::to_vec(&envelope)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("Failed to serialize envelope: {e}")))?;

        let length = envelope_bytes.len() as u32;
        let mut message = Vec::new();
        message.extend_from_slice(&length.to_le_bytes());
        message.extend_from_slice(&envelope_bytes);

        use tokio::io::AsyncWriteExt;
        callback_conn_guard
            .write_all(&message)
            .await
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("Failed to send callback envelope: {e}")))?;
        callback_conn_guard
            .flush()
            .await
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("Failed to flush callback envelope: {e}")))?;

        let callback_conn_for_reader = CALLBACK_CONNECTION
            .get()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("Callback connection not initialized"))?
            .clone();

        pyo3::Python::with_gil(|py| -> pyo3::PyResult<pyo3::Py<pyo3::PyAny>> {
            let iterator = CallbackAsyncIterator {
                callback_conn: callback_conn_for_reader,
                correlation_id,
                exhausted: Arc::new(std::sync::atomic::AtomicBool::new(false)),
                count: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            };
            let py_iterator = pyo3::Py::new(py, iterator)?;
            Ok(py_iterator.into())
        })
    })?.unbind())
}


