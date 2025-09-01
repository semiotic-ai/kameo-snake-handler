//! Child-side callback runtime: single reader/writer tasks and per-stream routing.
//!
//! This module provides a multiplexed callback runtime for the child process.
//! It uses the crate's framing and callback types to:
//! - Serialize `TypedCallbackEnvelope` writes via one writer task
//! - Demultiplex `TypedCallbackResponse` reads via one reader task
//! - Route responses by `correlation_id` to per-stream receivers
//!
//! The API is intentionally small:
//! - `init(stream)` — initialize once with a connected socket
//! - `send(envelope)` — enqueue a new envelope for writing
//! - `register_stream(correlation_id)` — create a receiver for responses with that id

use crate::callback::{TypedCallbackEnvelope, TypedCallbackResponse};
use crate::framing::{LengthPrefixedRead, LengthPrefixedWrite};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};

static MANAGER: std::sync::OnceLock<std::sync::Mutex<Option<Arc<InnerRuntime>>>> =
    std::sync::OnceLock::new();

struct InnerRuntime {
    write_tx: mpsc::UnboundedSender<TypedCallbackEnvelope>,
    routes: Arc<Mutex<HashMap<u64, mpsc::UnboundedSender<TypedCallbackResponse>>>>,
}

/// Initialize the callback runtime with a connected UnixStream.
///
/// Returns an error if the runtime is already initialized.
pub fn init(conn: tokio::net::UnixStream) -> Result<(), &'static str> {
    let mgr = MANAGER.get_or_init(|| std::sync::Mutex::new(None));
    // Replace any existing inner runtime
    let (read_half, write_half) = conn.into_split();
    let routes: Arc<Mutex<HashMap<u64, mpsc::UnboundedSender<TypedCallbackResponse>>>> = Arc::new(
        Mutex::new(HashMap::new()),
    );

    // Writer task
    let (tx, mut rx) = mpsc::unbounded_channel::<TypedCallbackEnvelope>();
    tokio::spawn(async move {
        let mut writer = LengthPrefixedWrite::new(write_half);
        while let Some(msg) = rx.recv().await {
            tracing::trace!(event = "callback_writer_send", correlation_id = msg.correlation_id);
            if let Err(e) = writer.write_msg(&msg).await {
                tracing::error!(error = ?e, "callback_writer_write_error");
            }
        }
        tracing::debug!("callback_writer_task_exit");
    });

    // Reader task
    {
        let routes = routes.clone();
        tokio::spawn(async move {
            let mut reader = LengthPrefixedRead::new(read_half);
            loop {
                match reader.read_msg::<TypedCallbackResponse>().await {
                    Ok(resp) => {
                        let cid = resp.correlation_id;
                        let is_final = resp.is_final;
                        tracing::trace!(event = "callback_reader_got", correlation_id = cid, is_final);
                        let target = {
                            let map = routes.lock().await;
                            tracing::trace!(event = "callback_routes_len", size = map.len());
                            map.get(&cid).cloned()
                        };
                        if let Some(sender) = target {
                            let _ = sender.send(resp);
                            if is_final {
                                let mut map = routes.lock().await;
                                map.remove(&cid);
                            }
                        } else {
                            tracing::warn!(correlation_id = cid, "no_route_for_callback_response");
                        }
                    }
                    Err(e) => {
                        tracing::info!(error = ?e, "callback_reader_task_exit");
                        break;
                    }
                }
            }
        });
    }

    let rt = Arc::new(InnerRuntime { write_tx: tx, routes });
    let mut guard = mgr.lock().map_err(|_| "poisoned")?;
    *guard = Some(rt);
    Ok(())
}

/// Queue a `TypedCallbackEnvelope` to be written by the writer task.
pub fn send(msg: TypedCallbackEnvelope) -> Result<(), &'static str> {
    let mgr = MANAGER.get().ok_or("runtime not initialized")?;
    let inner_opt = mgr.lock().map_err(|_| "poisoned")?.clone();
    let inner = inner_opt.ok_or("runtime not initialized")?;
    inner.write_tx.send(msg).map_err(|_| "writer closed")
}

/// Register a new stream route and obtain a receiver for responses.
pub async fn register_stream(
    correlation_id: u64,
) -> Result<mpsc::UnboundedReceiver<TypedCallbackResponse>, &'static str> {
    let mgr = MANAGER.get().ok_or("runtime not initialized")?;
    let (tx, rx) = mpsc::unbounded_channel();
    let inner_opt = mgr.lock().map_err(|_| "poisoned")?.clone();
    let inner = inner_opt.ok_or("runtime not initialized")?;
    let mut map = inner.routes.lock().await;
    map.insert(correlation_id, tx);
    Ok(rx)
}

/// Shutdown the runtime (tests): drop current inner to allow re-initialization.
pub fn shutdown() {
    if let Some(mgr) = MANAGER.get() {
        if let Ok(mut guard) = mgr.lock() {
            *guard = None;
        }
    }
}


