#![forbid(unsafe_code)]

use std::io;
use std::marker::PhantomData;
use std::sync::Arc;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use bincode::{Decode, Encode};
use opentelemetry::{global, Context as OTelContext};
use serde::de::DeserializeOwned;
use serde::Serialize;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{Mutex as TokioMutex, oneshot};
use tracing::{error, instrument, Instrument};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use rand::random;
use serde::Deserialize;
use tokio::net::UnixStream;
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::mpsc;

use crate::{AsyncReadWrite, TracingContext, WithTracingContext};
use crate::error::PythonExecutionError;

#[derive(Debug, Error)]
pub enum CallbackError {
    #[error("IPC error: {0}")]
    Ipc(#[from] io::Error),
    #[error("Serialization error: {0}")]
    Serialization(#[from] bincode::error::EncodeError),
    #[error("Deserialization error: {0}")]
    Deserialization(#[from] bincode::error::DecodeError),
    #[error("Connection closed")]
    ConnectionClosed,
}

pub trait ChildCallbackMessage:
    Send + Serialize + DeserializeOwned + Encode + Decode<()> + std::fmt::Debug + 'static
{
    type Reply: Send
        + Serialize
        + DeserializeOwned
        + Encode
        + Decode<()>
        + std::fmt::Debug
        + 'static;
}

#[async_trait]
pub trait CallbackSender<M, E>
where
    M: ChildCallbackMessage + Send + Sync + 'static,
    E: std::fmt::Debug + Send + Sync + 'static + bincode::Encode + bincode::Decode<()>,
{
    fn set_callback_handle(&mut self, handle: Arc<CallbackHandle<M, E>>);
}

#[derive(Serialize, Deserialize, Encode, Decode, Debug)]
pub struct CallbackEnvelope<T> {
    pub correlation_id: u64,
    pub inner: T,
    pub context: TracingContext,
}

#[derive(Debug)]
pub struct CallbackHandle<M, E>
where
    M: ChildCallbackMessage + Send + Sync + 'static,
    E: std::fmt::Debug + Send + Sync + 'static + bincode::Encode + bincode::Decode<()>,
{
    write_half: Arc<TokioMutex<tokio::net::unix::OwnedWriteHalf>>,
    _phantom: PhantomData<M>,
    in_flight: Arc<TokioMutex<HashMap<u64, oneshot::Sender<Result<M::Reply, E>>>>>,
    next_id: AtomicU64,
    shutdown_sender: Option<tokio::sync::oneshot::Sender<()>>,
}

impl<M, E> CallbackHandle<M, E>
where
    M: ChildCallbackMessage + Send + Sync + 'static,
    E: std::fmt::Debug + Send + Sync + 'static + bincode::Encode + bincode::Decode<()>,
{
    pub fn new(connection: Box<tokio::net::UnixStream>) -> Self {
        let (shutdown_sender, mut shutdown_receiver) = tokio::sync::oneshot::channel();
        // Split the connection into read and write halves
        let (read_half, write_half) = (*connection).into_split();
        let write_half = Arc::new(TokioMutex::new(write_half));
        let in_flight = Arc::new(TokioMutex::new(HashMap::<u64, oneshot::Sender<Result<M::Reply, E>>>::new()));
        // Spawn reply loop with read_half
        let reply_loop_in_flight = in_flight.clone();
        tokio::spawn(async move {
            let mut read_conn = read_half;
            loop {
                let mut buf = vec![0u8; 1024 * 64];
                tokio::select! {
                    _ = &mut shutdown_receiver => {
                        break;
                    }
                    read_result = read_conn.read(&mut buf) => {
                        let n = match read_result {
                            Ok(0) => break,
                            Ok(n) => n,
                            Err(_) => break,
                        };
                        let (event, step) = ("callback_reply_loop", "received_data");
                        tracing::trace!(event, step, n, "Received data in callback reply loop");
                        let (envelope, _): (CallbackEnvelope<Result<M::Reply, E>>, _) = match bincode::decode_from_slice(&buf[..n], bincode::config::standard()) {
                            Ok(val) => {
                                tracing::trace!(event = "bincode_decode", type_deserialized = std::any::type_name::<CallbackEnvelope<Result<M::Reply, E>>>(), len = n, "Decoding CallbackEnvelope<Result>");
                                val
                            },
                            Err(_) => continue,
                        };
                        let tx = {
                            let mut in_flight = reply_loop_in_flight.lock().await;
                            in_flight.remove(&envelope.correlation_id)
                        };
                        if let Some(tx) = tx {
                            let _ = tx.send(envelope.inner);
                        }
                    }
                }
            }
        });
        Self {
            write_half,
            _phantom: PhantomData,
            in_flight,
            next_id: AtomicU64::new(1),
            shutdown_sender: Some(shutdown_sender),
        }
    }

    pub async fn ask(&self, msg: M) -> Result<M::Reply, CallbackError> {
        let span_id = tracing::Span::current().id().map(|id| id.into_u64()).unwrap_or_else(|| self.next_id.fetch_add(1, Ordering::SeqCst));
        let mut trace_context_map = std::collections::HashMap::new();
        global::get_text_map_propagator(|propagator| {
            propagator.inject_context(&OTelContext::current(), &mut trace_context_map);
        });
        let trace_context = TracingContext(trace_context_map);
        let envelope = CallbackEnvelope {
            correlation_id: span_id,
            inner: msg,
            context: trace_context,
        };
        let msg_bytes = bincode::encode_to_vec(&envelope, bincode::config::standard())?;
        tracing::trace!(event = "bincode_encode", type_serialized = std::any::type_name::<CallbackEnvelope<M>>(), len = msg_bytes.len(), "Encoding CallbackEnvelope");
        let (tx, rx) = oneshot::channel();
        {
            let mut in_flight = self.in_flight.lock().await;
            in_flight.insert(span_id, tx);
        }
        {
            let mut write_half = self.write_half.lock().await;
            write_half.write_all(&msg_bytes).await?;
            write_half.flush().await?;
        }
        match rx.await {
            Ok(reply) => match reply {
                Ok(val) => Ok(val),
                Err(_e) => Err(CallbackError::Deserialization(bincode::error::DecodeError::Other("callback handler returned error"))),
            },
            Err(_) => Err(CallbackError::ConnectionClosed),
        }
    }
}

impl<M, E> Drop for CallbackHandle<M, E>
where
    M: ChildCallbackMessage + Send + Sync + 'static,
    E: std::fmt::Debug + Send + Sync + 'static + bincode::Encode + bincode::Decode<()>,
{
    fn drop(&mut self) {
        if let Some(sender) = self.shutdown_sender.take() {
            let _ = sender.send(());
        }
    }
}

#[async_trait]
pub trait CallbackHandler<C>: Send + Sync + 'static
where
    C: ChildCallbackMessage,
{
    async fn handle(&mut self, callback: C) -> Result<C::Reply, PythonExecutionError>;
}

#[derive(Clone)]
pub struct NoopCallbackHandler<C>(std::marker::PhantomData<C>);

impl<C> Default for NoopCallbackHandler<C> {
    fn default() -> Self {
        NoopCallbackHandler(std::marker::PhantomData)
    }
}

#[async_trait]
impl<C> CallbackHandler<C> for NoopCallbackHandler<C>
where
    C: ChildCallbackMessage + Sync + 'static,
{
    async fn handle(&mut self, _callback: C) -> Result<C::Reply, PythonExecutionError> {
        panic!("NoopCallbackHandler called but no Default for callback reply; implement your own handler if you need a real reply");
    }
}

pub struct CallbackReceiver<M, H>
where
    M: ChildCallbackMessage,
    H: CallbackHandler<M> + Clone,
{
    read_half: tokio::net::unix::OwnedReadHalf,
    write_half: tokio::net::unix::OwnedWriteHalf,
    handler: H,
    _phantom: PhantomData<(M, H)>,
}

impl<M, H> CallbackReceiver<M, H>
where
    M: ChildCallbackMessage,
    H: CallbackHandler<M> + Clone,
{
    pub fn new(connection: Box<tokio::net::UnixStream>, handler: H) -> Self {
        let (read_half, write_half) = (*connection).into_split();
        Self {
            read_half,
            write_half,
            handler,
            _phantom: PhantomData,
        }
    }

    #[instrument(skip(self), fields(message_type = std::any::type_name::<M>()))]
    pub async fn run(self) -> Result<(), CallbackError> {
        tracing::info!(event = "callback_receiver", step = "start", "CallbackReceiver started, waiting for callback messages");
        let (req_tx, mut req_rx) = mpsc::channel::<CallbackEnvelope<M>>(128);
        let (reply_tx, mut reply_rx) = mpsc::channel::<Vec<u8>>(128);
        let mut read_conn = self.read_half;
        let mut write_conn = self.write_half;
        let mut handler = self.handler;

        // Reader task
        let reader = tokio::spawn(async move {
            loop {
                let mut buf = vec![0u8; 4096];
                let n = match read_conn.read(&mut buf).await {
                    Ok(0) => break,
                    Ok(n) => n,
                    Err(_) => break,
                };
                if n == 0 { break; }
                let (envelope, _): (CallbackEnvelope<M>, _) = match bincode::decode_from_slice(&buf[..n], bincode::config::standard()) {
                    Ok(decoded) => decoded,
                    Err(_) => continue,
                };
                if req_tx.send(envelope).await.is_err() { break; }
            }
        });

        // Handler pool (spawns a task per request)
        let handler_pool = tokio::spawn(async move {
            while let Some(envelope) = req_rx.recv().await {
                let reply_tx = reply_tx.clone();
                let mut handler = handler.clone();
                tokio::spawn(async move {
                    let parent_cx = global::get_text_map_propagator(|propagator| {
                        propagator.extract(&envelope.context.0)
                    });
                    let span = tracing::info_span!("callback_handler", process_role = "child");
                    span.set_parent(parent_cx);
                    let reply = handler.handle(envelope.inner).instrument(span).await;
                    let mut trace_context_map = std::collections::HashMap::new();
                    global::get_text_map_propagator(|propagator| {
                        propagator.inject_context(&OTelContext::current(), &mut trace_context_map);
                    });
                    let trace_context = TracingContext(trace_context_map);
                    let reply_envelope = CallbackEnvelope {
                        correlation_id: envelope.correlation_id,
                        inner: reply,
                        context: trace_context,
                    };
                    let reply_bytes = match bincode::encode_to_vec(&reply_envelope, bincode::config::standard()) {
                        Ok(bytes) => bytes,
                        Err(_) => return,
                    };
                    let _ = reply_tx.send(reply_bytes).await;
                });
            }
        });

        // Writer task
        let writer = tokio::spawn(async move {
            while let Some(reply_bytes) = reply_rx.recv().await {
                if write_conn.write_all(&reply_bytes).await.is_err() { break; }
                if write_conn.flush().await.is_err() { break; }
            }
        });

        let _ = tokio::try_join!(reader, handler_pool, writer);
        Ok(())
    }
}
