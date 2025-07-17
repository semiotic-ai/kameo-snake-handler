#![forbid(unsafe_code)]

use std::io;
use std::marker::PhantomData;
use std::sync::Arc;

use async_trait::async_trait;
use bincode::{Decode, Encode};
use serde::Serialize;
use thiserror::Error;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::trace;
use tracing::{error, instrument};
use serde::Deserialize;

use crate::TracingContext;
use crate::error::PythonExecutionError;
use crate::framing::{LengthPrefixedRead, LengthPrefixedWrite};
use crate::InFlightMap;
use crate::ReplySlot;

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

#[derive(Serialize, Deserialize, Encode, Decode, Debug)]
pub struct CallbackEnvelope<T> {
    pub correlation_id: u64,
    pub inner: T,
    pub context: TracingContext,
}

#[async_trait]
pub trait CallbackHandler<C>: Send + Sync + 'static {
    async fn handle(&self, callback: C) -> Result<(), PythonExecutionError>;
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
    C: Send + Sync + 'static,
{
    async fn handle(&self, _callback: C) -> Result<(), PythonExecutionError> {
        panic!("NoopCallbackHandler called; implement your own handler if you need a real reply");
    }
}

/// Child-side message pump: forwards callback messages to the parent process over a channel.
/// Use this as the callback handler in the child process.
pub struct CallbackForwarder<C> {
    sender: tokio::sync::mpsc::UnboundedSender<C>,
}

impl<C> CallbackForwarder<C> {
    pub fn new(sender: tokio::sync::mpsc::UnboundedSender<C>) -> Self {
        Self { sender }
    }
}

#[async_trait]
impl<C> CallbackHandler<C> for CallbackForwarder<C>
where
    C: Send + Sync + 'static,
{
    async fn handle(&self, callback: C) -> Result<(), PythonExecutionError> {
        self.sender.send(callback).map_err(|_| PythonExecutionError::ExecutionError {
            message: "Failed to forward callback to parent (channel closed)".to_string(),
        })
    }
}

pub type CallbackHandle<C> = std::sync::Arc<dyn CallbackHandler<C>>;

/// Multiplexed callback protocol artefact for child processes.
/// Owns the callback socket, maintains in-flight map, and implements CallbackHandler<C>.
/// This is the only production callback handler for child processes.
pub struct CallbackIpcChild<C> {
    pub in_flight: InFlightMap<Result<(), PythonExecutionError>>,
    write_tx: tokio::sync::mpsc::UnboundedSender<CallbackWriteRequest<C>>,
    next_id: std::sync::atomic::AtomicU64,
    cancellation_token: tokio_util::sync::CancellationToken,
}

struct CallbackWriteRequest<C> {
    envelope: CallbackEnvelope<C>,
}

impl<C> CallbackIpcChild<C>
where
    C: Send + Sync + Encode + Decode<()> + 'static,
{
    pub fn from_duplex(duplex: crate::DuplexUnixStream) -> std::sync::Arc<Self> {
        let (read_half, write_half) = duplex.into_inner().into_split();
        Self::new(read_half, write_half)
    }
    pub fn new(
        read_half: tokio::net::unix::OwnedReadHalf,
        write_half: tokio::net::unix::OwnedWriteHalf,
    ) -> std::sync::Arc<Self> {
        use tokio::sync::mpsc::unbounded_channel;
        let (write_tx, mut write_rx) = unbounded_channel::<CallbackWriteRequest<C>>();
        let in_flight = InFlightMap::new();
        let in_flight_reader = in_flight.clone();
        let cancellation_token = tokio_util::sync::CancellationToken::new();
        let cancellation_token_writer = cancellation_token.clone();
        let cancellation_token_reader = cancellation_token.clone();
        // Writer task
        tokio::spawn(async move {
            let mut writer = LengthPrefixedWrite::new(write_half);
            loop {
                tokio::select! {
                    _ = cancellation_token_writer.cancelled() => break,
                    Some(write_req) = write_rx.recv() => {
                        let env = write_req.envelope;
                        if writer.write_msg(&env).await.is_err() {
                            break;
                        }
                    }
                }
            }
        });
        // Reader task
        tokio::spawn(async move {
            let mut reader = LengthPrefixedRead::new(read_half);
            loop {
                tokio::select! {
                    _ = cancellation_token_reader.cancelled() => break,
                    result = reader.read_msg::<CallbackEnvelope<Result<(), PythonExecutionError>>>() => {
                        match result {
                            Ok(env) => {
                                let correlation_id = env.correlation_id;
                                if let Some((_, mut slot)) = in_flight_reader.0.remove(&correlation_id) {
                                    slot.set_and_notify(env.inner).await;
                                }
                            }
                            Err(e) => {
                                if let std::io::ErrorKind::UnexpectedEof = e.kind() {
                                    tracing::debug!(event = "callback_ipc_child_read_eof", error = ?e, "CallbackIpcChild reader task got EOF (expected on clean shutdown)");
                                    // EOF is expected on clean shutdown
                                } else {
                                    tracing::error!(event = "callback_ipc_child_read_error", error = ?e, "CallbackIpcChild reader task error, exiting");
                                }
                                break;
                            }
                        }
                    }
                }
            }
            // After the reader loop, drain in_flight with error, signalling that the child process has exited
            in_flight_reader.0.retain(|_corr_id, slot| {
                slot.try_set_err(PythonExecutionError::ExecutionError { message: "Callback reply loop exited (EOF)".to_string() });
                false
            });
        });
        std::sync::Arc::new(Self {
            in_flight,
            write_tx,
            next_id: std::sync::atomic::AtomicU64::new(1),
            cancellation_token,
        })
    }
    fn next_correlation_id(&self) -> u64 {
        self.next_id.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }
    pub fn shutdown(&self) {
        self.cancellation_token.cancel();
    }
}

#[async_trait]
impl<C> CallbackHandler<C> for CallbackIpcChild<C>
where
    C: Send + Sync + Encode + Decode<()> + 'static,
{
    async fn handle(&self, callback: C) -> Result<(), PythonExecutionError> {
        let correlation_id = self.next_correlation_id();
        let envelope = CallbackEnvelope {
            correlation_id,
            inner: callback,
            context: TracingContext::default(),
        };
        let write_req = CallbackWriteRequest { envelope };
        let reply_slot: ReplySlot<Result<(), PythonExecutionError>> = ReplySlot::new();
        self.in_flight.0.insert(correlation_id, reply_slot);
        if let Err(e) = self.write_tx.send(write_req) {
            self.in_flight.0.remove(&correlation_id);
            return Err(PythonExecutionError::ExecutionError { message: format!("Failed to send callback write request: {e}") });
        }
        let mut slot = self.in_flight.0.get_mut(&correlation_id)
            .expect("ReplySlot must exist in in_flight map");
        let receiver = slot.receiver.take();
        drop(slot);
        match receiver {
            Some(receiver) => match receiver.await {
                Ok(res) => res,
                Err(_) => Err(PythonExecutionError::ExecutionError { message: "Reply missing after notify".to_string() }),
            },
            None => Err(PythonExecutionError::ExecutionError { message: "Reply slot already taken".to_string() }),
        }
    }
}

pub struct CallbackReceiver<M, H>
where
    M: Send + Sync + Decode<()> + 'static,
    H: CallbackHandler<M> + Clone + Send + Sync + 'static,
{
    read_half: tokio::net::unix::OwnedReadHalf,
    write_half: Option<tokio::net::unix::OwnedWriteHalf>,
    handler: H,
    cancellation_token: CancellationToken,
    _phantom: PhantomData<(M, H)>,
}

impl<M, H> CallbackReceiver<M, H>
where
    M: Send + Sync + Decode<()> + 'static,
    H: CallbackHandler<M> + Clone + Send + Sync + 'static,
{
    pub fn from_duplex(duplex: crate::DuplexUnixStream, handler: H) -> Self {
        let (read_half, write_half) = duplex.into_inner().into_split();
        let cancellation_token = CancellationToken::new();
        Self {
            read_half,
            write_half: Some(write_half),
            handler,
            cancellation_token,
            _phantom: PhantomData,
        }
    }
    pub fn shutdown(&self) {
        self.cancellation_token.cancel();
    }
    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancellation_token.clone()
    }
    #[instrument(skip(self), fields(message_type = std::any::type_name::<M>()))]
    pub async fn run(self) -> Result<(), CallbackError> {
        let CallbackReceiver { read_half, write_half, handler, cancellation_token, _phantom } = self;
        tracing::debug!(event = "callback_receiver", step = "start", "CallbackReceiver started, waiting for callback messages");
        let (req_tx, mut req_rx) = tokio::sync::mpsc::unbounded_channel::<CallbackEnvelope<M>>();
        let (reply_tx, mut reply_rx) = tokio::sync::mpsc::unbounded_channel::<CallbackEnvelope<Result<(), PythonExecutionError>>>();
        let reply_tx = Arc::new(reply_tx);
        let cancellation_token_reader = cancellation_token.clone();
        let reader_task = tokio::spawn(async move {
            let mut reader = LengthPrefixedRead::new(read_half);
            let req_tx = req_tx;
            loop {
                tokio::select! {
                    _ = cancellation_token_reader.cancelled() => {
                        tracing::debug!(event = "callback_receiver", task = "reader", step = "shutdown_signal", "Reader task received shutdown signal, exiting");
                        drop(req_tx);
                        break;
                    }
                    result = reader.read_msg::<CallbackEnvelope<M>>() => {
                        let envelope = match result {
                            Ok(env) => env,
                            Err(e) => {
                                if let std::io::ErrorKind::UnexpectedEof = e.kind() {
                                    tracing::debug!(event = "callback_receiver", task = "reader", step = "read_eof", error = ?e, "Reader task got EOF (expected on clean shutdown)");
                                } else {
                                    tracing::error!(event = "callback_receiver", task = "reader", step = "read_error", error = ?e, "Reader task read error, exiting");
                                }
                                drop(req_tx);
                                break;
                            }
                        };
                        tracing::trace!(event = "callback_receiver", task = "reader", step = "msg_received", correlation_id = envelope.correlation_id, "Read callback request from socket");
                        if req_tx.send(envelope).is_err() {
                            tracing::debug!(event = "callback_receiver", task = "reader", step = "req_tx_closed", "Request channel closed, exiting");
                            drop(req_tx);
                            break;
                        }
                    }
                }
            }
            tracing::debug!(event = "callback_receiver", task = "reader", step = "exit", "Reader task exiting");
            Ok::<(), CallbackError>(())
        });
        let reply_tx_handler = reply_tx.clone();
        let cancellation_token_handler = cancellation_token.clone();
        let handler_pool = tokio::spawn(async move {
            let mut tasks = FuturesUnordered::new();
            let mut req_rx_closed = false;
            let handler = handler;
            loop {
                tokio::select! {
                    biased;
                    _ = cancellation_token_handler.cancelled() => {
                        tracing::debug!(event = "callback_receiver", task = "handler_pool", step = "shutdown_signal", "Handler pool received shutdown signal, breaking loop");
                        break;
                    }
                    maybe_envelope = req_rx.recv(), if !req_rx_closed => {
                        match maybe_envelope {
                            Some(envelope) => {
                                let correlation_id = envelope.correlation_id;
                                let msg = envelope.inner;
                                let handler = handler.clone();
                                let reply_tx = reply_tx_handler.clone();
                                tasks.push(tokio::spawn(async move {
                                    let result = handler.handle(msg).await;
                                    let reply_envelope = CallbackEnvelope {
                                        correlation_id,
                                        inner: result,
                                        context: Default::default(),
                                    };
                                    if let Err(e) = reply_tx.send(reply_envelope) {
                                        tracing::error!(event = "callback_receiver", task = "handler_task", step = "send_error", correlation_id, error = ?e, "Failed to send reply envelope");
                                    }
                                }));
                            }
                            None => {
                                tracing::debug!(event = "callback_receiver", task = "handler_pool", step = "req_rx_closed", "Request channel closed");
                                req_rx_closed = true;
                            }
                        }
                    }
                    maybe_task = tasks.next() => {
                        if let Some(Err(e)) = maybe_task {
                            tracing::error!(event = "callback_receiver", task = "handler_pool", step = "task_error", error = ?e, "Handler task error");
                        }
                    }
                }
                if req_rx_closed && tasks.is_empty() {
                    tracing::debug!(event = "callback_receiver", task = "handler_pool", step = "drain_complete", "All handler tasks complete and request channel closed, breaking loop");
                    break;
                }
            }
            tracing::debug!(event = "callback_receiver", task = "handler_pool", step = "exit", "Handler pool future exiting");
            Ok::<(), CallbackError>(())
        });
        let cancellation_token_writer = cancellation_token.clone();
        let writer_task = tokio::spawn(async move {
            let mut writer = LengthPrefixedWrite::new(write_half.expect("write_half missing in CallbackReceiver"));
            loop {
                tokio::select! {
                    _ = cancellation_token_writer.cancelled() => {
                        trace!(event = "callback_receiver", task = "writer", step = "shutdown_signal", "Writer received shutdown, exiting");
                        break;
                    }
                    maybe_reply = reply_rx.recv() => {
                        if let Some(reply_envelope) = maybe_reply {
                            if let Err(e) = writer.write_msg(&reply_envelope).await {
                                tracing::error!(event = "callback_receiver", task = "writer", step = "write_error", correlation_id = reply_envelope.correlation_id, error = ?e, "Failed to write reply envelope to socket");
                                break;
                            }
                            tracing::debug!(event = "callback_receiver", task = "writer", step = "reply_written", correlation_id = reply_envelope.correlation_id, "Wrote reply envelope to socket");
                        } else {
                            trace!(event = "callback_receiver", task = "writer", step = "channel_closed", "Reply channel closed, exiting");
                            break;
                        }
                    }
                }
            }
            trace!(event = "callback_receiver", task = "writer", step = "exit", "Writer task exiting");
            Ok::<(), CallbackError>(())
        });
        drop(reply_tx);
        let (reader_res, handler_res, writer_res) = tokio::try_join!(reader_task, handler_pool, writer_task)
            .map_err(|e| CallbackError::Ipc(std::io::Error::new(std::io::ErrorKind::Other, format!("Join error: {e}"))))?;
        reader_res?;
        handler_res?;
        writer_res?;
        Ok(())
    }
}

