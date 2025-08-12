#![forbid(unsafe_code)]

use std::io;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

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

/// Control messages for streaming callbacks
#[derive(Serialize, Deserialize, Encode, Decode, Debug)]
pub enum CallbackControl {
    /// A stream item with bincode-serialized data
    StreamItem(Result<Vec<u8>, PythonExecutionError>),
    /// Signal that the stream is complete
    StreamComplete,
}

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

use futures::stream::Stream;
use std::pin::Pin;

/// Streaming callback handler trait - handles callbacks and returns a stream of responses
/// This trait is implemented by handlers that know their specific response type
#[async_trait]
pub trait CallbackStreamHandler<C>: Send + Sync + 'static {
    type Response: bincode::Encode + Send + 'static;
    async fn handle_stream(&self, callback: C) -> Result<Pin<Box<dyn Stream<Item = Result<Self::Response, PythonExecutionError>> + Send>>, PythonExecutionError>;
}

/// Helper function to serialize a stream of responses to bincode bytes
pub fn serialize_response_stream<R, S>(stream: S) -> impl Stream<Item = Result<Vec<u8>, PythonExecutionError>> + Send
where
    R: bincode::Encode + Send + 'static,
    S: Stream<Item = Result<R, PythonExecutionError>> + Send,
{
    futures::StreamExt::map(stream, |result| {
        match result {
            Ok(response) => {
                match bincode::encode_to_vec(&response, bincode::config::standard()) {
                    Ok(bytes) => Ok(bytes),
                    Err(e) => Err(PythonExecutionError::ExecutionError { 
                        message: format!("Failed to serialize response: {e}")
                    }),
                }
            },
            Err(e) => Err(e),
        }
    })
}



#[derive(Clone)]
pub struct NoopCallbackHandler<C>(std::marker::PhantomData<C>);

impl<C> Default for NoopCallbackHandler<C> {
    fn default() -> Self {
        NoopCallbackHandler(std::marker::PhantomData)
    }
}


#[async_trait]
impl<C> CallbackStreamHandler<C> for NoopCallbackHandler<C>
where
    C: Send + Sync + 'static,
{
    type Response = ();
    
    async fn handle_stream(&self, _callback: C) -> Result<Pin<Box<dyn Stream<Item = Result<Self::Response, PythonExecutionError>> + Send>>, PythonExecutionError> {
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
impl<C> CallbackStreamHandler<C> for CallbackForwarder<C>
where
    C: Send + Sync + 'static,
{
    type Response = ();
    
    async fn handle_stream(&self, callback: C) -> Result<Pin<Box<dyn Stream<Item = Result<Self::Response, PythonExecutionError>> + Send>>, PythonExecutionError> {
        use futures::stream;
        let result = self.sender.send(callback).map_err(|_| PythonExecutionError::ExecutionError {
            message: "Failed to forward callback to parent (channel closed)".to_string(),
        });
        Ok(Box::pin(stream::once(async move { result })))
    }
}

pub type CallbackHandle<C> = std::sync::Arc<CallbackIpcChild<C>>;

/// Multiplexed callback protocol artefact for child processes.
/// Owns the callback socket, maintains in-flight map, and implements `CallbackHandler<C>`.
/// This is the only production callback handler for child processes.
pub struct CallbackIpcChild<C> {
    pub in_flight: InFlightMap<Vec<u8>>,
    write_tx: tokio::sync::mpsc::UnboundedSender<CallbackWriteRequest<C>>,
    next_id: std::sync::atomic::AtomicU64,
    cancellation_token: tokio_util::sync::CancellationToken,
    // Track message stats for adaptive throttling
    pending_count: std::sync::atomic::AtomicUsize,
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
        
        // Create the shared struct first, so we can track pending counts
        let result = std::sync::Arc::new(Self {
            in_flight,
            write_tx,
            next_id: std::sync::atomic::AtomicU64::new(1),
            cancellation_token,
            pending_count: std::sync::atomic::AtomicUsize::new(0),
        });
        
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
        
        // Reader task with improved error handling and tracking
        let result_clone = result.clone();
        tokio::spawn(async move {
            let mut reader = LengthPrefixedRead::new(read_half);
            loop {
                tokio::select! {
                    _ = cancellation_token_reader.cancelled() => break,
                    result = reader.read_msg::<CallbackEnvelope<CallbackControl>>() => {
                        match result {
                            Ok(env) => {
                                let correlation_id = env.correlation_id;
                                match env.inner {
                                    CallbackControl::StreamItem(item) => {
                                        // Handle stream item - keep correlation ID alive
                                        if let Some(slot) = in_flight_reader.0.get(&correlation_id) {
                                            if let Some(sender) = &slot.stream_sender {
                                                // Send the stream item to the waiting task
                                                if sender.send(item).is_err() {
                                                    tracing::error!(event = "callback_ipc_child_read", correlation_id, "Failed to send stream item, receiver dropped");
                                                    // Remove the correlation ID since receiver is gone
                                                    in_flight_reader.0.remove(&correlation_id);
                                                    result_clone.pending_count.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                                                }
                                            } else {
                                                tracing::error!(event = "callback_ipc_child_read", correlation_id, "Stream item received but sender missing");
                                            }
                                        } else {
                                            tracing::error!(event = "callback_ipc_child_read", correlation_id, "Received stream item for unknown correlation id");
                                        }
                                    },
                                    CallbackControl::StreamComplete => {
                                        // Handle stream completion - remove correlation ID and close stream
                                        if let Some((_, mut slot)) = in_flight_reader.0.remove(&correlation_id) {
                                            // Track that we're processing one less in-flight request
                                            result_clone.pending_count.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                                            
                                            // Close the stream to signal completion
                                            slot.close_stream();
                                            tracing::debug!(event = "callback_ipc_child_read", correlation_id, "Stream completed");
                                        } else {
                                            tracing::error!(event = "callback_ipc_child_read", correlation_id, "Received stream completion for unknown correlation id");
                                        }
                                    }
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
            in_flight_reader.0.iter_mut().for_each(|mut item| {
                let (_, slot) = item.pair_mut();
                if let Some(sender) = slot.stream_sender.take() {
                    let err = PythonExecutionError::ExecutionError { message: "Callback reply loop exited (EOF)".to_string() };
                    if sender.send(Err(err)).is_err() {
                        tracing::error!(event = "callback_ipc_child_read", error = "Failed to send EOF error to waiting task", "Failed to notify waiting task about EOF");
                    }
                }
            });
            // Reset pending count to 0
            result_clone.pending_count.store(0, std::sync::atomic::Ordering::SeqCst);
            // Clear the map after notifying all waiting tasks
            in_flight_reader.0.clear();
        });
        
        result
    }
    fn next_correlation_id(&self) -> u64 {
        self.next_id.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }
    pub fn shutdown(&self) {
        self.cancellation_token.cancel();
    }
    
    // New method to get current pending count
    pub fn pending_count(&self) -> usize {
        self.pending_count.load(std::sync::atomic::Ordering::SeqCst)
    }
    
    /// Handle a callback and return a stream of bincode bytes
    pub async fn handle_stream_bincode(&self, callback: C) -> Result<Pin<Box<dyn Stream<Item = Result<Vec<u8>, PythonExecutionError>> + Send>>, PythonExecutionError> {
        let correlation_id = self.next_correlation_id();
        
        let envelope = CallbackEnvelope {
            correlation_id,
            inner: callback,
            context: TracingContext::default(),
        };
        
        // Create the reply slot BEFORE sending the message to prevent race conditions
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Result<Vec<u8>, PythonExecutionError>>();
        
        // Create tracker to automatically track metrics for this operation
        let _metrics_tracker = crate::metrics::OperationTracker::track_callback();
        
        // Insert into in_flight map and track pending count
        {
            let mut slot = ReplySlot::new();
            slot.stream_sender = Some(tx);
            slot.stream_receiver = None; // We'll return the receiver
            self.in_flight.0.insert(correlation_id, slot);
            
            // Track that we now have one more in-flight request
            self.pending_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        
        // If we have too many pending callbacks, introduce a small adaptive backoff
        let current_pending = self.pending_count();
        if current_pending > 1000 {
            // Adaptive backoff based on pending count
            let backoff_ms = std::cmp::min(current_pending / 100, 20); // Max 20ms backoff
            if backoff_ms > 0 {
                tokio::time::sleep(Duration::from_millis(backoff_ms as u64)).await;
            }
        }
        
        // Send the write request with careful error handling
        let write_req = CallbackWriteRequest { envelope };
        if let Err(e) = self.write_tx.send(write_req) {
            // Clean up in_flight entry on error and decrement pending count
            self.in_flight.0.remove(&correlation_id);
            self.pending_count.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
            
            // Track the error in metrics
            crate::metrics::MetricsHandle::callback().track_error("send_failed");
            
            return Err(PythonExecutionError::ExecutionError { 
                message: format!("Failed to send callback write request: {e}") 
            });
        }
        
        // Convert the receiver into a stream
        use tokio_stream::wrappers::UnboundedReceiverStream;
        let stream = UnboundedReceiverStream::new(rx);
        Ok(Box::pin(stream))
    }
}



pub struct CallbackReceiver<M, H>
where
    M: Send + Sync + Decode<()> + 'static,
    H: CallbackStreamHandler<M> + Clone + Send + Sync + 'static,
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
    H: CallbackStreamHandler<M> + Clone + Send + Sync + 'static,
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
        
        // Initialize metrics
        crate::metrics::init_metrics();
        
        let (req_tx, mut req_rx) = tokio::sync::mpsc::unbounded_channel::<CallbackEnvelope<M>>();
        let (reply_tx, mut reply_rx) = tokio::sync::mpsc::unbounded_channel::<CallbackEnvelope<CallbackControl>>();
        let reply_tx = Arc::new(reply_tx);
        let cancellation_token_reader = cancellation_token.clone();
        
        // Create a task to periodically log metrics
        let metrics_token = cancellation_token.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(5));
            loop {
                tokio::select! {
                    _ = metrics_token.cancelled() => {
                        break;
                    }
                    _ = interval.tick() => {
                        crate::metrics::MetricsReporter::log_metrics_state();
                    }
                }
            }
        });
        
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
        
        // Continue with the rest of the run method...
        let reply_tx_handler = reply_tx.clone();
        let cancellation_token_handler = cancellation_token.clone();
        let handler_pool = tokio::spawn(async move {
            // Create a more flexible concurrency management system
            // Instead of a fixed semaphore, use an adaptive approach with FuturesUnordered
            let mut tasks = FuturesUnordered::new();
            let mut req_rx_closed = false;
            let handler = handler;
            
            // Use a token bucket rate limiter for smoother handling
            let max_concurrent_tasks = 500; // Much higher limit but still bounded
            let mut active_tasks: usize = 0;
            
            loop {
                // First check if we need to exit
                if req_rx_closed && tasks.is_empty() {
                    tracing::debug!(event = "callback_receiver", task = "handler_pool", step = "drain_complete", 
                        "All handler tasks complete and request channel closed, breaking loop");
                    break;
                }

                tokio::select! {
                    biased;
                    
                    // Process completed tasks first to free up slots
                    Some(result) = tasks.next(), if !tasks.is_empty() => {
                        if let Err(e) = result {
                            tracing::error!(event = "callback_receiver", task = "handler_pool", step = "task_error", error = ?e, "Handler task error");
                        }
                        active_tasks = active_tasks.saturating_sub(1);
                    }
                    
                    // Check for cancellation
                    _ = cancellation_token_handler.cancelled() => {
                        tracing::debug!(event = "callback_receiver", task = "handler_pool", step = "shutdown_signal", "Handler pool received shutdown signal, breaking loop");
                        break;
                    }
                    
                    // Process new request or detect channel close
                    message = req_rx.recv() => {
                        match message {
                            Some(envelope) if active_tasks < max_concurrent_tasks => {
                                let correlation_id = envelope.correlation_id;
                                let msg = envelope.inner;
                                let handler = handler.clone();
                                let reply_tx = reply_tx_handler.clone();
                                
                                // Spawn a new task and track it
                                active_tasks += 1;
                                tasks.push(tokio::spawn(async move {
                                    // Create a metrics tracker for this handler operation
                                    let _metrics_tracker = crate::metrics::OperationTracker::track_callback();
                                    
                                    // Handle the streaming callback and serialize to bincode
                                    match handler.handle_stream(msg).await {
                                        Ok(mut stream) => {
                                            // Process each item in the stream, serializing to bincode
                                            while let Some(result) = stream.next().await {
                                                let bincode_result = match result {
                                                    Ok(response) => {
                                                        match bincode::encode_to_vec(&response, bincode::config::standard()) {
                                                            Ok(bytes) => Ok(bytes),
                                                            Err(e) => Err(PythonExecutionError::ExecutionError { 
                                                                message: format!("Failed to serialize response: {e}")
                                                            }),
                                                        }
                                                    },
                                                    Err(e) => Err(e),
                                                };
                                                
                                                let reply_envelope = CallbackEnvelope {
                                                    correlation_id,
                                                    inner: CallbackControl::StreamItem(bincode_result),
                                                    context: envelope.context.clone(),
                                                };
                                                if let Err(e) = reply_tx.send(reply_envelope) {
                                                    tracing::error!(event = "callback_receiver", task = "handler_task", step = "send_error", 
                                                        correlation_id, error = ?e, "Failed to send reply envelope");
                                                    
                                                    // Track error in metrics
                                                    crate::metrics::MetricsHandle::callback().track_error("reply_send_failed");
                                                    break; // Stop processing on send error
                                                }
                                            }
                                            
                                            // Send stream completion signal
                                            let completion_envelope = CallbackEnvelope {
                                                correlation_id,
                                                inner: CallbackControl::StreamComplete,
                                                context: envelope.context.clone(),
                                            };
                                            if let Err(e) = reply_tx.send(completion_envelope) {
                                                tracing::error!(event = "callback_receiver", task = "handler_task", step = "completion_send_error", 
                                                    correlation_id, error = ?e, "Failed to send stream completion signal");
                                                
                                                // Track error in metrics
                                                crate::metrics::MetricsHandle::callback().track_error("completion_send_failed");
                                            }
                                        },
                                        Err(e) => {
                                            // Send the error as a single stream item followed by completion
                                            let reply_envelope = CallbackEnvelope {
                                                correlation_id,
                                                inner: CallbackControl::StreamItem(Err(e)),
                                                context: envelope.context.clone(),
                                            };
                                            if let Err(send_err) = reply_tx.send(reply_envelope) {
                                                tracing::error!(event = "callback_receiver", task = "handler_task", step = "error_send_error", 
                                                    correlation_id, error = ?send_err, "Failed to send error reply envelope");
                                                
                                                // Track error in metrics
                                                crate::metrics::MetricsHandle::callback().track_error("error_reply_send_failed");
                                            } else {
                                                // Send completion signal even after error
                                                let completion_envelope = CallbackEnvelope {
                                                    correlation_id,
                                                    inner: CallbackControl::StreamComplete,
                                                    context: envelope.context,
                                                };
                                                if let Err(e) = reply_tx.send(completion_envelope) {
                                                    tracing::error!(event = "callback_receiver", task = "handler_task", step = "error_completion_send_error", 
                                                        correlation_id, error = ?e, "Failed to send error completion signal");
                                                    
                                                    // Track error in metrics
                                                    crate::metrics::MetricsHandle::callback().track_error("error_completion_send_failed");
                                                }
                                            }
                                        }
                                    }
                                }));
                            },
                            Some(_) => {
                                // Too many active tasks, sleep briefly and retry
                                tokio::time::sleep(Duration::from_millis(1)).await;
                            },
                            None => {
                                tracing::debug!(event = "callback_receiver", task = "handler_pool", step = "req_rx_closed", 
                                    "Request channel closed");
                                req_rx_closed = true;
                            }
                        }
                    }
                }
            }
            
            tracing::debug!(event = "callback_receiver", task = "handler_pool", step = "exit", "Handler pool future exiting");
            Ok::<(), CallbackError>(())
        });
        // Rest of the method remains the same...
        let cancellation_token_writer = cancellation_token.clone();
        let writer_task = tokio::spawn(async move {
            let mut writer = LengthPrefixedWrite::new(write_half.expect("write_half missing in CallbackReceiver"));
            
            loop {
                tokio::select! {
                    _ = cancellation_token_writer.cancelled() => {
                        trace!(event = "callback_receiver", task = "writer", step = "shutdown_signal", 
                            "Writer received shutdown, exiting");
                        break;
                    }
                    
                    message = reply_rx.recv() => {
                        match message {
                            Some(reply_envelope) => {
                                // Process write directly, one at a time
                                let result = writer.write_msg(&reply_envelope).await;
                                if let Err(e) = result {
                                    tracing::error!(event = "callback_receiver", task = "writer", 
                                        step = "write_error", correlation_id = reply_envelope.correlation_id, 
                                        error = ?e, "Failed to write reply envelope to socket");
                                    // Don't break on errors - just log them and continue
                                    
                                    // Track error in metrics
                                    crate::metrics::MetricsHandle::callback().track_error("write_failed");
                                } else {
                                    tracing::debug!(event = "callback_receiver", task = "writer", 
                                        step = "reply_written", correlation_id = reply_envelope.correlation_id,
                                        "Wrote reply envelope to socket");
                                }
                            },
                            None => {
                                trace!(event = "callback_receiver", task = "writer", step = "channel_closed", 
                                    "Reply channel closed, exiting");
                                break;
                            }
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

