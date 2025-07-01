#![forbid(unsafe_code)]

use std::io;
use std::marker::PhantomData;
use std::sync::Arc;

use async_trait::async_trait;
use bincode::{Decode, Encode};
use opentelemetry::{global, Context as OTelContext};
use serde::de::DeserializeOwned;
use serde::Serialize;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;
use tracing::{error, instrument, Instrument};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::{AsyncReadWrite, TracingContext, WithTracingContext};

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
    type Reply: Send + Serialize + DeserializeOwned + Encode + Decode<()> + std::fmt::Debug + 'static;
}

#[async_trait]
pub trait CallbackSender<M>
where
    M: ChildCallbackMessage,
{
    fn set_callback_handle(&mut self, handle: CallbackHandle<M>);
}

#[derive(Debug)]
pub struct CallbackHandle<M>
where
    M: ChildCallbackMessage,
{
    connection: Arc<Mutex<Box<dyn AsyncReadWrite>>>,
    _phantom: PhantomData<M>,
}

impl<M> Clone for CallbackHandle<M>
where
    M: ChildCallbackMessage,
{
    fn clone(&self) -> Self {
        Self {
            connection: self.connection.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<M> CallbackHandle<M>
where
    M: ChildCallbackMessage,
{
    pub fn new(connection: Box<dyn AsyncReadWrite>) -> Self {
        Self {
            connection: Arc::new(Mutex::new(connection)),
            _phantom: PhantomData,
        }
    }

    #[instrument(skip(self, msg), fields(message_type = std::any::type_name::<M>()))]
    pub async fn ask(&self, msg: M) -> Result<M::Reply, CallbackError> {
        let mut conn = self.connection.lock().await;

        let mut trace_context_map = std::collections::HashMap::new();
        global::get_text_map_propagator(|propagator| {
            propagator.inject_context(&OTelContext::current(), &mut trace_context_map);
        });
        let trace_context = TracingContext(trace_context_map);

        let wrapped_msg = WithTracingContext {
            inner: msg,
            context: trace_context,
        };

        let msg_bytes = bincode::encode_to_vec(&wrapped_msg, bincode::config::standard())?;
        conn.write_all(&msg_bytes).await?;
        conn.flush().await?;

        let mut resp_buf = vec![0u8; 1024 * 64];
        let n = conn.read(&mut resp_buf).await?;
        if n == 0 {
            return Err(CallbackError::ConnectionClosed);
        }

        let (wrapped_reply, _): (WithTracingContext<M::Reply>, _) =
            bincode::decode_from_slice(&resp_buf[..n], bincode::config::standard())?;

        Ok(wrapped_reply.inner)
    }
}

#[async_trait]
pub trait CallbackHandler<M>: Send + Sync + 'static
where
    M: ChildCallbackMessage,
{
    async fn handle(&mut self, callback: M) -> M::Reply;
}

/// Add a NoopCallbackHandler for callback IPC (crashes if called)
pub struct NoopCallbackHandler;

#[async_trait]
impl<C: ChildCallbackMessage + Sync + 'static> CallbackHandler<C> for NoopCallbackHandler
where
    C::Reply: Send,
{
    async fn handle(&mut self, callback: C) -> C::Reply {
        tracing::trace!(event = "callback_handler", ?callback, "NoopCallbackHandler received callback");
        tracing::info!(event = "noop_callback", ?callback, "NoopCallbackHandler received callback");
        panic!("NoopCallbackHandler called but no Default for callback reply; implement your own handler if you need a real reply");
    }
}

pub struct CallbackReceiver<M, H>
where
    M: ChildCallbackMessage,
    H: CallbackHandler<M>,
{
    connection: Box<dyn AsyncReadWrite>,
    handler: H,
    _phantom: PhantomData<M>,
}

impl<M, H> CallbackReceiver<M, H>
where
    M: ChildCallbackMessage,
    H: CallbackHandler<M>,
{
    pub fn new(connection: Box<dyn AsyncReadWrite>, handler: H) -> Self {
        Self {
            connection,
            handler,
            _phantom: PhantomData,
        }
    }

    #[instrument(skip(self), fields(message_type = std::any::type_name::<M>()))]
    pub async fn run(mut self) -> Result<(), CallbackError> {
        loop {
            let mut buf = vec![0u8; 4096];
            tracing::trace!(event = "callback_receiver", step = "before_read", "Callback receiver about to read from connection");
            let n = match self.connection.read(&mut buf).await {
                Ok(0) => {
                    tracing::debug!(status = "connection_closed", message_type = std::any::type_name::<M>());
                    break;
                }
                Ok(n) => n,
                Err(e) => {
                    tracing::error!(error = ?e, message_type = std::any::type_name::<M>(), message = "Callback read error");
                    return Err(e.into());
                }
            };
            tracing::trace!(event = "callback_receiver", step = "after_read", n = n, "Callback receiver read from connection");
            if n == 0 {
                tracing::debug!(status = "connection_closed_no_data", message_type = std::any::type_name::<M>());
                break;
            }
            tracing::trace!(event = "callback_receiver", step = "before_decode", "Callback receiver about to decode message");
            let (wrapped_msg, _): (WithTracingContext<M>, _) =
                match bincode::decode_from_slice(&buf[..n], bincode::config::standard()) {
                    Ok(decoded) => decoded,
                    Err(e) => {
                        tracing::error!(error = ?e, message_type = std::any::type_name::<M>(), message = "Failed to decode callback message");
                        continue;
                    }
                };

            let parent_cx = global::get_text_map_propagator(|propagator| {
                propagator.extract(&wrapped_msg.context.0)
            });
            let span = tracing::info_span!("callback_handler");
            span.set_parent(parent_cx);
            let reply = self.handler.handle(wrapped_msg.inner).instrument(span).await;
            let mut trace_context_map = std::collections::HashMap::new();
            global::get_text_map_propagator(|propagator| {
                propagator.inject_context(&OTelContext::current(), &mut trace_context_map);
            });
            let trace_context = TracingContext(trace_context_map);

            let wrapped_reply = WithTracingContext {
                inner: reply,
                context: trace_context,
            };

            let reply_bytes = match bincode::encode_to_vec(&wrapped_reply, bincode::config::standard()) {
                Ok(bytes) => bytes,
                Err(e) => {
                    tracing::error!(error = ?e, message_type = std::any::type_name::<M>(), message = "Failed to encode callback reply");
                    continue;
                }
            };

            if let Err(e) = self.connection.write_all(&reply_bytes).await {
                tracing::error!(error = ?e, message_type = std::any::type_name::<M>(), message = "Failed to write callback reply");
                break;
            }
        }

        Ok(())
    }
}

impl<M, C, E> crate::callback::CallbackSender<C> for crate::SubprocessActor<M, C, E>
where
    C: crate::callback::ChildCallbackMessage,
    E: crate::ProtocolError + std::fmt::Debug + Send + Sync + 'static,
{
    fn set_callback_handle(&mut self, handle: crate::callback::CallbackHandle<C>) {
        self.callback_handle = Some(handle);
    }
} 