use kameo_child_process::{CallbackHandler, ChildCallbackMessage};
use tracing::trace;

// Add a NoopCallbackHandler for callback IPC
pub struct NoopCallbackHandler;

#[async_trait::async_trait]
impl<C: ChildCallbackMessage + Sync + 'static> CallbackHandler<C> for NoopCallbackHandler
where
    C::Reply: Send,
{
    async fn handle(&mut self, callback: C) -> C::Reply {
        trace!(event = "callback_handler", ?callback, "NoopCallbackHandler received callback");
        tracing::info!(event = "noop_callback", ?callback, "NoopCallbackHandler received callback");
        panic!("NoopCallbackHandler called but no Default for callback reply; implement your own handler if you need a real reply");
    }
} 