use crate::error::PythonExecutionError;
use crate::framing::{LengthPrefixedRead, LengthPrefixedWrite};
use async_trait::async_trait;
use futures::{Stream, StreamExt};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
use std::pin::Pin;
use std::sync::Arc;
use tokio::task::JoinHandle;
use tracing::{error, trace};

type CallbackByteStream = Pin<Box<dyn Stream<Item = Result<Vec<u8>, PythonExecutionError>> + Send>>;
type HandlerFuture = Pin<Box<dyn std::future::Future<Output = Result<CallbackByteStream, PythonExecutionError>> + Send>>;

/// Simple tracing context for now - we can enhance this later
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TracingContext {
    pub trace_id: String,
    pub span_id: String,
}

impl Default for TracingContext {
    fn default() -> Self {
        Self {
            trace_id: "default".to_string(),
            span_id: "default".to_string(),
        }
    }
}

/// Dynamic callback module that manages multiple typed handlers for flexible routing.
///
/// This module implements a sophisticated callback system that allows Python child processes
/// to invoke different strongly-typed Rust handlers in the parent process. The system supports:
///
/// ## Key Features
///
/// - **Dynamic Registration**: Handlers can be registered at runtime by module and type
/// - **Type Safety**: Each handler is strongly typed for both request and response
/// - **Streaming Support**: All handlers return streaming responses via async iterators
/// - **Module Organization**: Handlers are organized into logical modules for better namespace management
/// - **Automatic Routing**: Requests are automatically routed to the correct handler based on callback path
///
/// ## Architecture
///
/// ```text
/// Python Child Process           Rust Parent Process
/// ┌─────────────────┐           ┌──────────────────────┐
/// │ kameo.basic.    │   IPC     │ DynamicCallback      │
/// │ TestCallback()  │ ────────> │ Module               │
/// │                 │           │ ┌──────────────────┐ │
/// │ kameo.trading.  │           │ │ "basic"          │ │
/// │ DataFetch()     │           │ │ ├─TestCallback   │ │
/// └─────────────────┘           │ │ └─OtherHandler   │ │
///                               │ │ "trading"        │ │
///                               │ │ ├─DataFetch      │ │
///                               │ │ └─TraderHandler  │ │
///                               │ └──────────────────┘ │
///                               └──────────────────────┘
/// ```
///
/// ## Registration Process
///
/// 1. **Builder Phase**: Handlers registered via `PythonChildProcessBuilder::with_callback_handler()`
/// 2. **Module Creation**: Registry serialized and passed to child process
/// 3. **Python Integration**: Child creates dynamic Python modules based on registry
/// 4. **Runtime Routing**: Requests routed via callback path (e.g., "trading.DataFetch")
///
/// ## Type Safety Guarantees
///
/// - Request types are validated at handler registration time
/// - Response types are enforced through the `TypedCallbackHandler` trait
/// - Serialization/deserialization errors are properly propagated
/// - Invalid callback paths result in clear error messages
pub struct DynamicCallbackModule {
    /// Map of callback_path -> handler implementation
    /// Key format: "{module}.{type_name}" (e.g., "trading.DataFetch")
    handlers: HashMap<String, Box<dyn CallbackHandlerTrait + Send + Sync>>,
    /// Registry of module_name -> list of handler type names
    /// Used for Python module generation and introspection
    module_registry: HashMap<String, Vec<String>>,
}

impl DynamicCallbackModule {
    pub fn new() -> Self {
        Self {
            handlers: HashMap::new(),
            module_registry: HashMap::new(),
        }
    }

    /// Register a handler in a specific module
    pub fn register_handler<C, H>(&mut self, module_name: &str, handler: H) -> Result<(), String>
    where
        C: Send + Sync + for<'de> serde::Deserialize<'de> + 'static,
        H: TypedCallbackHandler<C> + Clone + Send + Sync + 'static,
    {
        let type_name = handler.type_name().to_string();
        let full_name = format!("{}.{}", module_name, type_name);

        trace!(
            event = "register_handler_start",
            module_name,
            type_name,
            full_name,
            "Registering callback handler"
        );

        if self.handlers.contains_key(&full_name) {
            trace!(
                event = "register_handler_conflict",
                full_name,
                "Handler already exists"
            );
            return Err(format!("Handler for type '{}' already exists", full_name));
        }

        // Create a wrapper that implements CallbackHandlerTrait
        let wrapper = TypedCallbackHandlerWrapper {
            handler,
            _phantom: std::marker::PhantomData,
        };

        self.handlers.insert(full_name.clone(), Box::new(wrapper));

        // Register in module registry
        self.module_registry
            .entry(module_name.to_string())
            .or_default()
            .push(type_name.clone());

        trace!(
            event = "register_handler_success",
            module_name,
            type_name,
            full_name,
            "Handler registered successfully"
        );
        Ok(())
    }

    /// Register a handler with automatic module discovery
    /// Uses the module path from the handler's type
    pub fn auto_register_handler<C, H>(&mut self, handler: H) -> Result<(), String>
    where
        C: Send + Sync + for<'de> serde::Deserialize<'de> + 'static,
        H: TypedCallbackHandler<C> + Clone + Send + Sync + 'static,
    {
        trace!(
            event = "auto_register_handler_start",
            handler_type = std::any::type_name::<H>(),
            "Auto-registering handler"
        );

        // Extract module name from the handler's type
        let module_name = Self::extract_module_name::<H>();
        trace!(
            event = "auto_register_handler_discovered",
            module_name,
            "Discovered module name from handler type"
        );

        let result = self.register_handler(&module_name, handler);
        match &result {
            Ok(_) => trace!(
                event = "auto_register_handler_success",
                module_name,
                "Auto-registration successful"
            ),
            Err(e) => {
                trace!(event = "auto_register_handler_failed", module_name, error = %e, "Auto-registration failed")
            }
        }
        result
    }

    /// Extract module name from a type using reflection
    fn extract_module_name<T>() -> String {
        let type_name = std::any::type_name::<T>();

        // Parse the type name to extract module path
        // Format is typically: "crate_name::module::submodule::TypeName"
        let parts: Vec<&str> = type_name.split("::").collect();

        if parts.len() >= 2 {
            // Take everything except the last part (the type name)
            parts[..parts.len() - 1].join("::")
        } else {
            "unknown".to_string()
        }
    }

    /// Handle a callback using module-based routing
    /// Supports both "Module.Type" and "Type" formats
    pub async fn handle_callback(
        &self,
        callback_path: &str,
        callback_data: &[u8],
        correlation_id: u64,
        context: TracingContext,
    ) -> Result<
        Pin<Box<dyn Stream<Item = Result<Vec<u8>, PythonExecutionError>> + Send>>,
        PythonExecutionError,
    > {
        trace!(
            event = "handle_callback_start",
            callback_path,
            correlation_id,
            data_size = callback_data.len(),
            "Handling callback request"
        );

        // Try exact match first
        if let Some(handler) = self.handlers.get(callback_path) {
            trace!(
                event = "handle_callback_exact_match",
                callback_path,
                correlation_id,
                "Found exact handler match"
            );
            return handler
                .handle_callback_typed(callback_data, correlation_id, context)
                .await;
        }

        trace!(
            event = "handle_callback_no_exact_match",
            callback_path,
            correlation_id,
            "No exact match, trying type-based routing"
        );

        // Try to find by type name in any module
        for full_name in self.handlers.keys() {
            if full_name.ends_with(&format!(".{}", callback_path)) {
                if let Some(handler) = self.handlers.get(full_name) {
                    trace!(
                        event = "handle_callback_type_match",
                        callback_path,
                        full_name,
                        correlation_id,
                        "Found handler by type name"
                    );
                    return handler
                        .handle_callback_typed(callback_data, correlation_id, context)
                        .await;
                }
            }
        }

        trace!(
            event = "handle_callback_no_type_match",
            callback_path,
            correlation_id,
            "No type match, trying module-based routing"
        );

        // Try to find by module name
        if let Some(module_handlers) = self.module_registry.get(callback_path) {
            if module_handlers.len() == 1 {
                let full_name = format!("{}.{}", callback_path, module_handlers[0]);
                if let Some(handler) = self.handlers.get(&full_name) {
                    trace!(
                        event = "handle_callback_module_match",
                        callback_path,
                        full_name,
                        correlation_id,
                        "Found handler by module name"
                    );
                    return handler
                        .handle_callback_typed(callback_data, correlation_id, context)
                        .await;
                }
            }
        }

        trace!(
            event = "handle_callback_not_found",
            callback_path,
            correlation_id,
            "No handler found for callback path"
        );

        // Provide detailed debugging information for missing handlers
        let available_callbacks = self.available_callbacks();
        let available_modules = self.available_modules();

        // Show what routing attempts were made
        let routing_attempts = [
            format!("1. Exact match: '{}'", callback_path),
            format!("2. Type-based routing: '{}' (in any module)", callback_path),
            format!(
                "3. Module-based routing: '{}' (with single handler)",
                callback_path
            ),
        ];

        Err(PythonExecutionError::ExecutionError {
            message: format!(
                "No handler found for callback path: '{}' (correlation_id: {})\n\
                 Routing attempts made:\n{}\n\
                 Available callbacks: {}\n\
                 Available modules: {}\n\
                 This usually indicates:\n\
                 - The callback handler was not registered\n\
                 - A typo in the callback path\n\
                 - The handler was registered with a different module name\n\
                 - The Python code is calling the wrong callback path\n\
                 - The callback path format doesn't match the expected pattern",
                callback_path,
                correlation_id,
                routing_attempts.join("\n"),
                if available_callbacks.is_empty() {
                    "none".to_string()
                } else {
                    available_callbacks.join(", ")
                },
                if available_modules.is_empty() {
                    "none".to_string()
                } else {
                    available_modules.join(", ")
                }
            ),
        })
    }

    /// Get a handler by callback path
    pub fn get_handler(&self, callback_path: &str) -> Option<&dyn CallbackHandlerTrait> {
        // Try exact match first
        if let Some(handler) = self.handlers.get(callback_path) {
            return Some(handler.as_ref());
        }

        // Try to find by type name in any module
        for full_name in self.handlers.keys() {
            if full_name.ends_with(&format!(".{}", callback_path)) {
                if let Some(handler) = self.handlers.get(full_name) {
                    return Some(handler.as_ref());
                }
            }
        }

        // Try to find by module name
        if let Some(module_handlers) = self.module_registry.get(callback_path) {
            if module_handlers.len() == 1 {
                let full_name = format!("{}.{}", callback_path, module_handlers[0]);
                if let Some(handler) = self.handlers.get(&full_name) {
                    return Some(handler.as_ref());
                }
            }
        }

        None
    }

    /// Get all available callback paths
    pub fn available_callbacks(&self) -> Vec<String> {
        self.handlers.keys().cloned().collect()
    }

    /// Get all available modules
    pub fn available_modules(&self) -> Vec<String> {
        self.module_registry.keys().cloned().collect()
    }

    /// Get handlers in a specific module
    pub fn module_handlers(&self, module_name: &str) -> Vec<String> {
        self.module_registry
            .get(module_name)
            .cloned()
            .unwrap_or_default()
    }

    /// Get the full module registry for serialization
    /// Returns HashMap<module_name, Vec<handler_types>>
    pub fn get_registry(&self) -> &HashMap<String, Vec<String>> {
        &self.module_registry
    }

    /// Build a mapping of full callback path (e.g., "module.HandlerType") to the Rust response type name.
    /// This enables downstream systems (e.g., Python codegen) to annotate callback stubs precisely.
    pub fn get_response_types(&self) -> HashMap<String, String> {
        let mut out = HashMap::new();
        for (path, handler) in &self.handlers {
            out.insert(path.clone(), handler.response_type_name().to_string());
        }
        out
    }

    /// Build a mapping of full callback path to the Rust request type name.
    pub fn get_request_types(&self) -> HashMap<String, String> {
        let mut out = HashMap::new();
        for (path, handler) in &self.handlers {
            out.insert(path.clone(), handler.request_type_name().to_string());
        }
        out
    }
}

impl Default for DynamicCallbackModule {
    fn default() -> Self {
        Self::new()
    }
}

/// Strongly-typed callback handler trait
/// Each handler is responsible for a specific callback type
#[async_trait]
pub trait TypedCallbackHandler<C>: Send + Sync + 'static
where
    C: Send + Sync + 'static,
{
    type Response: Serialize + for<'de> Deserialize<'de> + Send + Debug + 'static;

    /// Handle a callback of the specific type
    async fn handle_callback(
        &self,
        callback: C,
    ) -> Result<
        Pin<Box<dyn Stream<Item = Result<Self::Response, PythonExecutionError>> + Send>>,
        PythonExecutionError,
    >;

    /// Return the type name for this handler
    fn type_name(&self) -> &'static str;
}

/// Internal trait for dynamic dispatch compatibility
/// This bridges the gap between async traits and trait objects
pub trait CallbackHandlerTrait: Send + Sync {
    fn type_name(&self) -> &str;

    fn response_type_name(&self) -> &str;

    fn request_type_name(&self) -> &str;

    fn handle_callback_typed(
        &self,
        callback_data: &[u8],
        correlation_id: u64,
        context: TracingContext,
    ) -> HandlerFuture;
}

/// Wrapper that implements CallbackHandlerTrait for TypedCallbackHandler
/// This handles the serialization/deserialization and async compatibility
struct TypedCallbackHandlerWrapper<C, H>
where
    C: Send + Sync + for<'de> serde::Deserialize<'de> + 'static,
    H: TypedCallbackHandler<C> + Clone + Send + Sync + 'static,
{
    handler: H,
    _phantom: std::marker::PhantomData<C>,
}

impl<C, H> CallbackHandlerTrait for TypedCallbackHandlerWrapper<C, H>
where
    C: Send + Sync + for<'de> serde::Deserialize<'de> + 'static,
    H: TypedCallbackHandler<C> + Clone + Send + Sync + 'static,
{
    fn type_name(&self) -> &str {
        self.handler.type_name()
    }

    fn response_type_name(&self) -> &str {
        std::any::type_name::<H::Response>()
    }

    fn request_type_name(&self) -> &str {
        std::any::type_name::<C>()
    }

    fn handle_callback_typed(
        &self,
        callback_data: &[u8],
        correlation_id: u64,
        _context: TracingContext,
    ) -> HandlerFuture {
        let handler = self.handler.clone();
        let data = callback_data.to_vec();
        let handler_type = self.handler.type_name();

        Box::pin(async move {
            trace!(
                event = "handler_wrapper_start",
                handler_type,
                correlation_id,
                data_size = data.len(),
                "Starting handler wrapper execution"
            );

            // Deserialize the callback data to the specific type
            let callback: C = match serde_brief::from_slice::<C>(&data) {
                Ok(callback) => {
                    trace!(
                        event = "handler_wrapper_deserialize_success_serde_brief",
                        handler_type,
                        correlation_id,
                        "Successfully deserialized callback data with serde-brief"
                    );
                    callback
                }
                Err(serde_brief_err) => {
                    // Fallback: try JSON decoding to support Python dict inputs
                    match serde_json::from_slice::<C>(&data) {
                        Ok(callback) => {
                            trace!(
                                event = "handler_wrapper_deserialize_success_json",
                                handler_type,
                                correlation_id,
                                "Successfully deserialized callback data with JSON"
                            );
                            callback
                        }
                        Err(json_err) => {
                            trace!(event = "handler_wrapper_deserialize_failed", handler_type, correlation_id, serde_brief_error = %serde_brief_err, json_error = %json_err, "Failed to deserialize callback data with both serde-brief and JSON");

                            // Provide detailed debugging information for deserialization failures
                            let data_hex = data
                                .iter()
                                .take(100)
                                .map(|b| format!("{:02x}", b))
                                .collect::<Vec<_>>()
                                .join(" ");
                            let data_preview = if data.len() > 100 {
                                format!("{}... (truncated, total {} bytes)", data_hex, data.len())
                            } else {
                                format!("{} ({} bytes)", data_hex, data.len())
                            };

                            return Err(PythonExecutionError::ExecutionError {
                                message: format!(
                                    "Failed to deserialize callback data for handler '{}' (correlation_id: {}):\n\
                                     - serde-brief error: {}\n\
                                     - json error: {}\n\
                                     Expected type: {}\n\
                                     Raw data (hex): {}\n\
                                     This usually indicates a type mismatch between Python and Rust or corrupted data.",
                                    handler_type, correlation_id, serde_brief_err, json_err,
                                    std::any::type_name::<C>(),
                                    data_preview
                                )
                            });
                        }
                    }
                }
            };

            trace!(
                event = "handler_wrapper_call_start",
                handler_type,
                correlation_id,
                "Calling handler.handle_callback"
            );

            // Handle the callback and get the response stream
            let response_stream = match handler.handle_callback(callback).await {
                Ok(stream) => {
                    trace!(
                        event = "handler_wrapper_call_success",
                        handler_type,
                        correlation_id,
                        "Handler returned response stream"
                    );
                    stream
                }
                Err(e) => {
                    trace!(event = "handler_wrapper_call_failed", handler_type, correlation_id, error = %e, "Handler failed to process callback");
                    return Err(e);
                }
            };

            trace!(
                event = "handler_wrapper_stream_convert_start",
                handler_type,
                correlation_id,
                "Converting response stream to serialized format"
            );

            // Convert the response stream to a stream of serialized data
            let handler_type_clone = handler_type.to_string();
            let serialized_stream = response_stream.map(move |result| {
                result.and_then(|response| {
                    serde_brief::to_vec(&response)
                        .map_err(|e| PythonExecutionError::ExecutionError {
                            message: format!(
                                "Failed to serialize response for handler '{}' (correlation_id: {}): {}\n\
                                 Response type: {}\n\
                                 Response value: {:?}\n\
                                 This usually indicates a serialization issue with the response type.",
                                handler_type_clone, correlation_id, e,
                                std::any::type_name::<H::Response>(),
                                response
                            )
                        })
                })
            });

            trace!(
                event = "handler_wrapper_complete",
                handler_type,
                correlation_id,
                "Handler wrapper execution completed successfully"
            );
            Ok(Box::pin(serialized_stream)
                as Pin<
                    Box<dyn Stream<Item = Result<Vec<u8>, PythonExecutionError>> + Send>,
                >)
        })
    }
}

/// Callback envelope for typed callbacks
#[derive(Serialize, Deserialize, Debug)]
pub struct TypedCallbackEnvelope {
    pub callback_path: String, // e.g., "DataFetch", "TraderCallback", etc.
    pub correlation_id: u64,
    pub callback_data: Vec<u8>, // Serialized callback data
    pub context: TracingContext,
}

/// Callback response envelope that includes type information
#[derive(Serialize, Deserialize, Debug)]
pub struct TypedCallbackResponse {
    pub callback_path: String,  // e.g., "trading.DataFetch"
    pub response_type: String,  // e.g., "DataFetchResponse"
    pub response_data: Vec<u8>, // Serialized response data
    pub correlation_id: u64,
    pub is_final: bool, // true if this is the last response in the stream
}

/// Main callback receiver that handles typed callbacks
pub struct TypedCallbackReceiver {
    dispatcher: Arc<DynamicCallbackModule>,
    reader: LengthPrefixedRead<tokio::net::unix::OwnedReadHalf>,
    writer: LengthPrefixedWrite<tokio::net::unix::OwnedWriteHalf>,
    cancellation_token: tokio_util::sync::CancellationToken,
    actor_type: &'static str,
}

impl TypedCallbackReceiver {
    pub fn new(
        dispatcher: Arc<DynamicCallbackModule>,
        reader: LengthPrefixedRead<tokio::net::unix::OwnedReadHalf>,
        writer: LengthPrefixedWrite<tokio::net::unix::OwnedWriteHalf>,
        cancellation_token: tokio_util::sync::CancellationToken,
        actor_type: &'static str,
    ) -> Self {
        Self {
            dispatcher,
            reader,
            writer,
            cancellation_token,
            actor_type,
        }
    }

    pub async fn run(mut self) -> Result<(), PythonExecutionError> {
        crate::metrics::init_metrics();
        tracing::info!(
            event = "callback_receiver_start",
            "Starting typed callback receiver"
        );

        let mut active_tasks: usize = 0;
        let mut tasks: Vec<JoinHandle<()>> = Vec::new();
        let (response_tx, mut response_rx) =
            tokio::sync::mpsc::unbounded_channel::<TypedCallbackResponse>();

        trace!(
            event = "callback_receiver_loop_start",
            "Starting main callback receiver loop"
        );

        loop {
            tokio::select! {
            // Check for cancellation
            _ = self.cancellation_token.cancelled() => {
                trace!(event = "callback_receiver_cancellation", "Cancellation requested, shutting down callback receiver");
                tracing::info!(event = "callback_receiver_cancellation", "Cancellation requested, shutting down callback receiver");
                        break;
            }

            // Send responses back to Python
            response = response_rx.recv() => {
                if let Some(response_envelope) = response {
                    trace!(event = "callback_receiver_send_response",
                           callback_path = %response_envelope.callback_path,
                           correlation_id = response_envelope.correlation_id,
                           response_type = %response_envelope.response_type,
                           is_final = response_envelope.is_final,
                           response_size = response_envelope.response_data.len(),
                           "Sending response back to Python");
                    if let Err(e) = self.writer.write_msg(&response_envelope).await {
                        error!(event = "callback_receiver_send_failed", error = %e, "Failed to send response");
                        crate::metrics::MetricsHandle::callback(self.actor_type).track_error("send_response_failed");
                    }
                }
            }

            // Read incoming callback
            result = self.reader.read_msg::<TypedCallbackEnvelope>() => {
                        match result {
                        Ok(envelope) => {
                            trace!("Received typed callback: {} (correlation_id: {})",
                                   envelope.callback_path, envelope.correlation_id);

                            // Spawn a task to handle this callback
                            let dispatcher = Arc::clone(&self.dispatcher);
                            let response_tx = response_tx.clone();
                            let callback_path = envelope.callback_path.clone();
                            let callback_data = envelope.callback_data.clone();
                            let correlation_id = envelope.correlation_id;
                            let context = envelope.context;
                            let actor_type = self.actor_type;

                            // Get the response type name before spawning the task
                            let response_type_name = if let Some(handler) = dispatcher.get_handler(&callback_path) {
                                handler.response_type_name().to_string()
                            } else {
                                "UnknownResponse".to_string()
                            };

                            let task = tokio::spawn(async move {
                                trace!(event = "callback_task_start", callback_path, correlation_id, "Starting callback handling task");
                                let _tracker = crate::metrics::OperationTracker::track_callback(actor_type);

                                match dispatcher.handle_callback(&callback_path, &callback_data, correlation_id, context).await {
                                    Ok(mut response_stream) => {
                                        trace!(event = "callback_task_stream_start", callback_path, correlation_id, "Processing response stream");

                                        // Process each response and wrap it in TypedCallbackResponse
                                        let mut response_count = 0;
                                        while let Some(response_result) = response_stream.next().await {
                                            match response_result {
                                                Ok(response_data) => {
                                                    response_count += 1;
                                                    trace!(event = "callback_task_response_item", callback_path, correlation_id, response_count, response_size = response_data.len(), "Processing response item");

                                                    // Create TypedCallbackResponse envelope with real type name
                                                    let response_envelope = TypedCallbackResponse {
                                                        callback_path: callback_path.clone(),
                                                        response_type: response_type_name.clone(),
                                                        response_data,
                                                        correlation_id,
                                                        is_final: false, // Not the final response
                                                    };

                                                    // Send response back through the channel
                                                    if let Err(e) = response_tx.send(response_envelope) {
                                                        error!(event = "callback_task_channel_send_failed", callback_path, correlation_id, error = %e, "Failed to send response through channel");
                                                        crate::metrics::MetricsHandle::callback(actor_type).track_error("channel_send_failed");
                                                    } else {
                                                        trace!(event = "callback_task_response_sent", callback_path, correlation_id, response_count, "Response sent through channel");
                                                    }
                                                }
                                                Err(e) => {
                                                    error!(event = "callback_task_stream_error", callback_path, correlation_id, error = %e, "Error in response stream");
                                                    crate::metrics::MetricsHandle::callback(actor_type).track_error("stream_error");
                                                    break;
                                                }
                                            }
                                        }

                                        trace!(event = "callback_task_stream_complete", callback_path, correlation_id, response_count, "Response stream completed, sending termination");

                                        // Send final termination signal after stream ends
                                        let final_response = TypedCallbackResponse {
                                            callback_path: callback_path.clone(),
                                            response_type: "StreamTermination".to_string(),
                                            response_data: Vec::new(), // Empty data for termination signal
                                            correlation_id,
                                            is_final: true, // This is the final response
                                        };
                                        if let Err(e) = response_tx.send(final_response) {
                                            error!(event = "callback_task_final_send_failed", callback_path, correlation_id, error = %e, "Failed to send final response through channel");
                                            crate::metrics::MetricsHandle::callback(actor_type).track_error("channel_send_final_failed");
                                        } else {
                                            trace!(event = "callback_task_termination_sent", callback_path, correlation_id, "Stream termination signal sent");
                                        }
                                    }
                                    Err(e) => {
                                        error!(event = "callback_task_handler_failed", callback_path, correlation_id, error = %e, "Failed to handle callback");
                                        crate::metrics::MetricsHandle::callback(actor_type).track_error("handler_failed");

                                        // Send error response back to Python
                                        let error_response = TypedCallbackResponse {
                                            callback_path: callback_path.clone(),
                                            response_type: "ErrorResponse".to_string(),
                                            response_data: serde_brief::to_vec(&e).unwrap_or_default(),
                                            correlation_id,
                                            is_final: true, // Error responses are also final
                                        };
                                        if let Err(e) = response_tx.send(error_response) {
                                            error!(event = "callback_task_error_send_failed", callback_path, correlation_id, error = %e, "Failed to send error response through channel");
                                            crate::metrics::MetricsHandle::callback(actor_type).track_error("channel_send_error_failed");
                                        } else {
                                            trace!(event = "callback_task_error_sent", callback_path, correlation_id, "Error response sent");
                                        }
                                    }
                                }

                                trace!(event = "callback_task_complete", callback_path, correlation_id, "Callback handling task completed");
                            });

                            tasks.push(task);
                            active_tasks += 1;
                            }
                            Err(e) => {
                                if let std::io::ErrorKind::UnexpectedEof = e.kind() {
                                tracing::info!(event = "callback_receiver_eof", "EOF received, shutting down callback receiver");
                                break;
                                } else {
                                error!("Error reading callback message: {}", e);
                                crate::metrics::MetricsHandle::callback(self.actor_type).track_error("read_error");
                                break;
                            }
                        }
                    }
                }
            }

            // Check for completed tasks outside of tokio::select!
            if let Some(task_result) = tasks
                .iter()
                .position(|task: &JoinHandle<()>| task.is_finished())
            {
                let task = tasks.swap_remove(task_result);
                if let Err(e) = task.await {
                    error!(event = "callback_task_failed", error = %e, "Task failed during execution");
                } else {
                    trace!(
                        event = "callback_task_completed",
                        "Task completed successfully"
                    );
                }
                active_tasks = active_tasks.saturating_sub(1);
                trace!(
                    event = "callback_task_cleanup",
                    active_tasks,
                    "Task cleaned up, active tasks remaining"
                );
            }
        }

        // Wait for remaining tasks to complete
        trace!(
            event = "callback_receiver_shutdown_start",
            remaining_tasks = tasks.len(),
            "Starting shutdown, waiting for remaining tasks"
        );

        for (i, task) in tasks.into_iter().enumerate() {
            trace!(
                event = "callback_receiver_shutdown_task",
                task_index = i,
                "Waiting for task to complete during shutdown"
            );
            if let Err(e) = task.await {
                error!(event = "callback_receiver_shutdown_task_failed", task_index = i, error = %e, "Task failed during shutdown");
            } else {
                trace!(
                    event = "callback_receiver_shutdown_task_success",
                    task_index = i,
                    "Task completed successfully during shutdown"
                );
            }
        }

        trace!(
            event = "callback_receiver_shutdown_complete",
            "All tasks completed, shutdown finished"
        );
        tracing::info!(
            event = "callback_receiver_shutdown_complete",
            "Typed callback receiver shutdown complete"
        );
        Ok(())
    }
}
