use crate::error::PythonExecutionError;
use crate::framing::{LengthPrefixedRead, LengthPrefixedWrite};
use async_trait::async_trait;
use bincode::{Decode, Encode};
use futures::{Stream, StreamExt};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use tokio::task::JoinHandle;
use tracing::{error, info, trace};

/// Simple tracing context for now - we can enhance this later
#[derive(Clone, Debug, Serialize, Deserialize, Encode, Decode)]
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

/// New: Dynamic module-based callback system
/// This allows handlers to be registered in modules and automatically discovered
pub struct DynamicCallbackModule {
    handlers: HashMap<String, Box<dyn CallbackHandlerTrait + Send + Sync>>,
    module_registry: HashMap<String, Vec<String>>, // module_name -> handler_types
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
        C: Send + Sync + Decode<()> + 'static,
        H: TypedCallbackHandler<C> + Clone + Send + Sync + 'static,
    {
        let type_name = handler.type_name().to_string();
        let full_name = format!("{}.{}", module_name, type_name);
        
        if self.handlers.contains_key(&full_name) {
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
            .or_insert_with(Vec::new)
            .push(type_name);
        
        Ok(())
    }
    
    /// Register a handler with automatic module discovery
    /// Uses the module path from the handler's type
    pub fn auto_register_handler<C, H>(&mut self, handler: H) -> Result<(), String>
    where
        C: Send + Sync + Decode<()> + 'static,
        H: TypedCallbackHandler<C> + Clone + Send + Sync + 'static,
    {
        // Extract module name from the handler's type
        let module_name = Self::extract_module_name::<H>();
        self.register_handler(&module_name, handler)
    }
    
    /// Extract module name from a type using reflection
    fn extract_module_name<T>() -> String {
        let type_name = std::any::type_name::<T>();
        
        // Parse the type name to extract module path
        // Format is typically: "crate_name::module::submodule::TypeName"
        let parts: Vec<&str> = type_name.split("::").collect();
        
        if parts.len() >= 2 {
            // Take everything except the last part (the type name)
            parts[..parts.len()-1].join("::")
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
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Vec<u8>, PythonExecutionError>> + Send>>, PythonExecutionError> {
        // Try exact match first
        if let Some(handler) = self.handlers.get(callback_path) {
            return handler.handle_callback_typed(callback_data, correlation_id, context).await;
        }
        
        // Try to find by type name in any module
        for (full_name, _) in &self.handlers {
            if full_name.ends_with(&format!(".{}", callback_path)) {
                if let Some(handler) = self.handlers.get(full_name) {
                    return handler.handle_callback_typed(callback_data, correlation_id, context).await;
                }
            }
        }
        
        // Try to find by module name
        if let Some(module_handlers) = self.module_registry.get(callback_path) {
            if module_handlers.len() == 1 {
                let full_name = format!("{}.{}", callback_path, module_handlers[0]);
                if let Some(handler) = self.handlers.get(&full_name) {
                    return handler.handle_callback_typed(callback_data, correlation_id, context).await;
                }
            }
        }
        
        Err(PythonExecutionError::ExecutionError {
            message: format!("No handler found for callback path: {}", callback_path)
        })
    }
    
    /// Get a handler by callback path
    pub fn get_handler(&self, callback_path: &str) -> Option<&dyn CallbackHandlerTrait> {
        // Try exact match first
        if let Some(handler) = self.handlers.get(callback_path) {
            return Some(handler.as_ref());
        }
        
        // Try to find by type name in any module
        for (full_name, _) in &self.handlers {
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
        self.module_registry.get(module_name)
            .cloned()
            .unwrap_or_default()
    }
    
    /// Get the full module registry for serialization
    /// Returns HashMap<module_name, Vec<handler_types>>
    pub fn get_registry(&self) -> &HashMap<String, Vec<String>> {
        &self.module_registry
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
    C: Send + Sync + Decode<()> + 'static,
{
    type Response: Encode + Send + 'static;
    
    /// Handle a callback of the specific type
    async fn handle_callback(&self, callback: C) -> Result<Pin<Box<dyn Stream<Item = Result<Self::Response, PythonExecutionError>> + Send>>, PythonExecutionError>;
    
    /// Return the type name for this handler
    fn type_name(&self) -> &'static str;
}

/// Internal trait for dynamic dispatch compatibility
/// This bridges the gap between async traits and trait objects
pub trait CallbackHandlerTrait: Send + Sync {
    fn type_name(&self) -> &str;
    
    fn response_type_name(&self) -> &str;
    
    fn handle_callback_typed(
        &self,
        callback_data: &[u8],
        correlation_id: u64,
        context: TracingContext,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Pin<Box<dyn Stream<Item = Result<Vec<u8>, PythonExecutionError>> + Send>>, PythonExecutionError>> + Send>>;
}

/// Wrapper that implements CallbackHandlerTrait for TypedCallbackHandler
/// This handles the serialization/deserialization and async compatibility
struct TypedCallbackHandlerWrapper<C, H>
where
    C: Send + Sync + Decode<()> + 'static,
    H: TypedCallbackHandler<C> + Clone + Send + Sync + 'static,
{
    handler: H,
    _phantom: std::marker::PhantomData<C>,
}

impl<C, H> CallbackHandlerTrait for TypedCallbackHandlerWrapper<C, H>
where
    C: Send + Sync + Decode<()> + 'static,
    H: TypedCallbackHandler<C> + Clone + Send + Sync + 'static,
{
    fn type_name(&self) -> &str {
        self.handler.type_name()
    }
    
    fn response_type_name(&self) -> &str {
        std::any::type_name::<H::Response>()
    }
    
    fn handle_callback_typed(
        &self,
        callback_data: &[u8],
        _correlation_id: u64,
        _context: TracingContext,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Pin<Box<dyn Stream<Item = Result<Vec<u8>, PythonExecutionError>> + Send>>, PythonExecutionError>> + Send>> {
        let handler = self.handler.clone();
        let data = callback_data.to_vec();
        
        Box::pin(async move {
            // Deserialize the callback data to the specific type
            let callback: C = bincode::decode_from_slice(&data, bincode::config::standard())
                .map_err(|e| PythonExecutionError::ExecutionError {
                    message: format!("Failed to deserialize callback: {}", e)
                })?
                .0;
            
            // Handle the callback and get the response stream
            let response_stream = handler.handle_callback(callback).await?;
            
            // Convert the response stream to a stream of serialized data
            let serialized_stream = response_stream.map(|result| {
                result.and_then(|response| {
                    bincode::encode_to_vec(&response, bincode::config::standard())
                        .map_err(|e| PythonExecutionError::ExecutionError {
                            message: format!("Failed to serialize response: {}", e)
                        })
                })
            });
            
            Ok(Box::pin(serialized_stream) as Pin<Box<dyn Stream<Item = Result<Vec<u8>, PythonExecutionError>> + Send>>)
        })
    }
}

/// Callback envelope for typed callbacks
#[derive(Serialize, Deserialize, Encode, Decode, Debug)]
pub struct TypedCallbackEnvelope {
    pub callback_path: String, // e.g., "DataFetch", "TraderCallback", etc.
    pub correlation_id: u64,
    pub callback_data: Vec<u8>, // Serialized callback data
    pub context: TracingContext,
}

/// Callback response envelope that includes type information
#[derive(Serialize, Deserialize, Encode, Decode, Debug)]
pub struct TypedCallbackResponse {
    pub callback_path: String,        // e.g., "trading.DataFetch"
    pub response_type: String,        // e.g., "DataFetchResponse" 
    pub response_data: Vec<u8>,      // Serialized response data
    pub correlation_id: u64,
}

/// Main callback receiver that handles typed callbacks
pub struct TypedCallbackReceiver {
    dispatcher: Arc<DynamicCallbackModule>,
    reader: LengthPrefixedRead<tokio::net::unix::OwnedReadHalf>,
    writer: LengthPrefixedWrite<tokio::net::unix::OwnedWriteHalf>,
    cancellation_token: tokio_util::sync::CancellationToken,
}

impl TypedCallbackReceiver {
    pub fn new(
        dispatcher: Arc<DynamicCallbackModule>,
        reader: LengthPrefixedRead<tokio::net::unix::OwnedReadHalf>,
        writer: LengthPrefixedWrite<tokio::net::unix::OwnedWriteHalf>,
        cancellation_token: tokio_util::sync::CancellationToken,
    ) -> Self {
        Self {
            dispatcher,
            reader,
            writer,
            cancellation_token,
        }
    }
    
    pub async fn run(mut self) -> Result<(), PythonExecutionError> {
        info!("Starting typed callback receiver");
        
        let mut active_tasks: usize = 0;
        let mut tasks: Vec<JoinHandle<()>> = Vec::new();
        let (response_tx, mut response_rx) = tokio::sync::mpsc::unbounded_channel::<TypedCallbackResponse>();
        
        loop {
            tokio::select! {
            // Check for cancellation
            _ = self.cancellation_token.cancelled() => {
                info!("Cancellation requested, shutting down callback receiver");
                        break;
            }
            
            // Send responses back to Python
            response = response_rx.recv() => {
                if let Some(response_envelope) = response {
                    trace!("Sending response back to Python: {:?}", response_envelope);
                    if let Err(e) = self.writer.write_msg(&response_envelope).await {
                        error!("Failed to send response: {}", e);
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
                            
                            // Get the response type name before spawning the task
                            let response_type_name = if let Some(handler) = dispatcher.get_handler(&callback_path) {
                                handler.response_type_name().to_string()
                            } else {
                                "UnknownResponse".to_string()
                            };
                            
                            let task = tokio::spawn(async move {
                                match dispatcher.handle_callback(&callback_path, &callback_data, correlation_id, context).await {
                                    Ok(mut response_stream) => {
                                        // Process each response and wrap it in TypedCallbackResponse
                                        while let Some(response_result) = response_stream.next().await {
                                            match response_result {
                                                Ok(response_data) => {
                                                    // Create TypedCallbackResponse envelope with real type name
                                                    let response_envelope = TypedCallbackResponse {
                                                        callback_path: callback_path.clone(),
                                                        response_type: response_type_name.clone(),
                                                        response_data,
                                                        correlation_id,
                                                    };
                                                    
                                                    // Send response back through the channel
                                                    if let Err(e) = response_tx.send(response_envelope) {
                                                        error!("Failed to send response through channel: {}", e);
                                                    }
                                                    trace!("Sent response for callback {} through channel", callback_path);
                                                }
                                                Err(e) => {
                                                    error!("Error in response stream: {}", e);
                                                    break;
                                                }
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        error!("Failed to handle callback {}: {}", callback_path, e);
                                        // Send error response back to Python
                                        let error_response = TypedCallbackResponse {
                                            callback_path: callback_path.clone(),
                                            response_type: "ErrorResponse".to_string(),
                                            response_data: bincode::encode_to_vec(&e, bincode::config::standard()).unwrap_or_default(),
                                            correlation_id,
                                        };
                                        if let Err(e) = response_tx.send(error_response) {
                                            error!("Failed to send error response through channel: {}", e);
                                        }
                                    }
                                }
                            });
                            
                            tasks.push(task);
                            active_tasks += 1;
                            }
                            Err(e) => {
                                if let std::io::ErrorKind::UnexpectedEof = e.kind() {
                                info!("EOF received, shutting down callback receiver");
                                break;
                                } else {
                                error!("Error reading callback message: {}", e);
                                break;
                            }
                        }
                    }
                }
            }
            
            // Check for completed tasks outside of tokio::select!
            if let Some(task_result) = tasks.iter().position(|task: &JoinHandle<()>| task.is_finished()) {
                let task = tasks.swap_remove(task_result);
                if let Err(e) = task.await {
                    error!("Task failed: {}", e);
                }
                active_tasks = active_tasks.saturating_sub(1);
            }
        }
        
        // Wait for remaining tasks to complete
        for task in tasks {
            if let Err(e) = task.await {
                error!("Task failed during shutdown: {}", e);
            }
        }
        
        info!("Typed callback receiver shutdown complete");
        Ok(())
    }
}

/// Macro to create a module with automatic handler registration
/// Usage: callback_module! { module_name => { handler1, handler2 } }
#[macro_export]
macro_rules! callback_module {
    ($module:ident => { $($handler:ident),* }) => {
        pub mod $module {
            use super::*;
            
            pub fn register_handlers(dispatcher: &mut DynamicCallbackModule) -> Result<(), String> {
                $(
                    dispatcher.register_handler(stringify!($module), $handler)?;
                )*
                Ok(())
            }
            
            pub fn available_handlers() -> Vec<&'static str> {
                vec![$(
                    stringify!($handler)
                ),*]
            }
        }
    };
}

/// Example usage of the new system
#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream;
    
    // Example callback types
    #[derive(Clone, Debug, Encode, Decode)]
    struct DataFetchRequest {
        pub symbol: String,
        pub start_date: String,
        pub end_date: String,
    }
    
    #[derive(Clone, Debug, Encode, Decode)]
    struct DataFetchResponse {
        pub data: Vec<f64>,
        pub timestamps: Vec<String>,
    }
    
    #[derive(Clone, Debug, Encode, Decode)]
    struct TraderCallback {
        pub action: String,
        pub quantity: f64,
    }
    
    #[derive(Clone, Debug, Encode, Decode)]
    struct TraderResponse {
        pub success: bool,
        pub message: String,
    }
    
    // Example handlers
    #[derive(Clone)]
    struct DataFetchHandler;
    
    #[async_trait]
    impl TypedCallbackHandler<DataFetchRequest> for DataFetchHandler {
        type Response = DataFetchResponse;
        
        async fn handle_callback(&self, callback: DataFetchRequest) -> Result<Pin<Box<dyn Stream<Item = Result<Self::Response, PythonExecutionError>> + Send>>, PythonExecutionError> {
            let response = DataFetchResponse {
                data: vec![100.0, 101.0, 102.0],
                timestamps: vec!["2024-01-01".to_string(), "2024-01-02".to_string(), "2024-01-03".to_string()],
            };
            
            let stream = stream::once(async move { Ok(response) });
            Ok(Box::pin(stream))
        }
        
        fn type_name(&self) -> &'static str {
            "DataFetch"
        }
    }
    
    #[derive(Clone)]
    struct TraderHandler;
    
    #[async_trait]
    impl TypedCallbackHandler<TraderCallback> for TraderHandler {
        type Response = TraderResponse;
        
        async fn handle_callback(&self, callback: TraderCallback) -> Result<Pin<Box<dyn Stream<Item = Result<Self::Response, PythonExecutionError>> + Send>>, PythonExecutionError> {
            let response = TraderResponse {
                success: true,
                message: format!("Executed {} of {}", callback.action, callback.quantity),
            };
            
            let stream = stream::once(async move { Ok(response) });
            Ok(Box::pin(stream))
        }
        
        fn type_name(&self) -> &'static str {
            "TraderCallback"
        }
    }
    
    // Create a module with handlers
    callback_module! {
        trading => { DataFetchHandler, TraderHandler }
    }
    
    #[tokio::test]
    async fn test_dynamic_callback_module() {
        let mut dispatcher = DynamicCallbackModule::new();
        
        // Register handlers from the module
        trading::register_handlers(&mut dispatcher).unwrap();
        
        // Test available callbacks
        let callbacks = dispatcher.available_callbacks();
        assert!(callbacks.contains(&"trading.DataFetch".to_string()));
        assert!(callbacks.contains(&"trading.TraderCallback".to_string()));
        
        // Test module handlers
        let module_handlers = dispatcher.module_handlers("trading");
        assert_eq!(module_handlers.len(), 2);
        assert!(module_handlers.contains(&"DataFetch".to_string()));
        assert!(module_handlers.contains(&"TraderCallback".to_string()));
    }
    
    #[tokio::test]
    async fn test_callback_routing() {
        let mut dispatcher = DynamicCallbackModule::new();
        
        // Register handlers
        dispatcher.register_handler("trading", DataFetchHandler).unwrap();
        dispatcher.register_handler("trading", TraderHandler).unwrap();
        
        // Test routing by full path
        let data_request = DataFetchRequest {
            symbol: "AAPL".to_string(),
            start_date: "2024-01-01".to_string(),
            end_date: "2024-01-03".to_string(),
        };
        
        let serialized = bincode::encode_to_vec(&data_request, bincode::config::standard()).unwrap();
        
        // This should work
        let result = dispatcher.handle_callback("trading.DataFetch", &serialized, 1, TracingContext::default()).await;
        assert!(result.is_ok());
        
        // Test routing by type name only
        let result = dispatcher.handle_callback("DataFetch", &serialized, 2, TracingContext::default()).await;
        assert!(result.is_ok());
        
        // Test routing by module name (should work if only one handler in module)
        let result = dispatcher.handle_callback("trading", &serialized, 3, TracingContext::default()).await;
        assert!(result.is_ok());
    }
}

