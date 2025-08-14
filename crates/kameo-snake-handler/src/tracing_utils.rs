//! Python-specific tracing utilities for OpenTelemetry span management.
//!
//! This module provides encapsulated functions for creating and managing spans
//! in Python subprocess communication, ensuring proper parent-child relationships
//! and trace context propagation between Rust and Python.

use opentelemetry::propagation::TextMapPropagator;
use opentelemetry::Context as OtelContext;
use tracing::info_span;
use tracing_opentelemetry::OpenTelemetrySpanExt;
// Removed unused imports
use pyo3::prelude::*;
use pyo3::types::PyDict;

/// Python helper for running a function with an OTEL context attached.
pub const PY_OTEL_RUNNER: &str = r#"
import asyncio
import inspect

def run_with_otel_context(carrier, user_func, *args, **kwargs):
    import sys
    sys.stderr.write("=== PYTHON HELPER START ===\n")
    sys.stderr.flush()
    sys.stderr.write(f"[DEBUG] run_with_otel_context ENTRY: carrier={carrier}\n")
    sys.stderr.flush()
    
    import opentelemetry.propagate, opentelemetry.context, opentelemetry.trace as trace
    
    # Extract the context from the carrier
    sys.stderr.write(f"[DEBUG] Carrier contents: {carrier}\n")
    sys.stderr.flush()
    ctx = opentelemetry.propagate.extract(carrier)
    sys.stderr.write(f"[DEBUG] Extracted context: {ctx}\n")
    sys.stderr.flush()
    
    # Debug: Check what span is in the extracted context
    try:
        current_span_in_ctx = trace.get_current_span(ctx)
        span_context_in_ctx = current_span_in_ctx.get_span_context()
        sys.stderr.write(f"[DEBUG] Context contains span: {current_span_in_ctx} trace_id=0x{span_context_in_ctx.trace_id:032x} span_id=0x{span_context_in_ctx.span_id:016x} is_remote={span_context_in_ctx.is_remote}\n")
        sys.stderr.flush()
    except Exception as e:
        sys.stderr.write(f"[DEBUG] Failed to get span from context: {e}\n")
        sys.stderr.flush()
    
    # Check current span before attaching
    current_span_before = trace.get_current_span()
    span_context_before = current_span_before.get_span_context()
    sys.stderr.write(f"[DEBUG] Before attach: current_span={current_span_before} trace_id=0x{span_context_before.trace_id:032x} span_id=0x{span_context_before.span_id:016x} is_remote={span_context_before.is_remote}\n")
    sys.stderr.flush()
    
    # Explicitly detach any existing context to ensure clean state
    try:
        opentelemetry.context.detach(opentelemetry.context.attach({}))
        sys.stderr.write("[DEBUG] Detached any existing context\n")
        sys.stderr.flush()
    except:
        pass  # No existing context to detach
    
    # Force reset the current context to ensure we're using the extracted context
    sys.stderr.write(f"[DEBUG] About to attach extracted context: {ctx}\n")
    sys.stderr.flush()
    token = opentelemetry.context.attach(ctx)
    sys.stderr.write(f"[DEBUG] Attached context with token: {token}\n")
    sys.stderr.flush()
    
    try:
        # Debug: Check context after attaching
        current_span_after = trace.get_current_span()
        span_context_after = current_span_after.get_span_context()
        sys.stderr.write(f"[DEBUG] After attach: current_span={current_span_after} trace_id=0x{span_context_after.trace_id:032x} span_id=0x{span_context_after.span_id:016x} is_remote={span_context_after.is_remote}\n")
        sys.stderr.flush()
        
        # Create a span for the user function call
        tracer = trace.get_tracer("kameo_snake_handler")
        
        # Call the user function and check if it's async
        result = user_func(*args, **kwargs)
        
        if inspect.iscoroutine(result):
            # For async functions, we need to wrap the coroutine in a span
            # Create an async wrapper that re-attaches the context and creates a span when awaited
            async def async_wrapper():
                # Re-attach the context inside the async wrapper to ensure it's available in async context
                token = opentelemetry.context.attach(ctx)
                try:
                    # Debug: Check if context is properly attached in async context
                    current_span_before_span = trace.get_current_span()
                    span_context_before_span = current_span_before_span.get_span_context()
                    sys.stderr.write(f"[DEBUG] Async wrapper before span: current_span={current_span_before_span} trace_id=0x{span_context_before_span.trace_id:032x} span_id=0x{span_context_before_span.span_id:016x} is_remote={span_context_before_span.is_remote}\n")
                    sys.stderr.flush()
                    
                    # Create the handle_message span using the extracted context
                    # This ensures it inherits from the Rust trace context
                    with tracer.start_as_current_span("handle_message") as span:
                        # Add message content as attributes to make each span unique
                        try:
                            span.set_attribute("message.content", str(args[0] if args else "no_args"))
                            sys.stderr.write(f"[DEBUG] Set message.content attribute: {str(args[0] if args else 'no_args')}\n")
                            sys.stderr.flush()
                        except Exception as e:
                            sys.stderr.write(f"[DEBUG] Failed to set message.content attribute: {e}\n")
                            sys.stderr.flush()
                        
                        try:
                            span.set_attribute("message.type", str(type(args[0] if args else None)))
                            sys.stderr.write(f"[DEBUG] Set message.type attribute: {str(type(args[0] if args else None))}\n")
                            sys.stderr.flush()
                        except Exception as e:
                            sys.stderr.write(f"[DEBUG] Failed to set message.type attribute: {e}\n")
                            sys.stderr.flush()
                        
                        try:
                            span.set_attribute("python.span", True)
                            sys.stderr.write("[DEBUG] Set python.span attribute: True\n")
                            sys.stderr.flush()
                        except Exception as e:
                            sys.stderr.write(f"[DEBUG] Failed to set python.span attribute: {e}\n")
                            sys.stderr.flush()
                        
                        sys.stderr.write(f"[DEBUG] Created span for async user function: {span}\n")
                        sys.stderr.flush()
                        current_span = trace.get_current_span()
                        span_context = current_span.get_span_context()
                        sys.stderr.write(f"[DEBUG] IN run_with_otel_context (async wrapper): current_span={current_span} trace_id=0x{span_context.trace_id:032x} span_id=0x{span_context.span_id:016x} is_remote={span_context.is_remote}\n")
                        sys.stderr.flush()
                        
                        # Debug: Check if span has parent
                        span_context = span.get_span_context()
                        sys.stderr.write(f"[DEBUG] Span context: trace_id=0x{span_context.trace_id:032x} span_id=0x{span_context.span_id:016x} is_remote={span_context.is_remote}\n")
                        sys.stderr.flush()
                        
                        # Debug: Check if span is root or has parent
                        if span_context.span_id == 0:
                            sys.stderr.write("[DEBUG] Span appears to be root span (span_id=0)\n")
                            sys.stderr.flush()
                        else:
                            sys.stderr.write(f"[DEBUG] Span has span_id: 0x{span_context.span_id:016x}\n")
                            sys.stderr.flush()
                        
                        # Debug: Check if span has parent span
                        try:
                            # Check if span has parent by looking at its attributes
                            span_attributes = span.get_attributes()
                            sys.stderr.write(f"[DEBUG] Span attributes: {span_attributes}\n")
                            sys.stderr.flush()
                            
                            # Check if span is a root span
                            if span_context.is_remote:
                                sys.stderr.write("[DEBUG] Span is marked as remote (has parent)\n")
                                sys.stderr.flush()
                            else:
                                sys.stderr.write("[DEBUG] Span is marked as local (no parent)\n")
                                sys.stderr.flush()
                        except Exception as e:
                            sys.stderr.write(f"[DEBUG] Failed to check span attributes: {e}\n")
                            sys.stderr.flush()
                    
                    # Debug: Check if span is being exported
                    try:
                        provider = trace.get_tracer_provider()
                        sys.stderr.write(f"[DEBUG] Tracer provider: {provider}\n")
                        sys.stderr.flush()
                        
                        # Check if span has parent
                        span_context = span.get_span_context()
                        sys.stderr.write(f"[DEBUG] Span context: trace_id=0x{span_context.trace_id:032x} span_id=0x{span_context.span_id:016x} is_remote={span_context.is_remote}\n")
                        sys.stderr.flush()
                        
                        if hasattr(provider, 'force_flush'):
                            provider.force_flush()
                            sys.stderr.write(f"[DEBUG] Forced flush of Python spans\n")
                            sys.stderr.flush()
                    except Exception as e:
                        sys.stderr.write(f"[DEBUG] Force flush failed: {e}\n")
                        sys.stderr.flush()
                    
                    return await result
                finally:
                    opentelemetry.context.detach(token)
            
            sys.stderr.write(f"[DEBUG] User function returned coroutine, returning wrapped coroutine\n")
            sys.stderr.flush()
            return async_wrapper()
        else:
            # For sync functions, create a span and call the function
            with tracer.start_as_current_span("handle_message") as span:
                sys.stderr.write(f"[DEBUG] Created span for sync user function: {span}\n")
                sys.stderr.flush()
                current_span = trace.get_current_span()
                span_context = current_span.get_span_context()
                sys.stderr.write(f"[DEBUG] IN run_with_otel_context (after sync user_func): current_span={current_span} trace_id=0x{span_context.trace_id:032x} span_id=0x{span_context.span_id:016x} is_remote={span_context.is_remote}\n")
                sys.stderr.flush()
                return result
    finally:
        opentelemetry.context.detach(token)
        sys.stderr.write(f"[DEBUG] Detached context with token: {token}\n")
        sys.stderr.flush()
        
        # Debug: Check current span after detachment
        current_span_after_detach = trace.get_current_span()
        span_context_after_detach = current_span_after_detach.get_span_context()
        sys.stderr.write(f"[DEBUG] After detachment: current_span={current_span_after_detach} trace_id=0x{span_context_after_detach.trace_id:032x} span_id=0x{span_context_after_detach.span_id:016x} is_remote={span_context_after_detach.is_remote}\n")
        sys.stderr.flush()
"#;

/// Create a Python message handler span with proper context.
///
/// This span represents the Python message handler processing a message
/// and should be nested under the ipc-child-receive span.
///
/// # Arguments
/// * `correlation_id` - Unique identifier for this message
/// * `message_type` - Type name of the message being processed
/// * `function_name` - Name of the Python function being called
/// * `is_async` - Whether the Python function is async
/// * `parent_span` - The parent span (ipc-child-receive) to nest under
///
/// # Returns
/// A span that can be used with .instrument()
pub fn create_python_message_handler_span(
    correlation_id: u64,
    message_type: &'static str,
    function_name: &str,
    is_async: bool,
    parent_span: &tracing::Span,
) -> tracing::Span {
    info_span!(
        parent: parent_span,
        "python_message_handler",
        correlation_id = correlation_id,
        message_type = message_type,
        function_name = function_name,
        is_async = is_async,
    )
}

/// Create a Python serialization span.
///
/// This span represents the serialization of a Rust message to Python format.
///
/// # Arguments
/// * `message_type` - Type name of the message being serialized
///
/// # Returns
/// A span that can be used with .instrument()
pub fn create_python_serialize_span(message_type: &'static str) -> tracing::Span {
    info_span!("python_serialize_message", message_type = message_type,)
}

/// Create a Python async call span.
///
/// This span represents the execution of an async Python function.
///
/// # Arguments
/// * `function_name` - Name of the Python function being called
///
/// # Returns
/// A span that can be used with .instrument()
pub fn create_python_async_call_span(function_name: &str) -> tracing::Span {
    info_span!("python_async_call", function_name = function_name,)
}

/// Create a Python sync call span.
///
/// This span represents the execution of a sync Python function.
///
/// # Arguments
/// * `function_name` - Name of the Python function being called
///
/// # Returns
/// A span that can be used with .instrument()
pub fn create_python_sync_call_span(function_name: &str) -> tracing::Span {
    info_span!("python_sync_call", function_name = function_name,)
}

/// Create a Python deserialization span.
///
/// This span represents the deserialization of a Python result to Rust format.
///
/// # Arguments
/// * `result_type` - Type name of the result being deserialized
///
/// # Returns
/// A span that can be used with .instrument()
pub fn create_python_deserialize_span(result_type: &'static str) -> tracing::Span {
    info_span!("python_deserialize_result", result_type = result_type,)
}

/// Create a callback handle span.
///
/// This span represents the handling of a callback message from Python.
///
/// # Arguments
/// * `correlation_id` - Unique identifier for this callback
///
/// # Returns
/// A span that can be used with .instrument()
pub fn create_callback_handle_span(correlation_id: u64) -> tracing::Span {
    info_span!("handle", correlation_id = correlation_id,)
}

/// Create an ipc-child-receive span with proper parent context.
///
/// This span represents the child process receiving a message from the parent.
///
/// # Arguments
/// * `correlation_id` - Unique identifier for this message
/// * `message_type` - Type name of the message being received
/// * `parent_cx` - OTEL context extracted from the envelope
///
/// # Returns
/// A span that can be used with .instrument()
pub fn create_ipc_child_receive_span(
    correlation_id: u64,
    message_type: &'static str,
    parent_cx: opentelemetry::Context,
) -> tracing::Span {
    let span = info_span!(
        "ipc-child-receive",
        correlation_id = correlation_id,
        message_type = message_type,
        messaging.system = "ipc",
        messaging.operation = "receive",
        messaging.source_kind = "queue"
    );
    span.set_parent(parent_cx);
    span
}

/// Set up OpenTelemetry context in Python using pyo3.
///
/// This function uses pyo3 to directly call OpenTelemetry functions in Python
/// to set up the trace context before calling the user's function.
/// This makes trace context handling completely orthogonal to the user's Python code.
pub fn setup_python_otel_context(context: &OtelContext) -> Result<(), Box<dyn std::error::Error>> {
    tracing::debug!("setup_python_otel_context CALLED");
    Python::with_gil(|py| {
        tracing::debug!("Setting up Python OTEL context using pyo3");

        // Initialize Python OpenTelemetry SDK if not already initialized
        let init_code = r#"
import opentelemetry
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import ConsoleSpanExporter, BatchSpanProcessor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace.sampling import ParentBased, ALWAYS_ON
import os

# Only initialize if no real TracerProvider is set (check for proxy or noop)
provider_class = trace.get_tracer_provider().__class__.__name__
if provider_class in ['NoOpTracerProvider', 'ProxyTracerProvider']:
    print("[DEBUG] Initializing Python OpenTelemetry SDK from Rust")
    
    # Create a resource with Python-specific attributes (let environment handle service attributes)
    resource = Resource.create({
        "telemetry.sdk.language": "python",
        "telemetry.sdk.name": "opentelemetry",
        "service.name": "kameo-snake",  # Match Rust service name
    })
    
    print(f"[DEBUG] Created Python resource: {resource}")
    print(f"[DEBUG] Resource attributes: {resource.attributes}")
    
    # Create a new TracerProvider with Python resource and proper sampling
    # Use ParentBased sampler to ensure remote spans are recorded
    provider = TracerProvider(
        resource=resource,
        sampler=ParentBased(ALWAYS_ON)
    )
    trace.set_tracer_provider(provider)
    
    print(f"[DEBUG] Set tracer provider with resource: {provider}")
    print(f"[DEBUG] Provider resource: {provider.resource}")
else:
    print("[DEBUG] Python OpenTelemetry SDK already initialized")

# Always try to add OTLP exporter if endpoint is configured (even if provider already exists)
otlp_endpoint = os.environ.get('OTEL_EXPORTER_OTLP_ENDPOINT')
print(f"[DEBUG] OTEL_EXPORTER_OTLP_ENDPOINT: {otlp_endpoint}")
if otlp_endpoint:
    try:
        print("[DEBUG] Attempting to import OTLP exporter...")
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
        print("[DEBUG] OTLP exporter imported successfully")
        otlp_exporter = OTLPSpanExporter(
            endpoint=otlp_endpoint,
            insecure=True
        )
        print(f"[DEBUG] OTLP exporter created with endpoint: {otlp_endpoint}")
        # Get the current provider (may have just been set above)
        provider = trace.get_tracer_provider()
        print(f"[DEBUG] Current provider: {provider}")
        provider.add_span_processor(
            BatchSpanProcessor(otlp_exporter)
        )
        print(f"[DEBUG] OTLP exporter configured with endpoint: {otlp_endpoint}")
    except ImportError as e:
        print(f"[DEBUG] OTLP exporter not available, skipping: {e}")
    except Exception as e:
        print(f"[DEBUG] OTLP exporter configuration failed: {e}")
else:
    print("[DEBUG] No OTEL_EXPORTER_OTLP_ENDPOINT found, skipping OTLP exporter")

# Console exporter removed - only use OTLP exporter for consistency with Rust
"#;

        match py.run(
            std::ffi::CString::new(init_code).unwrap().as_c_str(),
            None,
            None,
        ) {
            Ok(_) => {
                tracing::debug!("Successfully executed Python OpenTelemetry SDK initialization");
            }
            Err(e) => {
                tracing::error!("Failed to initialize Python OpenTelemetry SDK: {:?}", e);
                return Err(Box::new(e) as Box<dyn std::error::Error>);
            }
        };

        // Extract trace context to carrier format
        let mut carrier = std::collections::HashMap::new();
        let propagator = opentelemetry_sdk::propagation::TraceContextPropagator::new();

        // Debug: Check what's in the context before injection
        tracing::debug!("About to inject context: {:?}", context);

        // Always try to inject context - propagator will handle empty contexts gracefully
        propagator.inject_context(context, &mut carrier);

        tracing::debug!("Extracted trace context to carrier: {:?}", carrier);

        // Debug: Check if carrier is empty
        if carrier.is_empty() {
            tracing::debug!("Carrier is empty - no active span context to propagate");
        } else {
            tracing::debug!("Carrier has {} items", carrier.len());
            for (key, value) in &carrier {
                tracing::debug!("Carrier item: {} = {}", key, value);
            }
        }

        // Import OpenTelemetry modules
        let otel_trace = match py.import("opentelemetry.trace") {
            Ok(module) => {
                tracing::debug!("Successfully imported opentelemetry.trace");
                module
            }
            Err(e) => {
                tracing::error!("Failed to import opentelemetry.trace: {:?}", e);
                return Err(Box::new(e) as Box<dyn std::error::Error>);
            }
        };

        let _otel_context = match py.import("opentelemetry.context") {
            Ok(module) => {
                tracing::debug!("Successfully imported opentelemetry.context");
                module
            }
            Err(e) => {
                tracing::error!("Failed to import opentelemetry.context: {:?}", e);
                return Err(Box::new(e) as Box<dyn std::error::Error>);
            }
        };

        let otel_propagate = match py.import("opentelemetry.propagate") {
            Ok(module) => {
                tracing::debug!("Successfully imported opentelemetry.propagate");
                module
            }
            Err(e) => {
                tracing::error!("Failed to import opentelemetry.propagate: {:?}", e);
                return Err(Box::new(e) as Box<dyn std::error::Error>);
            }
        };

        // Create a Python dict from the carrier
        let carrier_dict = PyDict::new(py);
        for (key, value) in carrier {
            tracing::debug!("Setting carrier key: {} = {}", key, value);
            if let Err(e) = carrier_dict.set_item(key, value) {
                tracing::error!("Failed to set carrier item: {:?}", e);
                return Err(Box::new(e) as Box<dyn std::error::Error>);
            }
        }

        tracing::debug!("Created carrier dict: {:?}", carrier_dict);

        // Clone carrier_dict for debug code
        let carrier_dict_clone = carrier_dict.clone();

        // Extract the context using OpenTelemetry's extract function
        let _extracted_context = match otel_propagate.call_method1("extract", (carrier_dict,)) {
            Ok(context) => {
                tracing::debug!("Successfully extracted context: {:?}", context);
                context
            }
            Err(e) => {
                tracing::error!("Failed to extract context: {:?}", e);
                return Err(Box::new(e) as Box<dyn std::error::Error>);
            }
        };

        // Debug: Check what's in the extracted context (but don't attach globally)
        let debug_code = format!(
            r#"
import opentelemetry.trace as trace
import opentelemetry.propagate as otel_propagate

# Debug: Print carrier contents before extraction
carrier = {carrier_dict:?}
print(f"[DEBUG] Python carrier before extraction: {{carrier}}")

# Debug: Print result of extract (but don't attach globally)
extracted_context = otel_propagate.extract(carrier)
print(f"[DEBUG] Python extracted_context: {{extracted_context}}")

# Debug: Print current span (should be default/empty)
current_span = trace.get_current_span()
span_context = current_span.get_span_context()
print(f"[DEBUG] Current span (should be default): current_span={{current_span}} trace_id=0x{{span_context.trace_id:032x}} span_id=0x{{span_context.span_id:016x}} is_remote={{span_context.is_remote}}")
"#,
            carrier_dict = carrier_dict_clone
        );

        let _ = py
            .run(
                std::ffi::CString::new(debug_code).unwrap().as_c_str(),
                None,
                None,
            )
            .map_err(|e| {
                tracing::error!("Failed to run debug code: {:?}", e);
                e
            })?;

        // Get the current tracer
        let _tracer = match otel_trace.call_method1("get_tracer", ("kameo_snake_handler",)) {
            Ok(tracer) => {
                tracing::debug!("Successfully got tracer: {:?}", tracer);
                tracer
            }
            Err(e) => {
                tracing::error!("Failed to get tracer: {:?}", e);
                return Err(Box::new(e) as Box<dyn std::error::Error>);
            }
        };

        // DO NOT attach context globally here - it will be attached per-message in PY_OTEL_RUNNER
        tracing::debug!("Context setup complete - context will be attached per-message");

        // Force flush any pending spans to ensure they are exported
        let flush_code = r#"
try:
    provider = trace.get_tracer_provider()
    if hasattr(provider, 'force_flush'):
        provider.force_flush()
        print("[DEBUG] Forced flush of Python spans")
    else:
        print("[DEBUG] Provider does not support force_flush")
except Exception as e:
    print(f"[DEBUG] Force flush failed: {e}")
"#;

        let _ = py
            .run(
                std::ffi::CString::new(flush_code).unwrap().as_c_str(),
                None,
                None,
            )
            .map_err(|e| {
                tracing::error!("Failed to force flush Python spans: {:?}", e);
                e
            })?;

        tracing::debug!("Successfully set up Python OTEL context using pyo3");
        Ok(())
    })
}

/// Create a complete Python message processing span hierarchy.
///
/// This function creates all the necessary spans for Python message processing
/// with proper parent-child relationships and context propagation.
///
/// # Arguments
/// * `correlation_id` - Unique identifier for this message
/// * `message_type` - Type name of the message being processed
/// * `function_name` - Name of the Python function being called
/// * `is_async` - Whether the Python function is async
/// * `parent_span` - The parent span (ipc-child-receive) to nest under
///
/// # Returns
/// A tuple of spans that can be used with .instrument()
pub fn create_python_processing_spans(
    correlation_id: u64,
    message_type: &'static str,
    function_name: &str,
    is_async: bool,
    parent_span: &tracing::Span,
) -> (
    tracing::Span, // message_handler_span
    tracing::Span, // serialize_span
    tracing::Span, // call_span
    tracing::Span, // deserialize_span
) {
    let message_handler_span = create_python_message_handler_span(
        correlation_id,
        message_type,
        function_name,
        is_async,
        parent_span,
    );

    let serialize_span = create_python_serialize_span(message_type);
    let call_span = if is_async {
        create_python_async_call_span(function_name)
    } else {
        create_python_sync_call_span(function_name)
    };
    let deserialize_span = create_python_deserialize_span(message_type);

    (
        message_handler_span,
        serialize_span,
        call_span,
        deserialize_span,
    )
}
