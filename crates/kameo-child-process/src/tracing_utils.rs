//! Tracing helpers for message-centric OpenTelemetry span management.
//!
//! This module provides encapsulated functions for creating and managing spans
//! in the child process, ensuring proper parent-child relationships and
//! preventing span context inheritance issues.

use opentelemetry::Context as OtelContext;
use tracing::info_span;
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// Enter a context with no parent span to prevent ambient span context inheritance.
///
/// Use this before creating a future to ensure it does not inherit any ambient span context.
/// This is crucial for preventing span aliasing across different messages.
///
/// # Returns
/// A guard that must be held for the duration of future creation.
pub fn enter_no_parent_context() -> tracing::span::EnteredSpan {
    tracing::Span::none().entered()
}

/// Create a completely new trace context for message isolation.
///
/// Creates a new OTEL context with a fresh trace ID to ensure each message
/// gets its own unique trace tree. This prevents span aliasing across messages.
///
/// # Returns
/// A new OTEL context with a fresh trace ID.
pub fn create_isolated_trace_context() -> OtelContext {
    opentelemetry::Context::new()
}

/// Create a root ipc-message span that encompasses the entire IPC operation.
///
/// This span serves as the root for both send and receive operations,
/// providing correlation between parent and child processes.
///
/// # Arguments
/// * `correlation_id` - Unique identifier for this message
/// * `message_type` - Type name of the message being processed
///
/// # Returns
/// A span that can be used with .instrument() or entered directly
pub fn create_root_ipc_message_span(
    correlation_id: u64,
    message_type: &'static str,
) -> tracing::Span {
    info_span!(
        "ipc-message",
        correlation_id = correlation_id,
        message_type = message_type,
        messaging.system = "ipc",
        messaging.operation = "request"
    )
}

/// Create an ipc-parent-send span as a child of the root ipc-message span.
///
/// This span represents the parent process sending a message to the child.
///
/// # Arguments
/// * `correlation_id` - Unique identifier for this message
/// * `message_type` - Type name of the message being sent
/// * `parent_span` - The root ipc-message span to use as parent
///
/// # Returns
/// A span that can be used with .instrument()
pub fn create_ipc_parent_send_span(
    correlation_id: u64,
    message_type: &'static str,
    parent_span: &tracing::Span,
) -> tracing::Span {
    info_span!(
        parent: parent_span,
        "ipc-parent-send",
        correlation_id = correlation_id,
        message_type = message_type,
        messaging.system = "ipc",
        messaging.operation = "send"
    )
}

/// Create an ipc-child-receive span as a child of the root ipc-message span.
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
    parent_cx: OtelContext, // Now this is the ipc-message context
) -> tracing::Span {
    let span = info_span!(
        "ipc-child-receive",
        correlation_id = correlation_id,
        message_type = message_type,
        messaging.system = "ipc",
        messaging.operation = "receive",
        messaging.source_kind = "queue"
    );
    // Set parent to ipc-message context - this makes it a sibling of ipc-parent-send
    span.set_parent(parent_cx);
    span
}

/// Create an ipc-message span with a parent context (for child process message handling)
pub fn start_ipc_message_span(
    correlation_id: u64,
    message_type: &'static str,
    parent_cx: &opentelemetry::Context,
) -> tracing::Span {
    let span = tracing::info_span!(
        "ipc-message",
        correlation_id = correlation_id,
        message_type = message_type,
        messaging.system = "ipc"
    );
    span.set_parent(parent_cx.clone());
    tracing::debug!(
        event = "span_creation_debug",
        correlation_id = correlation_id,
        span_id = ?span.id(),
        parent_context_trace_id = ?opentelemetry::trace::TraceContextExt::span(parent_cx).span_context().trace_id(),
        parent_context_span_id = ?opentelemetry::trace::TraceContextExt::span(parent_cx).span_context().span_id(),
        "Created ipc-message span with parent context"
    );
    span
}

/// Start the `ipc-child-receive` span for the receive phase.
///
/// Creates a span for the message receive phase with proper OTEL semantic conventions.
/// This span represents the child process receiving a message from the queue.
///
/// # Arguments
/// * `correlation_id` - Unique identifier for this message
/// * `message_type` - Type name of the message being processed
///
/// # Returns
/// A guard that must be held to keep the span active.
pub fn start_ipc_child_receive_span(
    correlation_id: u64,
    message_type: &'static str,
) -> tracing::span::EnteredSpan {
    let span = info_span!(
        "ipc-child-receive",
        correlation_id = correlation_id,
        message_type = message_type,
        messaging.system = "ipc",
        messaging.operation = "receive",
        messaging.source_kind = "queue"
    );
    span.entered()
}

/// Start the `ipc-child-process` span for the processing phase.
///
/// Creates a span for the message processing phase with proper OTEL semantic conventions.
/// This span represents the child process executing the message logic.
///
/// # Arguments
/// * `correlation_id` - Unique identifier for this message
/// * `message_type` - Type name of the message being processed
///
/// # Returns
/// A guard that must be held to keep the span active.
pub fn start_ipc_child_process_span(
    correlation_id: u64,
    message_type: &'static str,
) -> tracing::span::EnteredSpan {
    let span = info_span!(
        "ipc-child-process",
        correlation_id = correlation_id,
        message_type = message_type,
        messaging.system = "ipc",
        messaging.operation = "process"
    );
    span.entered()
}

/// Start the `ipc-child-reply` span for the reply phase.
///
/// Creates a span for the message reply phase with proper OTEL semantic conventions.
/// This span represents the child process sending a reply back to the parent.
///
/// # Arguments
/// * `correlation_id` - Unique identifier for this message
/// * `reply_type` - Type name of the reply being sent
///
/// # Returns
/// A guard that must be held to keep the span active.
pub fn start_ipc_child_reply_span(
    correlation_id: u64,
    reply_type: &'static str,
) -> tracing::span::EnteredSpan {
    let span = info_span!(
        "ipc-child-reply",
        correlation_id = correlation_id,
        reply_type = reply_type,
        messaging.system = "ipc",
        messaging.operation = "send",
        messaging.destination_kind = "queue"
    );
    span.entered()
}

/// Start the `ipc-parent-send` span for the parent send phase.
///
/// Creates a span for the parent process sending a message with proper OTEL semantic conventions.
/// This span represents the parent process sending a message to the child.
///
/// # Arguments
/// * `correlation_id` - Unique identifier for this message
/// * `message_type` - Type name of the message being sent
///
/// # Returns
/// A guard that must be held to keep the span active.
pub fn start_ipc_parent_send_span(
    correlation_id: u64,
    message_type: &'static str,
) -> tracing::span::EnteredSpan {
    let span = info_span!(
        "ipc-parent-send",
        correlation_id = correlation_id,
        message_type = message_type,
        messaging.system = "ipc",
        messaging.operation = "send",
        messaging.destination_kind = "queue"
    );
    span.entered()
}

/// Start the `ipc-parent-receive` span for the parent receive phase.
///
/// Creates a span for the parent process receiving a reply with proper OTEL semantic conventions.
/// This span represents the parent process receiving a reply from the child.
///
/// # Arguments
/// * `correlation_id` - Unique identifier for this message
/// * `reply_type` - Type name of the reply being received
///
/// # Returns
/// A guard that must be held to keep the span active.
pub fn start_ipc_parent_receive_span(
    correlation_id: u64,
    reply_type: &'static str,
) -> tracing::span::EnteredSpan {
    let span = info_span!(
        "ipc-parent-receive",
        correlation_id = correlation_id,
        message_type = reply_type,
        messaging.system = "ipc",
        messaging.operation = "receive",
        messaging.source_kind = "queue"
    );
    span.entered()
}

/// Utility function to create a no-parent context guard for future creation.
///
/// This is a convenience function that creates a guard to prevent ambient span context inheritance.
///
/// # Returns
/// A guard that must be held for the duration of future creation.
pub fn create_no_parent_guard() -> impl std::ops::Drop {
    enter_no_parent_context()
}

/// Create a message span with proper parent context for use with .instrument()
///
/// This function creates a span that can be used with the .instrument() method
/// to properly nest spans without storing EnteredSpan guards across async boundaries.
///
/// # Arguments
/// * `correlation_id` - Unique identifier for this message
/// * `message_type` - Type name of the message being processed
/// * `parent_cx` - OTEL context to use as parent
///
/// # Returns
/// A span that can be used with .instrument()
pub fn create_message_span_with_parent(
    correlation_id: u64,
    message_type: &'static str,
    parent_cx: OtelContext,
) -> tracing::Span {
    let span = info_span!(
        "ipc-message",
        correlation_id = correlation_id,
        message_type = message_type,
        messaging.system = "ipc"
    );
    span.set_parent(parent_cx);
    span
}

/// Create a callback handle span.
///
/// This span represents the handling of a callback message.
///
/// # Arguments
/// * `correlation_id` - Unique identifier for this callback
///
/// # Returns
/// A span that can be used with .instrument()
pub fn create_callback_handle_span(correlation_id: u64) -> tracing::Span {
    info_span!("handle", correlation_id = correlation_id,)
}

/// Encapsulated message processing span lifecycle.
///
/// This function creates the proper span hierarchy for message processing:
/// - ipc-message (root with parent context)
/// - ipc-child-process (child span for processing)
/// - ipc-child-reply (child span for reply creation)
///
/// The spans are created and entered immediately, then dropped before async operations
/// to avoid Send trait issues.
///
/// # Arguments
/// * `correlation_id` - Unique identifier for this message
/// * `message_type` - Type name of the message being processed
/// * `parent_cx` - OTEL context to use as parent
///
/// # Returns
/// A tuple of (message_guard, process_span, reply_span) where the guards should be dropped
/// before async operations.
pub fn create_message_processing_spans(
    correlation_id: u64,
    message_type: &'static str,
    parent_cx: OtelContext,
) -> (tracing::span::EnteredSpan, tracing::Span, tracing::Span) {
    // Create the root message span with proper parent context
    let message_span = info_span!(
        "ipc-message",
        correlation_id = correlation_id,
        message_type = message_type,
        messaging.system = "ipc"
    );
    message_span.set_parent(parent_cx);
    let message_guard = message_span.entered();

    // Create child spans (not entered yet to avoid Send issues)
    let process_span = info_span!(
        "ipc-child-process",
        correlation_id = correlation_id,
        messaging.system = "ipc"
    );

    let reply_span = info_span!(
        "ipc-child-reply",
        correlation_id = correlation_id,
        messaging.system = "ipc"
    );

    (message_guard, process_span, reply_span)
}
