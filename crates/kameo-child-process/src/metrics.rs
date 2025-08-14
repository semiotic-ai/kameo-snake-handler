use metrics::{counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram};
use once_cell::sync::Lazy;
use opentelemetry::metrics::{Counter, Histogram};
use opentelemetry::KeyValue;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::sync::Once;
use std::time::Instant;

// Initialize metrics only once
static INIT_METRICS: Once = Once::new();

// Global metrics counters for local tracking
static PARENT_INFLIGHT_COUNT: AtomicU64 = AtomicU64::new(0);
static CALLBACK_INFLIGHT_COUNT: AtomicU64 = AtomicU64::new(0);
static PARENT_TOTAL_MESSAGES: AtomicU64 = AtomicU64::new(0);
static CALLBACK_TOTAL_MESSAGES: AtomicU64 = AtomicU64::new(0);
static PARENT_MAX_INFLIGHT: AtomicU64 = AtomicU64::new(0);
static CALLBACK_MAX_INFLIGHT: AtomicU64 = AtomicU64::new(0);
static PARENT_ERROR_COUNT: AtomicU64 = AtomicU64::new(0);
static CALLBACK_ERROR_COUNT: AtomicU64 = AtomicU64::new(0);

// OpenTelemetry instruments
static INSTRUMENTS: Lazy<Mutex<Option<OtelInstruments>>> = Lazy::new(|| Mutex::new(None));

struct OtelInstruments {
    parent_messages_counter: Counter<u64>,
    callback_messages_counter: Counter<u64>,
    parent_errors_counter: Counter<u64>,
    callback_errors_counter: Counter<u64>,
    parent_latency_histogram: Histogram<f64>,
    callback_latency_histogram: Histogram<f64>,
}

/// Tracks metrics for IPC operations
#[derive(Debug, Clone, Copy)]
pub struct MetricsHandle {
    operation_type: &'static str,
}

/// Higher-level metrics reporter that provides readable statistics
pub struct MetricsReporter;

impl MetricsHandle {
    pub fn parent() -> Self {
        Self {
            operation_type: "parent",
        }
    }

    pub fn callback() -> Self {
        Self {
            operation_type: "callback",
        }
    }

    // Update metrics for inflight increment
    pub fn track_inflight_increment(&self) {
        match self.operation_type {
            "parent" => {
                // Avoid overflow by checking first
                let current = PARENT_INFLIGHT_COUNT.load(Ordering::SeqCst);
                if current < u64::MAX {
                    let new_count = PARENT_INFLIGHT_COUNT.fetch_add(1, Ordering::SeqCst) + 1;

                    // Update max if needed (with overflow protection)
                    let current_max = PARENT_MAX_INFLIGHT.load(Ordering::SeqCst);
                    if new_count > current_max {
                        // Simple approach: just set it if larger, no need for atomic exchange
                        PARENT_MAX_INFLIGHT.store(new_count, Ordering::SeqCst);
                    }

                    // Also increment total messages
                    let _ = PARENT_TOTAL_MESSAGES.fetch_update(
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                        |x| {
                            if x < u64::MAX {
                                Some(x + 1)
                            } else {
                                Some(x)
                            }
                        },
                    );

                    // Record to OpenTelemetry if available
                    if let Some(instruments) = INSTRUMENTS.lock().unwrap().as_ref() {
                        instruments.parent_messages_counter.add(1, &[]);
                    }

                    // Record to metrics
                    gauge!("kameo_child_process_parent_inflight").increment(1.0);
                    counter!("kameo_child_process_parent_messages_total").increment(1);
                }
            }
            "callback" => {
                // Avoid overflow by checking first
                let current = CALLBACK_INFLIGHT_COUNT.load(Ordering::SeqCst);
                if current < u64::MAX {
                    let new_count = CALLBACK_INFLIGHT_COUNT.fetch_add(1, Ordering::SeqCst) + 1;

                    // Update max if needed (with overflow protection)
                    let current_max = CALLBACK_MAX_INFLIGHT.load(Ordering::SeqCst);
                    if new_count > current_max {
                        // Simple approach: just set it if larger, no need for atomic exchange
                        CALLBACK_MAX_INFLIGHT.store(new_count, Ordering::SeqCst);
                    }

                    // Also increment total messages
                    let _ = CALLBACK_TOTAL_MESSAGES.fetch_update(
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                        |x| {
                            if x < u64::MAX {
                                Some(x + 1)
                            } else {
                                Some(x)
                            }
                        },
                    );

                    // Record to OpenTelemetry if available
                    if let Some(instruments) = INSTRUMENTS.lock().unwrap().as_ref() {
                        instruments.callback_messages_counter.add(1, &[]);
                    }

                    // Record to metrics
                    gauge!("kameo_child_process_callback_inflight").increment(1.0);
                    counter!("kameo_child_process_callback_messages_total").increment(1);
                }
            }
            _ => {}
        }

        tracing::trace!(
            event = "metrics_track",
            operation_type = self.operation_type,
            operation = "increment",
            "Incremented in-flight counter"
        );
    }

    pub fn track_inflight_decrement(&self) {
        match self.operation_type {
            "parent" => {
                // Avoid underflow by checking first
                let current = PARENT_INFLIGHT_COUNT.load(Ordering::SeqCst);
                if current > 0 {
                    PARENT_INFLIGHT_COUNT.fetch_sub(1, Ordering::SeqCst);
                }

                // Record to metrics
                gauge!("kameo_child_process_parent_inflight").decrement(1.0);
            }
            "callback" => {
                // Avoid underflow by checking first
                let current = CALLBACK_INFLIGHT_COUNT.load(Ordering::SeqCst);
                if current > 0 {
                    CALLBACK_INFLIGHT_COUNT.fetch_sub(1, Ordering::SeqCst);
                }

                // Record to metrics
                gauge!("kameo_child_process_callback_inflight").decrement(1.0);
            }
            _ => {}
        }

        tracing::trace!(
            event = "metrics_track",
            operation_type = self.operation_type,
            operation = "decrement",
            "Decremented in-flight counter"
        );
    }

    pub fn track_latency(&self, start_time: Instant) {
        let duration = start_time.elapsed();
        let duration_ms = duration.as_secs_f64() * 1000.0;

        match self.operation_type {
            "parent" => {
                // Record to OpenTelemetry if available
                if let Some(instruments) = INSTRUMENTS.lock().unwrap().as_ref() {
                    instruments
                        .parent_latency_histogram
                        .record(duration_ms, &[]);
                }

                // Record to metrics
                histogram!("kameo_child_process_parent_latency_ms").record(duration_ms);

                tracing::trace!(
                    event = "metrics_latency",
                    operation_type = self.operation_type,
                    latency_ms = duration_ms,
                    inflight = PARENT_INFLIGHT_COUNT.load(Ordering::SeqCst),
                    "Operation latency"
                );
            }
            "callback" => {
                // Record to OpenTelemetry if available
                if let Some(instruments) = INSTRUMENTS.lock().unwrap().as_ref() {
                    instruments
                        .callback_latency_histogram
                        .record(duration_ms, &[]);
                }

                // Record to metrics
                histogram!("kameo_child_process_callback_latency_ms").record(duration_ms);

                tracing::trace!(
                    event = "metrics_latency",
                    operation_type = self.operation_type,
                    latency_ms = duration_ms,
                    inflight = CALLBACK_INFLIGHT_COUNT.load(Ordering::SeqCst),
                    "Operation latency"
                );
            }
            _ => {}
        }
    }

    pub fn track_error(&self, error_type: &str) {
        match self.operation_type {
            "parent" => {
                PARENT_ERROR_COUNT.fetch_add(1, Ordering::SeqCst);

                // Record to OpenTelemetry if available
                if let Some(instruments) = INSTRUMENTS.lock().unwrap().as_ref() {
                    instruments
                        .parent_errors_counter
                        .add(1, &[KeyValue::new("error_type", error_type.to_string())]);
                }

                // Record to metrics
                counter!("kameo_child_process_parent_errors_total").increment(1);
            }
            "callback" => {
                CALLBACK_ERROR_COUNT.fetch_add(1, Ordering::SeqCst);

                // Record to OpenTelemetry if available
                if let Some(instruments) = INSTRUMENTS.lock().unwrap().as_ref() {
                    instruments
                        .callback_errors_counter
                        .add(1, &[KeyValue::new("error_type", error_type.to_string())]);
                }

                // Record to metrics
                counter!("kameo_child_process_callback_errors_total").increment(1);
            }
            _ => {}
        }

        // Log the error type via tracing instead
        tracing::debug!(
            event = "metrics_error",
            operation_type = self.operation_type,
            error_type,
            "Error in IPC operation"
        );
    }
}

impl MetricsReporter {
    /// Get current inflight counts
    pub fn get_inflight_counts() -> (u64, u64) {
        let parent = PARENT_INFLIGHT_COUNT.load(Ordering::SeqCst);
        let callback = CALLBACK_INFLIGHT_COUNT.load(Ordering::SeqCst);
        (parent, callback)
    }

    /// Get total message counts
    pub fn get_total_counts() -> (u64, u64) {
        let parent = PARENT_TOTAL_MESSAGES.load(Ordering::SeqCst);
        let callback = CALLBACK_TOTAL_MESSAGES.load(Ordering::SeqCst);
        (parent, callback)
    }

    /// Get max inflight counts observed
    pub fn get_max_inflight_counts() -> (u64, u64) {
        let parent = PARENT_MAX_INFLIGHT.load(Ordering::SeqCst);
        let callback = CALLBACK_MAX_INFLIGHT.load(Ordering::SeqCst);
        (parent, callback)
    }

    /// Get error counts
    pub fn get_error_counts() -> (u64, u64) {
        let parent = PARENT_ERROR_COUNT.load(Ordering::SeqCst);
        let callback = CALLBACK_ERROR_COUNT.load(Ordering::SeqCst);
        (parent, callback)
    }

    /// Log the current metrics state
    pub fn log_metrics_state() {
        let (parent_inflight, callback_inflight) = Self::get_inflight_counts();
        let (parent_total, callback_total) = Self::get_total_counts();
        let (parent_max, callback_max) = Self::get_max_inflight_counts();
        let (parent_errors, callback_errors) = Self::get_error_counts();

        tracing::info!(
            event = "metrics_summary",
            parent_inflight,
            callback_inflight,
            parent_total,
            callback_total,
            parent_max,
            callback_max,
            parent_errors,
            callback_errors,
            "Metrics summary"
        );

        // Update absolute metrics values
        gauge!("kameo_child_process_parent_inflight_count").set(parent_inflight as f64);
        gauge!("kameo_child_process_callback_inflight_count").set(callback_inflight as f64);
        gauge!("kameo_child_process_parent_max_inflight").set(parent_max as f64);
        gauge!("kameo_child_process_callback_max_inflight").set(callback_max as f64);
    }
}

/// A tracker for a single operation's metrics
pub struct OperationTracker {
    handle: MetricsHandle,
    start_time: Instant,
}

impl OperationTracker {
    pub fn track_parent() -> Self {
        let handle = MetricsHandle::parent();
        handle.track_inflight_increment();
        Self {
            handle,
            start_time: Instant::now(),
        }
    }

    pub fn track_callback() -> Self {
        let handle = MetricsHandle::callback();
        handle.track_inflight_increment();
        Self {
            handle,
            start_time: Instant::now(),
        }
    }
}

impl Drop for OperationTracker {
    fn drop(&mut self) {
        self.handle.track_latency(self.start_time);
        self.handle.track_inflight_decrement();
    }
}

/// Initialize the metrics system
pub fn init_metrics() {
    INIT_METRICS.call_once(|| {
        // Reset all metrics counters
        PARENT_INFLIGHT_COUNT.store(0, Ordering::SeqCst);
        CALLBACK_INFLIGHT_COUNT.store(0, Ordering::SeqCst);
        PARENT_TOTAL_MESSAGES.store(0, Ordering::SeqCst);
        CALLBACK_TOTAL_MESSAGES.store(0, Ordering::SeqCst);
        PARENT_MAX_INFLIGHT.store(0, Ordering::SeqCst);
        CALLBACK_MAX_INFLIGHT.store(0, Ordering::SeqCst);
        PARENT_ERROR_COUNT.store(0, Ordering::SeqCst);
        CALLBACK_ERROR_COUNT.store(0, Ordering::SeqCst);

        // Get a meter from the global provider
        let meter = opentelemetry::global::meter("kameo_child_process");

        // Create counters
        let parent_messages_counter = meter
            .u64_counter("kameo_child_process_parent_messages")
            .with_description("Total number of parent messages processed")
            .build();

        let callback_messages_counter = meter
            .u64_counter("kameo_child_process_callback_messages")
            .with_description("Total number of callback messages processed")
            .build();

        let parent_errors_counter = meter
            .u64_counter("kameo_child_process_parent_errors")
            .with_description("Total number of parent operation errors")
            .build();

        let callback_errors_counter = meter
            .u64_counter("kameo_child_process_callback_errors")
            .with_description("Total number of callback operation errors")
            .build();

        // Create histograms
        let parent_latency_histogram = meter
            .f64_histogram("kameo_child_process_parent_latency")
            .with_description("Latency of parent operations")
            .build();

        let callback_latency_histogram = meter
            .f64_histogram("kameo_child_process_callback_latency")
            .with_description("Latency of callback operations")
            .build();

        // Register and describe metrics
        describe_gauge!(
            "kameo_child_process_parent_inflight",
            "Current number of in-flight parent operations"
        );
        describe_gauge!(
            "kameo_child_process_callback_inflight",
            "Current number of in-flight callback operations"
        );
        describe_gauge!(
            "kameo_child_process_parent_inflight_count",
            "Current number of in-flight parent operations"
        );
        describe_gauge!(
            "kameo_child_process_callback_inflight_count",
            "Current number of in-flight callback operations"
        );
        describe_gauge!(
            "kameo_child_process_parent_max_inflight",
            "Maximum number of concurrent parent operations observed"
        );
        describe_gauge!(
            "kameo_child_process_callback_max_inflight",
            "Maximum number of concurrent callback operations observed"
        );

        describe_counter!(
            "kameo_child_process_parent_messages_total",
            "Total number of parent messages processed"
        );
        describe_counter!(
            "kameo_child_process_callback_messages_total",
            "Total number of callback messages processed"
        );
        describe_counter!(
            "kameo_child_process_parent_errors_total",
            "Total number of parent operation errors"
        );
        describe_counter!(
            "kameo_child_process_callback_errors_total",
            "Total number of callback operation errors"
        );

        describe_histogram!(
            "kameo_child_process_parent_latency_ms",
            "Latency of parent operations in milliseconds"
        );
        describe_histogram!(
            "kameo_child_process_callback_latency_ms",
            "Latency of callback operations in milliseconds"
        );

        // Store instruments for later use
        let instruments = OtelInstruments {
            parent_messages_counter,
            callback_messages_counter,
            parent_errors_counter,
            callback_errors_counter,
            parent_latency_histogram,
            callback_latency_histogram,
        };

        *INSTRUMENTS.lock().unwrap() = Some(instruments);

        tracing::info!(
            event = "metrics_init",
            "OpenTelemetry metrics initialized with direct instruments"
        );
    });
}
