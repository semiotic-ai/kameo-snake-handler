// OpenTelemetry OTLP setup: protocol and endpoint are now fully driven by OTEL_* env vars.
use metrics;
use metrics_exporter_opentelemetry;
use opentelemetry::global;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry::KeyValue;
use opentelemetry_sdk::{
    metrics::SdkMeterProvider, propagation::TraceContextPropagator, trace::SdkTracerProvider,
    Resource,
};
use std::time::Duration;
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::filter::EnvFilter;
use tracing_subscriber::{layer::SubscriberExt, Registry};

/// Guard to keep the tracer provider alive for the process lifetime.
pub struct OtelGuard {
    tracer_provider: SdkTracerProvider,
    meter_provider: Option<SdkMeterProvider>,
}

impl Drop for OtelGuard {
    fn drop(&mut self) {
        // Best-effort flush and shutdown for traces
        let _ = self.tracer_provider.force_flush();
        std::thread::sleep(Duration::from_millis(50));
        if let Err(e) = self.tracer_provider.shutdown() {
            let msg = format!("{:?}", e);
            // Downgrade common transport errors to debug to avoid noisy shutdowns
            if msg.contains("Unavailable") || msg.contains("tcp connect error") {
                tracing::debug!(error = %msg, "Tracer provider shutdown finished with transport error");
            } else {
                tracing::warn!(error = %msg, "Failed to shut down tracer provider");
            }
        }

        // Ensure all metrics are exported on shutdown
        if let Some(meter_provider) = self.meter_provider.take() {
            let _ = meter_provider.force_flush();
            std::thread::sleep(Duration::from_millis(50));
            if let Err(e) = meter_provider.shutdown() {
                let msg = format!("{:?}", e);
                if msg.contains("Failed to shutdown") || msg.contains("Unavailable") {
                    tracing::debug!(error = %msg, "Meter provider shutdown finished with transport error");
                } else {
                    tracing::warn!(error = %msg, "Failed to shut down meter provider");
                }
            }
        }
    }
}

/// Configuration for telemetry exporters.
#[derive(Debug, Clone, Copy)]
pub struct TelemetryExportConfig {
    /// Enable OTLP exporter (default: true)
    pub otlp_enabled: bool,
    /// Enable metrics exporter (default: true)
    pub metrics_enabled: bool,
}

impl Default for TelemetryExportConfig {
    fn default() -> Self {
        Self {
            otlp_enabled: true,
            metrics_enabled: true,
        }
    }
}

/// Async OTLP + stdout tracing subscriber setup (for child process, after runtime is running).
/// Both exporters are enabled by default, but can be toggled via config.
pub async fn build_subscriber_with_otel_and_fmt_async_with_config(
    config: TelemetryExportConfig,
) -> (impl tracing::Subscriber + Send + Sync, OtelGuard) {
    tracing::debug!("build_subscriber_with_otel_and_fmt_async CALLED");

    let protocol =
        std::env::var("OTEL_EXPORTER_OTLP_PROTOCOL").unwrap_or_else(|_| "grpc".to_string());

    let mut provider_builder = SdkTracerProvider::builder();

    if config.otlp_enabled {
        tracing::debug!(protocol = %protocol, "Adding OTLP exporter");
        // Use the SpanExporter builder which respects OTEL_* env vars
        let builder = opentelemetry_otlp::SpanExporter::builder();
        let exporter = match protocol.as_str() {
            "grpc" => builder
                .with_tonic()
                .build()
                .expect("Failed to create OTLP span exporter"),
            "http/protobuf" => builder
                .with_http()
                .build()
                .expect("Failed to create OTLP span exporter"),
            other => panic!(
                "Unknown OTEL_EXPORTER_OTLP_PROTOCOL: {} (expected 'grpc' or 'http/protobuf')",
                other
            ),
        };
        provider_builder = provider_builder.with_batch_exporter(exporter);
    }
    // No stdout span exporter; traces go to OTEL only if enabled

    let tracer_provider = provider_builder
        .with_resource(resource_from_env_or_default("kameo-snake"))
        .build();

    tracing::debug!("setup_otel_layer_async CALLED");
    global::set_tracer_provider(tracer_provider.clone());

    // Set the global propagator for distributed tracing
    let propagator = TraceContextPropagator::new();
    global::set_text_map_propagator(propagator);

    let tracer = tracer_provider.tracer("rust_trace_exporter");
    let otel_layer = OpenTelemetryLayer::new(tracer);
    let subscriber = Registry::default()
        .with(otel_layer)
        .with(EnvFilter::from_default_env());

    // Initialize metrics if enabled
    let meter_provider = if config.metrics_enabled {
        // Setup metrics and get the provider for shutdown
        Some(setup_metrics())
    } else {
        None
    };

    (
        subscriber,
        OtelGuard {
            tracer_provider,
            meter_provider,
        },
    )
}

/// Sync OTLP + stdout tracing subscriber setup (for parent process, where runtime is already running).
/// Both exporters are enabled by default, but can be toggled via config.
pub fn setup_otel_layer_with_config(
    config: TelemetryExportConfig,
) -> impl tracing_subscriber::Layer<tracing_subscriber::Registry> {
    let protocol =
        std::env::var("OTEL_EXPORTER_OTLP_PROTOCOL").unwrap_or_else(|_| "grpc".to_string());

    let mut provider_builder = SdkTracerProvider::builder();

    if config.otlp_enabled {
        tracing::info!(protocol = %protocol, "Adding OTLP exporter");
        // Use the SpanExporter builder which respects OTEL_* env vars
        let builder = opentelemetry_otlp::SpanExporter::builder();
        let exporter = match protocol.as_str() {
            "grpc" => builder
                .with_tonic()
                .build()
                .expect("Failed to create OTLP span exporter"),
            "http/protobuf" => builder
                .with_http()
                .build()
                .expect("Failed to create OTLP span exporter"),
            other => panic!(
                "Unknown OTEL_EXPORTER_OTLP_PROTOCOL: {} (expected 'grpc' or 'http/protobuf')",
                other
            ),
        };
        provider_builder = provider_builder.with_batch_exporter(exporter);
    }
    // No stdout span exporter; traces go to OTEL only if enabled

    let tracer_provider = provider_builder
        .with_resource(resource_from_env_or_default("kameo-snake"))
        .build();

    let tracer = tracer_provider.tracer("rust_trace_exporter");
    global::set_tracer_provider(tracer_provider);

    // Set the global propagator for distributed tracing
    let propagator = TraceContextPropagator::new();
    global::set_text_map_propagator(propagator);

    // Initialize metrics if enabled
    if config.metrics_enabled {
        // Setup metrics and discard the provider since we don't need to handle shutdown
        // in the sync case (the process will handle it)
        let _ = setup_metrics();
    }

    tracing_opentelemetry::layer().with_tracer(tracer)
}

/// Construct an EnvFilter that clamps noisy external crates while keeping our
/// crates verbose at trace by default. If RUST_LOG is set, it is appended last
/// to allow users to override any defaults.
// Note: log filtering is controlled entirely by the executing environment via RUST_LOG/EnvFilter

/// Initialize OpenTelemetry metrics with OTLP exporter.
///
/// This function sets up the OpenTelemetry metrics pipeline:
/// 1. Creates an OTLP metrics exporter using environment variables for configuration
/// 2. Sets up a periodic reader to collect and export metrics
/// 3. Configures the global meter provider
/// 4. Bridges the metrics crate with OpenTelemetry using metrics-exporter-opentelemetry
///
/// Environment variables that affect the configuration:
/// - OTEL_EXPORTER_OTLP_ENDPOINT: The endpoint to send metrics to (default: http://localhost:4317)
/// - OTEL_EXPORTER_OTLP_PROTOCOL: The protocol to use (grpc or http/protobuf, default: grpc)
///
/// Returns the SdkMeterProvider for proper shutdown handling.
fn setup_metrics() -> SdkMeterProvider {
    tracing::debug!("Initializing OpenTelemetry metrics with OTLP exporter");

    // Create a metrics exporter - this will automatically use OTEL_* environment variables
    let exporter = match opentelemetry_otlp::MetricExporter::builder()
        .with_tonic() // Default to gRPC, environment variables will override if needed
        .build()
    {
        Ok(exporter) => exporter,
        Err(e) => {
            tracing::warn!(error = %e, "Failed to create OTLP metrics exporter, using basic provider");
            // Return a basic provider without reader if OTLP setup fails
            return opentelemetry_sdk::metrics::SdkMeterProvider::builder()
                .with_resource(resource_from_env_or_default("kameo-snake"))
                .build();
        }
    };

    // Create a periodic reader with the exporter
    let reader = opentelemetry_sdk::metrics::PeriodicReader::builder(exporter)
        .with_interval(Duration::from_secs(5))
        .build();

    // Build the meter provider with the reader
    let provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
        .with_reader(reader)
        .with_resource(resource_from_env_or_default("kameo-snake"))
        .build();

    // Set the global meter provider
    global::set_meter_provider(provider.clone());

    // Get a meter from the global provider
    let meter = global::meter("kameo-snake-handler");

    // Set up the metrics-exporter-opentelemetry bridge
    // This will connect the metrics crate to the OpenTelemetry SDK
    let recorder = metrics_exporter_opentelemetry::Recorder::with_meter(meter);

    // Install the recorder
    if let Err(err) = metrics::set_global_recorder(recorder) {
        tracing::warn!(error = %err, "Failed to install metrics-exporter-opentelemetry recorder");
    } else {
        tracing::debug!("OTLP metrics exporter and metrics-opentelemetry bridge configured");
    }

    provider
}

/// Build a Resource honoring OTEL_RESOURCE_ATTRIBUTES. If service.name is not provided,
/// default_service will be applied.
fn resource_from_env_or_default(default_service: &str) -> Resource {
    if let Ok(env) = std::env::var("OTEL_RESOURCE_ATTRIBUTES") {
        let mut attrs: Vec<KeyValue> = Vec::new();
        let mut has_service = false;
        for pair in env.split(',') {
            let mut iter = pair.splitn(2, '=');
            if let (Some(k), Some(v)) = (iter.next(), iter.next()) {
                let key = k.trim();
                let val = v.trim();
                if !key.is_empty() && !val.is_empty() {
                    if key == "service.name" {
                        has_service = true;
                    }
                    attrs.push(KeyValue::new(key.to_string(), val.to_string()));
                }
            }
        }
        if !has_service {
            attrs.push(KeyValue::new("service.name", default_service.to_string()));
        }
        return Resource::builder().with_attributes(attrs).build();
    }
    Resource::builder()
        .with_attribute(KeyValue::new("service.name", default_service.to_string()))
        .build()
}
