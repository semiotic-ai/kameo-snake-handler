// OpenTelemetry OTLP setup: protocol and endpoint are now fully driven by OTEL_* env vars.
// Do NOT call .with_tonic() or .with_http() so the builder honors the env protocol.
use opentelemetry::global;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_sdk::{trace::SdkTracerProvider, Resource};
use tracing_subscriber::{layer::SubscriberExt, Registry};
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing::Level;
use opentelemetry_stdout::SpanExporter;
use tracing_subscriber::filter::EnvFilter;

/// Guard to keep the tracer provider alive for the process lifetime.
pub struct OtelGuard {
    tracer_provider: SdkTracerProvider,
}

impl Drop for OtelGuard {
    fn drop(&mut self) {
        // Ensure all spans are exported on shutdown
        if let Err(e) = self.tracer_provider.shutdown() {
            eprintln!("Failed to shut down tracer provider: {:?}", e);
        }
    }
}

/// Configuration for telemetry exporters.
#[derive(Debug, Clone, Copy)]
pub struct TelemetryExportConfig {
    /// Enable OTLP exporter (default: true)
    pub otlp_enabled: bool,
    /// Enable stdout exporter (default: true)
    pub stdout_enabled: bool,
}

impl Default for TelemetryExportConfig {
    fn default() -> Self {
        Self {
            otlp_enabled: true,
            stdout_enabled: true,
        }
    }
}

/// Async OTLP + stdout tracing subscriber setup (for child process, after runtime is running).
/// Both exporters are enabled by default, but can be toggled via config.
pub async fn build_subscriber_with_otel_and_fmt_async_with_config(
    config: TelemetryExportConfig,
) -> (impl tracing::Subscriber + Send + Sync, OtelGuard) {
    tracing::warn!("build_subscriber_with_otel_and_fmt_async CALLED!");
    let protocol = std::env::var("OTEL_EXPORTER_OTLP_PROTOCOL").unwrap_or_else(|_| "grpc".to_string());
    let mut provider_builder = SdkTracerProvider::builder();

    if config.otlp_enabled {
        tracing::info!(protocol = %protocol, "Adding OTLP exporter");
        let builder = opentelemetry_otlp::SpanExporter::builder();
        let exporter = match protocol.as_str() {
            "grpc" => builder.with_tonic().build().expect("Failed to create OTLP span exporter"),
            "http/protobuf" => builder.with_http().build().expect("Failed to create OTLP span exporter"),
            other => panic!("Unknown OTEL_EXPORTER_OTLP_PROTOCOL: {} (expected 'grpc' or 'http/protobuf')", other),
        };
        provider_builder = provider_builder.with_batch_exporter(exporter);
    }
    if config.stdout_enabled {
        tracing::info!("Adding opentelemetry_stdout::SpanExporter for local debug");
        let exporter = SpanExporter::default();
        provider_builder = provider_builder.with_simple_exporter(exporter);
    }

    let tracer_provider = provider_builder
        .with_resource(Resource::builder().build())
        .build();

    tracing::warn!("setup_otel_layer_async CALLED!");
    global::set_tracer_provider(tracer_provider.clone());
    let tracer = tracer_provider.tracer("rust_trace_exporter");
    let otel_layer = OpenTelemetryLayer::new(tracer);
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_target(true)
        .with_level(true)
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_ansi(true);

    let subscriber = Registry::default()
        .with(otel_layer)
        .with(fmt_layer)
        .with(EnvFilter::from_default_env());

    (subscriber, OtelGuard { tracer_provider })
}

/// Sync OTLP + stdout tracing subscriber setup (for parent process, where runtime is already running).
/// Both exporters are enabled by default, but can be toggled via config.
pub fn setup_otel_layer_with_config(config: TelemetryExportConfig) -> impl tracing_subscriber::Layer<tracing_subscriber::Registry> {
    let protocol = std::env::var("OTEL_EXPORTER_OTLP_PROTOCOL").unwrap_or_else(|_| "grpc".to_string());
    let mut provider_builder = SdkTracerProvider::builder();

    if config.otlp_enabled {
        tracing::info!(protocol = %protocol, "Adding OTLP exporter");
        let builder = opentelemetry_otlp::SpanExporter::builder();
        let exporter = match protocol.as_str() {
            "grpc" => builder.with_tonic().build().expect("Failed to create OTLP span exporter"),
            "http/protobuf" => builder.with_http().build().expect("Failed to create OTLP span exporter"),
            other => panic!("Unknown OTEL_EXPORTER_OTLP_PROTOCOL: {} (expected 'grpc' or 'http/protobuf')", other),
        };
        provider_builder = provider_builder.with_batch_exporter(exporter);
    }
    if config.stdout_enabled {
        tracing::info!("Adding opentelemetry_stdout::SpanExporter for local debug");
        let exporter = SpanExporter::default();
        provider_builder = provider_builder.with_simple_exporter(exporter);
    }

    let tracer_provider = provider_builder
        .with_resource(Resource::builder().build())
        .build();

    let tracer = tracer_provider.tracer("rust_trace_exporter");
    global::set_tracer_provider(tracer_provider.clone());
    tracing_opentelemetry::layer().with_tracer(tracer)
}
