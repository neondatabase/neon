use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::prelude::*;

/// Initialize logging to stderr, and OpenTelemetry tracing and exporter.
///
/// Logging is configured using either `default_log_level` or
/// `RUST_LOG` environment variable as default log level.
///
/// OpenTelemetry is configured with OTLP/HTTP exporter. It picks up
/// configuration from environment variables. For example, to change the destination,
/// set `OTEL_EXPORTER_OTLP_ENDPOINT=http://jaeger:4318`. See
/// `tracing-utils` package description.
///
pub fn init_tracing_and_logging(default_log_level: &str) -> anyhow::Result<()> {
    // Initialize Logging
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(default_log_level));

    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_target(false)
        .with_writer(std::io::stderr);

    // Initialize OpenTelemetry
    let otlp_layer =
        tracing_utils::init_tracing_without_runtime("compute_ctl").map(OpenTelemetryLayer::new);

    // Put it all together
    tracing_subscriber::registry()
        .with(env_filter)
        .with(otlp_layer)
        .with(fmt_layer)
        .init();
    tracing::info!("logging and tracing started");

    Ok(())
}
