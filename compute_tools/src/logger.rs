use std::collections::HashMap;
use tracing::info;
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
pub fn init_tracing_and_logging(
    default_log_level: &str,
) -> anyhow::Result<Option<tracing_utils::Provider>> {
    // Initialize Logging
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(default_log_level));

    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_ansi(false)
        .with_target(false)
        .with_writer(std::io::stderr);

    // Initialize OpenTelemetry
    let provider =
        tracing_utils::init_tracing("compute_ctl", tracing_utils::ExportConfig::default());
    let otlp_layer = provider.as_ref().map(tracing_utils::layer);

    // Put it all together
    tracing_subscriber::registry()
        .with(env_filter)
        .with(otlp_layer)
        .with(fmt_layer)
        .init();
    tracing::info!("logging and tracing started");

    utils::logging::replace_panic_hook_with_tracing_panic_hook().forget();

    Ok(provider)
}

/// Replace all newline characters with a special character to make it
/// easier to grep for log messages.
pub fn inlinify(s: &str) -> String {
    s.replace('\n', "\u{200B}")
}

pub fn startup_context_from_env() -> Option<opentelemetry::Context> {
    // Extract OpenTelemetry context for the startup actions from the
    // TRACEPARENT and TRACESTATE env variables, and attach it to the current
    // tracing context.
    //
    // This is used to propagate the context for the 'start_compute' operation
    // from the neon control plane. This allows linking together the wider
    // 'start_compute' operation that creates the compute container, with the
    // startup actions here within the container.
    //
    // There is no standard for passing context in env variables, but a lot of
    // tools use TRACEPARENT/TRACESTATE, so we use that convention too. See
    // https://github.com/open-telemetry/opentelemetry-specification/issues/740
    //
    // Switch to the startup context here, and exit it once the startup has
    // completed and Postgres is up and running.
    //
    // If this pod is pre-created without binding it to any particular endpoint
    // yet, this isn't the right place to enter the startup context. In that
    // case, the control plane should pass the tracing context as part of the
    // /configure API call.
    //
    // NOTE: This is supposed to only cover the *startup* actions. Once
    // postgres is configured and up-and-running, we exit this span. Any other
    // actions that are performed on incoming HTTP requests, for example, are
    // performed in separate spans.
    //
    // XXX: If the pod is restarted, we perform the startup actions in the same
    // context as the original startup actions, which probably doesn't make
    // sense.
    let mut startup_tracing_carrier: HashMap<String, String> = HashMap::new();
    if let Ok(val) = std::env::var("TRACEPARENT") {
        startup_tracing_carrier.insert("traceparent".to_string(), val);
    }
    if let Ok(val) = std::env::var("TRACESTATE") {
        startup_tracing_carrier.insert("tracestate".to_string(), val);
    }
    if !startup_tracing_carrier.is_empty() {
        use opentelemetry::propagation::TextMapPropagator;
        use opentelemetry_sdk::propagation::TraceContextPropagator;
        info!("got startup tracing context from env variables");
        Some(TraceContextPropagator::new().extract(&startup_tracing_carrier))
    } else {
        None
    }
}
