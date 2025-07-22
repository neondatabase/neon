//! Helper functions to set up OpenTelemetry tracing.
//!
//! Example:
//!
//! ```rust,no_run
//! use tracing_subscriber::prelude::*;
//!
//! #[tokio::main]
//! async fn main() {
//!     // Set up logging to stderr
//!     let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
//!         .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
//!     let fmt_layer = tracing_subscriber::fmt::layer()
//!         .with_target(false)
//!         .with_writer(std::io::stderr);
//!
//!     // Initialize OpenTelemetry. Exports tracing spans as OpenTelemetry traces
//!     let provider = tracing_utils::init_tracing("my_application", tracing_utils::ExportConfig::default());
//!     let otlp_layer = provider.as_ref().map(tracing_utils::layer);
//!
//!     // Put it all together
//!     tracing_subscriber::registry()
//!         .with(env_filter)
//!         .with(otlp_layer)
//!         .with(fmt_layer)
//!         .init();
//! }
//! ```
#![deny(clippy::undocumented_unsafe_blocks)]

pub mod http;
pub mod perf_span;

use opentelemetry::trace::TracerProvider;
use opentelemetry_otlp::WithExportConfig;
pub use opentelemetry_otlp::{ExportConfig, Protocol};
use opentelemetry_sdk::trace::SdkTracerProvider;
use tracing::level_filters::LevelFilter;
use tracing::{Dispatch, Subscriber};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::registry::LookupSpan;

pub type Provider = SdkTracerProvider;

/// Set up OpenTelemetry exporter, using configuration from environment variables.
///
/// `service_name` is set as the OpenTelemetry 'service.name' resource (see
/// <https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/resource/semantic_conventions/README.md#service>)
///
/// We try to follow the conventions for the environment variables specified in
/// <https://opentelemetry.io/docs/reference/specification/sdk-environment-variables/>
///
/// However, we only support a subset of those options:
///
/// - OTEL_SDK_DISABLED is supported. The default is "false", meaning tracing
///   is enabled by default. Set it to "true" to disable.
///
/// - We use the OTLP exporter, with HTTP protocol. Most of the OTEL_EXPORTER_OTLP_*
///   settings specified in
///   <https://opentelemetry.io/docs/reference/specification/protocol/exporter/>
///   are supported, as they are handled by the `opentelemetry-otlp` crate.
///   Settings related to other exporters have no effect.
///
/// - Some other settings are supported by the `opentelemetry` crate.
///
/// If you need some other setting, please test if it works first. And perhaps
/// add a comment in the list above to save the effort of testing for the next
/// person.
pub fn init_tracing(service_name: &str, export_config: ExportConfig) -> Option<Provider> {
    if std::env::var("OTEL_SDK_DISABLED") == Ok("true".to_string()) {
        return None;
    };
    Some(init_tracing_internal(
        service_name.to_string(),
        export_config,
    ))
}

pub fn layer<S>(p: &Provider) -> impl Layer<S>
where
    S: Subscriber + for<'span> LookupSpan<'span>,
{
    tracing_opentelemetry::layer().with_tracer(p.tracer("global"))
}

fn init_tracing_internal(service_name: String, export_config: ExportConfig) -> Provider {
    // Sets up exporter from the provided [`ExportConfig`] parameter.
    // If the endpoint is not specified, it is loaded from the
    // OTEL_EXPORTER_OTLP_ENDPOINT environment variable.
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .with_export_config(export_config)
        .build()
        .expect("could not initialize opentelemetry exporter");

    // TODO: opentelemetry::global::set_error_handler() with custom handler that
    //       bypasses default tracing layers, but logs regular looking log
    //       messages.

    // Propagate trace information in the standard W3C TraceContext format.
    opentelemetry::global::set_text_map_propagator(
        opentelemetry_sdk::propagation::TraceContextPropagator::new(),
    );

    Provider::builder()
        .with_span_processor(
            opentelemetry_sdk::trace::span_processor_with_async_runtime::BatchSpanProcessor::builder(
                exporter,
                opentelemetry_sdk::runtime::Tokio,
            )
            .build(),
        )
        .with_resource(
            opentelemetry_sdk::Resource::builder()
                .with_service_name(service_name)
                .build(),
        )
        .build()
}

pub enum OtelEnablement {
    Disabled,
    Enabled {
        service_name: String,
        export_config: ExportConfig,
    },
}

pub struct OtelGuard {
    provider: Provider,
    pub dispatch: Dispatch,
}

impl Drop for OtelGuard {
    fn drop(&mut self) {
        _ = self.provider.shutdown();
    }
}

/// Initializes OTEL infrastructure for performance tracing according to the provided configuration
///
/// Performance tracing is handled by a different [`tracing::Subscriber`]. This functions returns
/// an [`OtelGuard`] containing a [`tracing::Dispatch`] associated with a newly created subscriber.
/// Applications should use this dispatch for their performance traces.
///
/// The lifetime of the guard should match taht of the application. On drop, it tears down the
/// OTEL infra.
pub fn init_performance_tracing(otel_enablement: OtelEnablement) -> Option<OtelGuard> {
    match otel_enablement {
        OtelEnablement::Disabled => None,
        OtelEnablement::Enabled {
            service_name,
            export_config,
        } => {
            let provider = init_tracing(&service_name, export_config)?;

            let otel_layer = layer(&provider).with_filter(LevelFilter::INFO);
            let otel_subscriber = tracing_subscriber::registry().with(otel_layer);
            let dispatch = Dispatch::new(otel_subscriber);

            Some(OtelGuard { dispatch, provider })
        }
    }
}
