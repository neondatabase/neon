//! Tracing wrapper for Hyper HTTP server

use hyper0::HeaderMap;
use hyper0::{Body, Request, Response};
use std::future::Future;
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// Configuration option for what to use as the "otel.name" field in the traces.
pub enum OtelName<'a> {
    /// Use a constant string
    Constant(&'a str),

    /// Use the path from the request.
    ///
    /// That's very useful information, but is not appropriate if the
    /// path contains parameters that differ on ever request, or worse,
    /// sensitive information like usernames or email addresses.
    ///
    /// See <https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/semantic_conventions/http.md#name>
    UriPath,
}

/// Handle an incoming HTTP request using the given handler function,
/// with OpenTelemetry tracing.
///
/// This runs 'handler' on the request in a new span, with fields filled in
/// from the request. Notably, if the request contains tracing information,
/// it is propagated to the span, so that this request is traced as part of
/// the same trace.
///
/// XXX: Usually, this is handled by existing libraries, or built
/// directly into HTTP servers. However, I couldn't find one for Hyper,
/// so I had to write our own. OpenTelemetry website has a registry of
/// instrumentation libraries at:
/// <https://opentelemetry.io/registry/?language=rust&component=instrumentation>
/// If a Hyper crate appears, consider switching to that.
pub async fn tracing_handler<F, R>(
    req: Request<Body>,
    handler: F,
    otel_name: OtelName<'_>,
) -> Response<Body>
where
    F: Fn(Request<Body>) -> R,
    R: Future<Output = Response<Body>>,
{
    // Create a tracing span, with context propagated from the incoming
    // request if any.
    //
    // See list of standard fields defined for HTTP requests at
    // https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/semantic_conventions/http.md
    // We only fill in a few of the most useful ones here.
    let otel_name = match otel_name {
        OtelName::Constant(s) => s,
        OtelName::UriPath => req.uri().path(),
    };

    let span = tracing::info_span!(
        "http request",
        otel.name= %otel_name,
        http.method = %req.method(),
        http.status_code = tracing::field::Empty,
    );
    let parent_ctx = extract_remote_context(req.headers());
    span.set_parent(parent_ctx);

    // Handle the request within the span
    let response = handler(req).instrument(span.clone()).await;

    // Fill in the fields from the response code
    let status = response.status();
    span.record("http.status_code", status.as_str());
    span.record(
        "otel.status_code",
        if status.is_success() { "OK" } else { "ERROR" },
    );

    response
}

// Extract remote tracing context from the HTTP headers
fn extract_remote_context(headers: &HeaderMap) -> opentelemetry::Context {
    struct HeaderExtractor<'a>(&'a HeaderMap);

    impl opentelemetry::propagation::Extractor for HeaderExtractor<'_> {
        fn get(&self, key: &str) -> Option<&str> {
            self.0.get(key).and_then(|value| value.to_str().ok())
        }

        fn keys(&self) -> Vec<&str> {
            self.0.keys().map(|value| value.as_str()).collect()
        }
    }
    let extractor = HeaderExtractor(headers);
    opentelemetry::global::get_text_map_propagator(|propagator| propagator.extract(&extractor))
}
