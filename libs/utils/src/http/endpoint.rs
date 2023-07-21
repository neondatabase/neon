use crate::auth::{Claims, JwtAuth};
use crate::http::error::{api_error_handler, route_error_handler, ApiError};
use anyhow::Context;
use hyper::header::{HeaderName, AUTHORIZATION};
use hyper::http::HeaderValue;
use hyper::Method;
use hyper::{header::CONTENT_TYPE, Body, Request, Response};
use metrics::{register_int_counter, Encoder, IntCounter, TextEncoder};
use once_cell::sync::Lazy;
use routerify::ext::RequestExt;
use routerify::{Middleware, RequestInfo, Router, RouterBuilder};
use tracing::{self, debug, info, info_span, warn, Instrument};

use std::future::Future;
use std::str::FromStr;

static SERVE_METRICS_COUNT: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "libmetrics_metric_handler_requests_total",
        "Number of metric requests made"
    )
    .expect("failed to define a metric")
});

static X_REQUEST_ID_HEADER_STR: &str = "x-request-id";

static X_REQUEST_ID_HEADER: HeaderName = HeaderName::from_static(X_REQUEST_ID_HEADER_STR);
#[derive(Debug, Default, Clone)]
struct RequestId(String);

/// Adds a tracing info_span! instrumentation around the handler events,
/// logs the request start and end events for non-GET requests and non-200 responses.
///
/// Usage: Replace `my_handler` with `|r| request_span(r, my_handler)`
///
/// Use this to distinguish between logs of different HTTP requests: every request handler wrapped
/// with this will get request info logged in the wrapping span, including the unique request ID.
///
/// This also handles errors, logging them and converting them to an HTTP error response.
///
/// NB: If the client disconnects, Hyper will drop the Future, without polling it to
/// completion. In other words, the handler must be async cancellation safe! request_span
/// prints a warning to the log when that happens, so that you have some trace of it in
/// the log.
///
///
/// There could be other ways to implement similar functionality:
///
/// * procmacros placed on top of all handler methods
/// With all the drawbacks of procmacros, brings no difference implementation-wise,
/// and little code reduction compared to the existing approach.
///
/// * Another `TraitExt` with e.g. the `get_with_span`, `post_with_span` methods to do similar logic,
/// implemented for [`RouterBuilder`].
/// Could be simpler, but we don't want to depend on [`routerify`] more, targeting to use other library later.
///
/// * In theory, a span guard could've been created in a pre-request middleware and placed into a global collection, to be dropped
/// later, in a post-response middleware.
/// Due to suspendable nature of the futures, would give contradictive results which is exactly the opposite of what `tracing-futures`
/// tries to achive with its `.instrument` used in the current approach.
///
/// If needed, a declarative macro to substitute the |r| ... closure boilerplate could be introduced.
pub async fn request_span<R, H>(request: Request<Body>, handler: H) -> R::Output
where
    R: Future<Output = Result<Response<Body>, ApiError>> + Send + 'static,
    H: FnOnce(Request<Body>) -> R + Send + Sync + 'static,
{
    let request_id = request.context::<RequestId>().unwrap_or_default().0;
    let method = request.method();
    let path = request.uri().path();
    let request_span = info_span!("request", %method, %path, %request_id);

    let log_quietly = method == Method::GET;
    async move {
        let cancellation_guard = RequestCancelled::warn_when_dropped_without_responding();
        if log_quietly {
            debug!("Handling request");
        } else {
            info!("Handling request");
        }

        // No special handling for panics here. There's a `tracing_panic_hook` from another
        // module to do that globally.
        let res = handler(request).await;

        cancellation_guard.disarm();

        // Log the result if needed.
        //
        // We also convert any errors into an Ok response with HTTP error code here.
        // `make_router` sets a last-resort error handler that would do the same, but
        // we prefer to do it here, before we exit the request span, so that the error
        // is still logged with the span.
        //
        // (Because we convert errors to Ok response, we never actually return an error,
        // and we could declare the function to return the never type (`!`). However,
        // using `routerify::RouterBuilder` requires a proper error type.)
        match res {
            Ok(response) => {
                let response_status = response.status();
                if log_quietly && response_status.is_success() {
                    debug!("Request handled, status: {response_status}");
                } else {
                    info!("Request handled, status: {response_status}");
                }
                Ok(response)
            }
            Err(err) => Ok(api_error_handler(err)),
        }
    }
    .instrument(request_span)
    .await
}

/// Drop guard to WARN in case the request was dropped before completion.
struct RequestCancelled {
    warn: Option<tracing::Span>,
}

impl RequestCancelled {
    /// Create the drop guard using the [`tracing::Span::current`] as the span.
    fn warn_when_dropped_without_responding() -> Self {
        RequestCancelled {
            warn: Some(tracing::Span::current()),
        }
    }

    /// Consume the drop guard without logging anything.
    fn disarm(mut self) {
        self.warn = None;
    }
}

impl Drop for RequestCancelled {
    fn drop(&mut self) {
        if std::thread::panicking() {
            // we are unwinding due to panicking, assume we are not dropped for cancellation
        } else if let Some(span) = self.warn.take() {
            // the span has all of the info already, but the outer `.instrument(span)` has already
            // been dropped, so we need to manually re-enter it for this message.
            //
            // this is what the instrument would do before polling so it is fine.
            let _g = span.entered();
            warn!("request was dropped before completing");
        }
    }
}

async fn prometheus_metrics_handler(_req: Request<Body>) -> Result<Response<Body>, ApiError> {
    use bytes::{Bytes, BytesMut};
    use std::io::Write as _;
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::ReceiverStream;

    SERVE_METRICS_COUNT.inc();

    /// An [`std::io::Write`] implementation on top of a channel sending [`bytes::Bytes`] chunks.
    struct ChannelWriter {
        buffer: BytesMut,
        tx: mpsc::Sender<std::io::Result<Bytes>>,
        written: usize,
    }

    impl ChannelWriter {
        fn new(buf_len: usize, tx: mpsc::Sender<std::io::Result<Bytes>>) -> Self {
            assert_ne!(buf_len, 0);
            ChannelWriter {
                // split about half off the buffer from the start, because we flush depending on
                // capacity. first flush will come sooner than without this, but now resizes will
                // have better chance of picking up the "other" half. not guaranteed of course.
                buffer: BytesMut::with_capacity(buf_len).split_off(buf_len / 2),
                tx,
                written: 0,
            }
        }

        fn flush0(&mut self) -> std::io::Result<usize> {
            let n = self.buffer.len();
            if n == 0 {
                return Ok(0);
            }

            tracing::trace!(n, "flushing");
            let ready = self.buffer.split().freeze();

            // not ideal to call from blocking code to block_on, but we are sure that this
            // operation does not spawn_blocking other tasks
            let res: Result<(), ()> = tokio::runtime::Handle::current().block_on(async {
                self.tx.send(Ok(ready)).await.map_err(|_| ())?;

                // throttle sending to allow reuse of our buffer in `write`.
                self.tx.reserve().await.map_err(|_| ())?;

                // now the response task has picked up the buffer and hopefully started
                // sending it to the client.
                Ok(())
            });
            if res.is_err() {
                return Err(std::io::ErrorKind::BrokenPipe.into());
            }
            self.written += n;
            Ok(n)
        }

        fn flushed_bytes(&self) -> usize {
            self.written
        }
    }

    impl std::io::Write for ChannelWriter {
        fn write(&mut self, mut buf: &[u8]) -> std::io::Result<usize> {
            let remaining = self.buffer.capacity() - self.buffer.len();

            let out_of_space = remaining < buf.len();

            let original_len = buf.len();

            if out_of_space {
                let can_still_fit = buf.len() - remaining;
                self.buffer.extend_from_slice(&buf[..can_still_fit]);
                buf = &buf[can_still_fit..];
                self.flush0()?;
            }

            // assume that this will often under normal operation just move the pointer back to the
            // beginning of allocation, because previous split off parts are already sent and
            // dropped.
            self.buffer.extend_from_slice(buf);
            Ok(original_len)
        }

        fn flush(&mut self) -> std::io::Result<()> {
            self.flush0().map(|_| ())
        }
    }

    let started_at = std::time::Instant::now();

    let (tx, rx) = mpsc::channel(1);

    let body = Body::wrap_stream(ReceiverStream::new(rx));

    let mut writer = ChannelWriter::new(128 * 1024, tx);

    let encoder = TextEncoder::new();

    let response = Response::builder()
        .status(200)
        .header(CONTENT_TYPE, encoder.format_type())
        .body(body)
        .unwrap();

    let span = info_span!("blocking");
    tokio::task::spawn_blocking(move || {
        let _span = span.entered();
        let metrics = metrics::gather();
        let res = encoder
            .encode(&metrics, &mut writer)
            .and_then(|_| writer.flush().map_err(|e| e.into()));

        match res {
            Ok(()) => {
                tracing::info!(
                    bytes = writer.flushed_bytes(),
                    elapsed_ms = started_at.elapsed().as_millis(),
                    "responded /metrics"
                );
            }
            Err(e) => {
                tracing::warn!("failed to write out /metrics response: {e:#}");
                // semantics of this error are quite... unclear. we want to error the stream out to
                // abort the response to somehow notify the client that we failed.
                //
                // though, most likely the reason for failure is that the receiver is already gone.
                drop(
                    writer
                        .tx
                        .blocking_send(Err(std::io::ErrorKind::BrokenPipe.into())),
                );
            }
        }
    });

    Ok(response)
}

pub fn add_request_id_middleware<B: hyper::body::HttpBody + Send + Sync + 'static>(
) -> Middleware<B, ApiError> {
    Middleware::pre(move |req| async move {
        let request_id = match req.headers().get(&X_REQUEST_ID_HEADER) {
            Some(request_id) => request_id
                .to_str()
                .expect("extract request id value")
                .to_owned(),
            None => {
                let request_id = uuid::Uuid::new_v4();
                request_id.to_string()
            }
        };
        req.set_context(RequestId(request_id));

        Ok(req)
    })
}

async fn add_request_id_header_to_response(
    mut res: Response<Body>,
    req_info: RequestInfo,
) -> Result<Response<Body>, ApiError> {
    if let Some(request_id) = req_info.context::<RequestId>() {
        if let Ok(request_header_value) = HeaderValue::from_str(&request_id.0) {
            res.headers_mut()
                .insert(&X_REQUEST_ID_HEADER, request_header_value);
        };
    };

    Ok(res)
}

pub fn make_router() -> RouterBuilder<hyper::Body, ApiError> {
    Router::builder()
        .middleware(add_request_id_middleware())
        .middleware(Middleware::post_with_info(
            add_request_id_header_to_response,
        ))
        .get("/metrics", |r| request_span(r, prometheus_metrics_handler))
        .err_handler(route_error_handler)
}

pub fn attach_openapi_ui(
    router_builder: RouterBuilder<hyper::Body, ApiError>,
    spec: &'static [u8],
    spec_mount_path: &'static str,
    ui_mount_path: &'static str,
) -> RouterBuilder<hyper::Body, ApiError> {
    router_builder
        .get(spec_mount_path,
            move |r| request_span(r, move |_| async move {
                Ok(Response::builder().body(Body::from(spec)).unwrap())
            })
        )
        .get(ui_mount_path,
             move |r| request_span(r, move |_| async move {
                 Ok(Response::builder().body(Body::from(format!(r#"
                <!DOCTYPE html>
                <html lang="en">
                <head>
                <title>rweb</title>
                <link href="https://cdn.jsdelivr.net/npm/swagger-ui-dist@3/swagger-ui.css" rel="stylesheet">
                </head>
                <body>
                    <div id="swagger-ui"></div>
                    <script src="https://cdn.jsdelivr.net/npm/swagger-ui-dist@3/swagger-ui-bundle.js" charset="UTF-8"> </script>
                    <script>
                        window.onload = function() {{
                        const ui = SwaggerUIBundle({{
                            "dom_id": "\#swagger-ui",
                            presets: [
                            SwaggerUIBundle.presets.apis,
                            SwaggerUIBundle.SwaggerUIStandalonePreset
                            ],
                            layout: "BaseLayout",
                            deepLinking: true,
                            showExtensions: true,
                            showCommonExtensions: true,
                            url: "{}",
                        }})
                        window.ui = ui;
                    }};
                </script>
                </body>
                </html>
            "#, spec_mount_path))).unwrap())
             })
        )
}

fn parse_token(header_value: &str) -> Result<&str, ApiError> {
    // header must be in form Bearer <token>
    let (prefix, token) = header_value
        .split_once(' ')
        .ok_or_else(|| ApiError::Unauthorized("malformed authorization header".to_string()))?;
    if prefix != "Bearer" {
        return Err(ApiError::Unauthorized(
            "malformed authorization header".to_string(),
        ));
    }
    Ok(token)
}

pub fn auth_middleware<B: hyper::body::HttpBody + Send + Sync + 'static>(
    provide_auth: fn(&Request<Body>) -> Option<&JwtAuth>,
) -> Middleware<B, ApiError> {
    Middleware::pre(move |req| async move {
        if let Some(auth) = provide_auth(&req) {
            match req.headers().get(AUTHORIZATION) {
                Some(value) => {
                    let header_value = value.to_str().map_err(|_| {
                        ApiError::Unauthorized("malformed authorization header".to_string())
                    })?;
                    let token = parse_token(header_value)?;

                    let data = auth
                        .decode(token)
                        .map_err(|_| ApiError::Unauthorized("malformed jwt token".to_string()))?;
                    req.set_context(data.claims);
                }
                None => {
                    return Err(ApiError::Unauthorized(
                        "missing authorization header".to_string(),
                    ))
                }
            }
        }
        Ok(req)
    })
}

pub fn add_response_header_middleware<B>(
    header: &str,
    value: &str,
) -> anyhow::Result<Middleware<B, ApiError>>
where
    B: hyper::body::HttpBody + Send + Sync + 'static,
{
    let name =
        HeaderName::from_str(header).with_context(|| format!("invalid header name: {header}"))?;
    let value =
        HeaderValue::from_str(value).with_context(|| format!("invalid header value: {value}"))?;
    Ok(Middleware::post_with_info(
        move |mut response, request_info| {
            let name = name.clone();
            let value = value.clone();
            async move {
                let headers = response.headers_mut();
                if headers.contains_key(&name) {
                    warn!(
                        "{} response already contains header {:?}",
                        request_info.uri(),
                        &name,
                    );
                } else {
                    headers.insert(name, value);
                }
                Ok(response)
            }
        },
    ))
}

pub fn check_permission_with(
    req: &Request<Body>,
    check_permission: impl Fn(&Claims) -> Result<(), anyhow::Error>,
) -> Result<(), ApiError> {
    match req.context::<Claims>() {
        Some(claims) => {
            Ok(check_permission(&claims).map_err(|err| ApiError::Forbidden(err.to_string()))?)
        }
        None => Ok(()), // claims is None because auth is disabled
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::future::poll_fn;
    use hyper::service::Service;
    use routerify::RequestServiceBuilder;
    use std::net::{IpAddr, SocketAddr};

    #[tokio::test]
    async fn test_request_id_returned() {
        let builder = RequestServiceBuilder::new(make_router().build().unwrap()).unwrap();
        let remote_addr = SocketAddr::new(IpAddr::from_str("127.0.0.1").unwrap(), 80);
        let mut service = builder.build(remote_addr);
        if let Err(e) = poll_fn(|ctx| service.poll_ready(ctx)).await {
            panic!("request service is not ready: {:?}", e);
        }

        let mut req: Request<Body> = Request::default();
        req.headers_mut()
            .append(&X_REQUEST_ID_HEADER, HeaderValue::from_str("42").unwrap());

        let resp: Response<hyper::body::Body> = service.call(req).await.unwrap();

        let header_val = resp.headers().get(&X_REQUEST_ID_HEADER).unwrap();

        assert!(header_val == "42", "response header mismatch");
    }

    #[tokio::test]
    async fn test_request_id_empty() {
        let builder = RequestServiceBuilder::new(make_router().build().unwrap()).unwrap();
        let remote_addr = SocketAddr::new(IpAddr::from_str("127.0.0.1").unwrap(), 80);
        let mut service = builder.build(remote_addr);
        if let Err(e) = poll_fn(|ctx| service.poll_ready(ctx)).await {
            panic!("request service is not ready: {:?}", e);
        }

        let req: Request<Body> = Request::default();
        let resp: Response<hyper::body::Body> = service.call(req).await.unwrap();

        let header_val = resp.headers().get(&X_REQUEST_ID_HEADER);

        assert_ne!(header_val, None, "response header should NOT be empty");
    }
}
