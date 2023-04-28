use crate::auth::{Claims, JwtAuth};
use crate::http::error;
use anyhow::Context;
use hyper::header::{HeaderName, AUTHORIZATION};
use hyper::http::HeaderValue;
use hyper::Method;
use hyper::{header::CONTENT_TYPE, Body, Request, Response};
use metrics::{register_int_counter, Encoder, IntCounter, TextEncoder};
use once_cell::sync::Lazy;
use routerify::ext::RequestExt;
use routerify::{Middleware, RequestInfo, Router, RouterBuilder};
use tokio::task::JoinError;
use tracing::{self, debug, info, info_span, warn, Instrument};

use std::future::Future;
use std::str::FromStr;

use super::error::ApiError;

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
/// Use this to distinguish between logs of different HTTP requests: every request handler wrapped
/// in this type will get request info logged in the wrapping span, including the unique request ID.
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
pub struct RequestSpan<E, R, H>(pub H)
where
    E: Into<Box<dyn std::error::Error + Send + Sync>> + 'static,
    R: Future<Output = Result<Response<Body>, E>> + Send + 'static,
    H: Fn(Request<Body>) -> R + Send + Sync + 'static;

impl<E, R, H> RequestSpan<E, R, H>
where
    E: Into<Box<dyn std::error::Error + Send + Sync>> + 'static,
    R: Future<Output = Result<Response<Body>, E>> + Send + 'static,
    H: Fn(Request<Body>) -> R + Send + Sync + 'static,
{
    /// Creates a tracing span around inner request handler and executes the request handler in the contex of that span.
    /// Use as `|r| RequestSpan(my_handler).handle(r)` instead of `my_handler` as the request handler to get the span enabled.
    pub async fn handle(self, request: Request<Body>) -> Result<Response<Body>, E> {
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

            // Note that we reuse `error::handler` here and not returning and error at all,
            // yet cannot use `!` directly in the method signature due to `routerify::RouterBuilder` limitation.
            // Usage of the error handler also means that we expect only the `ApiError` errors to be raised in this call.
            //
            // Panics are not handled separately, there's a `tracing_panic_hook` from another module to do that globally.
            let res = (self.0)(request).await;

            cancellation_guard.disarm();

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
                Err(e) => Ok(error::handler(e.into()).await),
            }
        }
        .instrument(request_span)
        .await
    }
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
        if let Some(span) = self.warn.take() {
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
    SERVE_METRICS_COUNT.inc();

    let mut buffer = vec![];
    let encoder = TextEncoder::new();

    let metrics = tokio::task::spawn_blocking(move || {
        // Currently we take a lot of mutexes while collecting metrics, so it's
        // better to spawn a blocking task to avoid blocking the event loop.
        metrics::gather()
    })
    .await
    .map_err(|e: JoinError| ApiError::InternalServerError(e.into()))?;
    encoder.encode(&metrics, &mut buffer).unwrap();

    let response = Response::builder()
        .status(200)
        .header(CONTENT_TYPE, encoder.format_type())
        .body(Body::from(buffer))
        .unwrap();

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
        .get("/metrics", |r| {
            RequestSpan(prometheus_metrics_handler).handle(r)
        })
        .err_handler(error::handler)
}

pub fn attach_openapi_ui(
    router_builder: RouterBuilder<hyper::Body, ApiError>,
    spec: &'static [u8],
    spec_mount_path: &'static str,
    ui_mount_path: &'static str,
) -> RouterBuilder<hyper::Body, ApiError> {
    router_builder
        .get(spec_mount_path, move |r| {
            RequestSpan(move |_| async move { Ok(Response::builder().body(Body::from(spec)).unwrap()) })
                .handle(r)
        })
        .get(ui_mount_path, move |r| RequestSpan( move |_| async move {
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
        }).handle(r))
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
