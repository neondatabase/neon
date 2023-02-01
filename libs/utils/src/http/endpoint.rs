use crate::auth::{Claims, JwtAuth};
use crate::http::error;
use anyhow::anyhow;
use hyper::header::AUTHORIZATION;
use hyper::{header::CONTENT_TYPE, Body, Request, Response, Server};
use metrics::{register_int_counter, Encoder, IntCounter, TextEncoder};
use once_cell::sync::Lazy;
use routerify::ext::RequestExt;
use routerify::RequestInfo;
use routerify::{Middleware, Router, RouterBuilder, RouterService};
use tokio::task::JoinError;
use tracing::info;

use std::future::Future;
use std::net::TcpListener;

use super::error::ApiError;

static SERVE_METRICS_COUNT: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "libmetrics_metric_handler_requests_total",
        "Number of metric requests made"
    )
    .expect("failed to define a metric")
});

async fn logger(res: Response<Body>, info: RequestInfo) -> Result<Response<Body>, ApiError> {
    info!("{} {} {}", info.method(), info.uri().path(), res.status(),);
    Ok(res)
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

pub fn make_router() -> RouterBuilder<hyper::Body, ApiError> {
    Router::builder()
        .middleware(Middleware::post_with_info(logger))
        .get("/metrics", prometheus_metrics_handler)
        .err_handler(error::handler)
}

pub fn attach_openapi_ui(
    router_builder: RouterBuilder<hyper::Body, ApiError>,
    spec: &'static [u8],
    spec_mount_path: &'static str,
    ui_mount_path: &'static str,
) -> RouterBuilder<hyper::Body, ApiError> {
    router_builder.get(spec_mount_path, move |_| async move {
        Ok(Response::builder().body(Body::from(spec)).unwrap())
    }).get(ui_mount_path, move |_| async move {
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

///
/// Start listening for HTTP requests on given socket.
///
/// 'shutdown_future' can be used to stop. If the Future becomes
/// ready, we stop listening for new requests, and the function returns.
///
pub fn serve_thread_main<S>(
    router_builder: RouterBuilder<hyper::Body, ApiError>,
    listener: TcpListener,
    shutdown_future: S,
) -> anyhow::Result<()>
where
    S: Future<Output = ()> + Send + Sync,
{
    info!("Starting an HTTP endpoint at {}", listener.local_addr()?);

    // Create a Service from the router above to handle incoming requests.
    let service = RouterService::new(router_builder.build().map_err(|err| anyhow!(err))?).unwrap();

    // Enter a single-threaded tokio runtime bound to the current thread
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    let _guard = runtime.enter();

    let server = Server::from_tcp(listener)?
        .serve(service)
        .with_graceful_shutdown(shutdown_future);

    runtime.block_on(server)?;

    Ok(())
}
