use crate::auth::{Claims, JwtAuth};
use crate::http::error;
use anyhow::{anyhow, Context};
use hyper::header::{HeaderName, AUTHORIZATION};
use hyper::http::HeaderValue;
use hyper::{header::CONTENT_TYPE, Body, Request, Response, Server};
use hyper::{Method, StatusCode};
use metrics::{register_int_counter, Encoder, IntCounter, TextEncoder};
use once_cell::sync::Lazy;
use routerify::ext::RequestExt;
use routerify::RequestInfo;
use routerify::{Middleware, Router, RouterBuilder, RouterService};
use std::borrow::Cow;
use tokio::task::JoinError;
use tracing;

use std::future::Future;
use std::net::TcpListener;
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

async fn logger(res: Response<Body>, info: RequestInfo) -> Result<Response<Body>, ApiError> {
    // cannot factor out the Level to avoid the repetition
    // because tracing can only work with const Level
    // which is not the case here
    let request_id_val = info.context::<String>();

    let request_id = request_id_val.unwrap_or_default();

    if info.method() == Method::GET && res.status() == StatusCode::OK {
        tracing::debug!(
            "{} {} {} {}",
            info.method(),
            info.uri().path(),
            request_id,
            res.status()
        );
    } else {
        tracing::info!(
            "{} {} {} {}",
            info.method(),
            info.uri().path(),
            request_id,
            res.status()
        );
    }
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

pub fn add_request_id_middleware<B: hyper::body::HttpBody + Send + Sync + 'static>(
) -> Middleware<B, ApiError> {
    Middleware::pre(move |req| async move {
        let request_id = match req.headers().get(&X_REQUEST_ID_HEADER) {
            Some(request_id) => {
                Cow::Borrowed(request_id.to_str().expect("extract request id value"))
            }
            None => {
                let request_id = uuid::Uuid::new_v4();
                Cow::Owned(request_id.to_string())
            }
        };

        req.set_context(request_id.to_string());

        if req.method() == Method::GET {
            tracing::debug!("{} {} {}", req.method(), req.uri().path(), request_id);
        } else {
            tracing::info!("{} {} {}", req.method(), req.uri().path(), request_id);
        }
        Ok(req)
    })
}

async fn add_request_id_header_to_response(
    mut res: Response<Body>,
    req_info: RequestInfo,
) -> Result<Response<Body>, ApiError> {
    if let Some(request_id) = req_info.context::<String>() {
        if let Ok(request_header_value) = HeaderValue::from_str(&request_id) {
            res.headers_mut()
                .insert(&X_REQUEST_ID_HEADER, request_header_value);
        };
    };

    Ok(res)
}

pub fn make_router() -> RouterBuilder<hyper::Body, ApiError> {
    Router::builder()
        .middleware(add_request_id_middleware())
        .middleware(Middleware::post_with_info(logger))
        .middleware(Middleware::post_with_info(
            add_request_id_header_to_response,
        ))
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
                    tracing::warn!(
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
    tracing::info!("Starting an HTTP endpoint at {}", listener.local_addr()?);

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
