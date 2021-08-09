use std::sync::Arc;

use crate::auth::{self, Claims, JwtAuth};
use crate::http::error;
use crate::zid::ZTenantId;
use anyhow::anyhow;
use hyper::header::AUTHORIZATION;
use hyper::{header::CONTENT_TYPE, Body, Request, Response, Server};
use lazy_static::lazy_static;
use routerify::ext::RequestExt;
use routerify::RequestInfo;
use routerify::{Middleware, Router, RouterBuilder, RouterService};
use zenith_metrics::{register_int_counter, IntCounter};
use zenith_metrics::{Encoder, TextEncoder};

use super::error::ApiError;

lazy_static! {
    static ref SERVE_METRICS_COUNT: IntCounter = register_int_counter!(
        "pageserver_serve_metrics_count",
        "Number of metric requests made"
    )
    .expect("failed to define a metric");
}

async fn logger(res: Response<Body>, info: RequestInfo) -> Result<Response<Body>, ApiError> {
    log::info!("{} {} {}", info.method(), info.uri().path(), res.status(),);
    Ok(res)
}

async fn prometheus_metrics_handler(_req: Request<Body>) -> Result<Response<Body>, ApiError> {
    SERVE_METRICS_COUNT.inc();

    let mut buffer = vec![];
    let encoder = TextEncoder::new();
    let metrics = zenith_metrics::gather();
    encoder.encode(&metrics, &mut buffer).unwrap();

    let response = Response::builder()
        .status(200)
        .header(CONTENT_TYPE, encoder.format_type())
        .body(Body::from(buffer))
        .unwrap();

    Ok(response)
}

pub fn get_router() -> RouterBuilder<hyper::Body, ApiError> {
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

pub trait AuthProvider {
    fn provide_auth(&self, req: &Request<Body>) -> Arc<Option<JwtAuth>>;
}

fn parse_token(header_value: &str) -> Result<&str, ApiError> {
    // header must be in form Bearer <token>
    let (prefix, token) = header_value.split_once(' ').ok_or(ApiError::Unauthorized(
        "malformed authorization header".to_string(),
    ))?;
    if prefix != "Bearer" {
        Err(ApiError::Unauthorized(
            "malformed authorization header".to_string(),
        ))?
    }
    Ok(token)
}

pub async fn auth_middleware<S: AuthProvider + Send + Sync + 'static>(
    req: Request<Body>,
) -> Result<Request<Body>, ApiError> {
    // unwrap is ok because this is called in auth middleware
    // which should be enabled only when auth is some
    let state_auth = req
        .data::<Arc<S>>()
        .expect("state is always in request data")
        .provide_auth(&req);

    if let Some(auth) = state_auth.as_ref().as_ref() {
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
            None => Err(ApiError::Unauthorized(
                "missing authorization header".to_string(),
            ))?,
        }
    }
    Ok(req)
}

pub fn check_permission(req: &Request<Body>, tenantid: Option<ZTenantId>) -> Result<(), ApiError> {
    match req.context::<Claims>() {
        Some(claims) => Ok(auth::check_permission(&claims, tenantid)
            .map_err(|err| ApiError::Forbidden(err.to_string()))?),
        None => Ok(()), // claims is None because auth is disabled
    }
}

pub fn serve_thread_main(
    router_builder: RouterBuilder<hyper::Body, ApiError>,
    addr: String,
) -> anyhow::Result<()> {
    let addr = addr.parse()?;
    log::info!("Starting a http endoint at {}", addr);

    // Create a Service from the router above to handle incoming requests.
    let service = RouterService::new(router_builder.build().map_err(|err| anyhow!(err))?).unwrap();

    // Enter a single-threaded tokio runtime bound to the current thread
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    let _guard = runtime.enter();

    let server = Server::bind(&addr).serve(service);

    runtime.block_on(server)?;

    Ok(())
}
