use anyhow::{anyhow, bail};
use hyper::{header::CONTENT_TYPE, Body, Request, Response, StatusCode};
use measured::{metric::name::WithNamespace, text::TextEncoder, MetricGroup};
use metrics::LibMetrics;
use routerify::{Middleware, Router};
use std::{
    convert::Infallible,
    net::TcpListener,
    sync::{Arc, Mutex},
};
use tracing::{info, info_span};
use utils::http::{
    endpoint::{add_request_id_header_to_response, add_request_id_middleware, request_span},
    error::{route_error_handler, ApiError},
    json::json_response,
    RouterBuilder, RouterService,
};

use crate::jemalloc;

async fn status_handler(_: Request<Body>) -> Result<Response<Body>, ApiError> {
    json_response(StatusCode::OK, "")
}

fn make_router(
    libmetrics: LibMetrics,
    jemalloc: Option<jemalloc::MetricRecorder>,
) -> RouterBuilder<hyper::Body, ApiError> {
    let state = Arc::new(Mutex::new(PrometheusHandler {
        encoder: TextEncoder::new(),
        jemalloc,
        libmetrics,
    }));

    Router::builder()
        .middleware(add_request_id_middleware())
        .middleware(Middleware::post_with_info(
            add_request_id_header_to_response,
        ))
        .get("/metrics", move |r| {
            let state = state.clone();
            request_span(r, move |b| prometheus_metrics_handler(b, state))
        })
        .err_handler(route_error_handler)
        .get("/v1/status", status_handler)
}

pub async fn task_main(
    http_listener: TcpListener,
    libmetrics: LibMetrics,
    jemalloc: Option<jemalloc::MetricRecorder>,
) -> anyhow::Result<Infallible> {
    scopeguard::defer! {
        info!("http has shut down");
    }

    let service = || RouterService::new(make_router(libmetrics, jemalloc).build()?);

    hyper::Server::from_tcp(http_listener)?
        .serve(service().map_err(|e| anyhow!(e))?)
        .await?;

    bail!("hyper server without shutdown handling cannot shutdown successfully");
}

struct PrometheusHandler {
    encoder: TextEncoder,

    jemalloc: Option<jemalloc::MetricRecorder>,
    libmetrics: LibMetrics,
}

async fn prometheus_metrics_handler(
    _req: Request<Body>,
    state: Arc<Mutex<PrometheusHandler>>,
) -> Result<Response<Body>, ApiError> {
    let started_at = std::time::Instant::now();

    let span = info_span!("blocking");
    let body = tokio::task::spawn_blocking(move || {
        let _span = span.entered();

        let mut state = state.lock().unwrap();
        let PrometheusHandler {
            encoder,
            jemalloc,
            libmetrics,
        } = &mut *state;

        WithNamespace::new("libmetrics", &*libmetrics).collect_into(&mut *encoder);
        if let Some(jemalloc) = &jemalloc {
            WithNamespace::new("jemalloc", jemalloc).collect_into(&mut *encoder);
        }
        crate::metrics::Metrics::get().collect_into(&mut *encoder);

        let body = encoder.finish();

        tracing::info!(
            bytes = body.len(),
            elapsed_ms = started_at.elapsed().as_millis(),
            "responded /metrics"
        );

        body
    })
    .await
    .unwrap();

    let response = Response::builder()
        .status(200)
        .header(CONTENT_TYPE, "text/plain; version=0.0.4")
        .body(Body::from(body))
        .unwrap();

    Ok(response)
}
