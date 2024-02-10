use anyhow::{anyhow, bail};
use hyper::{header::CONTENT_TYPE, Body, Request, Response, StatusCode};
use measured::{text::BufferedTextEncoder, MetricGroup};
use std::{
    convert::Infallible,
    net::TcpListener,
    sync::{Mutex, OnceLock},
};
use tracing::{info, info_span};
use utils::http::{
    endpoint::{self, request_span},
    error::ApiError,
    json::json_response,
    RouterBuilder, RouterService,
};

async fn status_handler(_: Request<Body>) -> Result<Response<Body>, ApiError> {
    json_response(StatusCode::OK, "")
}

fn make_router() -> RouterBuilder<hyper::Body, ApiError> {
    endpoint::make_router()
        .get("/metrics", move |r| {
            request_span(r, prometheus_metrics_handler)
        })
        .get("/v1/status", status_handler)
}

pub async fn task_main(http_listener: TcpListener) -> anyhow::Result<Infallible> {
    scopeguard::defer! {
        info!("http has shut down");
    }

    let service = || RouterService::new(make_router().build()?);

    hyper::Server::from_tcp(http_listener)?
        .serve(service().map_err(|e| anyhow!(e))?)
        .await?;

    bail!("hyper server without shutdown handling cannot shutdown successfully");
}

// static SERVE_METRICS_COUNT: Lazy<IntCounter> = Lazy::new(|| {
//     register_int_counter!(
//         "libmetrics_metric_handler_requests_total",
//         "Number of metric requests made"
//     )
//     .expect("failed to define a metric")
// });

async fn prometheus_metrics_handler(_req: Request<Body>) -> Result<Response<Body>, ApiError> {
    // SERVE_METRICS_COUNT.inc();
    static ENCODER: OnceLock<Mutex<BufferedTextEncoder>> =
        OnceLock::<Mutex<BufferedTextEncoder>>::new();

    let started_at = std::time::Instant::now();

    let span = info_span!("blocking");
    let body = tokio::task::spawn_blocking(move || {
        let _span = span.entered();

        let mut enc = ENCODER
            .get_or_init(|| Mutex::new(BufferedTextEncoder::new()))
            .lock()
            .unwrap();
        crate::metrics::Metrics::get()
            .collect_group_into(&mut *enc)
            .unwrap_or_else(|infallible| match infallible {});

        let body = enc.finish();

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
