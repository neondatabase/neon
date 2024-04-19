use anyhow::{anyhow, bail};
use hyper::{header::CONTENT_TYPE, Body, Request, Response, StatusCode};
use measured::{text::BufferedTextEncoder, MetricGroup};
use metrics::NeonMetrics;
use std::{
    convert::Infallible,
    net::TcpListener,
    sync::{Arc, Mutex},
};
use tracing::{info, info_span};
use utils::http::{
    endpoint::{self, request_span},
    error::ApiError,
    json::json_response,
    RouterBuilder, RouterService,
};

use crate::jemalloc;

async fn status_handler(_: Request<Body>) -> Result<Response<Body>, ApiError> {
    json_response(StatusCode::OK, "")
}

fn make_router(metrics: AppMetrics) -> RouterBuilder<hyper::Body, ApiError> {
    let state = Arc::new(Mutex::new(PrometheusHandler {
        encoder: BufferedTextEncoder::new(),
        metrics,
    }));

    endpoint::make_router()
        .get("/metrics", move |r| {
            let state = state.clone();
            request_span(r, move |b| prometheus_metrics_handler(b, state))
        })
        .get("/v1/status", status_handler)
}

pub async fn task_main(
    http_listener: TcpListener,
    metrics: AppMetrics,
) -> anyhow::Result<Infallible> {
    scopeguard::defer! {
        info!("http has shut down");
    }

    let service = || RouterService::new(make_router(metrics).build()?);

    hyper::Server::from_tcp(http_listener)?
        .serve(service().map_err(|e| anyhow!(e))?)
        .await?;

    bail!("hyper server without shutdown handling cannot shutdown successfully");
}

struct PrometheusHandler {
    encoder: BufferedTextEncoder,
    metrics: AppMetrics,
}

#[derive(MetricGroup)]
pub struct AppMetrics {
    #[metric(namespace = "jemalloc")]
    pub jemalloc: Option<jemalloc::MetricRecorder>,
    #[metric(flatten)]
    pub neon_metrics: NeonMetrics,
    #[metric(flatten)]
    pub proxy: &'static crate::metrics::Metrics,
}

async fn prometheus_metrics_handler(
    _req: Request<Body>,
    state: Arc<Mutex<PrometheusHandler>>,
) -> Result<Response<Body>, ApiError> {
    let started_at = std::time::Instant::now();

    let span = info_span!("blocking");
    let body = tokio::task::spawn_blocking(move || {
        // there are situations where we lose scraped metrics under load, try to gather some clues
        // since all nodes are queried this, keep the message count low.
        let spawned_at = std::time::Instant::now();

        let _span = span.entered();

        let mut state = state.lock().unwrap();
        let PrometheusHandler { encoder, metrics } = &mut *state;

        metrics
            .collect_group_into(&mut *encoder)
            .unwrap_or_else(|infallible| match infallible {});

        let encoded_at = std::time::Instant::now();

        let body = encoder.finish();

        let spawned_in = spawned_at - started_at;
        let encoded_in = encoded_at - spawned_at;
        let total = encoded_at - started_at;

        tracing::info!(
            bytes = body.len(),
            total_ms = total.as_millis(),
            spawning_ms = spawned_in.as_millis(),
            encoding_ms = encoded_in.as_millis(),
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
