use anyhow::{anyhow, bail};
use hyper::{body::to_bytes, header::CONTENT_TYPE, Body, Request, Response, StatusCode};
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

use crate::{auth::backend::local::JWKS_ROLE_MAP, console::messages::JwksRoleMapping, jemalloc};

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
        .post("/v1/jwks", update_jwks)
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
        let _span = span.entered();

        let mut state = state.lock().unwrap();
        let PrometheusHandler { encoder, metrics } = &mut *state;

        metrics
            .collect_group_into(&mut *encoder)
            .unwrap_or_else(|infallible| match infallible {});

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

async fn update_jwks(req: Request<Body>) -> Result<Response<Body>, ApiError> {
    let bytes = to_bytes(req.into_body())
        .await
        .map_err(|e| ApiError::BadRequest(e.into()))?;
    let data: JwksRoleMapping =
        serde_json::from_slice(&bytes).map_err(|e| ApiError::BadRequest(e.into()))?;

    JWKS_ROLE_MAP.store(Some(Arc::new(data)));

    json_response(StatusCode::OK, ())
}
