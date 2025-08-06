use std::path::Path;
use std::sync::Arc;

use anyhow::Context;
use axum::body::Body;
use axum::extract::State;
use axum::response::Response;
use http::header::CONTENT_TYPE;
use http_body_util::BodyExt;
use hyper::{Request, StatusCode};
use metrics::proto::MetricFamily;
use metrics::{Encoder, TextEncoder};

use crate::communicator_socket_client::connect_communicator_socket;
use crate::compute::ComputeNode;
use crate::hadron_metrics;
use crate::http::JsonResponse;
use crate::metrics::collect;

/// Expose Prometheus metrics.
pub(in crate::http) async fn get_metrics() -> Response {
    // When we call TextEncoder::encode() below, it will immediately return an
    // error if a metric family has no metrics, so we need to preemptively
    // filter out metric families with no metrics.
    let mut metrics = collect()
        .into_iter()
        .filter(|m| !m.get_metric().is_empty())
        .collect::<Vec<MetricFamily>>();

    // Add Hadron metrics.
    let hadron_metrics: Vec<MetricFamily> = hadron_metrics::collect()
        .into_iter()
        .filter(|m| !m.get_metric().is_empty())
        .collect();
    metrics.extend(hadron_metrics);

    let encoder = TextEncoder::new();
    let mut buffer = vec![];

    if let Err(e) = encoder.encode(&metrics, &mut buffer) {
        return JsonResponse::error(StatusCode::INTERNAL_SERVER_ERROR, e);
    }

    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, encoder.format_type())
        .body(Body::from(buffer))
        .unwrap()
}

/// Fetch and forward metrics from the Postgres neon extension's metrics
/// exporter that are used by autoscaling-agent.
///
/// The neon extension exposes these metrics over a Unix domain socket
/// in the data directory. That's not accessible directly from the outside
/// world, so we have this endpoint in compute_ctl to expose it
pub(in crate::http) async fn get_autoscaling_metrics(
    State(compute): State<Arc<ComputeNode>>,
) -> Result<Response, Response> {
    let pgdata = Path::new(&compute.params.pgdata);

    // Connect to the communicator process's metrics socket
    let mut metrics_client = connect_communicator_socket(pgdata)
        .await
        .map_err(|e| JsonResponse::error(StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}")))?;

    // Make a request for /autoscaling_metrics
    let request = Request::builder()
        .method("GET")
        .uri("/autoscaling_metrics")
        .header("Host", "localhost") // hyper requires Host, even though the server won't care
        .body(Body::from(""))
        .unwrap();
    let resp = metrics_client
        .send_request(request)
        .await
        .context("fetching metrics from Postgres metrics service")
        .map_err(|e| JsonResponse::error(StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}")))?;

    // Build a response that just forwards the response we got.
    let mut response = Response::builder();
    response = response.status(resp.status());
    if let Some(content_type) = resp.headers().get(CONTENT_TYPE) {
        response = response.header(CONTENT_TYPE, content_type);
    }
    let body = tonic::service::AxumBody::from_stream(resp.into_body().into_data_stream());
    Ok(response.body(body).unwrap())
}
