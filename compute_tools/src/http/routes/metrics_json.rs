use std::sync::Arc;

use axum::extract::State;
use axum::response::Response;
use http::StatusCode;

use crate::compute::ComputeNode;
use crate::http::JsonResponse;

/// Get startup metrics.
pub(in crate::http) async fn get_metrics(State(compute): State<Arc<ComputeNode>>) -> Response {
    let metrics = compute.state.lock().unwrap().metrics.clone();
    JsonResponse::success(StatusCode::OK, metrics)
}
