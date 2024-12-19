use std::sync::Arc;

use axum::{extract::State, http::StatusCode, response::Response};

use crate::{compute::ComputeNode, http::JsonResponse};

pub(in crate::http) async fn get_metrics(State(compute): State<Arc<ComputeNode>>) -> Response {
    let metrics = compute.state.lock().unwrap().metrics.clone();
    JsonResponse::success(StatusCode::OK, metrics)
}
