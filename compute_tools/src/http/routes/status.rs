use std::ops::Deref;
use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::Response;
use compute_api::responses::ComputeStatusResponse;

use crate::compute::ComputeNode;
use crate::http::JsonResponse;

/// Retrieve the state of the comute.
pub(in crate::http) async fn get_status(State(compute): State<Arc<ComputeNode>>) -> Response {
    let state = compute.state.lock().unwrap();
    let body = ComputeStatusResponse::from(state.deref());

    JsonResponse::success(StatusCode::OK, body)
}
