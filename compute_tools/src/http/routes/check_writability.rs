use std::sync::Arc;

use axum::extract::State;
use axum::response::Response;
use compute_api::responses::ComputeStatus;
use http::StatusCode;

use crate::checker::check_writability;
use crate::compute::ComputeNode;
use crate::http::JsonResponse;

/// Check that the compute is currently running.
pub(in crate::http) async fn is_writable(State(compute): State<Arc<ComputeNode>>) -> Response {
    let status = compute.get_status();
    match status {
        // If we are running, or just reloading the config, we are ok to write a new config.
        ComputeStatus::Running | ComputeStatus::Reloading => {}
        _ => return JsonResponse::invalid_status(status),
    }

    match check_writability(&compute).await {
        Ok(_) => JsonResponse::success(StatusCode::OK, true),
        Err(e) => JsonResponse::error(StatusCode::INTERNAL_SERVER_ERROR, e),
    }
}
