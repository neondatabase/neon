use axum::response::Response;
use compute_api::responses::PromoteState;
use http::StatusCode;

use crate::http::JsonResponse;
type Compute = axum::extract::State<std::sync::Arc<crate::compute::ComputeNode>>;

/// Returns only when promote failes or succeeds.
/// If a network error occurs, this does not stop promotion, and subsequent
/// calls block until first error or success
pub(in crate::http) async fn promote(compute: Compute) -> Response {
    let state = compute.promote().await;
    if let PromoteState::Failed { error } = state {
        return JsonResponse::error(StatusCode::INTERNAL_SERVER_ERROR, error);
    }
    JsonResponse::success(StatusCode::OK, state)
}
