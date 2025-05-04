use crate::http::JsonResponse;
use axum::response::Response;
use axum::{Json, http::StatusCode};
use compute_api::responses::PrewarmOffloadState;
type Compute = axum::extract::State<std::sync::Arc<crate::compute::ComputeNode>>;

pub(in crate::http) async fn state(compute: Compute) -> Json<crate::compute_prewarm::PrewarmState> {
    Json(compute.prewarm_state().await)
}

// Following functions are marked async for axum, as it's more convenient than wrapping these
// in async lambdas at call site

pub(in crate::http) async fn offload_state(compute: Compute) -> Json<PrewarmOffloadState> {
    Json(compute.prewarm_offload_state())
}

pub(in crate::http) async fn prewarm(compute: Compute) -> Response {
    if compute.prewarm() {
        JsonResponse::success(StatusCode::OK, "")
    } else {
        JsonResponse::error(
            StatusCode::TOO_MANY_REQUESTS,
            "Multiple requests for prewarm are not allowed",
        )
    }
}

pub(in crate::http) async fn offload(compute: Compute) -> Response {
    if compute.prewarm_offload() {
        JsonResponse::success(StatusCode::OK, "")
    } else {
        JsonResponse::error(
            StatusCode::TOO_MANY_REQUESTS,
            "Multiple requests for prewarm offload are not allowed",
        )
    }
}
