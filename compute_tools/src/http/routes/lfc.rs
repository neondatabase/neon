use crate::compute_prewarm::LfcPrewarmStateWithProgress;
use crate::http::JsonResponse;
use axum::response::{IntoResponse, Response};
use axum::{Json, http::StatusCode};
use compute_api::responses::LfcOffloadState;
type Compute = axum::extract::State<std::sync::Arc<crate::compute::ComputeNode>>;

pub(in crate::http) async fn prewarm_state(compute: Compute) -> Json<LfcPrewarmStateWithProgress> {
    Json(compute.lfc_prewarm_state().await)
}

// Following functions are marked async for axum, as it's more convenient than wrapping these
// in async lambdas at call site

pub(in crate::http) async fn offload_state(compute: Compute) -> Json<LfcOffloadState> {
    Json(compute.lfc_offload_state())
}

pub(in crate::http) async fn prewarm(compute: Compute) -> Response {
    if compute.prewarm_lfc() {
        StatusCode::ACCEPTED.into_response()
    } else {
        JsonResponse::error(
            StatusCode::TOO_MANY_REQUESTS,
            "Multiple requests for prewarm are not allowed",
        )
    }
}

pub(in crate::http) async fn offload(compute: Compute) -> Response {
    if compute.offload_lfc() {
        StatusCode::ACCEPTED.into_response()
    } else {
        JsonResponse::error(
            StatusCode::TOO_MANY_REQUESTS,
            "Multiple requests for prewarm offload are not allowed",
        )
    }
}
