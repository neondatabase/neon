use crate::http::JsonResponse;
use axum::{Form, response::Response};
use http::StatusCode;
type Compute = axum::extract::State<std::sync::Arc<crate::compute::ComputeNode>>;

pub(in crate::http) async fn promote(
    compute: Compute,
    Form(safekeepers_lsn): Form<compute_api::responses::SafekeepersLsn>,
) -> Response {
    let state = compute.promote(safekeepers_lsn).await;
    if let compute_api::responses::PromoteState::Failed { error } = state {
        return JsonResponse::error(StatusCode::INTERNAL_SERVER_ERROR, error);
    }
    JsonResponse::success(StatusCode::OK, state)
}

pub(in crate::http) async fn safekeepers_lsn(compute: Compute) -> Response {
    match compute.safekeepers_lsn().await {
        Ok(v) => JsonResponse::success(StatusCode::OK, v),
        Err(error) => JsonResponse::error(StatusCode::INTERNAL_SERVER_ERROR, error),
    }
}
