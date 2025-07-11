use crate::http::JsonResponse;
use axum::extract::Json;
use http::StatusCode;

pub(in crate::http) async fn promote(
    compute: axum::extract::State<std::sync::Arc<crate::compute::ComputeNode>>,
    Json(cfg): Json<compute_api::responses::PromoteConfig>,
) -> axum::response::Response {
    let state = compute.promote(cfg).await;
    if let compute_api::responses::PromoteState::Failed { error } = state {
        return JsonResponse::error(StatusCode::INTERNAL_SERVER_ERROR, error);
    }
    JsonResponse::success(StatusCode::OK, state)
}
