use crate::http::JsonResponse;
use axum::Form;
use http::StatusCode;

pub(in crate::http) async fn promote(
    compute: axum::extract::State<std::sync::Arc<crate::compute::ComputeNode>>,
    Form(safekeepers_lsn): Form<compute_api::responses::SafekeepersLsn>,
) -> axum::response::Response {
    let state = compute.promote(safekeepers_lsn).await;
    if let compute_api::responses::PromoteState::Failed { error } = state {
        return JsonResponse::error(StatusCode::INTERNAL_SERVER_ERROR, error);
    }
    JsonResponse::success(StatusCode::OK, state)
}
