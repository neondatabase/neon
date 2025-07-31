use crate::http::JsonResponse;
use axum::extract::Json;
use compute_api::responses::PromoteConfig;
use http::StatusCode;

pub(in crate::http) async fn promote(
    compute: axum::extract::State<std::sync::Arc<crate::compute::ComputeNode>>,
    Json(cfg): Json<PromoteConfig>,
) -> axum::response::Response {
    // Return early at the cost of extra parsing spec
    let pspec = match crate::compute::ParsedSpec::try_from(cfg.spec) {
        Ok(p) => p,
        Err(e) => return JsonResponse::error(StatusCode::BAD_REQUEST, e),
    };

    let cfg = PromoteConfig {
        spec: pspec.spec,
        wal_flush_lsn: cfg.wal_flush_lsn,
    };
    let state = compute.promote(cfg).await;
    if let compute_api::responses::PromoteState::Failed { error: _ } = state {
        return JsonResponse::create_response(StatusCode::INTERNAL_SERVER_ERROR, state);
    }
    JsonResponse::success(StatusCode::OK, state)
}
