use crate::compute::{ComputeNode, EndpointStoragePair, PrewarmStatus};
use axum::response::{IntoResponse, Response};
use axum::{Json, http::StatusCode};
use compute_api::responses::PrewarmOffloadState;
use std::sync::Arc;
use tracing::error;

type State = axum::extract::State<Arc<ComputeNode>>;

impl axum::extract::FromRequestParts<Arc<ComputeNode>> for EndpointStoragePair {
    type Rejection = Response;
    async fn from_request_parts(
        _: &mut http::request::Parts,
        state: &Arc<ComputeNode>,
    ) -> Result<Self, Self::Rejection> {
        let Some(ref pspec) = state.state.lock().unwrap().pspec else {
            error!("pspec missing");
            return Err(StatusCode::BAD_REQUEST.into_response());
        };
        pspec.try_into().map_err(|e| {
            error!("{e}");
            StatusCode::BAD_REQUEST.into_response()
        })
    }
}

pub(in crate::http) async fn status(state: State) -> Json<PrewarmStatus> {
    Json(state.prewarm_status().await)
}

pub(in crate::http) async fn offload_status(state: State) -> Json<PrewarmOffloadState> {
    Json(state.prewarm_offload_status().await)
}

pub(in crate::http) async fn prewarm(state: State, pair: EndpointStoragePair) -> StatusCode {
    if state.prewarm(pair).await {
        StatusCode::OK
    } else {
        StatusCode::TOO_MANY_REQUESTS
    }
}

pub(in crate::http) async fn offload(state: State, pair: EndpointStoragePair) -> StatusCode {
    if state.prewarm_offload(pair).await {
        return StatusCode::OK;
    } else {
        return StatusCode::TOO_MANY_REQUESTS;
    }
}
