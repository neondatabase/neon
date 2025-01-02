use std::sync::Arc;

use axum::{extract::State, response::Response};
use compute_api::{
    requests::ConfigurationRequest,
    responses::{ComputeStatus, ComputeStatusResponse},
};
use http::StatusCode;
use tokio::task;
use tracing::info;

use crate::{
    compute::{ComputeNode, ParsedSpec},
    http::{extract::Json, JsonResponse},
};

pub(in crate::http) async fn configure(
    State(compute): State<Arc<ComputeNode>>,
    request: Json<ConfigurationRequest>,
) -> Response {
    if !compute.live_config_allowed {
        return JsonResponse::error(
            StatusCode::PRECONDITION_FAILED,
            "live configuration is not allowed for this compute node".to_string(),
        );
    }

    let pspec = match ParsedSpec::try_from(request.spec.clone()) {
        Ok(p) => p,
        Err(e) => return JsonResponse::error(StatusCode::BAD_REQUEST, e),
    };

    {
        let mut state = compute.state.lock().unwrap();
        if !matches!(state.status, ComputeStatus::Empty | ComputeStatus::Running) {
            return JsonResponse::invalid_status(state.status);
        }

        state.pspec = Some(pspec);
        state.set_status(ComputeStatus::ConfigurationPending, &compute.state_changed);
        drop(state);
    }

    let c = compute.clone();
    let completed = task::spawn_blocking(move || {
        let mut state = c.state.lock().unwrap();
        while state.status != ComputeStatus::Running {
            state = c.state_changed.wait(state).unwrap();
            info!(
                "waiting for compute to become {}, current status: {}",
                ComputeStatus::Running,
                state.status
            );

            if state.status == ComputeStatus::Failed {
                let err = state.error.as_ref().map_or("unknown error", |x| x);
                let msg = format!("compute configuration failed: {:?}", err);
                return Err(msg);
            }
        }

        Ok(())
    })
    .await
    .unwrap();

    if let Err(e) = completed {
        return JsonResponse::error(StatusCode::INTERNAL_SERVER_ERROR, e);
    }

    let state = compute.state.lock().unwrap().clone();
    let body = ComputeStatusResponse::from(&state);

    JsonResponse::success(StatusCode::ACCEPTED, body)
}
