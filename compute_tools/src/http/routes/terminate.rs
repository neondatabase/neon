use crate::compute::{ComputeNode, forward_termination_signal};
use crate::http::JsonResponse;
use axum::extract::State;
use axum::response::Response;
use axum_extra::extract::OptionalQuery;
use compute_api::responses::{ComputeStatus, TerminateResponse};
use http::StatusCode;
use serde::Deserialize;
use std::sync::Arc;
use tokio::task;
use tracing::info;

#[derive(Deserialize, Default)]
pub struct TerminateQuery {
    mode: compute_api::responses::TerminateMode,
}

/// Terminate the compute.
pub(in crate::http) async fn terminate(
    State(compute): State<Arc<ComputeNode>>,
    OptionalQuery(terminate): OptionalQuery<TerminateQuery>,
) -> Response {
    let mode = terminate.unwrap_or_default().mode;
    {
        let mut state = compute.state.lock().unwrap();
        if state.status == ComputeStatus::Terminated {
            return JsonResponse::success(StatusCode::CREATED, state.terminate_flush_lsn);
        }

        if !matches!(state.status, ComputeStatus::Empty | ComputeStatus::Running) {
            return JsonResponse::invalid_status(state.status);
        }
        state.set_status(
            ComputeStatus::TerminationPending { mode },
            &compute.state_changed,
        );
    }

    forward_termination_signal();
    info!("sent signal and notified waiters");

    // Spawn a blocking thread to wait for compute to become Terminated.
    // This is needed to do not block the main pool of workers and
    // be able to serve other requests while some particular request
    // is waiting for compute to finish configuration.
    let c = compute.clone();
    let lsn = task::spawn_blocking(move || {
        let mut state = c.state.lock().unwrap();
        while state.status != ComputeStatus::Terminated {
            state = c.state_changed.wait(state).unwrap();
            info!(
                "waiting for compute to become {}, current status: {:?}",
                ComputeStatus::Terminated,
                state.status
            );
        }
        state.terminate_flush_lsn
    })
    .await
    .unwrap();
    info!("terminated Postgres");
    JsonResponse::success(StatusCode::OK, TerminateResponse { lsn })
}
