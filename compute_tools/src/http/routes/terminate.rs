use crate::compute::{ComputeNode, forward_termination_signal};
use crate::http::JsonResponse;
use axum::extract::State;
use axum::response::Response;
use axum_extra::extract::OptionalQuery;
use compute_api::responses::ComputeStatus;
use http::StatusCode;
use serde::Deserialize;
use std::sync::Arc;
use tokio::task;
use tracing::info;

#[derive(Deserialize, Default)]
pub struct TerminateQuery {
    //axum has issues deserializing enums i.e.
    // invalid type: string "immediate", expected internally tagged enum
    /// "fast": wait 30s till returning from /terminate to allow control plane to get the error
    /// "immediate": return from /terminate immediately as soon as all components are terminated
    mode: String,
}

/// Terminate the compute.
pub(in crate::http) async fn terminate(
    State(compute): State<Arc<ComputeNode>>,
    OptionalQuery(terminate): OptionalQuery<TerminateQuery>,
) -> Response {
    let immediate = match terminate.unwrap_or_default().mode.as_str() {
        "fast" => false,
        "immediate" => true,
        v => {
            return JsonResponse::error(
                StatusCode::BAD_REQUEST,
                format!("Expected \"fast\" or \"immediate\", got {v}"),
            );
        }
    };

    {
        let mut state = compute.state.lock().unwrap();
        if state.status == ComputeStatus::Terminated {
            return JsonResponse::success(StatusCode::CREATED, state.terminate_flush_lsn);
        }

        if !matches!(state.status, ComputeStatus::Empty | ComputeStatus::Running) {
            return JsonResponse::invalid_status(state.status);
        }

        let status = if immediate {
            ComputeStatus::Terminated
        } else {
            ComputeStatus::TerminationPending
        };
        state.set_status(status, &compute.state_changed);
    }

    forward_termination_signal(false);
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

    info!(%lsn, "terminated Postgres");
    JsonResponse::success(StatusCode::OK, lsn)
}
