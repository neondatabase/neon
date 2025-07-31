use crate::compute::{ComputeNode, forward_termination_signal};
use crate::http::JsonResponse;
use axum::extract::State;
use axum::response::{IntoResponse, Response};
use axum_extra::extract::OptionalQuery;
use compute_api::responses::{ComputeStatus, TerminateMode, TerminateResponse};
use http::StatusCode;
use serde::Deserialize;
use std::sync::Arc;
use tokio::task;
use tracing::info;

#[derive(Deserialize, Default)]
pub struct TerminateQuery {
    mode: TerminateMode,
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
            let response = TerminateResponse {
                lsn: state.terminate_flush_lsn,
            };
            return JsonResponse::success(StatusCode::CREATED, response);
        }

        if !matches!(state.status, ComputeStatus::Empty | ComputeStatus::Running) {
            return JsonResponse::invalid_status(state.status);
        }

        // If compute is Empty, there's no Postgres to terminate. The regular compute_ctl termination path
        // assumes Postgres to be configured and running, so we just special-handle this case by exiting
        // the process directly.
        if compute.params.lakebase_mode && state.status == ComputeStatus::Empty {
            drop(state);
            info!("terminating empty compute - will exit process");

            // Queue a task to exit the process after 5 seconds. The 5-second delay aims to
            // give enough time for the HTTP response to be sent so that HCM doesn't get an abrupt
            // connection termination.
            tokio::spawn(async {
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                info!("exiting process after terminating empty compute");
                std::process::exit(0);
            });

            return StatusCode::OK.into_response();
        }

        // For Running status, proceed with normal termination
        state.set_status(mode.into(), &compute.state_changed);
        drop(state);
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
    info!("terminated Postgres");
    JsonResponse::success(StatusCode::OK, TerminateResponse { lsn })
}
