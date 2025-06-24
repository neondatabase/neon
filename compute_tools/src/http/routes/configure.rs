use std::sync::Arc;

use axum::extract::State;
use axum::response::Response;
use compute_api::requests::ConfigurationRequest;
use compute_api::responses::{ComputeStatus, ComputeStatusResponse};
use http::StatusCode;
use tokio::task;
use tracing::info;

use crate::compute::{ComputeNode, ParsedSpec};
use crate::http::JsonResponse;
use crate::http::extract::Json;

// Accept spec in JSON format and request compute configuration. If anything
// goes wrong after we set the compute status to `ConfigurationPending` and
// update compute state with new spec, we basically leave compute in the
// potentially wrong state. That said, it's control-plane's responsibility to
// watch compute state after reconfiguration request and to clean restart in
// case of errors.
pub(in crate::http) async fn configure(
    State(compute): State<Arc<ComputeNode>>,
    request: Json<ConfigurationRequest>,
) -> Response {
    let pspec = match ParsedSpec::try_from(request.0.spec) {
        Ok(p) => p,
        Err(e) => return JsonResponse::error(StatusCode::BAD_REQUEST, e),
    };

    // Spawn a blocking thread to wait for compute to become Running. This is
    // needed to not block the main pool of workers and to be able to serve
    // other requests while some particular request is waiting for compute to
    // finish configuration.
    let c = compute.clone();
    let span = tracing::Span::current();
    let completed = task::spawn_blocking(move || {
        let mut state = c.state.lock().unwrap();
        loop {
            match state.status {
                // ideal state.
                ComputeStatus::Empty | ComputeStatus::Running => break,
                // we need to wait until reloaded
                ComputeStatus::Reloading => {
                    state = c.state_changed.wait(state).unwrap();
                }
                // All other cases are unexpected.
                _ => return Err(JsonResponse::invalid_status(state.status)),
            }
        }

        // Pass the tracing span to the main thread that performs the startup,
        // so that the start_compute operation is considered a child of this
        // configure request for tracing purposes.
        state.startup_span = Some(span);

        state.pspec = Some(pspec);
        state.set_status(ComputeStatus::ConfigurationPending, &c.state_changed);

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
                return Err(JsonResponse::error(StatusCode::INTERNAL_SERVER_ERROR, msg));
            }
        }

        Ok(())
    })
    .await
    .unwrap();

    if let Err(e) = completed {
        return e;
    }

    // Return current compute state if everything went well.
    let state = compute.state.lock().unwrap().clone();
    let body = ComputeStatusResponse::from(&state);

    JsonResponse::success(StatusCode::OK, body)
}
