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
    let pspec = match ParsedSpec::try_from(request.spec.clone()) {
        Ok(p) => p,
        Err(e) => return JsonResponse::error(StatusCode::BAD_REQUEST, e),
    };

    // Now validate the given configuration (safekeeper_connstrings)
    match check_config(&pspec) {
        Ok(b) => b,
        Err(e) => return JsonResponse::error(StatusCode::BAD_REQUEST, e),
    };

    // XXX: wrap state update under lock in a code block. Otherwise, we will try
    // to `Send` `mut state` into the spawned thread bellow, which will cause
    // the following rustc error:
    //
    // error: future cannot be sent between threads safely
    {
        let mut state = compute.state.lock().unwrap();
        if !matches!(state.status, ComputeStatus::Empty | ComputeStatus::Running) {
            return JsonResponse::invalid_status(state.status);
        }

        // Pass the tracing span to the main thread that performs the startup,
        // so that the start_compute operation is considered a child of this
        // configure request for tracing purposes.
        state.startup_span = Some(tracing::Span::current());

        state.pspec = Some(pspec);
        state.set_status(ComputeStatus::ConfigurationPending, &compute.state_changed);
        drop(state);
    }

    // Spawn a blocking thread to wait for compute to become Running. This is
    // needed to not block the main pool of workers and to be able to serve
    // other requests while some particular request is waiting for compute to
    // finish configuration.
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

    // Return current compute state if everything went well.
    let state = compute.state.lock().unwrap().clone();
    let body = ComputeStatusResponse::from(&state);

    JsonResponse::success(StatusCode::OK, body)
}

// Accept spec in JSON format and validate the configuration
// At the moment the validation is limited to the connstrings vector
// for the safekeeper, and we only check that we have at least one entry
// in there and no duplicate values
pub(in crate::http) async fn check(
    State(_compute): State<Arc<ComputeNode>>,
    request: Json<ConfigurationRequest>,
) -> Response {
    let pspec = match ParsedSpec::try_from(request.spec.clone()) {
        Ok(p) => p,
        Err(e) => return JsonResponse::error(StatusCode::BAD_REQUEST, e),
    };

    match check_config(&pspec) {
        Ok(b) => b,
        Err(e) => return JsonResponse::error(StatusCode::BAD_REQUEST, e),
    };

    JsonResponse::success(StatusCode::OK, true)
}

// check an already parsed configuration
fn check_config(pspec: &ParsedSpec) -> Result<bool, &'static str> {
    // at least one entry is expected in the safekeeper connstrings vector
    if pspec.safekeeper_connstrings.len() < 1 {
        return Err("safekeeper_connstrings is empty");
    }

    // check for unicity of the connection strings
    let mut connstrings = pspec.safekeeper_connstrings.clone();

    connstrings.sort();
    let previous = &connstrings[0];

    for current in connstrings.iter().skip(1) {
        // duplicate entry
        if current == previous {
            return Err("duplicate entry in safekeeper_connstrings");
        }
    }

    Ok(true)
}
