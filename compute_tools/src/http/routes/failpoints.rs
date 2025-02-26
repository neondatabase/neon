use axum::response::{IntoResponse, Response};
use http::StatusCode;
use serde::{Deserialize, Serialize};
use tracing::info;
use utils::failpoint_support::apply_failpoint;

pub type ConfigureFailpointsRequest = Vec<FailpointConfig>;

/// Information for configuring a single fail point
#[derive(Debug, Serialize, Deserialize)]
pub struct FailpointConfig {
    /// Name of the fail point
    pub name: String,
    /// List of actions to take, using the format described in `fail::cfg`
    ///
    /// We also support `actions = "exit"` to cause the fail point to immediately exit.
    pub actions: String,
}

use crate::http::{JsonResponse, extract::Json};

/// Configure failpoints for testing purposes.
pub(in crate::http) async fn configure_failpoints(
    failpoints: Json<ConfigureFailpointsRequest>,
) -> Response {
    if !fail::has_failpoints() {
        return JsonResponse::error(
            StatusCode::PRECONDITION_FAILED,
            "Cannot manage failpoints because neon was compiled without failpoints support",
        );
    }

    for fp in &*failpoints {
        info!("cfg failpoint: {} {}", fp.name, fp.actions);

        // We recognize one extra "action" that's not natively recognized
        // by the failpoints crate: exit, to immediately kill the process
        let cfg_result = apply_failpoint(&fp.name, &fp.actions);

        if let Err(e) = cfg_result {
            return JsonResponse::error(
                StatusCode::BAD_REQUEST,
                format!("failed to configure failpoints: {e}"),
            );
        }
    }

    StatusCode::OK.into_response()
}
