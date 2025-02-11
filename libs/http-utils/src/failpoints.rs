use crate::error::ApiError;
use crate::json::{json_request, json_response};

use hyper::{Body, Request, Response, StatusCode};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

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

/// Configure failpoints through http.
pub async fn failpoints_handler(
    mut request: Request<Body>,
    _cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    if !fail::has_failpoints() {
        return Err(ApiError::BadRequest(anyhow::anyhow!(
            "Cannot manage failpoints because neon was compiled without failpoints support"
        )));
    }

    let failpoints: ConfigureFailpointsRequest = json_request(&mut request).await?;
    for fp in failpoints {
        tracing::info!("cfg failpoint: {} {}", fp.name, fp.actions);

        // We recognize one extra "action" that's not natively recognized
        // by the failpoints crate: exit, to immediately kill the process
        let cfg_result = apply_failpoint(&fp.name, &fp.actions);

        if let Err(err_msg) = cfg_result {
            return Err(ApiError::BadRequest(anyhow::anyhow!(
                "Failed to configure failpoints: {err_msg}"
            )));
        }
    }

    json_response(StatusCode::OK, ())
}
