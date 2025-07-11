use hyper::{Body, Request, Response, StatusCode};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;
use neon_failpoint::{configure_failpoint, has_failpoints};

use crate::error::ApiError;
use crate::json::{json_request, json_response};

pub type ConfigureFailpointsRequest = Vec<FailpointConfig>;

/// Information for configuring a single fail point
#[derive(Debug, Serialize, Deserialize)]
pub struct FailpointConfig {
    /// Name of the fail point
    pub name: String,
    /// List of actions to take, using the format described in neon_failpoint
    ///
    /// We support actions: "pause", "sleep(N)", "return", "return(value)", "exit", "off"
    pub actions: String,
}

/// Configure failpoints through http.
pub async fn failpoints_handler(
    mut request: Request<Body>,
    _cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    if !has_failpoints() {
        return Err(ApiError::BadRequest(anyhow::anyhow!(
            "Cannot manage failpoints because neon was compiled without failpoints support"
        )));
    }

    let failpoints: ConfigureFailpointsRequest = json_request(&mut request).await?;
    for fp in failpoints {
        tracing::info!("cfg failpoint: {} {}", fp.name, fp.actions);

        let cfg_result = configure_failpoint(&fp.name, &fp.actions);

        if let Err(err) = cfg_result {
            return Err(ApiError::BadRequest(anyhow::anyhow!(
                "Failed to configure failpoints: {err}"
            )));
        }
    }

    json_response(StatusCode::OK, ())
}
