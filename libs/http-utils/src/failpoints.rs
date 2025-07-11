use hyper::{Body, Request, Response, StatusCode};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio_util::sync::CancellationToken;
use neon_failpoint::{configure_failpoint, configure_failpoint_with_context, has_failpoints};

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
    /// We support actions: "pause", "sleep(N)", "return", "return(value)", "exit", "off", "panic(message)"
    /// Plus probability-based actions: "N%return(value)", "N%M*return(value)", "N%action", "N%M*action"
    pub actions: String,
    /// Optional context matching rules for conditional failpoints
    /// Each key-value pair specifies a context key and a regex pattern to match against
    /// All context matchers must match for the failpoint to trigger
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_matchers: Option<HashMap<String, String>>,
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
        tracing::info!("cfg failpoint: {} {} (context: {:?})", fp.name, fp.actions, fp.context_matchers);

        let cfg_result = if let Some(context_matchers) = fp.context_matchers {
            configure_failpoint_with_context(&fp.name, &fp.actions, context_matchers)
        } else {
            configure_failpoint(&fp.name, &fp.actions)
        };

        if let Err(err) = cfg_result {
            return Err(ApiError::BadRequest(anyhow::anyhow!(
                "Failed to configure failpoint '{}': {}", fp.name, err
            )));
        }
    }

    json_response(StatusCode::OK, ())
}
