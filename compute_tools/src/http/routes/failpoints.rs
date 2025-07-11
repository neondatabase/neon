use axum::response::{IntoResponse, Response};
use http::StatusCode;
use neon_failpoint::{configure_failpoint, configure_failpoint_with_context};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::info;

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

use crate::http::JsonResponse;
use crate::http::extract::Json;

/// Configure failpoints for testing purposes.
pub(in crate::http) async fn configure_failpoints(
    failpoints: Json<ConfigureFailpointsRequest>,
) -> Response {
    if !neon_failpoint::has_failpoints() {
        return JsonResponse::error(
            StatusCode::PRECONDITION_FAILED,
            "Cannot manage failpoints because neon was compiled without failpoints support",
        );
    }

    for fp in &*failpoints {
        info!(
            "cfg failpoint: {} {} (context: {:?})",
            fp.name, fp.actions, fp.context_matchers
        );

        let cfg_result = if let Some(context_matchers) = fp.context_matchers.clone() {
            configure_failpoint_with_context(&fp.name, &fp.actions, context_matchers)
        } else {
            configure_failpoint(&fp.name, &fp.actions)
        };

        if let Err(e) = cfg_result {
            return JsonResponse::error(
                StatusCode::BAD_REQUEST,
                format!("failed to configure failpoint '{}': {e}", fp.name),
            );
        }
    }

    StatusCode::OK.into_response()
}
