use crate::pg_isready::pg_isready;
use crate::{compute::ComputeNode, http::JsonResponse};
use axum::{extract::State, http::StatusCode, response::Response};
use std::sync::Arc;

/// NOTE: NOT ENABLED YET
/// Detect if the compute is alive.
/// Called by the liveness probe of the compute container.
pub(in crate::http) async fn hadron_liveness_probe(
    State(compute): State<Arc<ComputeNode>>,
) -> Response {
    let port = match compute.params.connstr.port() {
        Some(port) => port,
        None => {
            return JsonResponse::error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to get the port from the connection string",
            );
        }
    };
    match pg_isready(&compute.params.pg_isready_bin, port) {
        Ok(_) => {
            // The connection is successful, so the compute is alive.
            // Return a 200 OK response.
            JsonResponse::success(StatusCode::OK, "ok")
        }
        Err(e) => {
            tracing::error!("Hadron liveness probe failed: {}", e);
            // The connection failed, so the compute is not alive.
            // Return a 500 Internal Server Error response.
            JsonResponse::error(StatusCode::INTERNAL_SERVER_ERROR, e)
        }
    }
}
