//! Export information about Postgres, the communicator process, file cache etc. as
//! prometheus metrics.
//!
//! The exporter speaks HTTP, listens on a Unix Domain Socket under the Postgres
//! data directory. For debugging, you can access it with curl:
//!
//!    curl --unix-socket neon-communicator.socket http://localhost/metrics
//!
use axum::Router;
use axum::body::Body;
use axum::extract::State;
use axum::response::Response;
use http::StatusCode;
use http::header::CONTENT_TYPE;

use measured::MetricGroup;
use measured::text::BufferedTextEncoder;

use std::io::ErrorKind;

use tokio::net::UnixListener;

use crate::worker_process::main_loop::CommunicatorWorkerProcessStruct;

const NEON_COMMUNICATOR_SOCKET_NAME: &str = "neon-communicator.socket";

impl CommunicatorWorkerProcessStruct {
    /// Launch the metrics exporter
    pub(crate) async fn launch_metrics_exporter(&'static self) -> Result<(), std::io::Error> {
        use axum::routing::get;
        let app = Router::new()
            .route("/metrics", get(get_metrics))
            .route("/autoscaling_metrics", get(get_autoscaling_metrics))
            .route("/debug/panic", get(handle_debug_panic))
            .with_state(self);

        // If the server is restarted, there might be an old socket still
        // lying around. Remove it first.
        match std::fs::remove_file(NEON_COMMUNICATOR_SOCKET_NAME) {
            Ok(()) => {
                tracing::warn!("removed stale {NEON_COMMUNICATOR_SOCKET_NAME}");
            },
            Err(e) if e.kind() == ErrorKind::NotFound => {},
            Err(e) => {
                tracing::error!("could not remove stale {NEON_COMMUNICATOR_SOCKET_NAME}: {e:#}");
                // Try to proceed anyway. It will likely fail below though.
            }
        };

        // Create the unix domain socket and start listening on it
        let listener = UnixListener::bind(NEON_COMMUNICATOR_SOCKET_NAME)?;

        tokio::spawn(async {
            tracing::info!("metrics listener spawned");
            axum::serve(listener, app)
                .await
                .expect("axum::serve never returns")
        });

        Ok(())
    }
}

/// Expose all Prometheus metrics.
async fn get_metrics(State(state): State<&CommunicatorWorkerProcessStruct>) -> Response {
    tracing::trace!("/metrics requested");
    metrics_to_response(&state).await
}

/// Expose Prometheus metrics, for use by the autoscaling agent.
///
/// This is a subset of all the metrics.
async fn get_autoscaling_metrics(
    State(state): State<&CommunicatorWorkerProcessStruct>,
) -> Response {
    tracing::trace!("/metrics requested");
    metrics_to_response(&state.lfc_metrics).await
}

async fn handle_debug_panic(State(_state): State<&CommunicatorWorkerProcessStruct>) -> Response {
    panic!("test HTTP handler task panic");
}

/// Helper function to convert prometheus metrics to a text response
async fn metrics_to_response(metrics: &(dyn MetricGroup<BufferedTextEncoder> + Sync)) -> Response {
    let mut enc = BufferedTextEncoder::new();
    metrics
        .collect_group_into(&mut enc)
        .unwrap_or_else(|never| match never {});

    Response::builder()
        .status(StatusCode::INTERNAL_SERVER_ERROR)
        .header(CONTENT_TYPE, "application/text")
        .body(Body::from(enc.finish()))
        .unwrap()
}
