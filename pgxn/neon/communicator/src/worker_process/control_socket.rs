//! Communicator control socket.
//!
//! Currently, the control socket is used to provide information about the communicator
//! process, file cache etc. as prometheus metrics. In the future, it can be used to
//! expose more things.
//!
//! The exporter speaks HTTP, listens on a Unix Domain Socket under the Postgres
//! data directory. For debugging, you can access it with curl:
//!
//! ```sh
//! curl --unix-socket neon-communicator.socket http://localhost/metrics
//! ```
//!
use axum::Router;
use axum::body::Body;
use axum::extract::State;
use axum::response::Response;
use http::StatusCode;
use http::header::CONTENT_TYPE;

use measured::MetricGroup;
use measured::metric::MetricEncoding;
use measured::metric::gauge::GaugeState;
use measured::metric::group::Encoding;
use measured::text::BufferedTextEncoder;

use std::io::ErrorKind;
use std::sync::Arc;

use tokio::net::UnixListener;

use crate::NEON_COMMUNICATOR_SOCKET_NAME;
use crate::worker_process::main_loop::CommunicatorWorkerProcessStruct;
use crate::worker_process::lfc_metrics::LfcMetricsCollector;

enum ControlSocketState<'a> {
    Full(&'a CommunicatorWorkerProcessStruct<'a>),
    Legacy(LegacyControlSocketState),
}

struct LegacyControlSocketState {
    pub(crate) lfc_metrics: LfcMetricsCollector,
}

impl<T> MetricGroup<T> for LegacyControlSocketState
where
    T: Encoding,
    GaugeState: MetricEncoding<T>,
{
    fn collect_group_into(&self, enc: &mut T) -> Result<(), T::Err> {
        self.lfc_metrics.collect_group_into(enc)?;
        Ok(())
    }
}

/// Launch the listener
pub(crate) async fn launch_listener(
    worker: Option<&'static CommunicatorWorkerProcessStruct<'static>>,
) -> Result<(), std::io::Error> {
    use axum::routing::get;

    let state = match worker {
        Some(worker) => ControlSocketState::Full(worker),
        None => ControlSocketState::Legacy(LegacyControlSocketState {
            lfc_metrics: LfcMetricsCollector,
        }),
    };

    let app = Router::new()
        .route("/metrics", get(get_metrics))
        .route("/autoscaling_metrics", get(get_autoscaling_metrics))
        .route("/debug/panic", get(handle_debug_panic))
        .route("/debug/dump_cache_map", get(dump_cache_map))
        .with_state(Arc::new(state));

    // If the server is restarted, there might be an old socket still
    // lying around. Remove it first.
    match std::fs::remove_file(NEON_COMMUNICATOR_SOCKET_NAME) {
        Ok(()) => {
            tracing::warn!("removed stale control socket");
        }
        Err(e) if e.kind() == ErrorKind::NotFound => {}
        Err(e) => {
            tracing::error!("could not remove stale control socket: {e:#}");
            // Try to proceed anyway. It will likely fail below though.
        }
    };

    // Create the unix domain socket and start listening on it
    let listener = UnixListener::bind(NEON_COMMUNICATOR_SOCKET_NAME)?;

    tokio::spawn(async {
        tracing::info!("control socket listener spawned");
        axum::serve(listener, app)
            .await
            .expect("axum::serve never returns")
    });

    Ok(())
}

/// Expose all Prometheus metrics.
async fn get_metrics(State(state): State<Arc<ControlSocketState<'_>>>) -> Response {
    match state.as_ref() {
        ControlSocketState::Full(worker) => metrics_to_response(&worker).await,
        ControlSocketState::Legacy(legacy) => metrics_to_response(&legacy).await,
    }
}

/// Expose Prometheus metrics, for use by the autoscaling agent.
///
/// This is a subset of all the metrics.
async fn get_autoscaling_metrics(State(state): State<Arc<ControlSocketState<'_>>>) -> Response {
    match state.as_ref() {
        ControlSocketState::Full(worker) => metrics_to_response(&worker.lfc_metrics).await,
        ControlSocketState::Legacy(legacy) => metrics_to_response(&legacy.lfc_metrics).await,
    }
}

async fn handle_debug_panic(State(_state): State<Arc<ControlSocketState<'_>>>) -> Response {
    panic!("test HTTP handler task panic");
}

/// Helper function to convert prometheus metrics to a text response
async fn metrics_to_response(metrics: &(dyn MetricGroup<BufferedTextEncoder> + Sync)) -> Response {
    let mut enc = BufferedTextEncoder::new();
    metrics
        .collect_group_into(&mut enc)
        .unwrap_or_else(|never| match never {});

    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "application/text")
        .body(Body::from(enc.finish()))
        .unwrap()
}

async fn dump_cache_map(State(state): State<Arc<ControlSocketState<'_>>>) -> Response {
    match state.as_ref() {
        ControlSocketState::Full(worker) => {
            let mut buf: Vec<u8> = Vec::new();
            worker.cache.dump_map(&mut buf);

            Response::builder()
                .status(StatusCode::OK)
                .header(CONTENT_TYPE, "application/text")
                .body(Body::from(buf))
                .unwrap()
        }
        ControlSocketState::Legacy(_) => {
            Response::builder()
                .status(StatusCode::NOT_FOUND)
                .header(CONTENT_TYPE, "application/text")
                .body(Body::from(Vec::new()))
                .unwrap()
        }
    }
}
