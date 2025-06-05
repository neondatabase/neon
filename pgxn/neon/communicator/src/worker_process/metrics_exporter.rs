//! Export information about Postgres, the communicator process, file cache etc. as
//! prometheus metrics.

use axum::Router;
use axum::body::Body;
use axum::extract::State;
use axum::response::Response;
use http::StatusCode;
use http::header::CONTENT_TYPE;

use metrics;
use metrics::proto::MetricFamily;
use metrics::{Encoder, TextEncoder};

use std::path::PathBuf;

use tokio::net::UnixListener;

use crate::worker_process::main_loop::CommunicatorWorkerProcessStruct;

impl<'a> CommunicatorWorkerProcessStruct<'a> {
    pub(crate) async fn launch_exporter_task(&'static self) {
        use axum::routing::get;
        let app = Router::new()
            .route("/metrics", get(get_metrics))
            .route("/dump_cache_map", get(dump_cache_map))
            .with_state(self);

        // Listen on unix domain socket, in the data directory. That should be unique.
        let path = PathBuf::from(".metrics.socket");

        let listener = UnixListener::bind(path.clone()).unwrap();

        tokio::spawn(async {
            tracing::info!("metrics listener spawned");
            axum::serve(listener, app).await.unwrap()
        });
    }
}

async fn dump_cache_map(
    State(state): State<&CommunicatorWorkerProcessStruct<'static>>,
) -> Response {
    let mut buf: Vec<u8> = Vec::new();
    state.cache.dump_map(&mut buf);

    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "application/text")
        .body(Body::from(buf))
        .unwrap()
}

/// Expose Prometheus metrics.
async fn get_metrics(State(state): State<&CommunicatorWorkerProcessStruct<'static>>) -> Response {
    use metrics::core::Collector;
    let metrics = state.collect();

    // When we call TextEncoder::encode() below, it will immediately return an
    // error if a metric family has no metrics, so we need to preemptively
    // filter out metric families with no metrics.
    let metrics = metrics
        .into_iter()
        .filter(|m| !m.get_metric().is_empty())
        .collect::<Vec<MetricFamily>>();

    let encoder = TextEncoder::new();
    let mut buffer = vec![];

    if let Err(e) = encoder.encode(&metrics, &mut buffer) {
        Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .header(CONTENT_TYPE, "application/text")
            .body(Body::from(e.to_string()))
            .unwrap()
    } else {
        Response::builder()
            .status(StatusCode::OK)
            .header(CONTENT_TYPE, encoder.format_type())
            .body(Body::from(buffer))
            .unwrap()
    }
}
