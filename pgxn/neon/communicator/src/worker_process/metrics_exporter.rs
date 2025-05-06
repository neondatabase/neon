//! Export information about Postgres, the communicator process, file cache etc. as
//! prometheus metrics.

use axum::Router;
use axum::extract::State;
use axum::body::Body;
use axum::response::Response;
use http::StatusCode;
use http::header::CONTENT_TYPE;

use metrics::proto::MetricFamily;
use metrics::{Encoder, TextEncoder};
use metrics;

use crate::worker_process::main_loop::CommunicatorWorkerProcessStruct;

impl<'a> CommunicatorWorkerProcessStruct<'a> {
    pub(crate) async fn launch_exporter_task(&'static self) {
        use axum::routing::get;
        let app = Router::new()
            .route("/metrics", get(get_metrics))
            .with_state(self);            

        // TODO: make configurable. Or listen on unix domain socket?
        let listener = tokio::net::TcpListener::bind("127.0.0.1:9090").await.unwrap();

        tokio::spawn(async {
            tracing::info!("metrics listener spawned");
            axum::serve(listener, app).await.unwrap()
        });
    }
}

/// Expose Prometheus metrics.
async fn get_metrics(
    State(state): State<&CommunicatorWorkerProcessStruct<'static>>
) -> Response {
    tracing::warn!("get_metrics called");

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

    tracing::warn!("get_metrics done");
    
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
