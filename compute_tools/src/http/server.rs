use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    thread,
    time::Duration,
};

use anyhow::Result;
use axum::{
    response::{IntoResponse, Response},
    routing::{get, post},
    Router,
};
use http::StatusCode;
use tokio::net::TcpListener;
use tower::ServiceBuilder;
use tower_http::{
    request_id::{MakeRequestId, PropagateRequestIdLayer, RequestId, SetRequestIdLayer},
    trace::TraceLayer,
};
use tracing::{debug, info, Span};

use super::routes::{
    check_writability, configure, database_schema, dbs_and_roles, extension_server, extensions,
    grants, info as info_route, insights, installed_extensions, metrics, metrics_json, status,
    terminate,
};
use crate::compute::ComputeNode;

async fn handle_404() -> Response {
    StatusCode::NOT_FOUND.into_response()
}

#[derive(Clone, Default)]
struct ComputeMakeRequestId(Arc<AtomicU64>);

impl MakeRequestId for ComputeMakeRequestId {
    fn make_request_id<B>(
        &mut self,
        _request: &http::Request<B>,
    ) -> Option<tower_http::request_id::RequestId> {
        let request_id = self
            .0
            .fetch_add(1, Ordering::SeqCst)
            .to_string()
            .parse()
            .unwrap();

        Some(RequestId::new(request_id))
    }
}

/// Run the HTTP server and wait on it forever.
#[tokio::main]
async fn serve(port: u16, compute: Arc<ComputeNode>) {
    const X_REQUEST_ID: &str = "x-request-id";

    let app = Router::new()
        .route("/check_writability", get(check_writability::writable))
        .route("/configure", post(configure::configure))
        .route("/database_schema", get(database_schema::get_schema_dump))
        .route("/dbs_and_roles", get(dbs_and_roles::get_catalog_objects))
        .route(
            "/extension_server/*filename",
            post(extension_server::install_extension),
        )
        .route("/extensions", post(extensions::install_extension))
        .route("/grants", post(grants::add_grant))
        .route("/info", get(info_route::get_info))
        .route("/insights", get(insights::get_insights))
        .route(
            "/installed_extensions",
            get(installed_extensions::get_installed_extensions),
        )
        .route("/metrics", get(metrics::get_metrics))
        .route("/metrics.json", get(metrics_json::get_metrics))
        .route("/status", get(status::get_status))
        .route("/terminate", post(terminate::terminate))
        .fallback(handle_404)
        .layer(
            ServiceBuilder::new()
                .layer(SetRequestIdLayer::x_request_id(
                    ComputeMakeRequestId::default(),
                ))
                .layer(
                    TraceLayer::new_for_http()
                        .on_request(|request: &http::Request<_>, _span: &Span| {
                            let request_id = request
                                .headers()
                                .get(X_REQUEST_ID)
                                .unwrap()
                                .to_str()
                                .unwrap();

                            let log = format!("{} {}", request.method(), request.uri());
                            match request.uri().path() {
                                "/metrics" => debug!(%request_id, "{}", log),
                                _ => info!(%request_id, "{}", log),
                            };
                        })
                        .on_response(
                            |response: &http::Response<_>, latency: Duration, _span: &Span| {
                                let request_id = response
                                    .headers()
                                    .get(X_REQUEST_ID)
                                    .unwrap()
                                    .to_str()
                                    .unwrap();

                                info!(
                                    %request_id,
                                    code = response.status().as_u16(),
                                    latency = latency.as_millis()
                                )
                            },
                        ),
                )
                .layer(PropagateRequestIdLayer::x_request_id()),
        )
        .with_state(compute);

    let listener = TcpListener::bind(format!("127.0.0.1:{port}"))
        .await
        .unwrap();

    info!("listening on {}", listener.local_addr().unwrap());

    axum::serve(listener, app).await.unwrap();
}

/// Launch a separate HTTP server thread and return its `JoinHandle`.
pub fn launch_http_server(port: u16, state: &Arc<ComputeNode>) -> Result<thread::JoinHandle<()>> {
    let state = Arc::clone(state);

    Ok(thread::Builder::new()
        .name("http-server".into())
        .spawn(move || serve(port, state))?)
}
