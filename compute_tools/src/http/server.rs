use std::fmt::Display;
use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use axum::Router;
use axum::middleware::{self};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use compute_api::responses::ComputeCtlConfig;
use http::StatusCode;
use tokio::net::TcpListener;
use tower::ServiceBuilder;
use tower_http::{
    auth::AsyncRequireAuthorizationLayer, request_id::PropagateRequestIdLayer, trace::TraceLayer,
};
use tracing::{Span, error, info};

use super::middleware::request_id::maybe_add_request_id_header;
use super::{
    headers::X_REQUEST_ID,
    middleware::authorize::Authorize,
    routes::{
        check_writability, configure, database_schema, dbs_and_roles, extension_server, extensions,
        grants, insights, lfc, metrics, metrics_json, status, terminate,
    },
};
use crate::compute::ComputeNode;

/// `compute_ctl` has two servers: internal and external. The internal server
/// binds to the loopback interface and handles communication from clients on
/// the compute. The external server is what receives communication from the
/// control plane, the metrics scraper, etc. We make the distinction because
/// certain routes in `compute_ctl` only need to be exposed to local processes
/// like Postgres via the neon extension and local_proxy.
#[derive(Clone, Debug)]
pub enum Server {
    Internal {
        port: u16,
    },
    External {
        port: u16,
        config: ComputeCtlConfig,
        compute_id: String,
    },
}

impl Display for Server {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Server::Internal { .. } => f.write_str("internal"),
            Server::External { .. } => f.write_str("external"),
        }
    }
}

impl From<&Server> for Router<Arc<ComputeNode>> {
    fn from(server: &Server) -> Self {
        let mut router = Router::<Arc<ComputeNode>>::new();

        router = match server {
            Server::Internal { .. } => {
                router = router
                    .route(
                        "/extension_server/{*filename}",
                        post(extension_server::download_extension),
                    )
                    .route("/extensions", post(extensions::install_extension))
                    .route("/grants", post(grants::add_grant));

                // Add in any testing support
                if cfg!(feature = "testing") {
                    use super::routes::failpoints;

                    router = router.route("/failpoints", post(failpoints::configure_failpoints));
                }

                router
            }
            Server::External {
                config, compute_id, ..
            } => {
                let unauthenticated_router =
                    Router::<Arc<ComputeNode>>::new().route("/metrics", get(metrics::get_metrics));

                let authenticated_router = Router::<Arc<ComputeNode>>::new()
                    .route("/lfc/prewarm", get(lfc::prewarm_state).post(lfc::prewarm))
                    .route("/lfc/offload", get(lfc::offload_state).post(lfc::offload))
                    .route("/check_writability", post(check_writability::is_writable))
                    .route("/configure", post(configure::configure))
                    .route("/database_schema", get(database_schema::get_schema_dump))
                    .route("/dbs_and_roles", get(dbs_and_roles::get_catalog_objects))
                    .route("/insights", get(insights::get_insights))
                    .route("/metrics.json", get(metrics_json::get_metrics))
                    .route("/status", get(status::get_status))
                    .route("/terminate", post(terminate::terminate))
                    .layer(AsyncRequireAuthorizationLayer::new(Authorize::new(
                        compute_id.clone(),
                        config.jwks.clone(),
                    )));

                router
                    .merge(unauthenticated_router)
                    .merge(authenticated_router)
            }
        };

        router
            .fallback(Server::handle_404)
            .method_not_allowed_fallback(Server::handle_405)
            .layer(
                ServiceBuilder::new()
                    .layer(tower_otel::trace::HttpLayer::server(tracing::Level::INFO))
                    // Add this middleware since we assume the request ID exists
                    .layer(middleware::from_fn(maybe_add_request_id_header))
                    .layer(
                        TraceLayer::new_for_http()
                            .on_request(|request: &http::Request<_>, _span: &Span| {
                                let request_id = request
                                    .headers()
                                    .get(X_REQUEST_ID)
                                    .unwrap()
                                    .to_str()
                                    .unwrap();

                                info!(%request_id, "{} {}", request.method(), request.uri());
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
                                    );
                                },
                            ),
                    )
                    .layer(PropagateRequestIdLayer::x_request_id()),
            )
    }
}

impl Server {
    async fn handle_404() -> impl IntoResponse {
        StatusCode::NOT_FOUND
    }

    async fn handle_405() -> impl IntoResponse {
        StatusCode::METHOD_NOT_ALLOWED
    }

    async fn listener(&self) -> Result<TcpListener> {
        let addr = SocketAddr::new(self.ip(), self.port());
        let listener = TcpListener::bind(&addr).await?;

        Ok(listener)
    }

    fn ip(&self) -> IpAddr {
        match self {
            // TODO: Change this to Ipv6Addr::LOCALHOST when the GitHub runners
            // allow binding to localhost
            Server::Internal { .. } => IpAddr::from(Ipv6Addr::UNSPECIFIED),
            Server::External { .. } => IpAddr::from(Ipv6Addr::UNSPECIFIED),
        }
    }

    fn port(&self) -> u16 {
        match self {
            Server::Internal { port, .. } => *port,
            Server::External { port, .. } => *port,
        }
    }

    async fn serve(self, compute: Arc<ComputeNode>) {
        let listener = self.listener().await.unwrap_or_else(|e| {
            // If we can't bind, the compute cannot operate correctly
            panic!(
                "failed to bind the compute_ctl {} HTTP server to {}: {}",
                self,
                SocketAddr::new(self.ip(), self.port()),
                e
            );
        });

        if tracing::enabled!(tracing::Level::INFO) {
            let local_addr = match listener.local_addr() {
                Ok(local_addr) => local_addr,
                Err(_) => SocketAddr::new(self.ip(), self.port()),
            };

            info!(
                "compute_ctl {} HTTP server listening at {}",
                self, local_addr
            );
        }

        let router = Router::from(&self)
            .with_state(compute)
            .into_make_service_with_connect_info::<SocketAddr>();

        if let Err(e) = axum::serve(listener, router).await {
            error!("compute_ctl {} HTTP server error: {}", self, e);
        }
    }

    pub fn launch(self, compute: &Arc<ComputeNode>) {
        let state = Arc::clone(compute);

        info!("Launching the {} server", self);

        tokio::spawn(self.serve(state));
    }
}
