pub mod routes;
use std::sync::Arc;

pub use routes::make_router;
pub use safekeeper_api::models;
use tokio_util::sync::CancellationToken;

use crate::{GlobalTimelines, SafeKeeperConf};

pub async fn task_main_http(
    conf: Arc<SafeKeeperConf>,
    http_listener: std::net::TcpListener,
    global_timelines: Arc<GlobalTimelines>,
) -> anyhow::Result<()> {
    let router = make_router(conf, global_timelines)
        .build()
        .map_err(|err| anyhow::anyhow!(err))?;

    let service = Arc::new(
        http_utils::RequestServiceBuilder::new(router).map_err(|err| anyhow::anyhow!(err))?,
    );
    let server = http_utils::server::Server::new(service, http_listener, None)?;
    server.serve(CancellationToken::new()).await?;
    Ok(()) // unreachable
}

pub async fn task_main_https(
    conf: Arc<SafeKeeperConf>,
    https_listener: std::net::TcpListener,
    tls_config: Arc<rustls::ServerConfig>,
    global_timelines: Arc<GlobalTimelines>,
) -> anyhow::Result<()> {
    let tls_acceptor = tokio_rustls::TlsAcceptor::from(tls_config);

    let router = make_router(conf, global_timelines)
        .build()
        .map_err(|err| anyhow::anyhow!(err))?;

    let service = Arc::new(
        http_utils::RequestServiceBuilder::new(router).map_err(|err| anyhow::anyhow!(err))?,
    );
    let server = http_utils::server::Server::new(service, https_listener, Some(tls_acceptor))?;
    server.serve(CancellationToken::new()).await?;
    Ok(()) // unreachable
}
