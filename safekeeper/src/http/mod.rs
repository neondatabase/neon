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
    global_timelines: Arc<GlobalTimelines>,
) -> anyhow::Result<()> {
    let certs = http_utils::tls_certs::load_cert_chain(&conf.ssl_cert_file)?;
    let key = http_utils::tls_certs::load_private_key(&conf.ssl_key_file)?;

    let server_config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)?;

    let tls_acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server_config));

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
