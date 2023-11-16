pub mod routes;
pub use routes::make_router;

pub use safekeeper_api::models;

use crate::SafeKeeperConf;

pub async fn task_main(
    conf: SafeKeeperConf,
    http_listener: std::net::TcpListener,
) -> anyhow::Result<()> {
    let router = make_router(conf)
        .build()
        .map_err(|err| anyhow::anyhow!(err))?;
    let service = utils::http::RouterService::new(router).unwrap();
    let server = hyper::Server::from_tcp(http_listener)?;
    server.serve(service).await?;
    Ok(()) // unreachable
}
