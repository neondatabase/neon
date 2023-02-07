use anyhow::anyhow;
use hyper::{Body, Request, Response, StatusCode};
use std::net::TcpListener;
use tracing::info;
use utils::http::{endpoint, error::ApiError, json::json_response, RouterBuilder, RouterService};

async fn status_handler(_: Request<Body>) -> Result<Response<Body>, ApiError> {
    json_response(StatusCode::OK, "")
}

fn make_router() -> RouterBuilder<hyper::Body, ApiError> {
    endpoint::make_router().get("/v1/status", status_handler)
}

pub async fn task_main(http_listener: TcpListener) -> anyhow::Result<()> {
    scopeguard::defer! {
        info!("http has shut down");
    }

    let service = || RouterService::new(make_router().build()?);

    hyper::Server::from_tcp(http_listener)?
        .serve(service().map_err(|e| anyhow!(e))?)
        .await?;

    Ok(())
}
