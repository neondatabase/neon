use anyhow::anyhow;
use hyper::{Body, Request, Response, StatusCode};
use std::net::TcpListener;
use utils::http::{endpoint, error::ApiError, json::json_response, RouterBuilder, RouterService};

async fn status_handler(_: Request<Body>) -> Result<Response<Body>, ApiError> {
    json_response(StatusCode::OK, "OK")
}

fn make_router() -> RouterBuilder<hyper::Body, ApiError> {
    let router = endpoint::make_router();
    router.get("/v1/status", status_handler)
}

pub async fn thread_main(http_listener: TcpListener) -> anyhow::Result<()> {
    scopeguard::defer! {
        println!("http has shut down");
    }

    let service = || RouterService::new(make_router().build()?);

    hyper::Server::from_tcp(http_listener)?
        .serve(service().map_err(|e| anyhow!(e))?)
        .await?;

    Ok(())
}
