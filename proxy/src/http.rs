use anyhow::anyhow;
use hyper::{Body, Request, Response, StatusCode};
use std::net::TcpListener;
use zenith_utils::http::endpoint;
use zenith_utils::http::error::ApiError;
use zenith_utils::http::json::json_response;
use zenith_utils::http::{RouterBuilder, RouterService};

async fn status_handler(_: Request<Body>) -> Result<Response<Body>, ApiError> {
    json_response(StatusCode::OK, "")
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
