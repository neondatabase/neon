use anyhow::anyhow;
use http::{Request, Response};
use hyper::StatusCode;
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::conn,
};
use std::{convert::Infallible, net::TcpListener};
use tracing::info;
use utils::http::{
    endpoint, error::ApiError, json::json_response, Body, RequestServiceBuilder, RouterBuilder,
};

async fn status_handler(_: Request<Body>) -> Result<Response<Body>, ApiError> {
    json_response(StatusCode::OK, "").map(|req| req.map(Body::new))
}

fn make_router() -> RouterBuilder<Body, ApiError> {
    endpoint::make_router().get("/v1/status", status_handler)
}

pub async fn task_main(http_listener: TcpListener) -> anyhow::Result<Infallible> {
    scopeguard::defer! {
        info!("http has shut down");
    }

    let router = make_router().build().map_err(|e| anyhow!(e))?;
    let builder = RequestServiceBuilder::new(router).map_err(|e| anyhow!(e))?;
    let listener = tokio::net::TcpListener::from_std(http_listener)?;

    loop {
        let (stream, remote_addr) = listener.accept().await.unwrap();
        let io = TokioIo::new(stream);
        let service = builder.build(remote_addr);
        tokio::task::spawn(async move {
            let builder = conn::auto::Builder::new(TokioExecutor::new());
            let res = builder.serve_connection(io, service).await;
            if let Err(err) = res {
                println!("Error serving connection: {:?}", err);
            }
        });
    }
}
