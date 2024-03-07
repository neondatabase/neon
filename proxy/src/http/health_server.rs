use anyhow::{anyhow, bail};
use hyper::{
    body::HttpBody,
    server::conn::{AddrIncoming, AddrStream},
    service::Service,
    Body, Request, Response, StatusCode,
};
use routerify::{RequestService, RequestServiceBuilder, Router};
use std::{
    convert::Infallible,
    future::{ready, Ready},
    task::{Context, Poll},
};
use tokio::net::TcpListener;
use tracing::info;
use utils::http::{endpoint, error::ApiError, json::json_response, RouterBuilder};

use crate::protocol2::{ProxyProtocolAccept, WithClientIp, WithConnectionGuard};

async fn status_handler(_: Request<Body>) -> Result<Response<Body>, ApiError> {
    json_response(StatusCode::OK, "")
}

fn make_router() -> RouterBuilder<hyper::Body, ApiError> {
    endpoint::make_router().get("/v1/status", status_handler)
}

pub async fn task_main(http_listener: TcpListener) -> anyhow::Result<Infallible> {
    scopeguard::defer! {
        info!("http has shut down");
    }

    let service = || RouterService2::new(make_router().build()?);

    let mut addr_incoming = AddrIncoming::from_listener(http_listener)?;
    let _ = addr_incoming.set_nodelay(true);
    let addr_incoming = ProxyProtocolAccept {
        incoming: addr_incoming,
        protocol: "health_checks",
    };

    hyper::Server::builder(addr_incoming)
        .serve(service().map_err(|e| anyhow!(e))?)
        .await?;

    bail!("hyper server without shutdown handling cannot shutdown successfully");
}

#[derive(Debug)]
pub struct RouterService2<B, E> {
    builder: RequestServiceBuilder<B, E>,
}

impl<
        B: HttpBody + Send + Sync + 'static,
        E: Into<Box<dyn std::error::Error + Send + Sync>> + 'static,
    > RouterService2<B, E>
{
    /// Creates a new service with the provided router and it's ready to be used with the hyper [`serve`](https://docs.rs/hyper/0.14.4/hyper/server/struct.Builder.html#method.serve)
    /// method.
    pub fn new(router: Router<B, E>) -> routerify::Result<RouterService2<B, E>> {
        let builder = RequestServiceBuilder::new(router)?;
        Ok(RouterService2 { builder })
    }
}

impl<
        B: HttpBody + Send + Sync + 'static,
        E: Into<Box<dyn std::error::Error + Send + Sync>> + 'static,
    > Service<&WithConnectionGuard<WithClientIp<AddrStream>>> for RouterService2<B, E>
{
    type Response = RequestService<B, E>;
    type Error = Infallible;
    type Future = Ready<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, conn: &WithConnectionGuard<WithClientIp<AddrStream>>) -> Self::Future {
        let req_service = self.builder.build(conn.inner.inner.remote_addr());

        ready(Ok(req_service))
    }
}
