use crate::console;
use crate::console::messages::KickSession;
use crate::waiters::NotifyError;
use anyhow::{anyhow, Context};
use hyper::{body, Body, Request, Response, StatusCode};
use std::net::TcpListener;
use tracing::info;
use utils::http::{endpoint, error::ApiError, json::json_response, RouterBuilder, RouterService};

async fn status_handler(_: Request<Body>) -> Result<Response<Body>, ApiError> {
    json_response(StatusCode::OK, "")
}

/// Process a session kick callback from the control plane. The body is a
/// KickSession as a JSON document.
///
/// TODO: authentication
async fn kick_session_handler(req: Request<Body>) -> Result<Response<Body>, ApiError> {
    let body = &body::to_bytes(req.into_body())
        .await
        .context("Failed to get request body")
        .map_err(ApiError::BadRequest)?;
    let kick_session_json: KickSession = serde_json::from_slice(body)
        .context("Failed to parse query as json")
        .map_err(ApiError::BadRequest)?;

    match console::mgmt::notify(kick_session_json.session_id, Ok(kick_session_json.result)) {
        Ok(()) => json_response(StatusCode::OK, ""),
        Err(NotifyError::NotFound(s)) => Err(ApiError::NotFound(anyhow::anyhow!(s))),
        Err(e @ NotifyError::Hangup) => Err(ApiError::NotFound(anyhow::anyhow!(e))),
    }
}

fn make_router() -> RouterBuilder<hyper::Body, ApiError> {
    endpoint::make_router()
        .get("/v1/status", status_handler)
        .post("/v1/kick_session", kick_session_handler)
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
