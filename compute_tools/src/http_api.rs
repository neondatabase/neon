use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use std::thread;

use anyhow::Result;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Method, Request, Response, Server, StatusCode};
use log::{error, info};

use crate::zenith::*;

// Service function to handle all available routes.
fn routes(req: Request<Body>, state: Arc<RwLock<ComputeState>>) -> Response<Body> {
    match (req.method(), req.uri().path()) {
        // Timestamp of the last Postgres activity in the plain text.
        (&Method::GET, "/last_activity") => {
            info!("serving /last_active GET request");
            let state = state.read().unwrap();

            // Use RFC3339 format for consistency.
            Response::new(Body::from(state.last_active.to_rfc3339()))
        }

        // Has compute setup process finished? -> true/false
        (&Method::GET, "/ready") => {
            info!("serving /ready GET request");
            let state = state.read().unwrap();
            Response::new(Body::from(format!("{}", state.ready)))
        }

        // Return the `404 Not Found` for any other routes.
        _ => {
            let mut not_found = Response::new(Body::from("404 Not Found"));
            *not_found.status_mut() = StatusCode::NOT_FOUND;
            not_found
        }
    }
}

// Main Hyper HTTP server function that runs it and blocks waiting on it forever.
#[tokio::main]
async fn serve(state: Arc<RwLock<ComputeState>>) {
    let addr = SocketAddr::from(([0, 0, 0, 0], 3080));

    let make_service = make_service_fn(move |_conn| {
        let state = state.clone();
        async move {
            Ok::<_, Infallible>(service_fn(move |req: Request<Body>| {
                let state = state.clone();
                async move { Ok::<_, Infallible>(routes(req, state)) }
            }))
        }
    });

    info!("starting HTTP server on {}", addr);

    let server = Server::bind(&addr).serve(make_service);

    // Run this server forever
    if let Err(e) = server.await {
        error!("server error: {}", e);
    }
}

/// Launch a separate Hyper HTTP API server thread and return its `JoinHandle`.
pub fn launch_http_server(state: &Arc<RwLock<ComputeState>>) -> Result<thread::JoinHandle<()>> {
    let state = Arc::clone(state);

    Ok(thread::Builder::new()
        .name("http-endpoint".into())
        .spawn(move || serve(state))?)
}
