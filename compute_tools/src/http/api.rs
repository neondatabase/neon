use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use std::thread;

use anyhow::Result;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Method, Request, Response, Server, StatusCode};
use log::{error, info};
use serde_json;

use crate::compute::{ComputeNode, ComputeStatus};

// Service function to handle all available routes.
async fn routes(req: Request<Body>, compute: Arc<ComputeNode>) -> Response<Body> {
    match (req.method(), req.uri().path()) {
        // Timestamp of the last Postgres activity in the plain text.
        // DEPRECATED in favour of /status
        (&Method::GET, "/last_activity") => {
            info!("serving /last_active GET request");
            let state = compute.state.read().unwrap();

            // Use RFC3339 format for consistency.
            Response::new(Body::from(state.last_active.to_rfc3339()))
        }

        // Has compute setup process finished? -> true/false.
        // DEPRECATED in favour of /status
        (&Method::GET, "/ready") => {
            info!("serving /ready GET request");
            let status = compute.get_status();
            Response::new(Body::from(format!("{}", status == ComputeStatus::Running)))
        }

        // Serialized compute state.
        (&Method::GET, "/status") => {
            info!("serving /status GET request");
            let state = compute.state.read().unwrap();
            Response::new(Body::from(serde_json::to_string(&*state).unwrap()))
        }

        // Startup metrics in JSON format. Keep /metrics reserved for a possible
        // future use for Prometheus metrics format.
        (&Method::GET, "/metrics.json") => {
            info!("serving /metrics.json GET request");
            Response::new(Body::from(serde_json::to_string(&compute.metrics).unwrap()))
        }

        // DEPRECATED, use POST instead
        (&Method::GET, "/check_writability") => {
            info!("serving /check_writability GET request");
            let res = crate::checker::check_writability(&compute).await;
            match res {
                Ok(_) => Response::new(Body::from("true")),
                Err(e) => Response::new(Body::from(e.to_string())),
            }
        }

        (&Method::POST, "/check_writability") => {
            info!("serving /check_writability POST request");
            let res = crate::checker::check_writability(&compute).await;
            match res {
                Ok(_) => Response::new(Body::from("true")),
                Err(e) => Response::new(Body::from(e.to_string())),
            }
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
async fn serve(state: Arc<ComputeNode>) {
    let addr = SocketAddr::from(([0, 0, 0, 0], 3080));

    let make_service = make_service_fn(move |_conn| {
        let state = state.clone();
        async move {
            Ok::<_, Infallible>(service_fn(move |req: Request<Body>| {
                let state = state.clone();
                async move { Ok::<_, Infallible>(routes(req, state).await) }
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
pub fn launch_http_server(state: &Arc<ComputeNode>) -> Result<thread::JoinHandle<()>> {
    let state = Arc::clone(state);

    Ok(thread::Builder::new()
        .name("http-endpoint".into())
        .spawn(move || serve(state))?)
}
