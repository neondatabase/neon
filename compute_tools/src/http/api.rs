use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use std::thread;

use crate::compute::ComputeNode;
use compute_api::models::ComputeStatus;
use compute_api::spec::ComputeSpec;

use anyhow::Result;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Method, Request, Response, Server, StatusCode};
use num_cpus;
use serde_json;
use tokio::sync::mpsc::UnboundedSender;
use tracing::{error, info};
use tracing_utils::http::OtelName;

// Service function to handle all available routes.
async fn routes(
    req: Request<Body>,
    compute: &Arc<ComputeNode>,
    tx: &UnboundedSender<ComputeSpec>,
) -> Response<Body> {
    //
    // NOTE: The URI path is currently included in traces. That's OK because
    // it doesn't contain any variable parts or sensitive information. But
    // please keep that in mind if you change the routing here.
    //
    match (req.method(), req.uri().path()) {
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
            let metrics = compute.metrics.read().unwrap();
            Response::new(Body::from(serde_json::to_string(&*metrics).unwrap()))
        }

        // Collect Postgres current usage insights
        (&Method::GET, "/insights") => {
            info!("serving /insights GET request");
            let insights = compute.collect_insights().await;
            Response::new(Body::from(insights))
        }

        (&Method::POST, "/check_writability") => {
            info!("serving /check_writability POST request");
            let res = crate::checker::check_writability(compute).await;
            match res {
                Ok(_) => Response::new(Body::from("true")),
                Err(e) => Response::new(Body::from(e.to_string())),
            }
        }

        (&Method::GET, "/info") => {
            let num_cpus = num_cpus::get_physical();
            info!("serving /info GET request. num_cpus: {}", num_cpus);
            Response::new(Body::from(
                serde_json::json!({
                    "num_cpus": num_cpus,
                })
                .to_string(),
            ))
        }

        // Accept spec in JSON format and request compute reconfiguration from
        // the configurator thread. If anything goes wrong after we set the
        // compute state to `ConfigurationPending` and / or sent spec to the
        // configurator thread, we basically leave compute in the potentially
        // wrong state. That said, it's control-plane's responsibility to
        // watch compute state after reconfiguration request and to clean
        // restart in case of errors.
        //
        // TODO: Errors should be in JSON format
        (&Method::POST, "/spec") => {
            info!("serving /spec POST request");
            let body_bytes = hyper::body::to_bytes(req.into_body()).await.unwrap();
            let spec_raw = String::from_utf8(body_bytes.to_vec()).unwrap();
            if let Ok(spec) = serde_json::from_str::<ComputeSpec>(&spec_raw) {
                let mut state = compute.state.write().unwrap();
                if !(state.status == ComputeStatus::WaitingSpec
                    || state.status == ComputeStatus::Running)
                {
                    let msg = format!(
                        "invalid compute status for reconfiguration request: {}",
                        serde_json::to_string(&*state).unwrap()
                    );
                    error!(msg);
                    return Response::new(Body::from(msg));
                }
                state.status = ComputeStatus::ConfigurationPending;
                drop(state);

                if let Err(e) = tx.send(spec) {
                    error!("failed to send spec request to configurator thread: {}", e);
                    Response::new(Body::from(format!(
                        "could not request reconfiguration: {}",
                        e
                    )))
                } else {
                    info!("sent spec request to configurator");
                    Response::new(Body::from("ok"))
                }
            } else {
                let msg = "invalid spec";
                error!(msg);
                Response::new(Body::from(msg))
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
async fn serve(state: Arc<ComputeNode>, tx: UnboundedSender<ComputeSpec>) {
    let addr = SocketAddr::from(([0, 0, 0, 0], 3080));

    let make_service = make_service_fn(move |_conn| {
        let state = state.clone();
        let tx = tx.clone();
        async move {
            Ok::<_, Infallible>(service_fn(move |req: Request<Body>| {
                let state = state.clone();
                let tx = tx.clone();
                async move {
                    Ok::<_, Infallible>(
                        // NOTE: We include the URI path in the string. It
                        // doesn't contain any variable parts or sensitive
                        // information in this API.
                        tracing_utils::http::tracing_handler(
                            req,
                            |req| routes(req, &state, &tx),
                            OtelName::UriPath,
                        )
                        .await,
                    )
                }
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
pub fn launch_http_server(
    state: &Arc<ComputeNode>,
    tx: UnboundedSender<ComputeSpec>,
) -> Result<thread::JoinHandle<()>> {
    let state = Arc::clone(state);
    Ok(thread::Builder::new()
        .name("http-endpoint".into())
        .spawn(move || serve(state, tx))?)
}
