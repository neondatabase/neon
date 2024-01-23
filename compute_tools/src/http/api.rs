use std::convert::Infallible;
use std::net::IpAddr;
use std::net::Ipv6Addr;
use std::net::SocketAddr;
use std::sync::Arc;
use std::thread;

use crate::compute::{ComputeNode, ComputeState, ParsedSpec};
use bytes::Bytes;
use compute_api::requests::ConfigurationRequest;
use compute_api::responses::{ComputeStatus, ComputeStatusResponse, GenericAPIError};

use anyhow::Result;
use http_body_util::BodyExt;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioExecutor;
use hyper_util::rt::TokioIo;
use hyper_util::server::conn;
use num_cpus;
use serde_json;
use tokio::net::TcpListener;
use tokio::task;
use tracing::{error, info, warn};
use tracing_utils::http::OtelName;

fn status_response_from_state(state: &ComputeState) -> ComputeStatusResponse {
    ComputeStatusResponse {
        start_time: state.start_time,
        tenant: state
            .pspec
            .as_ref()
            .map(|pspec| pspec.tenant_id.to_string()),
        timeline: state
            .pspec
            .as_ref()
            .map(|pspec| pspec.timeline_id.to_string()),
        status: state.status,
        last_active: state.last_active,
        error: state.error.clone(),
    }
}

// Service function to handle all available routes.
async fn routes(req: Request<Incoming>, compute: &Arc<ComputeNode>) -> Response<Full<Bytes>> {
    //
    // NOTE: The URI path is currently included in traces. That's OK because
    // it doesn't contain any variable parts or sensitive information. But
    // please keep that in mind if you change the routing here.
    //
    match (req.method(), req.uri().path()) {
        // Serialized compute state.
        (&Method::GET, "/status") => {
            info!("serving /status GET request");
            let state = compute.state.lock().unwrap();
            let status_response = status_response_from_state(&state);
            Response::new(Full::from(serde_json::to_string(&status_response).unwrap()))
        }

        // Startup metrics in JSON format. Keep /metrics reserved for a possible
        // future use for Prometheus metrics format.
        (&Method::GET, "/metrics.json") => {
            info!("serving /metrics.json GET request");
            let metrics = compute.state.lock().unwrap().metrics.clone();
            Response::new(Full::from(serde_json::to_string(&metrics).unwrap()))
        }

        // Collect Postgres current usage insights
        (&Method::GET, "/insights") => {
            info!("serving /insights GET request");
            let status = compute.get_status();
            if status != ComputeStatus::Running {
                let msg = format!("compute is not running, current status: {:?}", status);
                error!(msg);
                return Response::new(Full::from(msg));
            }

            let insights = compute.collect_insights().await;
            Response::new(Full::from(insights))
        }

        (&Method::POST, "/check_writability") => {
            info!("serving /check_writability POST request");
            let status = compute.get_status();
            if status != ComputeStatus::Running {
                let msg = format!(
                    "invalid compute status for check_writability request: {:?}",
                    status
                );
                error!(msg);
                return Response::new(Full::from(msg));
            }

            let res = crate::checker::check_writability(compute).await;
            match res {
                Ok(_) => Response::new(Full::from("true")),
                Err(e) => {
                    error!("check_writability failed: {}", e);
                    Response::new(Full::from(e.to_string()))
                }
            }
        }

        (&Method::GET, "/info") => {
            let num_cpus = num_cpus::get_physical();
            info!("serving /info GET request. num_cpus: {}", num_cpus);
            Response::new(Full::from(
                serde_json::json!({
                    "num_cpus": num_cpus,
                })
                .to_string(),
            ))
        }

        // Accept spec in JSON format and request compute configuration. If
        // anything goes wrong after we set the compute status to `ConfigurationPending`
        // and update compute state with new spec, we basically leave compute
        // in the potentially wrong state. That said, it's control-plane's
        // responsibility to watch compute state after reconfiguration request
        // and to clean restart in case of errors.
        (&Method::POST, "/configure") => {
            info!("serving /configure POST request");
            match handle_configure_request(req, compute).await {
                Ok(msg) => Response::new(Full::from(msg)),
                Err((msg, code)) => {
                    error!("error handling /configure request: {msg}");
                    render_json_error(&msg, code)
                }
            }
        }

        // download extension files from remote extension storage on demand
        (&Method::POST, route) if route.starts_with("/extension_server/") => {
            info!("serving {:?} POST request", route);
            info!("req.uri {:?}", req.uri());

            // don't even try to download extensions
            // if no remote storage is configured
            if compute.ext_remote_storage.is_none() {
                info!("no extensions remote storage configured");
                let mut resp = Response::new(Full::from("no remote storage configured"));
                *resp.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                return resp;
            }

            let mut is_library = false;
            if let Some(params) = req.uri().query() {
                info!("serving {:?} POST request with params: {}", route, params);
                if params == "is_library=true" {
                    is_library = true;
                } else {
                    let mut resp = Response::new(Full::from("Wrong request parameters"));
                    *resp.status_mut() = StatusCode::BAD_REQUEST;
                    return resp;
                }
            }
            let filename = route.split('/').last().unwrap().to_string();
            info!("serving /extension_server POST request, filename: {filename:?} is_library: {is_library}");

            // get ext_name and path from spec
            // don't lock compute_state for too long
            let ext = {
                let compute_state = compute.state.lock().unwrap();
                let pspec = compute_state.pspec.as_ref().expect("spec must be set");
                let spec = &pspec.spec;

                // debug only
                info!("spec: {:?}", spec);

                let remote_extensions = match spec.remote_extensions.as_ref() {
                    Some(r) => r,
                    None => {
                        info!("no remote extensions spec was provided");
                        let mut resp = Response::new(Full::from("no remote storage configured"));
                        *resp.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                        return resp;
                    }
                };

                remote_extensions.get_ext(
                    &filename,
                    is_library,
                    &compute.build_tag,
                    &compute.pgversion,
                )
            };

            match ext {
                Ok((ext_name, ext_path)) => {
                    match compute.download_extension(ext_name, ext_path).await {
                        Ok(_) => Response::new(Full::from("OK")),
                        Err(e) => {
                            error!("extension download failed: {}", e);
                            let mut resp = Response::new(Full::from(e.to_string()));
                            *resp.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                            resp
                        }
                    }
                }
                Err(e) => {
                    warn!("extension download failed to find extension: {}", e);
                    let mut resp = Response::new(Full::from("failed to find file"));
                    *resp.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                    resp
                }
            }
        }

        // Return the `404 Not Found` for any other routes.
        _ => {
            let mut not_found = Response::new(Full::from("404 Not Found"));
            *not_found.status_mut() = StatusCode::NOT_FOUND;
            not_found
        }
    }
}

async fn handle_configure_request(
    req: Request<Incoming>,
    compute: &Arc<ComputeNode>,
) -> Result<String, (String, StatusCode)> {
    if !compute.live_config_allowed {
        return Err((
            "live configuration is not allowed for this compute node".to_string(),
            StatusCode::PRECONDITION_FAILED,
        ));
    }

    let body_bytes = req.into_body().collect().await.unwrap().to_bytes();
    let spec_raw = String::from_utf8(body_bytes.to_vec()).unwrap();
    if let Ok(request) = serde_json::from_str::<ConfigurationRequest>(&spec_raw) {
        let spec = request.spec;

        let parsed_spec = match ParsedSpec::try_from(spec) {
            Ok(ps) => ps,
            Err(msg) => return Err((msg, StatusCode::BAD_REQUEST)),
        };

        // XXX: wrap state update under lock in code blocks. Otherwise,
        // we will try to `Send` `mut state` into the spawned thread
        // bellow, which will cause error:
        // ```
        // error: future cannot be sent between threads safely
        // ```
        {
            let mut state = compute.state.lock().unwrap();
            if state.status != ComputeStatus::Empty && state.status != ComputeStatus::Running {
                let msg = format!(
                    "invalid compute status for configuration request: {:?}",
                    state.status.clone()
                );
                return Err((msg, StatusCode::PRECONDITION_FAILED));
            }
            state.pspec = Some(parsed_spec);
            state.status = ComputeStatus::ConfigurationPending;
            compute.state_changed.notify_all();
            drop(state);
            info!("set new spec and notified waiters");
        }

        // Spawn a blocking thread to wait for compute to become Running.
        // This is needed to do not block the main pool of workers and
        // be able to serve other requests while some particular request
        // is waiting for compute to finish configuration.
        let c = compute.clone();
        task::spawn_blocking(move || {
            let mut state = c.state.lock().unwrap();
            while state.status != ComputeStatus::Running {
                state = c.state_changed.wait(state).unwrap();
                info!(
                    "waiting for compute to become Running, current status: {:?}",
                    state.status
                );

                if state.status == ComputeStatus::Failed {
                    let err = state.error.as_ref().map_or("unknown error", |x| x);
                    let msg = format!("compute configuration failed: {:?}", err);
                    return Err((msg, StatusCode::INTERNAL_SERVER_ERROR));
                }
            }

            Ok(())
        })
        .await
        .unwrap()?;

        // Return current compute state if everything went well.
        let state = compute.state.lock().unwrap().clone();
        let status_response = status_response_from_state(&state);
        Ok(serde_json::to_string(&status_response).unwrap())
    } else {
        Err(("invalid spec".to_string(), StatusCode::BAD_REQUEST))
    }
}

fn render_json_error(e: &str, status: StatusCode) -> Response<Full<Bytes>> {
    let error = GenericAPIError {
        error: e.to_string(),
    };
    Response::builder()
        .status(status)
        .body(Full::from(serde_json::to_string(&error).unwrap()))
        .unwrap()
}

// Main Hyper HTTP server function that runs it and blocks waiting on it forever.
#[tokio::main]
async fn serve(port: u16, state: Arc<ComputeNode>) {
    // this usually binds to both IPv4 and IPv6 on linux
    // see e.g. https://github.com/rust-lang/rust/pull/34440
    let addr = SocketAddr::new(IpAddr::from(Ipv6Addr::UNSPECIFIED), port);

    let service = service_fn(move |req: Request<Incoming>| {
        let state = state.clone();
        async move {
            Ok::<_, Infallible>(
                // NOTE: We include the URI path in the string. It
                // doesn't contain any variable parts or sensitive
                // information in this API.
                tracing_utils::http::tracing_handler(
                    req,
                    |req| routes(req, &state),
                    OtelName::UriPath,
                )
                .await,
            )
        }
    });

    info!("starting HTTP server on {}", addr);

    let listener = TcpListener::bind(addr).await.unwrap();
    loop {
        let (stream, _) = match listener.accept().await {
            Ok(r) => r,
            Err(e) => {
                error!("server error: {}", e);
                return;
            }
        };
        let io = TokioIo::new(stream);
        let service = service.clone();
        tokio::task::spawn(async move {
            let builder = conn::auto::Builder::new(TokioExecutor::new());
            let res = builder.serve_connection(io, service).await;
            if let Err(err) = res {
                println!("Error serving connection: {:?}", err);
            }
        });
    }
}

/// Launch a separate Hyper HTTP API server thread and return its `JoinHandle`.
pub fn launch_http_server(port: u16, state: &Arc<ComputeNode>) -> Result<thread::JoinHandle<()>> {
    let state = Arc::clone(state);

    Ok(thread::Builder::new()
        .name("http-endpoint".into())
        .spawn(move || serve(port, state))?)
}
