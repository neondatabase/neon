//! Mock proxy control plane API.
//!
//! Two modes for `/wake_compute`:
//!
//! - **Static mode** (default): returns the fixed `--compute-address`. Useful for
//!   benchmarks where one long-running compute backs every request.
//! - **neon_local mode** (`--neon-local-repo-dir`): looks the endpoint up in
//!   `<repo-dir>/endpoints/<id>/endpoint.json`, returns the per-endpoint
//!   `pg_port`, and shells out to `neon_local endpoint start <id>` if the
//!   compute isn't already listening. Returns 404 if the endpoint doesn't
//!   exist.
//!
//! `/get_endpoint_access_control` returns a single static SCRAM secret
//! (`--scram-secret`) for every role; operators wire matching credentials by
//! creating compute roles whose stored `pg_authid.rolpassword` is the same
//! literal string.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use clap::Parser;
use serde::{Deserialize, Serialize};
use tokio::signal::unix::{SignalKind, signal};

/// Pre-hashed SCRAM-SHA-256 secret for the literal password `"password"`.
/// Mirrors the constant used by hadron's `proxy-bench/cplane-mock`.
const DEFAULT_SCRAM_SECRET: &str = "SCRAM-SHA-256$4096:M2ZX/kfDSd3vv5iFO/QNUA==$mookt3EiEpd/vMqGbd7df3qVwfyUfM91Ps72sNewNg4=:3nMi8eBSHggIBNSgAik6lQnE3hQcsS+myylZlYgNA1U=";

#[derive(Clone)]
struct AppState {
    scram_secret: String,
    compute_address: String,
    jwks_config: Option<JwksConfig>,
    neon_local: Option<NeonLocalConfig>,
}

#[derive(Clone)]
struct NeonLocalConfig {
    repo_dir: PathBuf,
    bin: PathBuf,
    start_timeout: Duration,
}

#[derive(Clone, Deserialize)]
struct JwksConfig {
    jwks: Vec<JwksEntry>,
}

#[derive(Clone, Serialize, Deserialize)]
struct JwksEntry {
    id: String,
    role_names: Vec<String>,
    jwks_url: String,
    provider_name: String,
    jwt_audience: Option<String>,
}

#[derive(Parser)]
struct Args {
    /// Static SCRAM-SHA-256 secret returned by `/get_endpoint_access_control`
    /// for every role. The default matches the literal password "password".
    #[clap(long, default_value = DEFAULT_SCRAM_SECRET)]
    scram_secret: String,

    /// Compute address returned by `/wake_compute` in static mode.
    /// Ignored when `--neon-local-repo-dir` is set.
    #[clap(long, default_value = "127.0.0.1:5432")]
    compute_address: String,

    #[clap(long, default_value = "0.0.0.0:3010")]
    listen: SocketAddr,

    /// Path to a JSON file with JWKS config (same format as local_proxy.json).
    /// If provided, the /endpoints/{id}/jwks endpoint will serve these JWKS.
    #[clap(long)]
    jwks_config: Option<PathBuf>,

    /// Path to a `neon_local` `.neon` data directory. When set, `/wake_compute`
    /// resolves the compute address by reading endpoint configs from this
    /// directory and starts the endpoint via `neon_local` if it isn't running.
    #[clap(long)]
    neon_local_repo_dir: Option<PathBuf>,

    /// Path to the `neon_local` binary. Defaults to looking up `neon_local`
    /// on PATH.
    #[clap(long, default_value = "neon_local")]
    neon_local_bin: PathBuf,

    /// How long to wait for a compute to come up after spawning
    /// `neon_local endpoint start`.
    #[clap(long, default_value = "30s", value_parser = humantime::parse_duration)]
    start_timeout: Duration,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let jwks_config = if let Some(path) = &args.jwks_config {
        let data = std::fs::read_to_string(path)
            .unwrap_or_else(|e| panic!("failed to read jwks config file {}: {e}", path.display()));
        let config: JwksConfig = serde_json::from_str(&data)
            .unwrap_or_else(|e| panic!("failed to parse jwks config file {}: {e}", path.display()));
        eprintln!(
            "loaded {} JWKS entries from {}",
            config.jwks.len(),
            path.display()
        );
        Some(config)
    } else {
        None
    };

    let neon_local = args.neon_local_repo_dir.map(|repo_dir| {
        let repo_dir = repo_dir
            .canonicalize()
            .unwrap_or_else(|e| panic!("--neon-local-repo-dir {}: {e}", repo_dir.display()));
        eprintln!("neon_local mode enabled, repo dir: {}", repo_dir.display());
        NeonLocalConfig {
            repo_dir,
            bin: args.neon_local_bin,
            start_timeout: args.start_timeout,
        }
    });

    let state = AppState {
        scram_secret: args.scram_secret,
        compute_address: args.compute_address,
        jwks_config,
        neon_local,
    };

    let app = axum::Router::new()
        .route(
            "/get_endpoint_access_control",
            get(get_endpoint_access_control),
        )
        .route("/wake_compute", get(wake_compute))
        .route("/endpoints/{id}/jwks", get(get_jwks))
        .with_state(state);

    let mut sigterm = signal(SignalKind::terminate()).unwrap();
    let listener = tokio::net::TcpListener::bind(args.listen).await.unwrap();
    eprintln!("listening on {}", args.listen);
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            sigterm.recv().await;
        })
        .await
        .unwrap();
}

#[derive(Deserialize)]
struct AccessControlQuery {
    #[allow(dead_code)]
    role: Option<String>,
    #[allow(dead_code)]
    endpointish: Option<String>,
}

#[derive(Serialize)]
struct AccessControlResponse {
    role_secret: String,
    allowed_ips: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    project_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    account_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    allowed_vpc_endpoint_ids: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    block_public_connections: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    block_vpc_connections: Option<bool>,
}

async fn get_endpoint_access_control(
    State(state): State<AppState>,
    Query(_query): Query<AccessControlQuery>,
) -> Json<AccessControlResponse> {
    Json(AccessControlResponse {
        role_secret: state.scram_secret.clone(),
        allowed_ips: vec![],
        project_id: None,
        account_id: None,
        allowed_vpc_endpoint_ids: None,
        block_public_connections: None,
        block_vpc_connections: None,
    })
}

#[derive(Deserialize)]
struct WakeComputeQuery {
    endpointish: Option<String>,
}

#[derive(Serialize)]
struct WakeComputeResponse {
    address: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    server_name: Option<String>,
    aux: MetricsAuxInfo,
}

#[derive(Serialize)]
struct MetricsAuxInfo {
    endpoint_id: String,
    project_id: String,
    branch_id: String,
    compute_id: String,
    cold_start_info: String,
}

async fn wake_compute(
    State(state): State<AppState>,
    Query(query): Query<WakeComputeQuery>,
) -> Result<Json<WakeComputeResponse>, ControlPlaneErrorResponse> {
    let endpoint_id = query.endpointish.unwrap_or_default();

    let (address, cold_start_info) = match &state.neon_local {
        Some(cfg) => resolve_via_neon_local(cfg, &endpoint_id).await?,
        None => (state.compute_address.clone(), "warm".to_owned()),
    };

    Ok(Json(WakeComputeResponse {
        address,
        server_name: None,
        aux: MetricsAuxInfo {
            endpoint_id,
            project_id: "project-mock".into(),
            branch_id: "branch-mock".into(),
            compute_id: "compute-mock".into(),
            cold_start_info,
        },
    }))
}

/// Subset of the `endpoint.json` written by `neon_local`. Only the field we
/// need is parsed; serde ignores everything else.
#[derive(Deserialize)]
struct EndpointConf {
    pg_port: u16,
}

async fn resolve_via_neon_local(
    cfg: &NeonLocalConfig,
    endpoint_id: &str,
) -> Result<(String, String), ControlPlaneErrorResponse> {
    if endpoint_id.is_empty() {
        return Err(ControlPlaneErrorResponse::endpoint_not_found(
            "missing endpointish query parameter",
        ));
    }

    let conf_path = cfg
        .repo_dir
        .join("endpoints")
        .join(endpoint_id)
        .join("endpoint.json");

    let conf_bytes = match std::fs::read(&conf_path) {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Err(ControlPlaneErrorResponse::endpoint_not_found(&format!(
                "endpoint {endpoint_id} not found at {}",
                conf_path.display()
            )));
        }
        Err(e) => {
            return Err(ControlPlaneErrorResponse::internal(format!(
                "failed to read {}: {e}",
                conf_path.display()
            )));
        }
    };

    let conf: EndpointConf = serde_json::from_slice(&conf_bytes).map_err(|e| {
        ControlPlaneErrorResponse::internal(format!("failed to parse {}: {e}", conf_path.display()))
    })?;

    let addr = SocketAddr::from(([127, 0, 0, 1], conf.pg_port));
    let already_up =
        std::net::TcpStream::connect_timeout(&addr, Duration::from_millis(200)).is_ok();

    let cold_start_info = if already_up {
        "warm".to_owned()
    } else {
        eprintln!("starting endpoint {endpoint_id} via neon_local");
        let status = tokio::process::Command::new(&cfg.bin)
            .args(["endpoint", "start", endpoint_id])
            .env("NEON_REPO_DIR", &cfg.repo_dir)
            .status()
            .await
            .map_err(|e| {
                ControlPlaneErrorResponse::internal(format!(
                    "failed to spawn {}: {e}",
                    cfg.bin.display()
                ))
            })?;
        if !status.success() {
            return Err(ControlPlaneErrorResponse::internal(format!(
                "neon_local endpoint start {endpoint_id} exited with {status}"
            )));
        }
        wait_for_tcp(addr, cfg.start_timeout).await?;
        "pool_miss".to_owned()
    };

    Ok((addr.to_string(), cold_start_info))
}

async fn wait_for_tcp(
    addr: SocketAddr,
    timeout: Duration,
) -> Result<(), ControlPlaneErrorResponse> {
    let deadline = Instant::now() + timeout;
    loop {
        if std::net::TcpStream::connect_timeout(&addr, Duration::from_millis(200)).is_ok() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(ControlPlaneErrorResponse::internal(format!(
                "compute at {addr} did not become reachable within {timeout:?}"
            )));
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Mirrors the proxy-side `ControlPlaneErrorMessage` shape so proxy decodes
/// the error reason and treats `ENDPOINT_NOT_FOUND` as non-retryable.
#[derive(Serialize)]
struct ControlPlaneErrorBody {
    error: String,
    status: ControlPlaneErrorStatus,
}

#[derive(Serialize)]
struct ControlPlaneErrorStatus {
    code: &'static str,
    message: String,
    details: ControlPlaneErrorDetails,
}

#[derive(Serialize)]
struct ControlPlaneErrorDetails {
    error_info: Option<ControlPlaneErrorInfo>,
    retry_info: Option<()>,
    user_facing_message: Option<UserFacingMessage>,
}

#[derive(Serialize)]
struct ControlPlaneErrorInfo {
    reason: &'static str,
}

#[derive(Serialize)]
struct UserFacingMessage {
    message: String,
}

struct ControlPlaneErrorResponse {
    status: StatusCode,
    body: ControlPlaneErrorBody,
}

impl ControlPlaneErrorResponse {
    fn endpoint_not_found(message: &str) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            body: ControlPlaneErrorBody {
                error: message.to_owned(),
                status: ControlPlaneErrorStatus {
                    code: "NOT_FOUND",
                    message: message.to_owned(),
                    details: ControlPlaneErrorDetails {
                        error_info: Some(ControlPlaneErrorInfo {
                            reason: "ENDPOINT_NOT_FOUND",
                        }),
                        retry_info: None,
                        user_facing_message: Some(UserFacingMessage {
                            message: message.to_owned(),
                        }),
                    },
                },
            },
        }
    }

    fn internal(message: String) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            body: ControlPlaneErrorBody {
                error: message.clone(),
                status: ControlPlaneErrorStatus {
                    code: "INTERNAL",
                    message: message.clone(),
                    details: ControlPlaneErrorDetails {
                        error_info: None,
                        retry_info: None,
                        user_facing_message: Some(UserFacingMessage { message }),
                    },
                },
            },
        }
    }
}

impl IntoResponse for ControlPlaneErrorResponse {
    fn into_response(self) -> Response {
        (self.status, Json(self.body)).into_response()
    }
}

#[derive(Serialize)]
struct JwksResponse {
    jwks: Vec<JwksEntry>,
}

async fn get_jwks(State(state): State<AppState>, Path(_id): Path<String>) -> Json<JwksResponse> {
    let jwks = match &state.jwks_config {
        Some(config) => config.jwks.clone(),
        None => vec![],
    };
    Json(JwksResponse { jwks })
}
