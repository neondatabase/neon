/// The attachment service mimics the aspects of the control plane API
/// that are required for a pageserver to operate.
///
/// This enables running & testing pageservers without a full-blown
/// deployment of the Neon cloud platform.
///
use anyhow::anyhow;
use clap::Parser;
use hex::FromHex;
use hyper::StatusCode;
use hyper::{Body, Request, Response};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::{collections::HashMap, sync::Arc};
use utils::logging::{self, LogFormat};

use utils::{
    http::{
        endpoint::{self},
        error::ApiError,
        json::{json_request, json_response},
        RequestExt, RouterBuilder,
    },
    id::{NodeId, TenantId},
    tcp_listener,
};

use pageserver_api::control_api::{
    ReAttachRequest, ReAttachResponse, ReAttachResponseTenant, ValidateRequest, ValidateResponse,
    ValidateResponseTenant,
};

use control_plane::attachment_service::{AttachHookRequest, AttachHookResponse};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(arg_required_else_help(true))]
struct Cli {
    /// Host and port to listen on, like `127.0.0.1:1234`
    #[arg(short, long)]
    listen: std::net::SocketAddr,

    /// Path to the .json file to store state (will be created if it doesn't exist)
    #[arg(short, long)]
    path: PathBuf,
}

// The persistent state of each Tenant
#[derive(Serialize, Deserialize, Clone)]
struct TenantState {
    // Currently attached pageserver
    pageserver: Option<NodeId>,

    // Latest generation number: next time we attach, increment this
    // and use the incremented number when attaching
    generation: u32,
}

fn to_hex_map<S, V>(input: &HashMap<TenantId, V>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
    V: Clone + Serialize,
{
    let transformed = input.iter().map(|(k, v)| (hex::encode(k), v.clone()));

    transformed
        .collect::<HashMap<String, V>>()
        .serialize(serializer)
}

fn from_hex_map<'de, D, V>(deserializer: D) -> Result<HashMap<TenantId, V>, D::Error>
where
    D: serde::de::Deserializer<'de>,
    V: Deserialize<'de>,
{
    let hex_map = HashMap::<String, V>::deserialize(deserializer)?;
    hex_map
        .into_iter()
        .map(|(k, v)| {
            TenantId::from_hex(k)
                .map(|k| (k, v))
                .map_err(serde::de::Error::custom)
        })
        .collect()
}

// Top level state available to all HTTP handlers
#[derive(Serialize, Deserialize)]
struct PersistentState {
    #[serde(serialize_with = "to_hex_map", deserialize_with = "from_hex_map")]
    tenants: HashMap<TenantId, TenantState>,

    #[serde(skip)]
    path: PathBuf,
}

impl PersistentState {
    async fn save(&self) -> anyhow::Result<()> {
        let bytes = serde_json::to_vec(self)?;
        tokio::fs::write(&self.path, &bytes).await?;

        Ok(())
    }

    async fn load(path: &Path) -> anyhow::Result<Self> {
        let bytes = tokio::fs::read(path).await?;
        let mut decoded = serde_json::from_slice::<Self>(&bytes)?;
        decoded.path = path.to_owned();
        Ok(decoded)
    }

    async fn load_or_new(path: &Path) -> Self {
        match Self::load(path).await {
            Ok(s) => {
                tracing::info!("Loaded state file at {}", path.display());
                s
            }
            Err(e)
                if e.downcast_ref::<std::io::Error>()
                    .map(|e| e.kind() == std::io::ErrorKind::NotFound)
                    .unwrap_or(false) =>
            {
                tracing::info!("Will create state file at {}", path.display());
                Self {
                    tenants: HashMap::new(),
                    path: path.to_owned(),
                }
            }
            Err(e) => {
                panic!("Failed to load state from '{}': {e:#} (maybe your .neon/ dir was written by an older version?)", path.display())
            }
        }
    }
}

/// State available to HTTP request handlers
#[derive(Clone)]
struct State {
    inner: Arc<tokio::sync::RwLock<PersistentState>>,
}

impl State {
    fn new(persistent_state: PersistentState) -> State {
        Self {
            inner: Arc::new(tokio::sync::RwLock::new(persistent_state)),
        }
    }
}

#[inline(always)]
fn get_state(request: &Request<Body>) -> &State {
    request
        .data::<Arc<State>>()
        .expect("unknown state type")
        .as_ref()
}

/// Pageserver calls into this on startup, to learn which tenants it should attach
async fn handle_re_attach(mut req: Request<Body>) -> Result<Response<Body>, ApiError> {
    let reattach_req = json_request::<ReAttachRequest>(&mut req).await?;

    let state = get_state(&req).inner.clone();
    let mut locked = state.write().await;

    let mut response = ReAttachResponse {
        tenants: Vec::new(),
    };
    for (t, state) in &mut locked.tenants {
        if state.pageserver == Some(reattach_req.node_id) {
            state.generation += 1;
            response.tenants.push(ReAttachResponseTenant {
                id: *t,
                generation: state.generation,
            });
        }
    }

    locked.save().await.map_err(ApiError::InternalServerError)?;

    json_response(StatusCode::OK, response)
}

/// Pageserver calls into this before doing deletions, to confirm that it still
/// holds the latest generation for the tenants with deletions enqueued
async fn handle_validate(mut req: Request<Body>) -> Result<Response<Body>, ApiError> {
    let validate_req = json_request::<ValidateRequest>(&mut req).await?;

    let locked = get_state(&req).inner.read().await;

    let mut response = ValidateResponse {
        tenants: Vec::new(),
    };

    for req_tenant in validate_req.tenants {
        if let Some(tenant_state) = locked.tenants.get(&req_tenant.id) {
            let valid = tenant_state.generation == req_tenant.gen;
            response.tenants.push(ValidateResponseTenant {
                id: req_tenant.id,
                valid,
            });
        }
    }

    json_response(StatusCode::OK, response)
}
/// Call into this before attaching a tenant to a pageserver, to acquire a generation number
/// (in the real control plane this is unnecessary, because the same program is managing
///  generation numbers and doing attachments).
async fn handle_attach_hook(mut req: Request<Body>) -> Result<Response<Body>, ApiError> {
    let attach_req = json_request::<AttachHookRequest>(&mut req).await?;

    let state = get_state(&req).inner.clone();
    let mut locked = state.write().await;

    let tenant_state = locked
        .tenants
        .entry(attach_req.tenant_id)
        .or_insert_with(|| TenantState {
            pageserver: attach_req.pageserver_id,
            generation: 0,
        });

    if attach_req.pageserver_id.is_some() {
        tenant_state.generation += 1;
    }
    let generation = tenant_state.generation;

    locked.save().await.map_err(ApiError::InternalServerError)?;

    json_response(
        StatusCode::OK,
        AttachHookResponse {
            gen: attach_req.pageserver_id.map(|_| generation),
        },
    )
}

fn make_router(persistent_state: PersistentState) -> RouterBuilder<hyper::Body, ApiError> {
    endpoint::make_router()
        .data(Arc::new(State::new(persistent_state)))
        .post("/re-attach", handle_re_attach)
        .post("/validate", handle_validate)
        .post("/attach_hook", handle_attach_hook)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    logging::init(
        LogFormat::Plain,
        logging::TracingErrorLayerEnablement::Disabled,
    )?;

    let args = Cli::parse();
    tracing::info!(
        "Starting, state at {}, listening on {}",
        args.path.to_string_lossy(),
        args.listen
    );

    let persistent_state = PersistentState::load_or_new(&args.path).await;

    let http_listener = tcp_listener::bind(args.listen)?;
    let router = make_router(persistent_state)
        .build()
        .map_err(|err| anyhow!(err))?;
    let service = utils::http::RouterService::new(router).unwrap();
    let server = hyper::Server::from_tcp(http_listener)?.serve(service);

    tracing::info!("Serving on {0}", args.listen);
    server.await?;

    Ok(())
}
