/// The attachment service mimics the aspects of the control plane API
/// that are required for a pageserver to operate.
///
/// This enables running & testing pageservers without a full-blown
/// deployment of the Neon cloud platform.
///
use anyhow::anyhow;
use clap::Parser;
use hyper::{Body, Request, Response};
use hyper::{Method, StatusCode};
use pageserver_api::models::{
    LocationConfig, LocationConfigMode, TenantConfig, TenantCreateRequest,
    TenantLocationConfigRequest, TimelineCreateRequest,
};
use pageserver_api::shard::{ShardCount, ShardIdentity, ShardNumber, TenantShardId};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use utils::http::endpoint::request_span;
use utils::http::request::parse_request_param;
use utils::id::TenantId;
use utils::logging::{self, LogFormat};
use utils::signals::{ShutdownSignals, Signal};

use utils::{
    http::{
        endpoint::{self},
        error::ApiError,
        json::{json_request, json_response},
        RequestExt, RouterBuilder,
    },
    id::NodeId,
    tcp_listener,
};

use pageserver_api::control_api::{
    ReAttachRequest, ReAttachResponse, ReAttachResponseTenant, ValidateRequest, ValidateResponse,
    ValidateResponseTenant,
};

use control_plane::attachment_service::{
    AttachHookRequest, AttachHookResponse, InspectRequest, InspectResponse, NodeRegisterRequest,
    TenantCreateResponse, TenantCreateResponseShard, TenantLocateResponse,
    TenantLocateResponseShard,
};

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

/// Our latest knowledge of how this tenant is configured in the outside world.
///
/// Meaning:
///     * No instance of this type exists for a node: we are certain that we have nothing configured on that
///       node for this shard.
///     * Instance exists with conf==None: we *might* have some state on that node, but we don't know
///       what it is (e.g. we failed partway through configuring it)
///     * Instance exists with conf==Some: this tells us what we last successfully configured on this node,
///       and that configuration will still be present unless something external interfered.
#[derive(Serialize, Deserialize)]
struct ObservedStateLocation {
    /// If None, it means we do not know the status of this shard's location on this node, but
    /// we know that we might have some state on this node.
    conf: Option<LocationConfig>,
}

#[derive(Serialize, Deserialize, Default)]
struct ObservedState {
    locations: HashMap<NodeId, ObservedStateLocation>,
}

#[derive(Serialize, Deserialize)]
struct TenantState {
    tenant_shard_id: TenantShardId,

    shard: ShardIdentity,

    // Currently attached pageserver
    pageserver: Option<NodeId>,

    // Latest generation number: next time we attach, increment this
    // and use the incremented number when attaching
    generation: u32,

    observed: ObservedState,

    config: TenantConfig,
}

#[derive(Serialize, Deserialize, Clone)]
struct NodeState {
    id: NodeId,

    listen_http_addr: String,
    listen_http_port: u16,

    listen_pg_addr: String,
    listen_pg_port: u16,
}

impl NodeState {
    fn base_url(&self) -> String {
        format!(
            "http://{}:{}/v1",
            self.listen_http_addr, self.listen_http_port
        )
    }
}

// Top level state available to all HTTP handlers
#[derive(Serialize, Deserialize)]
struct PersistentState {
    tenants: BTreeMap<TenantShardId, TenantState>,

    pageservers: HashMap<NodeId, NodeState>,

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
                    tenants: BTreeMap::new(),
                    pageservers: HashMap::new(),
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

impl TenantState {
    async fn location_config(
        &self,
        node: &NodeState,
        config: LocationConfig,
    ) -> anyhow::Result<()> {
        let configure_request = TenantLocationConfigRequest {
            tenant_shard_id: self.tenant_shard_id,
            config,
        };

        let client = Client::new();
        let response = client
            .request(
                Method::PUT,
                format!(
                    "{}/tenant/{}/location_config",
                    node.base_url(),
                    self.tenant_shard_id
                ),
            )
            .json(&configure_request)
            .send()
            .await?;
        response.error_for_status()?;

        Ok(())
    }

    async fn timeline_create(
        &self,
        node: &NodeState,
        req: &TimelineCreateRequest,
    ) -> anyhow::Result<()> {
        let client = Client::new();
        let response = client
            .request(
                Method::POST,
                format!(
                    "{}/tenant/{}/timeline",
                    node.base_url(),
                    self.tenant_shard_id
                ),
            )
            .json(req)
            .send()
            .await?;
        response.error_for_status()?;

        Ok(())
    }

    fn schedule(&mut self, scheduler: &mut Scheduler) -> Result<(), ScheduleError> {
        if self.pageserver.is_some() {
            return Ok(());
        }

        self.pageserver = Some(scheduler.schedule_shard()?);

        Ok(())
    }

    async fn reconcile(
        &mut self,
        pageservers: &HashMap<NodeId, NodeState>,
    ) -> Result<(), ReconcileError> {
        let wanted_conf = LocationConfig {
            mode: LocationConfigMode::AttachedSingle,
            generation: Some(self.generation),
            secondary_conf: None,
            shard_number: self.shard.number.0,
            shard_count: self.shard.count.0,
            shard_stripe_size: self.shard.stripe_size.0,
            tenant_conf: self.config.clone(),
        };

        match self.pageserver {
            Some(node_id) => {
                match self.observed.locations.get(&node_id) {
                    Some(conf) if conf.conf.as_ref() == Some(&wanted_conf) => {
                        // Nothing to do
                        tracing::info!("Observed configuration already correct.")
                    }
                    Some(_) | None => {
                        // If there is no observed configuration, or if its value does not equal our intent, then we must call out to the pageserver.
                        tracing::info!("Observed configuration requires update.");
                        let node = pageservers
                            .get(&node_id)
                            .expect("Pageserver may not be removed while referenced");
                        self.location_config(&node, wanted_conf).await?;
                    }
                }
            }
            None => {
                // Detach everything
                for (node_id, _old_state) in &self.observed.locations {
                    let node = pageservers
                        .get(node_id)
                        .expect("Pageserver may not be removed while referenced");
                    self.location_config(
                        &node,
                        LocationConfig {
                            mode: LocationConfigMode::Detached,
                            generation: None,
                            secondary_conf: None,
                            shard_number: self.shard.number.0,
                            shard_count: self.shard.count.0,
                            shard_stripe_size: self.shard.stripe_size.0,
                            tenant_conf: self.config.clone(),
                        },
                    )
                    .await?;
                }
            }
        }

        Ok(())
    }
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
                gen: state.generation,
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
            tracing::info!(
                "handle_validate: {}(gen {}): valid={valid} (latest {})",
                req_tenant.id,
                req_tenant.gen,
                tenant_state.generation
            );
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
        .entry(attach_req.tenant_shard_id)
        .or_insert_with(|| TenantState {
            tenant_shard_id: attach_req.tenant_shard_id,
            pageserver: attach_req.node_id,
            generation: 0,
            shard: ShardIdentity::unsharded(),
            observed: ObservedState::default(),
            config: TenantConfig::default(),
        });

    if let Some(attaching_pageserver) = attach_req.node_id.as_ref() {
        tenant_state.generation += 1;
        tracing::info!(
            tenant_id = %attach_req.tenant_shard_id,
            ps_id = %attaching_pageserver,
            generation = %tenant_state.generation,
            "issuing",
        );
    } else if let Some(ps_id) = tenant_state.pageserver {
        tracing::info!(
            tenant_id = %attach_req.tenant_shard_id,
            %ps_id,
            generation = %tenant_state.generation,
            "dropping",
        );
    } else {
        tracing::info!(
            tenant_id = %attach_req.tenant_shard_id,
            "no-op: tenant already has no pageserver");
    }
    tenant_state.pageserver = attach_req.node_id;
    let generation = tenant_state.generation;

    tracing::info!(
        "handle_attach_hook: tenant {} set generation {}, pageserver {}",
        attach_req.tenant_shard_id,
        tenant_state.generation,
        attach_req.node_id.unwrap_or(utils::id::NodeId(0xfffffff))
    );

    locked.save().await.map_err(ApiError::InternalServerError)?;

    json_response(
        StatusCode::OK,
        AttachHookResponse {
            gen: attach_req.node_id.map(|_| generation),
        },
    )
}

async fn handle_inspect(mut req: Request<Body>) -> Result<Response<Body>, ApiError> {
    let inspect_req = json_request::<InspectRequest>(&mut req).await?;

    let state = get_state(&req).inner.clone();
    let locked = state.write().await;
    let tenant_state = locked.tenants.get(&inspect_req.tenant_shard_id);

    json_response(
        StatusCode::OK,
        InspectResponse {
            attachment: tenant_state.and_then(|s| s.pageserver.map(|ps| (s.generation, ps))),
        },
    )
}

/// Scenarios in which we cannot find a suitable location for a tenant shard
#[derive(thiserror::Error, Debug)]
enum ScheduleError {
    #[error("No pageservers found")]
    NoPageservers,
}

impl From<ScheduleError> for ApiError {
    fn from(value: ScheduleError) -> Self {
        ApiError::Conflict(format!("Scheduling error: {}", value))
    }
}

#[derive(thiserror::Error, Debug)]
enum ReconcileError {
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl From<ReconcileError> for ApiError {
    fn from(value: ReconcileError) -> Self {
        ApiError::Conflict(format!("Reconciliation error: {}", value))
    }
}

struct Scheduler {
    tenant_counts: HashMap<NodeId, usize>,
}

impl Scheduler {
    fn new(persistent_state: &PersistentState) -> Self {
        let mut tenant_counts = HashMap::new();
        for (node_id, _) in &persistent_state.pageservers {
            tenant_counts.insert(*node_id, 0);
        }

        for (_id, tenant) in &persistent_state.tenants {
            if let Some(ps) = tenant.pageserver {
                let entry = tenant_counts.entry(ps).or_insert(0);
                *entry += 1;
            }
        }
        Self { tenant_counts }
    }

    fn schedule_shard(&mut self) -> Result<NodeId, ScheduleError> {
        if self.tenant_counts.is_empty() {
            return Err(ScheduleError::NoPageservers);
        }

        let mut tenant_counts: Vec<(NodeId, usize)> =
            self.tenant_counts.iter().map(|(k, v)| (*k, *v)).collect();
        tenant_counts.sort_by_key(|i| i.1);

        for (node_id, count) in &tenant_counts {
            tracing::info!("tenant_counts[{node_id}]={count}");
        }

        let node_id = tenant_counts.first().unwrap().0;
        tracing::info!("scheduler selected node {node_id}");
        *self.tenant_counts.get_mut(&node_id).unwrap() += 1;
        Ok(node_id)
    }
}

async fn handle_tenant_create(mut req: Request<Body>) -> Result<Response<Body>, ApiError> {
    let create_req = json_request::<TenantCreateRequest>(&mut req).await?;

    let state = get_state(&req).inner.clone();
    let mut locked = state.write().await;

    tracing::info!(
        "Creating tenant {}, have {} pageservers",
        create_req.new_tenant_id,
        locked.pageservers.len()
    );

    // This service expects to handle sharding itself: it is an error to try and directly create
    // a particular shard here.
    let tenant_id = if create_req.new_tenant_id.shard_count > ShardCount(1) {
        return Err(ApiError::BadRequest(anyhow::anyhow!(
            "Attempted to create a specific shard, this API is for creating the whole tenant"
        )));
    } else {
        create_req.new_tenant_id.tenant_id
    };

    // Shard count 0 is valid: it means create a single shard (ShardCount(0) means "unsharded")
    let literal_shard_count = if create_req.shard_parameters.is_unsharded() {
        1
    } else {
        create_req.shard_parameters.count.0
    };

    let mut response_shards = Vec::new();

    let mut scheduler = Scheduler::new(&locked);

    for i in 0..literal_shard_count {
        let shard_number = ShardNumber(i);

        let tenant_shard_id = TenantShardId {
            tenant_id,
            shard_number,
            shard_count: create_req.shard_parameters.count,
        };

        use std::collections::btree_map::Entry;
        match locked.tenants.entry(tenant_shard_id) {
            Entry::Occupied(mut entry) => {
                tracing::info!("Tenant shard {tenant_shard_id} already exists while creating");
                if entry.get_mut().pageserver.is_none() {
                    entry.get_mut().pageserver = Some(scheduler.schedule_shard().map_err(|e| {
                        ApiError::Conflict(format!(
                            "Failed to schedule shard {tenant_shard_id}: {e}"
                        ))
                    })?);
                }

                response_shards.push(TenantCreateResponseShard {
                    node_id: entry
                        .get()
                        .pageserver
                        .expect("We just set pageserver if it was None"),
                    generation: entry.get().generation,
                });

                continue;
            }
            Entry::Vacant(entry) => {
                let state = TenantState {
                    tenant_shard_id,
                    pageserver: Some(scheduler.schedule_shard().map_err(|e| {
                        ApiError::Conflict(format!(
                            "Failed to schedule shard {tenant_shard_id}: {e}"
                        ))
                    })?),
                    generation: create_req.generation.unwrap_or(1),
                    shard: ShardIdentity::from_params(shard_number, &create_req.shard_parameters),
                    observed: ObservedState::default(),
                    config: create_req.config.clone(),
                };
                response_shards.push(TenantCreateResponseShard {
                    node_id: state
                        .pageserver
                        .expect("We just set pageserver if it was None"),
                    generation: state.generation,
                });
                entry.insert(state)
            }
        };
    }

    // Take a snapshot of pageservers
    let pageservers = locked.pageservers.clone();

    for (tenant_shard_id, shard) in locked
        .tenants
        .range_mut(TenantShardId::tenant_range(tenant_id))
    {
        shard.reconcile(&pageservers).await.map_err(|e| {
            ApiError::Conflict(format!(
                "Failed to reconcile tenant shard {}: {}",
                tenant_shard_id, e
            ))
        })?;
    }

    json_response(
        StatusCode::OK,
        TenantCreateResponse {
            shards: response_shards,
        },
    )
}

async fn handle_tenant_timeline_create(mut req: Request<Body>) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&req, "tenant_id")?;
    let create_req = json_request::<TimelineCreateRequest>(&mut req).await?;

    let state = get_state(&req).inner.clone();
    let mut locked = state.write().await;

    tracing::info!(
        "Creating timeline {}/{}, have {} pageservers",
        tenant_id,
        create_req.new_timeline_id,
        locked.pageservers.len()
    );

    let mut scheduler = Scheduler::new(&locked);

    // Take a snapshot of pageservers
    let pageservers = locked.pageservers.clone();

    for (_tenant_shard_id, shard) in locked
        .tenants
        .range_mut(TenantShardId::tenant_range(tenant_id))
    {
        shard.schedule(&mut scheduler)?;
        shard.reconcile(&pageservers).await?;

        let node_id = shard.pageserver.expect("We just scheduled successfully");
        let node = pageservers
            .get(&node_id)
            .expect("Pageservers may not be deleted while referenced");

        shard
            .timeline_create(node, &create_req)
            .await
            .map_err(|e| ApiError::Conflict(format!("Failed to create timeline: {e}")))?;
    }

    json_response(StatusCode::OK, ())
}

async fn handle_tenant_locate(req: Request<Body>) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&req, "tenant_id")?;

    let state = get_state(&req).inner.clone();
    let mut locked = state.write().await;

    tracing::info!("Locating shards for tenant {tenant_id}");

    // Take a snapshot of pageservers
    let pageservers = locked.pageservers.clone();

    let mut result = Vec::new();

    for (tenant_shard_id, shard) in locked
        .tenants
        .range_mut(TenantShardId::tenant_range(tenant_id))
    {
        let node_id = shard
            .pageserver
            .ok_or(ApiError::BadRequest(anyhow::anyhow!(
                "Cannot locate a tenant that is not attached"
            )))?;

        let node = pageservers
            .get(&node_id)
            .expect("Pageservers may not be deleted while referenced");

        result.push(TenantLocateResponseShard {
            shard_id: *tenant_shard_id,
            node_id,
            listen_http_addr: node.listen_http_addr.clone(),
            listen_http_port: node.listen_http_port,
            listen_pg_addr: node.listen_pg_addr.clone(),
            listen_pg_port: node.listen_pg_port,
        });
    }

    json_response(StatusCode::OK, TenantLocateResponse { shards: result })
}

async fn handle_node_register(mut req: Request<Body>) -> Result<Response<Body>, ApiError> {
    let register_req = json_request::<NodeRegisterRequest>(&mut req).await?;
    let state = get_state(&req).inner.clone();
    let mut locked = state.write().await;

    locked.pageservers.insert(
        register_req.node_id,
        NodeState {
            id: register_req.node_id,
            listen_http_addr: register_req.listen_http_addr,
            listen_http_port: register_req.listen_http_port,
            listen_pg_addr: register_req.listen_pg_addr,
            listen_pg_port: register_req.listen_pg_port,
        },
    );

    tracing::info!(
        "Registered pageserver {}, now have {} pageservers",
        register_req.node_id,
        locked.pageservers.len()
    );

    json_response(StatusCode::OK, ())
}

/// Status endpoint is just used for checking that our HTTP listener is up
async fn handle_status(_req: Request<Body>) -> Result<Response<Body>, ApiError> {
    json_response(StatusCode::OK, ())
}

fn make_router(persistent_state: PersistentState) -> RouterBuilder<hyper::Body, ApiError> {
    endpoint::make_router()
        .data(Arc::new(State::new(persistent_state)))
        .get("/status", |r| request_span(r, handle_status))
        .post("/re-attach", |r| request_span(r, handle_re_attach))
        .post("/validate", |r| request_span(r, handle_validate))
        .post("/attach-hook", |r| request_span(r, handle_attach_hook))
        .post("/inspect", |r| request_span(r, handle_inspect))
        .post("/node", |r| request_span(r, handle_node_register))
        .post("/tenant", |r| request_span(r, handle_tenant_create))
        .post("/tenant/:tenant_id/timeline", |r| {
            request_span(r, handle_tenant_timeline_create)
        })
        .get("/tenant/:tenant_id/locate", |r| {
            request_span(r, handle_tenant_locate)
        })
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    logging::init(
        LogFormat::Plain,
        logging::TracingErrorLayerEnablement::Disabled,
        logging::Output::Stdout,
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

    tokio::task::spawn(server);

    ShutdownSignals::handle(|signal| match signal {
        Signal::Interrupt | Signal::Terminate | Signal::Quit => {
            tracing::info!("Got {}. Terminating", signal.name());
            // We're just a test helper: no graceful shutdown.
            std::process::exit(0);
        }
    })?;

    Ok(())
}
