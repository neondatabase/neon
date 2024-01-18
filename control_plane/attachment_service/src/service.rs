use std::{
    collections::{BTreeMap, HashMap},
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};

use control_plane::attachment_service::{
    AttachHookRequest, AttachHookResponse, InspectRequest, InspectResponse, NodeAvailability,
    NodeConfigureRequest, NodeRegisterRequest, NodeSchedulingPolicy, TenantCreateResponse,
    TenantCreateResponseShard, TenantLocateResponse, TenantLocateResponseShard,
    TenantShardMigrateRequest, TenantShardMigrateResponse,
};
use diesel::result::DatabaseErrorKind;
use hyper::StatusCode;
use pageserver_api::{
    control_api::{
        ReAttachRequest, ReAttachResponse, ReAttachResponseTenant, ValidateRequest,
        ValidateResponse, ValidateResponseTenant,
    },
    models,
    models::{
        LocationConfig, LocationConfigMode, ShardParameters, TenantConfig, TenantCreateRequest,
        TimelineCreateRequest, TimelineInfo,
    },
    shard::{ShardCount, ShardIdentity, ShardNumber, ShardStripeSize, TenantShardId},
};
use pageserver_client::mgmt_api;
use utils::{
    completion::Barrier,
    generation::Generation,
    http::error::ApiError,
    id::{NodeId, TenantId, TimelineId},
    seqwait::SeqWait,
};

use crate::{
    compute_hook::ComputeHook,
    node::Node,
    persistence::{DatabaseError, NodePersistence, Persistence, TenantShardPersistence},
    scheduler::Scheduler,
    tenant_state::{
        IntentState, ObservedState, ObservedStateLocation, ReconcileResult, ReconcileWaitError,
        ReconcilerWaiter, TenantState,
    },
    PlacementPolicy, Sequence,
};

const RECONCILE_TIMEOUT: Duration = Duration::from_secs(30);

/// How long [`Service::startup_reconcile`] is allowed to take before it should give
/// up on unresponsive pageservers and proceed.
pub(crate) const STARTUP_RECONCILE_TIMEOUT: Duration = Duration::from_secs(30);

// Top level state available to all HTTP handlers
struct ServiceState {
    tenants: BTreeMap<TenantShardId, TenantState>,

    nodes: Arc<HashMap<NodeId, Node>>,

    compute_hook: Arc<ComputeHook>,

    result_tx: tokio::sync::mpsc::UnboundedSender<ReconcileResult>,
}

impl ServiceState {
    fn new(
        result_tx: tokio::sync::mpsc::UnboundedSender<ReconcileResult>,
        nodes: HashMap<NodeId, Node>,
        tenants: BTreeMap<TenantShardId, TenantState>,
    ) -> Self {
        Self {
            tenants,
            nodes: Arc::new(nodes),
            compute_hook: Arc::new(ComputeHook::new()),
            result_tx,
        }
    }
}

#[derive(Clone)]
pub struct Config {
    // All pageservers managed by one instance of this service must have
    // the same public key.
    pub jwt_token: Option<String>,
}

impl From<DatabaseError> for ApiError {
    fn from(err: DatabaseError) -> ApiError {
        match err {
            DatabaseError::Query(e) => ApiError::InternalServerError(e.into()),
            // FIXME: ApiError doesn't have an Unavailable variant, but ShuttingDown maps to 503.
            DatabaseError::Connection(_e) => ApiError::ShuttingDown,
            DatabaseError::Logical(reason) => {
                ApiError::InternalServerError(anyhow::anyhow!(reason))
            }
        }
    }
}

pub struct Service {
    inner: Arc<std::sync::RwLock<ServiceState>>,
    config: Config,
    persistence: Arc<Persistence>,

    /// This waits for initial reconciliation with pageservers to complete.  Until this barrier
    /// passes, it isn't safe to do any actions that mutate tenants.
    pub(crate) startup_complete: Barrier,
}

impl From<ReconcileWaitError> for ApiError {
    fn from(value: ReconcileWaitError) -> Self {
        match value {
            ReconcileWaitError::Shutdown => ApiError::ShuttingDown,
            e @ ReconcileWaitError::Timeout(_) => ApiError::Timeout(format!("{e}").into()),
            e @ ReconcileWaitError::Failed(..) => ApiError::InternalServerError(anyhow::anyhow!(e)),
        }
    }
}

impl Service {
    pub fn get_config(&self) -> &Config {
        &self.config
    }

    /// TODO: don't allow other API calls until this is done, don't start doing any background housekeeping
    /// until this is done.
    async fn startup_reconcile(&self) {
        // For all tenant shards, a vector of observed states on nodes (where None means
        // indeterminate, same as in [`ObservedStateLocation`])
        let mut observed = HashMap::new();

        let nodes = {
            let locked = self.inner.read().unwrap();
            locked.nodes.clone()
        };

        // TODO: issue these requests concurrently
        for node in nodes.values() {
            let client = mgmt_api::Client::new(node.base_url(), self.config.jwt_token.as_deref());

            tracing::info!("Scanning shards on node {}...", node.id);
            match client.list_location_config().await {
                Err(e) => {
                    tracing::warn!("Could not contact pageserver {} ({e})", node.id);
                    // TODO: be more tolerant, apply a generous 5-10 second timeout with retries, in case
                    // pageserver is being restarted at the same time as we are
                }
                Ok(listing) => {
                    tracing::info!(
                        "Received {} shard statuses from pageserver {}, setting it to Active",
                        listing.tenant_shards.len(),
                        node.id
                    );

                    for (tenant_shard_id, conf_opt) in listing.tenant_shards {
                        observed.insert(tenant_shard_id, (node.id, conf_opt));
                    }
                }
            }
        }

        let mut cleanup = Vec::new();

        // Populate intent and observed states for all tenants, based on reported state on pageservers
        let shard_count = {
            let mut locked = self.inner.write().unwrap();
            for (tenant_shard_id, (node_id, observed_loc)) in observed {
                let Some(tenant_state) = locked.tenants.get_mut(&tenant_shard_id) else {
                    cleanup.push((tenant_shard_id, node_id));
                    continue;
                };

                tenant_state
                    .observed
                    .locations
                    .insert(node_id, ObservedStateLocation { conf: observed_loc });
            }

            // Populate each tenant's intent state
            let mut scheduler = Scheduler::new(&locked.tenants, &nodes);
            for (tenant_shard_id, tenant_state) in locked.tenants.iter_mut() {
                tenant_state.intent_from_observed();
                if let Err(e) = tenant_state.schedule(&mut scheduler) {
                    // Non-fatal error: we are unable to properly schedule the tenant, perhaps because
                    // not enough pageservers are available.  The tenant may well still be available
                    // to clients.
                    tracing::error!("Failed to schedule tenant {tenant_shard_id} at startup: {e}");
                }
            }

            locked.tenants.len()
        };

        // TODO: if any tenant's intent now differs from its loaded generation_pageserver, we should clear that
        // generation_pageserver in the database.

        // Clean up any tenants that were found on pageservers but are not known to us.
        for (tenant_shard_id, node_id) in cleanup {
            // A node reported a tenant_shard_id which is unknown to us: detach it.
            let node = nodes
                .get(&node_id)
                .expect("Always exists: only known nodes are scanned");

            let client = mgmt_api::Client::new(node.base_url(), self.config.jwt_token.as_deref());
            match client
                .location_config(
                    tenant_shard_id,
                    LocationConfig {
                        mode: LocationConfigMode::Detached,
                        generation: None,
                        secondary_conf: None,
                        shard_number: tenant_shard_id.shard_number.0,
                        shard_count: tenant_shard_id.shard_count.0,
                        shard_stripe_size: 0,
                        tenant_conf: models::TenantConfig::default(),
                    },
                    None,
                )
                .await
            {
                Ok(()) => {
                    tracing::info!(
                        "Detached unknown shard {tenant_shard_id} on pageserver {node_id}"
                    );
                }
                Err(e) => {
                    // Non-fatal error: leaving a tenant shard behind that we are not managing shouldn't
                    // break anything.
                    tracing::error!(
                        "Failed to detach unknkown shard {tenant_shard_id} on pageserver {node_id}: {e}"
                    );
                }
            }
        }

        // Finally, now that the service is up and running, launch reconcile operations for any tenants
        // which require it: under normal circumstances this should only include tenants that were in some
        // transient state before we restarted.
        let reconcile_tasks = self.reconcile_all();
        tracing::info!("Startup complete, spawned {reconcile_tasks} reconciliation tasks ({shard_count} shards total)");
    }

    pub async fn spawn(config: Config, persistence: Arc<Persistence>) -> anyhow::Result<Arc<Self>> {
        let (result_tx, mut result_rx) = tokio::sync::mpsc::unbounded_channel();

        tracing::info!("Loading nodes from database...");
        let nodes = persistence.list_nodes().await?;
        let nodes: HashMap<NodeId, Node> = nodes.into_iter().map(|n| (n.id, n)).collect();
        tracing::info!("Loaded {} nodes from database.", nodes.len());

        tracing::info!("Loading shards from database...");
        let tenant_shard_persistence = persistence.list_tenant_shards().await?;
        tracing::info!(
            "Loaded {} shards from database.",
            tenant_shard_persistence.len()
        );

        let mut tenants = BTreeMap::new();

        for tsp in tenant_shard_persistence {
            let tenant_shard_id = TenantShardId {
                tenant_id: TenantId::from_str(tsp.tenant_id.as_str())?,
                shard_number: ShardNumber(tsp.shard_number as u8),
                shard_count: ShardCount(tsp.shard_count as u8),
            };
            let shard_identity = if tsp.shard_count == 0 {
                ShardIdentity::unsharded()
            } else {
                ShardIdentity::new(
                    ShardNumber(tsp.shard_number as u8),
                    ShardCount(tsp.shard_count as u8),
                    ShardStripeSize(tsp.shard_stripe_size as u32),
                )?
            };

            // We will populate intent properly later in [`Self::startup_reconcile`], initially populate
            // it with what we can infer: the node for which a generation was most recently issued.
            let mut intent = IntentState::new();
            if tsp.generation_pageserver != i64::MAX {
                intent.attached = Some(NodeId(tsp.generation_pageserver as u64))
            }

            let new_tenant = TenantState {
                tenant_shard_id,
                shard: shard_identity,
                sequence: Sequence::initial(),
                generation: Generation::new(tsp.generation as u32),
                policy: serde_json::from_str(&tsp.placement_policy).unwrap(),
                intent,
                observed: ObservedState::new(),
                config: serde_json::from_str(&tsp.config).unwrap(),
                reconciler: None,
                waiter: Arc::new(SeqWait::new(Sequence::initial())),
                error_waiter: Arc::new(SeqWait::new(Sequence::initial())),
                last_error: Arc::default(),
            };

            tenants.insert(tenant_shard_id, new_tenant);
        }

        let (startup_completion, startup_complete) = utils::completion::channel();

        let this = Arc::new(Self {
            inner: Arc::new(std::sync::RwLock::new(ServiceState::new(
                result_tx, nodes, tenants,
            ))),
            config,
            persistence,
            startup_complete,
        });

        let result_task_this = this.clone();
        tokio::task::spawn(async move {
            while let Some(result) = result_rx.recv().await {
                tracing::info!(
                    "Reconcile result for sequence {}, ok={}",
                    result.sequence,
                    result.result.is_ok()
                );
                let mut locked = result_task_this.inner.write().unwrap();
                let Some(tenant) = locked.tenants.get_mut(&result.tenant_shard_id) else {
                    // A reconciliation result might race with removing a tenant: drop results for
                    // tenants that aren't in our map.
                    continue;
                };

                // Usually generation should only be updated via this path, so the max() isn't
                // needed, but it is used to handle out-of-band updates via. e.g. test hook.
                tenant.generation = std::cmp::max(tenant.generation, result.generation);

                match result.result {
                    Ok(()) => {
                        for (node_id, loc) in &result.observed.locations {
                            if let Some(conf) = &loc.conf {
                                tracing::info!(
                                    "Updating observed location {}: {:?}",
                                    node_id,
                                    conf
                                );
                            } else {
                                tracing::info!("Setting observed location {} to None", node_id,)
                            }
                        }
                        tenant.observed = result.observed;
                        tenant.waiter.advance(result.sequence);
                    }
                    Err(e) => {
                        tracing::warn!(
                            "Reconcile error on tenant {}: {}",
                            tenant.tenant_shard_id,
                            e
                        );

                        // Ordering: populate last_error before advancing error_seq,
                        // so that waiters will see the correct error after waiting.
                        *(tenant.last_error.lock().unwrap()) = format!("{e}");
                        tenant.error_waiter.advance(result.sequence);

                        for (node_id, o) in result.observed.locations {
                            tenant.observed.locations.insert(node_id, o);
                        }
                    }
                }
            }
        });

        let startup_reconcile_this = this.clone();
        tokio::task::spawn(async move {
            // Block the [`Service::startup_complete`] barrier until we're done
            let _completion = startup_completion;

            startup_reconcile_this.startup_reconcile().await
        });

        Ok(this)
    }

    pub(crate) async fn attach_hook(
        &self,
        attach_req: AttachHookRequest,
    ) -> anyhow::Result<AttachHookResponse> {
        // This is a test hook.  To enable using it on tenants that were created directly with
        // the pageserver API (not via this service), we will auto-create any missing tenant
        // shards with default state.
        let insert = {
            let locked = self.inner.write().unwrap();
            !locked.tenants.contains_key(&attach_req.tenant_shard_id)
        };
        if insert {
            let tsp = TenantShardPersistence {
                tenant_id: attach_req.tenant_shard_id.tenant_id.to_string(),
                shard_number: attach_req.tenant_shard_id.shard_number.0 as i32,
                shard_count: attach_req.tenant_shard_id.shard_count.0 as i32,
                shard_stripe_size: 0,
                generation: 0,
                generation_pageserver: i64::MAX,
                placement_policy: serde_json::to_string(&PlacementPolicy::default()).unwrap(),
                config: serde_json::to_string(&TenantConfig::default()).unwrap(),
            };

            match self.persistence.insert_tenant_shards(vec![tsp]).await {
                Err(e) => match e {
                    DatabaseError::Query(diesel::result::Error::DatabaseError(
                        DatabaseErrorKind::UniqueViolation,
                        _,
                    )) => {
                        tracing::info!(
                            "Raced with another request to insert tenant {}",
                            attach_req.tenant_shard_id
                        )
                    }
                    _ => return Err(e.into()),
                },
                Ok(()) => {
                    tracing::info!("Inserted shard {} in database", attach_req.tenant_shard_id);

                    let mut locked = self.inner.write().unwrap();
                    locked.tenants.insert(
                        attach_req.tenant_shard_id,
                        TenantState::new(
                            attach_req.tenant_shard_id,
                            ShardIdentity::unsharded(),
                            PlacementPolicy::Single,
                        ),
                    );
                    tracing::info!("Inserted shard {} in memory", attach_req.tenant_shard_id);
                }
            }
        }

        let new_generation = if let Some(req_node_id) = attach_req.node_id {
            Some(
                self.persistence
                    .increment_generation(attach_req.tenant_shard_id, req_node_id)
                    .await?,
            )
        } else {
            self.persistence.detach(attach_req.tenant_shard_id).await?;
            None
        };

        let mut locked = self.inner.write().unwrap();
        let tenant_state = locked
            .tenants
            .get_mut(&attach_req.tenant_shard_id)
            .expect("Checked for existence above");

        if let Some(new_generation) = new_generation {
            tenant_state.generation = new_generation;
        } else {
            // This is a detach notification.  We must update placement policy to avoid re-attaching
            // during background scheduling/reconciliation, or during attachment service restart.
            assert!(attach_req.node_id.is_none());
            tenant_state.policy = PlacementPolicy::Detached;
        }

        if let Some(attaching_pageserver) = attach_req.node_id.as_ref() {
            tracing::info!(
                tenant_id = %attach_req.tenant_shard_id,
                ps_id = %attaching_pageserver,
                generation = ?tenant_state.generation,
                "issuing",
            );
        } else if let Some(ps_id) = tenant_state.intent.attached {
            tracing::info!(
                tenant_id = %attach_req.tenant_shard_id,
                %ps_id,
                generation = ?tenant_state.generation,
                "dropping",
            );
        } else {
            tracing::info!(
            tenant_id = %attach_req.tenant_shard_id,
            "no-op: tenant already has no pageserver");
        }
        tenant_state.intent.attached = attach_req.node_id;

        tracing::info!(
            "attach_hook: tenant {} set generation {:?}, pageserver {}",
            attach_req.tenant_shard_id,
            tenant_state.generation,
            // TODO: this is an odd number of 0xf's
            attach_req.node_id.unwrap_or(utils::id::NodeId(0xfffffff))
        );

        Ok(AttachHookResponse {
            gen: attach_req
                .node_id
                .map(|_| tenant_state.generation.into().unwrap()),
        })
    }

    pub(crate) fn inspect(&self, inspect_req: InspectRequest) -> InspectResponse {
        let locked = self.inner.read().unwrap();

        let tenant_state = locked.tenants.get(&inspect_req.tenant_shard_id);

        InspectResponse {
            attachment: tenant_state.and_then(|s| {
                s.intent
                    .attached
                    .map(|ps| (s.generation.into().unwrap(), ps))
            }),
        }
    }

    pub(crate) async fn re_attach(
        &self,
        reattach_req: ReAttachRequest,
    ) -> anyhow::Result<ReAttachResponse> {
        // Ordering: we must persist generation number updates before making them visible in the in-memory state
        let incremented_generations = self.persistence.re_attach(reattach_req.node_id).await?;

        // Apply the updated generation to our in-memory state
        let mut locked = self.inner.write().unwrap();

        let mut response = ReAttachResponse {
            tenants: Vec::new(),
        };

        for (tenant_shard_id, new_gen) in incremented_generations {
            response.tenants.push(ReAttachResponseTenant {
                id: tenant_shard_id,
                gen: new_gen.into().unwrap(),
            });

            // Apply the new generation number to our in-memory state
            let shard_state = locked.tenants.get_mut(&tenant_shard_id);
            let Some(shard_state) = shard_state else {
                // Not fatal.  This edge case requires a re-attach to happen
                // between inserting a new tenant shard in to the database, and updating our in-memory
                // state to know about the shard, _and_ that the state inserted to the database referenced
                // a pageserver.  Should never happen, but handle it rather than panicking, since it should
                // be harmless.
                tracing::error!(
                    "Shard {} is in database for node {} but not in-memory state",
                    tenant_shard_id,
                    reattach_req.node_id
                );
                continue;
            };

            shard_state.generation = std::cmp::max(shard_state.generation, new_gen);

            // TODO: cancel/restart any running reconciliation for this tenant, it might be trying
            // to call location_conf API with an old generation.  Wait for cancellation to complete
            // before responding to this request.  Requires well implemented CancellationToken logic
            // all the way to where we call location_conf.  Even then, there can still be a location_conf
            // request in flight over the network: TODO handle that by making location_conf API refuse
            // to go backward in generations.
        }
        Ok(response)
    }

    pub(crate) fn validate(&self, validate_req: ValidateRequest) -> ValidateResponse {
        let locked = self.inner.read().unwrap();

        let mut response = ValidateResponse {
            tenants: Vec::new(),
        };

        for req_tenant in validate_req.tenants {
            if let Some(tenant_state) = locked.tenants.get(&req_tenant.id) {
                let valid = tenant_state.generation == Generation::new(req_tenant.gen);
                tracing::info!(
                    "handle_validate: {}(gen {}): valid={valid} (latest {:?})",
                    req_tenant.id,
                    req_tenant.gen,
                    tenant_state.generation
                );
                response.tenants.push(ValidateResponseTenant {
                    id: req_tenant.id,
                    valid,
                });
            } else {
                // After tenant deletion, we may approve any validation.  This avoids
                // spurious warnings on the pageserver if it has pending LSN updates
                // at the point a deletion happens.
                response.tenants.push(ValidateResponseTenant {
                    id: req_tenant.id,
                    valid: true,
                });
            }
        }
        response
    }

    pub(crate) async fn tenant_create(
        &self,
        create_req: TenantCreateRequest,
    ) -> Result<TenantCreateResponse, ApiError> {
        // Shard count 0 is valid: it means create a single shard (ShardCount(0) means "unsharded")
        let literal_shard_count = if create_req.shard_parameters.is_unsharded() {
            1
        } else {
            create_req.shard_parameters.count.0
        };

        // This service expects to handle sharding itself: it is an error to try and directly create
        // a particular shard here.
        let tenant_id = if create_req.new_tenant_id.shard_count > ShardCount(1) {
            return Err(ApiError::BadRequest(anyhow::anyhow!(
                "Attempted to create a specific shard, this API is for creating the whole tenant"
            )));
        } else {
            create_req.new_tenant_id.tenant_id
        };

        tracing::info!(
            "Creating tenant {}, shard_count={:?}",
            create_req.new_tenant_id,
            create_req.shard_parameters.count,
        );

        let create_ids = (0..literal_shard_count)
            .map(|i| TenantShardId {
                tenant_id,
                shard_number: ShardNumber(i),
                shard_count: create_req.shard_parameters.count,
            })
            .collect::<Vec<_>>();

        // TODO: enable specifying this.  Using Single as a default helps legacy tests to work (they
        // have no expectation of HA).
        let placement_policy: PlacementPolicy = PlacementPolicy::Single;

        // Ordering: we persist tenant shards before creating them on the pageserver.  This enables a caller
        // to clean up after themselves by issuing a tenant deletion if something goes wrong and we restart
        // during the creation, rather than risking leaving orphan objects in S3.
        let persist_tenant_shards = create_ids
            .iter()
            .map(|tenant_shard_id| TenantShardPersistence {
                tenant_id: tenant_shard_id.tenant_id.to_string(),
                shard_number: tenant_shard_id.shard_number.0 as i32,
                shard_count: tenant_shard_id.shard_count.0 as i32,
                shard_stripe_size: create_req.shard_parameters.stripe_size.0 as i32,
                generation: 0,
                generation_pageserver: i64::MAX,
                placement_policy: serde_json::to_string(&placement_policy).unwrap(),
                config: serde_json::to_string(&create_req.config).unwrap(),
            })
            .collect();
        self.persistence
            .insert_tenant_shards(persist_tenant_shards)
            .await
            .map_err(|e| {
                // TODO: distinguish primary key constraint (idempotent, OK), from other errors
                ApiError::InternalServerError(anyhow::anyhow!(e))
            })?;

        let (waiters, response_shards) = {
            let mut locked = self.inner.write().unwrap();

            let mut response_shards = Vec::new();

            let mut scheduler = Scheduler::new(&locked.tenants, &locked.nodes);

            for tenant_shard_id in create_ids {
                tracing::info!("Creating shard {tenant_shard_id}...");

                use std::collections::btree_map::Entry;
                match locked.tenants.entry(tenant_shard_id) {
                    Entry::Occupied(mut entry) => {
                        tracing::info!(
                            "Tenant shard {tenant_shard_id} already exists while creating"
                        );

                        // TODO: schedule() should take an anti-affinity expression that pushes
                        // attached and secondary locations (independently) away frorm those
                        // pageservers also holding a shard for this tenant.

                        entry.get_mut().schedule(&mut scheduler).map_err(|e| {
                            ApiError::Conflict(format!(
                                "Failed to schedule shard {tenant_shard_id}: {e}"
                            ))
                        })?;

                        response_shards.push(TenantCreateResponseShard {
                            node_id: entry
                                .get()
                                .intent
                                .attached
                                .expect("We just set pageserver if it was None"),
                            generation: entry.get().generation.into().unwrap(),
                        });

                        continue;
                    }
                    Entry::Vacant(entry) => {
                        let mut state = TenantState::new(
                            tenant_shard_id,
                            ShardIdentity::from_params(
                                tenant_shard_id.shard_number,
                                &create_req.shard_parameters,
                            ),
                            placement_policy.clone(),
                        );

                        if let Some(create_gen) = create_req.generation {
                            state.generation = Generation::new(create_gen);
                        }
                        state.config = create_req.config.clone();

                        state.schedule(&mut scheduler).map_err(|e| {
                            ApiError::Conflict(format!(
                                "Failed to schedule shard {tenant_shard_id}: {e}"
                            ))
                        })?;

                        response_shards.push(TenantCreateResponseShard {
                            node_id: state
                                .intent
                                .attached
                                .expect("We just set pageserver if it was None"),
                            generation: state.generation.into().unwrap(),
                        });
                        entry.insert(state)
                    }
                };
            }

            // Take a snapshot of pageservers
            let pageservers = locked.nodes.clone();

            let result_tx = locked.result_tx.clone();
            let compute_hook = locked.compute_hook.clone();

            let waiters = locked
                .tenants
                .range_mut(TenantShardId::tenant_range(tenant_id))
                .filter_map(|(_shard_id, shard)| {
                    shard.maybe_reconcile(
                        result_tx.clone(),
                        &pageservers,
                        &compute_hook,
                        &self.config,
                        &self.persistence,
                    )
                })
                .collect::<Vec<_>>();
            (waiters, response_shards)
        };

        let deadline = Instant::now().checked_add(Duration::from_secs(5)).unwrap();
        for waiter in waiters {
            let timeout = deadline.duration_since(Instant::now());
            waiter.wait_timeout(timeout).await?;
        }
        Ok(TenantCreateResponse {
            shards: response_shards,
        })
    }

    pub(crate) async fn tenant_delete(&self, tenant_id: TenantId) -> Result<StatusCode, ApiError> {
        // TODO: refactor into helper
        let targets = {
            let locked = self.inner.read().unwrap();
            let mut targets = Vec::new();

            for (tenant_shard_id, shard) in
                locked.tenants.range(TenantShardId::tenant_range(tenant_id))
            {
                let node_id = shard.intent.attached.ok_or_else(|| {
                    ApiError::InternalServerError(anyhow::anyhow!("Shard not scheduled"))
                })?;
                let node = locked
                    .nodes
                    .get(&node_id)
                    .expect("Pageservers may not be deleted while referenced");

                targets.push((*tenant_shard_id, node.clone()));
            }
            targets
        };

        // TODO: error out if the tenant is not attached anywhere.

        // Phase 1: delete on the pageservers
        let mut any_pending = false;
        for (tenant_shard_id, node) in targets {
            let client = mgmt_api::Client::new(node.base_url(), self.config.jwt_token.as_deref());
            // TODO: this, like many other places, requires proper retry handling for 503, timeout: those should not
            // surface immediately as an error to our caller.
            let status = client.tenant_delete(tenant_shard_id).await.map_err(|e| {
                ApiError::InternalServerError(anyhow::anyhow!(
                    "Error deleting shard {tenant_shard_id} on node {}: {e}",
                    node.id
                ))
            })?;
            tracing::info!(
                "Shard {tenant_shard_id} on node {}, delete returned {}",
                node.id,
                status
            );
            if status == StatusCode::ACCEPTED {
                any_pending = true;
            }
        }

        if any_pending {
            // Caller should call us again later.  When we eventually see 404s from
            // all the shards, we may proceed to delete our records of the tenant.
            tracing::info!(
                "Tenant {} has some shards pending deletion, returning 202",
                tenant_id
            );
            return Ok(StatusCode::ACCEPTED);
        }

        // Fall through: deletion of the tenant on pageservers is complete, we may proceed to drop
        // our in-memory state and database state.

        // Ordering: we delete persistent state first: if we then
        // crash, we will drop the in-memory state.

        // Drop persistent state.
        self.persistence.delete_tenant(tenant_id).await?;

        // Drop in-memory state
        {
            let mut locked = self.inner.write().unwrap();
            locked
                .tenants
                .retain(|tenant_shard_id, _shard| tenant_shard_id.tenant_id != tenant_id);
        };

        // Success is represented as 404, to imitate the existing pageserver deletion API
        Ok(StatusCode::NOT_FOUND)
    }

    pub(crate) async fn tenant_timeline_create(
        &self,
        tenant_id: TenantId,
        mut create_req: TimelineCreateRequest,
    ) -> Result<TimelineInfo, ApiError> {
        let mut timeline_info = None;

        tracing::info!(
            "Creating timeline {}/{}",
            tenant_id,
            create_req.new_timeline_id,
        );

        self.ensure_attached_wait(tenant_id).await?;

        // TODO: refuse to do this if shard splitting is in progress
        let targets = {
            let locked = self.inner.read().unwrap();
            let mut targets = Vec::new();

            for (tenant_shard_id, shard) in
                locked.tenants.range(TenantShardId::tenant_range(tenant_id))
            {
                let node_id = shard.intent.attached.ok_or_else(|| {
                    ApiError::InternalServerError(anyhow::anyhow!("Shard not scheduled"))
                })?;
                let node = locked
                    .nodes
                    .get(&node_id)
                    .expect("Pageservers may not be deleted while referenced");

                targets.push((*tenant_shard_id, node.clone()));
            }
            targets
        };

        if targets.is_empty() {
            return Err(ApiError::NotFound(
                anyhow::anyhow!("Tenant not found").into(),
            ));
        }

        for (tenant_shard_id, node) in targets {
            // TODO: issue shard timeline creates in parallel, once the 0th is done.

            let client = mgmt_api::Client::new(node.base_url(), self.config.jwt_token.as_deref());

            tracing::info!(
                "Creating timeline on shard {}/{}, attached to node {}",
                tenant_shard_id,
                create_req.new_timeline_id,
                node.id
            );

            let shard_timeline_info = client
                .timeline_create(tenant_shard_id, &create_req)
                .await
                .map_err(|e| match e {
                    mgmt_api::Error::ApiError(status, msg)
                        if status == StatusCode::INTERNAL_SERVER_ERROR
                            || status == StatusCode::NOT_ACCEPTABLE =>
                    {
                        // TODO: handle more error codes, e.g. 503 should be passed through.  Make a general wrapper
                        // for pass-through API calls.
                        ApiError::InternalServerError(anyhow::anyhow!(msg))
                    }
                    _ => ApiError::Conflict(format!("Failed to create timeline: {e}")),
                })?;

            if timeline_info.is_none() {
                // If the caller specified an ancestor but no ancestor LSN, we are responsible for
                // propagating the LSN chosen by the first shard to the other shards: it is important
                // that all shards end up with the same ancestor_start_lsn.
                if create_req.ancestor_timeline_id.is_some()
                    && create_req.ancestor_start_lsn.is_none()
                {
                    create_req.ancestor_start_lsn = shard_timeline_info.ancestor_lsn;
                }

                // We will return the TimelineInfo from the first shard
                timeline_info = Some(shard_timeline_info);
            }
        }
        Ok(timeline_info.expect("targets cannot be empty"))
    }

    pub(crate) async fn tenant_timeline_delete(
        &self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
    ) -> Result<StatusCode, ApiError> {
        tracing::info!("Deleting timeline {}/{}", tenant_id, timeline_id,);

        self.ensure_attached_wait(tenant_id).await?;

        // TODO: refuse to do this if shard splitting is in progress
        let targets = {
            let locked = self.inner.read().unwrap();
            let mut targets = Vec::new();

            for (tenant_shard_id, shard) in
                locked.tenants.range(TenantShardId::tenant_range(tenant_id))
            {
                let node_id = shard.intent.attached.ok_or_else(|| {
                    ApiError::InternalServerError(anyhow::anyhow!("Shard not scheduled"))
                })?;
                let node = locked
                    .nodes
                    .get(&node_id)
                    .expect("Pageservers may not be deleted while referenced");

                targets.push((*tenant_shard_id, node.clone()));
            }
            targets
        };

        if targets.is_empty() {
            return Err(ApiError::NotFound(
                anyhow::anyhow!("Tenant not found").into(),
            ));
        }

        // TODO: call into shards concurrently
        let mut any_pending = false;
        for (tenant_shard_id, node) in targets {
            let client = mgmt_api::Client::new(node.base_url(), self.config.jwt_token.as_deref());

            tracing::info!(
                "Deleting timeline on shard {}/{}, attached to node {}",
                tenant_shard_id,
                timeline_id,
                node.id
            );

            let status = client
                .timeline_delete(tenant_shard_id, timeline_id)
                .await
                .map_err(|e| {
                    ApiError::InternalServerError(anyhow::anyhow!(
                    "Error deleting timeline {timeline_id} on {tenant_shard_id} on node {}: {e}",
                    node.id
                ))
                })?;

            if status == StatusCode::ACCEPTED {
                any_pending = true;
            }
        }

        if any_pending {
            Ok(StatusCode::ACCEPTED)
        } else {
            Ok(StatusCode::NOT_FOUND)
        }
    }

    pub(crate) fn tenant_locate(
        &self,
        tenant_id: TenantId,
    ) -> Result<TenantLocateResponse, ApiError> {
        let locked = self.inner.read().unwrap();
        tracing::info!("Locating shards for tenant {tenant_id}");

        // Take a snapshot of pageservers
        let pageservers = locked.nodes.clone();

        let mut result = Vec::new();
        let mut shard_params: Option<ShardParameters> = None;

        for (tenant_shard_id, shard) in locked.tenants.range(TenantShardId::tenant_range(tenant_id))
        {
            let node_id = shard
                .intent
                .attached
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

            match &shard_params {
                None => {
                    shard_params = Some(ShardParameters {
                        stripe_size: shard.shard.stripe_size,
                        count: shard.shard.count,
                    });
                }
                Some(params) => {
                    if params.stripe_size != shard.shard.stripe_size {
                        // This should never happen.  We enforce at runtime because it's simpler than
                        // adding an extra per-tenant data structure to store the things that should be the same
                        return Err(ApiError::InternalServerError(anyhow::anyhow!(
                            "Inconsistent shard stripe size parameters!"
                        )));
                    }
                }
            }
        }

        if result.is_empty() {
            return Err(ApiError::NotFound(
                anyhow::anyhow!("No shards for this tenant ID found").into(),
            ));
        }
        let shard_params = shard_params.expect("result is non-empty, therefore this is set");
        tracing::info!(
            "Located tenant {} with params {:?} on shards {}",
            tenant_id,
            shard_params,
            result
                .iter()
                .map(|s| format!("{:?}", s))
                .collect::<Vec<_>>()
                .join(",")
        );

        Ok(TenantLocateResponse {
            shards: result,
            shard_params,
        })
    }

    pub(crate) async fn tenant_shard_migrate(
        &self,
        tenant_shard_id: TenantShardId,
        migrate_req: TenantShardMigrateRequest,
    ) -> Result<TenantShardMigrateResponse, ApiError> {
        let waiter = {
            let mut locked = self.inner.write().unwrap();

            let result_tx = locked.result_tx.clone();
            let pageservers = locked.nodes.clone();
            let compute_hook = locked.compute_hook.clone();

            let Some(shard) = locked.tenants.get_mut(&tenant_shard_id) else {
                return Err(ApiError::NotFound(
                    anyhow::anyhow!("Tenant shard not found").into(),
                ));
            };

            if shard.intent.attached == Some(migrate_req.node_id) {
                // No-op case: we will still proceed to wait for reconciliation in case it is
                // incomplete from an earlier update to the intent.
                tracing::info!("Migrating: intent is unchanged {:?}", shard.intent);
            } else {
                let old_attached = shard.intent.attached;

                match shard.policy {
                    PlacementPolicy::Single => {
                        shard.intent.secondary.clear();
                    }
                    PlacementPolicy::Double(_n) => {
                        // If our new attached node was a secondary, it no longer should be.
                        shard.intent.secondary.retain(|s| s != &migrate_req.node_id);

                        // If we were already attached to something, demote that to a secondary
                        if let Some(old_attached) = old_attached {
                            shard.intent.secondary.push(old_attached);
                        }
                    }
                    PlacementPolicy::Detached => {
                        return Err(ApiError::BadRequest(anyhow::anyhow!(
                            "Cannot migrate a tenant that is PlacementPolicy::Detached: configure it to an attached policy first"
                        )))
                    }
                }
                shard.intent.attached = Some(migrate_req.node_id);

                tracing::info!("Migrating: new intent {:?}", shard.intent);
                shard.sequence = shard.sequence.next();
            }

            shard.maybe_reconcile(
                result_tx,
                &pageservers,
                &compute_hook,
                &self.config,
                &self.persistence,
            )
        };

        if let Some(waiter) = waiter {
            waiter.wait_timeout(RECONCILE_TIMEOUT).await?;
        } else {
            tracing::warn!("Migration is a no-op");
        }

        Ok(TenantShardMigrateResponse {})
    }

    pub(crate) async fn node_list(&self) -> Result<Vec<NodePersistence>, ApiError> {
        // It is convenient to avoid taking the big lock and converting Node to a serializable
        // structure, by fetching from storage instead of reading in-memory state.
        let nodes = self
            .persistence
            .list_nodes()
            .await?
            .into_iter()
            .map(|n| n.to_persistent())
            .collect();

        Ok(nodes)
    }

    pub(crate) async fn node_register(
        &self,
        register_req: NodeRegisterRequest,
    ) -> Result<(), ApiError> {
        // Pre-check for an already-existing node
        {
            let locked = self.inner.read().unwrap();
            if let Some(node) = locked.nodes.get(&register_req.node_id) {
                // Note that we do not do a total equality of the struct, because we don't require
                // the availability/scheduling states to agree for a POST to be idempotent.
                if node.listen_http_addr == register_req.listen_http_addr
                    && node.listen_http_port == register_req.listen_http_port
                    && node.listen_pg_addr == register_req.listen_pg_addr
                    && node.listen_pg_port == register_req.listen_pg_port
                {
                    tracing::info!(
                        "Node {} re-registered with matching address",
                        register_req.node_id
                    );
                    return Ok(());
                } else {
                    // TODO: decide if we want to allow modifying node addresses without removing and re-adding
                    // the node.  Safest/simplest thing is to refuse it, and usually we deploy with
                    // a fixed address through the lifetime of a node.
                    tracing::warn!(
                        "Node {} tried to register with different address",
                        register_req.node_id
                    );
                    return Err(ApiError::Conflict(
                        "Node is already registered with different address".to_string(),
                    ));
                }
            }
        }

        // Ordering: we must persist the new node _before_ adding it to in-memory state.
        // This ensures that before we use it for anything or expose it via any external
        // API, it is guaranteed to be available after a restart.
        let new_node = Node {
            id: register_req.node_id,
            listen_http_addr: register_req.listen_http_addr,
            listen_http_port: register_req.listen_http_port,
            listen_pg_addr: register_req.listen_pg_addr,
            listen_pg_port: register_req.listen_pg_port,
            scheduling: NodeSchedulingPolicy::Filling,
            // TODO: we shouldn't really call this Active until we've heartbeated it.
            availability: NodeAvailability::Active,
        };
        // TODO: idempotency if the node already exists in the database
        self.persistence.insert_node(&new_node).await?;

        let mut locked = self.inner.write().unwrap();
        let mut new_nodes = (*locked.nodes).clone();

        new_nodes.insert(register_req.node_id, new_node);

        locked.nodes = Arc::new(new_nodes);

        tracing::info!(
            "Registered pageserver {}, now have {} pageservers",
            register_req.node_id,
            locked.nodes.len()
        );
        Ok(())
    }

    pub(crate) fn node_configure(&self, config_req: NodeConfigureRequest) -> Result<(), ApiError> {
        let mut locked = self.inner.write().unwrap();
        let result_tx = locked.result_tx.clone();
        let compute_hook = locked.compute_hook.clone();

        let mut new_nodes = (*locked.nodes).clone();

        let Some(node) = new_nodes.get_mut(&config_req.node_id) else {
            return Err(ApiError::NotFound(
                anyhow::anyhow!("Node not registered").into(),
            ));
        };

        let mut offline_transition = false;
        let mut active_transition = false;

        if let Some(availability) = &config_req.availability {
            match (availability, &node.availability) {
                (NodeAvailability::Offline, NodeAvailability::Active) => {
                    tracing::info!("Node {} transition to offline", config_req.node_id);
                    offline_transition = true;
                }
                (NodeAvailability::Active, NodeAvailability::Offline) => {
                    tracing::info!("Node {} transition to active", config_req.node_id);
                    active_transition = true;
                }
                _ => {
                    tracing::info!("Node {} no change during config", config_req.node_id);
                    // No change
                }
            };
            node.availability = *availability;
        }

        if let Some(scheduling) = config_req.scheduling {
            node.scheduling = scheduling;

            // TODO: once we have a background scheduling ticker for fill/drain, kick it
            // to wake up and start working.
        }

        let new_nodes = Arc::new(new_nodes);

        let mut scheduler = Scheduler::new(&locked.tenants, &new_nodes);
        if offline_transition {
            for (tenant_shard_id, tenant_state) in &mut locked.tenants {
                if let Some(observed_loc) =
                    tenant_state.observed.locations.get_mut(&config_req.node_id)
                {
                    // When a node goes offline, we set its observed configuration to None, indicating unknown: we will
                    // not assume our knowledge of the node's configuration is accurate until it comes back online
                    observed_loc.conf = None;
                }

                if tenant_state.intent.notify_offline(config_req.node_id) {
                    tenant_state.sequence = tenant_state.sequence.next();
                    match tenant_state.schedule(&mut scheduler) {
                        Err(e) => {
                            // It is possible that some tenants will become unschedulable when too many pageservers
                            // go offline: in this case there isn't much we can do other than make the issue observable.
                            // TODO: give TenantState a scheduling error attribute to be queried later.
                            tracing::warn!(%tenant_shard_id, "Scheduling error when marking pageserver {} offline: {e}", config_req.node_id);
                        }
                        Ok(()) => {
                            tenant_state.maybe_reconcile(
                                result_tx.clone(),
                                &new_nodes,
                                &compute_hook,
                                &self.config,
                                &self.persistence,
                            );
                        }
                    }
                }
            }
        }

        if active_transition {
            // When a node comes back online, we must reconcile any tenant that has a None observed
            // location on the node.
            for tenant_state in locked.tenants.values_mut() {
                if let Some(observed_loc) =
                    tenant_state.observed.locations.get_mut(&config_req.node_id)
                {
                    if observed_loc.conf.is_none() {
                        tenant_state.maybe_reconcile(
                            result_tx.clone(),
                            &new_nodes,
                            &compute_hook,
                            &self.config,
                            &self.persistence,
                        );
                    }
                }
            }

            // TODO: in the background, we should balance work back onto this pageserver
        }

        locked.nodes = new_nodes;

        Ok(())
    }

    /// Helper for methods that will try and call pageserver APIs for
    /// a tenant, such as timeline CRUD: they cannot proceed unless the tenant
    /// is attached somewhere.
    fn ensure_attached_schedule(
        &self,
        mut locked: std::sync::RwLockWriteGuard<'_, ServiceState>,
        tenant_id: TenantId,
    ) -> Result<Vec<ReconcilerWaiter>, anyhow::Error> {
        let mut waiters = Vec::new();
        let result_tx = locked.result_tx.clone();
        let compute_hook = locked.compute_hook.clone();
        let mut scheduler = Scheduler::new(&locked.tenants, &locked.nodes);
        let pageservers = locked.nodes.clone();

        for (_tenant_shard_id, shard) in locked
            .tenants
            .range_mut(TenantShardId::tenant_range(tenant_id))
        {
            shard.schedule(&mut scheduler)?;

            if let Some(waiter) = shard.maybe_reconcile(
                result_tx.clone(),
                &pageservers,
                &compute_hook,
                &self.config,
                &self.persistence,
            ) {
                waiters.push(waiter);
            }
        }
        Ok(waiters)
    }

    async fn ensure_attached_wait(&self, tenant_id: TenantId) -> Result<(), ApiError> {
        let ensure_waiters = {
            let locked = self.inner.write().unwrap();

            self.ensure_attached_schedule(locked, tenant_id)
                .map_err(ApiError::InternalServerError)?
        };

        let deadline = Instant::now().checked_add(Duration::from_secs(5)).unwrap();
        for waiter in ensure_waiters {
            let timeout = deadline.duration_since(Instant::now());
            waiter.wait_timeout(timeout).await?;
        }

        Ok(())
    }

    /// Check all tenants for pending reconciliation work, and reconcile those in need
    ///
    /// Returns how many reconciliation tasks were started
    fn reconcile_all(&self) -> usize {
        let mut locked = self.inner.write().unwrap();
        let result_tx = locked.result_tx.clone();
        let compute_hook = locked.compute_hook.clone();
        let pageservers = locked.nodes.clone();
        locked
            .tenants
            .iter_mut()
            .filter_map(|(_tenant_shard_id, shard)| {
                shard.maybe_reconcile(
                    result_tx.clone(),
                    &pageservers,
                    &compute_hook,
                    &self.config,
                    &self.persistence,
                )
            })
            .count()
    }
}
