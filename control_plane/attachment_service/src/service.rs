use std::{
    cmp::Ordering,
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
use futures::StreamExt;
use hyper::StatusCode;
use pageserver_api::{
    control_api::{
        ReAttachRequest, ReAttachResponse, ReAttachResponseTenant, ValidateRequest,
        ValidateResponse, ValidateResponseTenant,
    },
    models,
    models::{
        LocationConfig, LocationConfigMode, ShardParameters, TenantConfig, TenantCreateRequest,
        TenantLocationConfigRequest, TenantLocationConfigResponse, TenantShardLocation,
        TenantShardSplitRequest, TenantShardSplitResponse, TimelineCreateRequest, TimelineInfo,
    },
    shard::{ShardCount, ShardIdentity, ShardNumber, ShardStripeSize, TenantShardId},
};
use pageserver_client::mgmt_api;
use tokio_util::sync::CancellationToken;
use utils::{
    completion::Barrier,
    generation::Generation,
    http::error::ApiError,
    id::{NodeId, TenantId, TimelineId},
    seqwait::SeqWait,
};

use crate::{
    compute_hook::{self, ComputeHook},
    node::Node,
    persistence::{
        split_state::SplitState, DatabaseError, NodePersistence, Persistence,
        TenantShardPersistence,
    },
    reconciler::attached_location_conf,
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
        config: Config,
        result_tx: tokio::sync::mpsc::UnboundedSender<ReconcileResult>,
        nodes: HashMap<NodeId, Node>,
        tenants: BTreeMap<TenantShardId, TenantState>,
    ) -> Self {
        Self {
            tenants,
            nodes: Arc::new(nodes),
            compute_hook: Arc::new(ComputeHook::new(config)),
            result_tx,
        }
    }
}

#[derive(Clone)]
pub struct Config {
    // All pageservers managed by one instance of this service must have
    // the same public key.  This JWT token will be used to authenticate
    // this service to the pageservers it manages.
    pub jwt_token: Option<String>,

    // This JWT token will be used to authenticate this service to the control plane.
    pub control_plane_jwt_token: Option<String>,

    /// Where the compute hook should send notifications of pageserver attachment locations
    /// (this URL points to the control plane in prod). If this is None, the compute hook will
    /// assume it is running in a test environment and try to update neon_local.
    pub compute_hook_url: Option<String>,
}

impl From<DatabaseError> for ApiError {
    fn from(err: DatabaseError) -> ApiError {
        match err {
            DatabaseError::Query(e) => ApiError::InternalServerError(e.into()),
            // FIXME: ApiError doesn't have an Unavailable variant, but ShuttingDown maps to 503.
            DatabaseError::Connection(_) | DatabaseError::ConnectionPool(_) => {
                ApiError::ShuttingDown
            }
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

        let mut compute_notifications = Vec::new();

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
                } else {
                    // If we're both intending and observed to be attached at a particular node, we will
                    // emit a compute notification for this. In the case where our observed state does not
                    // yet match our intent, we will eventually reconcile, and that will emit a compute notification.
                    if let Some(attached_at) = tenant_state.stably_attached() {
                        compute_notifications.push((*tenant_shard_id, attached_at));
                    }
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

        // Emit compute hook notifications for all tenants which are already stably attached.  Other tenants
        // will emit compute hook notifications when they reconcile.
        //
        // Ordering: we must complete these notification attempts before doing any other reconciliation for the
        // tenants named here, because otherwise our calls to notify() might race with more recent values
        // generated by reconciliation.

        // Compute notify is fallible.  If it fails here, do not delay overall startup: set the
        // flag on these shards that they have a pending notification.
        let compute_hook = self.inner.read().unwrap().compute_hook.clone();

        // Construct an async stream of futures to invoke the compute notify function: we do this
        // in order to subsequently use .buffered() on the stream to execute with bounded parallelism.
        let stream = futures::stream::iter(compute_notifications.into_iter())
            .map(|(tenant_shard_id, node_id)| {
                let compute_hook = compute_hook.clone();
                async move {
                    // TODO: give Service a cancellation token for clean shutdown
                    let cancel = CancellationToken::new();
                    if let Err(e) = compute_hook.notify(tenant_shard_id, node_id, &cancel).await {
                        tracing::error!(
                            tenant_shard_id=%tenant_shard_id,
                            node_id=%node_id,
                            "Failed to notify compute on startup for shard: {e}"
                        );
                        Some(tenant_shard_id)
                    } else {
                        None
                    }
                }
            })
            .buffered(compute_hook::API_CONCURRENCY);
        let notify_results = stream.collect::<Vec<_>>().await;

        // Update tenant state for any that failed to do their initial compute notify, so that they'll retry later.
        {
            let mut locked = self.inner.write().unwrap();
            for tenant_shard_id in notify_results.into_iter().flatten() {
                if let Some(shard) = locked.tenants.get_mut(&tenant_shard_id) {
                    shard.pending_compute_notification = true;
                }
            }
        }

        // Finally, now that the service is up and running, launch reconcile operations for any tenants
        // which require it: under normal circumstances this should only include tenants that were in some
        // transient state before we restarted, or any tenants whose compute hooks failed above.
        let reconcile_tasks = self.reconcile_all();
        // We will not wait for these reconciliation tasks to run here: we're now done with startup and
        // normal operations may proceed.

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
                pending_compute_notification: false,
            };

            tenants.insert(tenant_shard_id, new_tenant);
        }

        let (startup_completion, startup_complete) = utils::completion::channel();

        let this = Arc::new(Self {
            inner: Arc::new(std::sync::RwLock::new(ServiceState::new(
                config.clone(),
                result_tx,
                nodes,
                tenants,
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

                // If the reconciler signals that it failed to notify compute, set this state on
                // the shard so that a future [`TenantState::maybe_reconcile`] will try again.
                tenant.pending_compute_notification = result.pending_compute_notification;

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
                splitting: SplitState::default(),
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
                generation: create_req.generation.map(|g| g as i32).unwrap_or(0),
                generation_pageserver: i64::MAX,
                placement_policy: serde_json::to_string(&placement_policy).unwrap(),
                config: serde_json::to_string(&create_req.config).unwrap(),
                splitting: SplitState::default(),
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
                            shard_id: tenant_shard_id,
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
                            shard_id: tenant_shard_id,
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

        self.await_waiters(waiters).await?;

        Ok(TenantCreateResponse {
            shards: response_shards,
        })
    }

    /// Helper for functions that reconcile a number of shards, and would like to do a timeout-bounded
    /// wait for reconciliation to complete before responding.
    async fn await_waiters(
        &self,
        waiters: Vec<ReconcilerWaiter>,
    ) -> Result<(), ReconcileWaitError> {
        let deadline = Instant::now().checked_add(Duration::from_secs(30)).unwrap();
        for waiter in waiters {
            let timeout = deadline.duration_since(Instant::now());
            waiter.wait_timeout(timeout).await?;
        }

        Ok(())
    }

    /// This API is used by the cloud control plane to do coarse-grained control of tenants:
    /// - Call with mode Attached* to upsert the tenant.
    /// - Call with mode Detached to switch to PolicyMode::Detached
    ///
    /// In future, calling with mode Secondary may switch to a detach-lite mode in which a tenant only has
    /// secondary locations.
    pub(crate) async fn tenant_location_config(
        &self,
        tenant_id: TenantId,
        req: TenantLocationConfigRequest,
    ) -> Result<TenantLocationConfigResponse, ApiError> {
        if req.tenant_id.shard_count.0 > 1 {
            return Err(ApiError::BadRequest(anyhow::anyhow!(
                "This API is for importing single-sharded or unsharded tenants"
            )));
        }

        let mut waiters = Vec::new();
        let mut result = TenantLocationConfigResponse { shards: Vec::new() };
        let maybe_create = {
            let mut locked = self.inner.write().unwrap();
            let result_tx = locked.result_tx.clone();
            let compute_hook = locked.compute_hook.clone();
            let pageservers = locked.nodes.clone();

            let mut scheduler = Scheduler::new(&locked.tenants, &locked.nodes);

            // Maybe we have existing shards
            let mut create = true;
            for (shard_id, shard) in locked
                .tenants
                .range_mut(TenantShardId::tenant_range(tenant_id))
            {
                // Saw an existing shard: this is not a creation
                create = false;

                // Note that for existing tenants we do _not_ respect the generation in the request: this is likely
                // to be stale.  Once a tenant is created in this service, our view of generation is authoritative, and
                // callers' generations may be ignored.  This represents a one-way migration of tenants from the outer
                // cloud control plane into this service.

                // Use location config mode as an indicator of policy: if they ask for
                // attached we go to default HA attached mode.  If they ask for secondary
                // we go to secondary-only mode.  If they ask for detached we detach.
                match req.config.mode {
                    LocationConfigMode::Detached => {
                        shard.policy = PlacementPolicy::Detached;
                    }
                    LocationConfigMode::Secondary => {
                        // TODO: implement secondary-only mode.
                        todo!();
                    }
                    LocationConfigMode::AttachedMulti
                    | LocationConfigMode::AttachedSingle
                    | LocationConfigMode::AttachedStale => {
                        // TODO: persistence for changes in policy
                        if pageservers.len() > 1 {
                            shard.policy = PlacementPolicy::Double(1)
                        } else {
                            // Convenience for dev/test: if we just have one pageserver, import
                            // tenants into Single mode so that scheduling will succeed.
                            shard.policy = PlacementPolicy::Single
                        }
                    }
                }

                shard.schedule(&mut scheduler)?;

                let maybe_waiter = shard.maybe_reconcile(
                    result_tx.clone(),
                    &pageservers,
                    &compute_hook,
                    &self.config,
                    &self.persistence,
                );
                if let Some(waiter) = maybe_waiter {
                    waiters.push(waiter);
                }

                if let Some(node_id) = shard.intent.attached {
                    result.shards.push(TenantShardLocation {
                        shard_id: *shard_id,
                        node_id,
                    })
                }
            }

            if create {
                // Validate request mode
                match req.config.mode {
                    LocationConfigMode::Detached | LocationConfigMode::Secondary => {
                        // When using this API to onboard an existing tenant to this service, it must start in
                        // an attached state, because we need the request to come with a generation
                        return Err(ApiError::BadRequest(anyhow::anyhow!(
                            "Imported tenant must be in attached mode"
                        )));
                    }

                    LocationConfigMode::AttachedMulti
                    | LocationConfigMode::AttachedSingle
                    | LocationConfigMode::AttachedStale => {
                        // Pass
                    }
                }

                // Validate request generation
                let Some(generation) = req.config.generation else {
                    // We can only import attached tenants, because we need the request to come with a generation
                    return Err(ApiError::BadRequest(anyhow::anyhow!(
                        "Generation is mandatory when importing tenant"
                    )));
                };

                // Synthesize a creation request
                Some(TenantCreateRequest {
                    new_tenant_id: TenantShardId::unsharded(tenant_id),
                    generation: Some(generation),
                    shard_parameters: ShardParameters {
                        // Must preserve the incoming shard_count do distinguish unsharded (0)
                        // from single-sharded (1): this distinction appears in the S3 keys of the tenant.
                        count: req.tenant_id.shard_count,
                        // We only import un-sharded or single-sharded tenants, so stripe
                        // size can be made up arbitrarily here.
                        stripe_size: ShardParameters::DEFAULT_STRIPE_SIZE,
                    },
                    config: req.config.tenant_conf,
                })
            } else {
                None
            }
        };

        if let Some(create_req) = maybe_create {
            let create_resp = self.tenant_create(create_req).await?;
            result.shards = create_resp
                .shards
                .into_iter()
                .map(|s| TenantShardLocation {
                    node_id: s.node_id,
                    shard_id: s.shard_id,
                })
                .collect();
        } else {
            // This was an update, wait for reconciliation
            if let Err(e) = self.await_waiters(waiters).await {
                // Do not treat a reconcile error as fatal: we have already applied any requested
                // Intent changes, and the reconcile can fail for external reasons like unavailable
                // compute notification API.  In these cases, it is important that we do not
                // cause the cloud control plane to retry forever on this API.
                tracing::warn!(
                    "Failed to reconcile after /location_config: {e}, returning success anyway"
                );
            }
        }

        Ok(result)
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
            tracing::info!(
                "Deleted tenant {tenant_id}, now have {} tenants",
                locked.tenants.len()
            );
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

    /// When you need to send an HTTP request to the pageserver that holds shard0 of a tenant, this
    /// function looks it up and returns the url.  If the tenant isn't found, returns Err(ApiError::NotFound)
    pub(crate) fn tenant_shard0_baseurl(
        &self,
        tenant_id: TenantId,
    ) -> Result<(String, TenantShardId), ApiError> {
        let locked = self.inner.read().unwrap();
        let Some((tenant_shard_id, shard)) = locked
            .tenants
            .range(TenantShardId::tenant_range(tenant_id))
            .next()
        else {
            return Err(ApiError::NotFound(
                anyhow::anyhow!("Tenant {tenant_id} not found").into(),
            ));
        };

        // TODO: should use the ID last published to compute_hook, rather than the intent: the intent might
        // point to somewhere we haven't attached yet.
        let Some(node_id) = shard.intent.attached else {
            return Err(ApiError::Conflict(
                "Cannot call timeline API on non-attached tenant".to_string(),
            ));
        };

        let Some(node) = locked.nodes.get(&node_id) else {
            // This should never happen
            return Err(ApiError::InternalServerError(anyhow::anyhow!(
                "Shard refers to nonexistent node"
            )));
        };

        Ok((node.base_url(), *tenant_shard_id))
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

    pub(crate) async fn tenant_shard_split(
        &self,
        tenant_id: TenantId,
        split_req: TenantShardSplitRequest,
    ) -> Result<TenantShardSplitResponse, ApiError> {
        let mut policy = None;
        let mut shard_ident = None;

        // TODO: put a cancellation token on Service for clean shutdown
        let cancel = CancellationToken::new();

        // A parent shard which will be split
        struct SplitTarget {
            parent_id: TenantShardId,
            node: Node,
            child_ids: Vec<TenantShardId>,
        }

        // Validate input, and calculate which shards we will create
        let (old_shard_count, targets, compute_hook) = {
            let locked = self.inner.read().unwrap();

            let pageservers = locked.nodes.clone();

            let mut targets = Vec::new();

            // In case this is a retry, count how many already-split shards we found
            let mut children_found = Vec::new();
            let mut old_shard_count = None;

            for (tenant_shard_id, shard) in
                locked.tenants.range(TenantShardId::tenant_range(tenant_id))
            {
                match shard.shard.count.0.cmp(&split_req.new_shard_count) {
                    Ordering::Equal => {
                        //  Already split this
                        children_found.push(*tenant_shard_id);
                        continue;
                    }
                    Ordering::Greater => {
                        return Err(ApiError::BadRequest(anyhow::anyhow!(
                            "Requested count {} but already have shards at count {}",
                            split_req.new_shard_count,
                            shard.shard.count.0
                        )));
                    }
                    Ordering::Less => {
                        // Fall through: this shard has lower count than requested,
                        // is a candidate for splitting.
                    }
                }

                match old_shard_count {
                    None => old_shard_count = Some(shard.shard.count),
                    Some(old_shard_count) => {
                        if old_shard_count != shard.shard.count {
                            // We may hit this case if a caller asked for two splits to
                            // different sizes, before the first one is complete.
                            // e.g. 1->2, 2->4, where the 4 call comes while we have a mixture
                            // of shard_count=1 and shard_count=2 shards in the map.
                            return Err(ApiError::Conflict(
                                "Cannot split, currently mid-split".to_string(),
                            ));
                        }
                    }
                }
                if policy.is_none() {
                    policy = Some(shard.policy.clone());
                }
                if shard_ident.is_none() {
                    shard_ident = Some(shard.shard);
                }

                if tenant_shard_id.shard_count == ShardCount(split_req.new_shard_count) {
                    tracing::info!(
                        "Tenant shard {} already has shard count {}",
                        tenant_shard_id,
                        split_req.new_shard_count
                    );
                    continue;
                }

                let node_id =
                    shard
                        .intent
                        .attached
                        .ok_or(ApiError::BadRequest(anyhow::anyhow!(
                            "Cannot split a tenant that is not attached"
                        )))?;

                let node = pageservers
                    .get(&node_id)
                    .expect("Pageservers may not be deleted while referenced");

                // TODO: if any reconciliation is currently in progress for this shard, wait for it.

                targets.push(SplitTarget {
                    parent_id: *tenant_shard_id,
                    node: node.clone(),
                    child_ids: tenant_shard_id.split(ShardCount(split_req.new_shard_count)),
                });
            }

            if targets.is_empty() {
                if children_found.len() == split_req.new_shard_count as usize {
                    return Ok(TenantShardSplitResponse {
                        new_shards: children_found,
                    });
                } else {
                    // No shards found to split, and no existing children found: the
                    // tenant doesn't exist at all.
                    return Err(ApiError::NotFound(
                        anyhow::anyhow!("Tenant {} not found", tenant_id).into(),
                    ));
                }
            }

            (old_shard_count, targets, locked.compute_hook.clone())
        };

        // unwrap safety: we would have returned above if we didn't find at least one shard to split
        let old_shard_count = old_shard_count.unwrap();
        let shard_ident = shard_ident.unwrap();
        let policy = policy.unwrap();

        // FIXME: we have dropped self.inner lock, and not yet written anything to the database: another
        // request could occur here, deleting or mutating the tenant.  begin_shard_split checks that the
        // parent shards exist as expected, but it would be neater to do the above pre-checks within the
        // same database transaction rather than pre-check in-memory and then maybe-fail the database write.

        // Before creating any new child shards in memory or on the pageservers, persist them: this
        // enables us to ensure that we will always be able to clean up if something goes wrong.  This also
        // acts as the protection against two concurrent attempts to split: one of them will get a database
        // error trying to insert the child shards.
        let mut child_tsps = Vec::new();
        for target in &targets {
            let mut this_child_tsps = Vec::new();
            for child in &target.child_ids {
                let mut child_shard = shard_ident;
                child_shard.number = child.shard_number;
                child_shard.count = child.shard_count;

                this_child_tsps.push(TenantShardPersistence {
                    tenant_id: child.tenant_id.to_string(),
                    shard_number: child.shard_number.0 as i32,
                    shard_count: child.shard_count.0 as i32,
                    shard_stripe_size: shard_ident.stripe_size.0 as i32,
                    // Note: this generation is a placeholder, [`Persistence::begin_shard_split`] will
                    // populate the correct generation as part of its transaction, to protect us
                    // against racing with changes in the state of the parent.
                    generation: 0,
                    generation_pageserver: target.node.id.0 as i64,
                    placement_policy: serde_json::to_string(&policy).unwrap(),
                    // TODO: get the config out of the map
                    config: serde_json::to_string(&TenantConfig::default()).unwrap(),
                    splitting: SplitState::Splitting,
                });
            }

            child_tsps.push((target.parent_id, this_child_tsps));
        }

        if let Err(e) = self
            .persistence
            .begin_shard_split(old_shard_count, tenant_id, child_tsps)
            .await
        {
            match e {
                DatabaseError::Query(diesel::result::Error::DatabaseError(
                    DatabaseErrorKind::UniqueViolation,
                    _,
                )) => {
                    // Inserting a child shard violated a unique constraint: we raced with another call to
                    // this function
                    tracing::warn!("Conflicting attempt to split {tenant_id}: {e}");
                    return Err(ApiError::Conflict("Tenant is already splitting".into()));
                }
                _ => return Err(ApiError::InternalServerError(e.into())),
            }
        }

        // TODO: issue split calls concurrently (this only matters once we're splitting
        // N>1 shards into M shards -- initially we're usually splitting 1 shard into N).

        for target in &targets {
            let SplitTarget {
                parent_id,
                node,
                child_ids,
            } = target;
            let client = mgmt_api::Client::new(node.base_url(), self.config.jwt_token.as_deref());
            let response = client
                .tenant_shard_split(
                    *parent_id,
                    TenantShardSplitRequest {
                        new_shard_count: split_req.new_shard_count,
                    },
                )
                .await
                .map_err(|e| ApiError::Conflict(format!("Failed to split {}: {}", parent_id, e)))?;

            tracing::info!(
                "Split {} into {}",
                parent_id,
                response
                    .new_shards
                    .iter()
                    .map(|s| format!("{:?}", s))
                    .collect::<Vec<_>>()
                    .join(",")
            );

            if &response.new_shards != child_ids {
                // This should never happen: the pageserver should agree with us on how shard splits work.
                return Err(ApiError::InternalServerError(anyhow::anyhow!(
                    "Splitting shard {} resulted in unexpected IDs: {:?} (expected {:?})",
                    parent_id,
                    response.new_shards,
                    child_ids
                )));
            }
        }

        // TODO: if the pageserver restarted concurrently with our split API call,
        // the actual generation of the child shard might differ from the generation
        // we expect it to have.  In order for our in-database generation to end up
        // correct, we should carry the child generation back in the response and apply it here
        // in complete_shard_split (and apply the correct generation in memory)
        // (or, we can carry generation in the request and reject the request if
        //  it doesn't match, but that requires more retry logic on this side)

        self.persistence
            .complete_shard_split(tenant_id, old_shard_count)
            .await?;

        // Replace all the shards we just split with their children
        let mut response = TenantShardSplitResponse {
            new_shards: Vec::new(),
        };
        let mut child_locations = Vec::new();
        {
            let mut locked = self.inner.write().unwrap();
            for target in targets {
                let SplitTarget {
                    parent_id,
                    node: _node,
                    child_ids,
                } = target;
                let (pageserver, generation, config) = {
                    let old_state = locked
                        .tenants
                        .remove(&parent_id)
                        .expect("It was present, we just split it");
                    (
                        old_state.intent.attached.unwrap(),
                        old_state.generation,
                        old_state.config.clone(),
                    )
                };

                locked.tenants.remove(&parent_id);

                for child in child_ids {
                    let mut child_shard = shard_ident;
                    child_shard.number = child.shard_number;
                    child_shard.count = child.shard_count;

                    let mut child_observed: HashMap<NodeId, ObservedStateLocation> = HashMap::new();
                    child_observed.insert(
                        pageserver,
                        ObservedStateLocation {
                            conf: Some(attached_location_conf(generation, &child_shard, &config)),
                        },
                    );

                    let mut child_state = TenantState::new(child, child_shard, policy.clone());
                    child_state.intent = IntentState::single(Some(pageserver));
                    child_state.observed = ObservedState {
                        locations: child_observed,
                    };
                    child_state.generation = generation;
                    child_state.config = config.clone();

                    child_locations.push((child, pageserver));

                    locked.tenants.insert(child, child_state);
                    response.new_shards.push(child);
                }
            }
        }

        // Send compute notifications for all the new shards
        let mut failed_notifications = Vec::new();
        for (child_id, child_ps) in child_locations {
            if let Err(e) = compute_hook.notify(child_id, child_ps, &cancel).await {
                tracing::warn!("Failed to update compute of {}->{} during split, proceeding anyway to complete split ({e})",
                        child_id, child_ps);
                failed_notifications.push(child_id);
            }
        }

        // If we failed any compute notifications, make a note to retry later.
        if !failed_notifications.is_empty() {
            let mut locked = self.inner.write().unwrap();
            for failed in failed_notifications {
                if let Some(shard) = locked.tenants.get_mut(&failed) {
                    shard.pending_compute_notification = true;
                }
            }
        }

        Ok(response)
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
