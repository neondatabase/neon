use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::{Duration, Instant},
};

use control_plane::attachment_service::{
    AttachHookRequest, AttachHookResponse, InspectRequest, InspectResponse, NodeAvailability,
    NodeConfigureRequest, NodeRegisterRequest, NodeSchedulingPolicy, TenantCreateResponse,
    TenantCreateResponseShard, TenantLocateResponse, TenantLocateResponseShard,
    TenantShardMigrateRequest, TenantShardMigrateResponse,
};
use hyper::Method;
use pageserver_api::{
    control_api::{
        ReAttachRequest, ReAttachResponse, ReAttachResponseTenant, ValidateRequest,
        ValidateResponse, ValidateResponseTenant,
    },
    models::{
        ShardParameters, TenantCreateRequest, TenantShardSplitRequest, TenantShardSplitResponse,
        TimelineCreateRequest, TimelineInfo,
    },
    shard::{ShardCount, ShardIdentity, ShardNumber, TenantShardId},
};
use reqwest::Client;
use utils::{
    generation::Generation,
    http::error::ApiError,
    id::{NodeId, TenantId},
};

use crate::{
    compute_hook::ComputeHook,
    node::Node,
    reconciler::attached_location_conf,
    scheduler::Scheduler,
    tenant_state::{
        IntentState, ObservedState, ObservedStateLocation, ReconcileResult, ReconcilerWaiter,
        TenantState,
    },
    PlacementPolicy,
};

const RECONCILE_TIMEOUT: Duration = Duration::from_secs(30);

// Top level state available to all HTTP handlers
struct ServiceState {
    tenants: BTreeMap<TenantShardId, TenantState>,

    nodes: Arc<HashMap<NodeId, Node>>,

    compute_hook: Arc<ComputeHook>,

    result_tx: tokio::sync::mpsc::UnboundedSender<ReconcileResult>,
}

impl ServiceState {
    fn new(result_tx: tokio::sync::mpsc::UnboundedSender<ReconcileResult>) -> Self {
        Self {
            tenants: BTreeMap::new(),
            nodes: Arc::new(HashMap::new()),
            compute_hook: Arc::new(ComputeHook::new()),
            result_tx,
        }
    }
}

pub struct Service {
    inner: Arc<std::sync::RwLock<ServiceState>>,
}

impl Service {
    pub fn spawn() -> Arc<Self> {
        let (result_tx, mut result_rx) = tokio::sync::mpsc::unbounded_channel();

        let this = Arc::new(Self {
            inner: Arc::new(std::sync::RwLock::new(ServiceState::new(result_tx))),
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
                if let Some(tenant) = locked.tenants.get_mut(&result.tenant_shard_id) {
                    tenant.generation = result.generation;
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
                            // TODO: some observability, record on teh tenant its last reconcile error
                            tracing::warn!(
                                "Reconcile error on tenant {}: {}",
                                tenant.tenant_shard_id,
                                e
                            );

                            for (node_id, o) in result.observed.locations {
                                tenant.observed.locations.insert(node_id, o);
                            }
                        }
                    }
                }
            }
        });

        this
    }

    pub(crate) fn attach_hook(&self, attach_req: AttachHookRequest) -> AttachHookResponse {
        let mut locked = self.inner.write().unwrap();

        let tenant_state = locked
            .tenants
            .entry(attach_req.tenant_shard_id)
            .or_insert_with(|| {
                TenantState::new(
                    attach_req.tenant_shard_id,
                    ShardIdentity::unsharded(),
                    PlacementPolicy::Single,
                )
            });

        if let Some(attaching_pageserver) = attach_req.node_id.as_ref() {
            tenant_state.generation = tenant_state.generation.next();
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
        let generation = tenant_state.generation;

        tracing::info!(
            "attach_hook: tenant {} set generation {:?}, pageserver {}",
            attach_req.tenant_shard_id,
            tenant_state.generation,
            attach_req.node_id.unwrap_or(utils::id::NodeId(0xfffffff))
        );

        AttachHookResponse {
            gen: attach_req.node_id.map(|_| generation.into().unwrap()),
        }
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

    pub(crate) fn re_attach(&self, reattach_req: ReAttachRequest) -> ReAttachResponse {
        let mut locked = self.inner.write().unwrap();

        let mut response = ReAttachResponse {
            tenants: Vec::new(),
        };
        for (t, state) in &mut locked.tenants {
            if state.intent.attached == Some(reattach_req.node_id) {
                state.generation = state.generation.next();
                response.tenants.push(ReAttachResponseTenant {
                    id: *t,
                    gen: state.generation.into().unwrap(),
                });
            }
        }
        response
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
            }
        }
        response
    }

    pub(crate) async fn tenant_create(
        &self,
        create_req: TenantCreateRequest,
    ) -> Result<TenantCreateResponse, ApiError> {
        let (waiters, response_shards) = {
            let mut locked = self.inner.write().unwrap();

            tracing::info!(
                "Creating tenant {}, shard_count={:?}, have {} pageservers",
                create_req.new_tenant_id,
                create_req.shard_parameters.count,
                locked.nodes.len()
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

            let mut scheduler = Scheduler::new(&locked.tenants, &locked.nodes);

            for i in 0..literal_shard_count {
                let shard_number = ShardNumber(i);

                let tenant_shard_id = TenantShardId {
                    tenant_id,
                    shard_number,
                    shard_count: create_req.shard_parameters.count,
                };
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
                            ShardIdentity::from_params(shard_number, &create_req.shard_parameters),
                            PlacementPolicy::Single,
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

            let mut waiters = Vec::new();
            let result_tx = locked.result_tx.clone();
            let compute_hook = locked.compute_hook.clone();

            for (_tenant_shard_id, shard) in locked
                .tenants
                .range_mut(TenantShardId::tenant_range(tenant_id))
            {
                if let Some(waiter) =
                    shard.maybe_reconcile(result_tx.clone(), &pageservers, &compute_hook)
                {
                    waiters.push(waiter);
                }
            }
            (waiters, response_shards)
        };

        let deadline = Instant::now().checked_add(Duration::from_secs(5)).unwrap();
        for waiter in waiters {
            let timeout = deadline.duration_since(Instant::now());
            waiter.wait_timeout(timeout).await.map_err(|_| {
                ApiError::Timeout(
                    format!(
                        "Timeout waiting for reconciliation of tenant shard {}",
                        waiter.tenant_shard_id
                    )
                    .into(),
                )
            })?;
        }
        Ok(TenantCreateResponse {
            shards: response_shards,
        })
    }

    pub(crate) async fn tenant_timeline_create(
        &self,
        tenant_id: TenantId,
        mut create_req: TimelineCreateRequest,
    ) -> Result<TimelineInfo, ApiError> {
        let mut timeline_info = None;
        let ensure_waiters = {
            let locked = self.inner.write().unwrap();

            tracing::info!(
                "Creating timeline {}/{}, have {} pageservers",
                tenant_id,
                create_req.new_timeline_id,
                locked.nodes.len()
            );

            ensure_attached(locked, tenant_id).map_err(ApiError::InternalServerError)?
        };

        let deadline = Instant::now().checked_add(Duration::from_secs(5)).unwrap();
        for waiter in ensure_waiters {
            let timeout = deadline.duration_since(Instant::now());
            waiter.wait_timeout(timeout).await.map_err(|_| {
                ApiError::Timeout(
                    format!(
                        "Timeout waiting for reconciliation of tenant shard {}",
                        waiter.tenant_shard_id
                    )
                    .into(),
                )
            })?;
        }

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

            let shard_timeline_info = timeline_create(&tenant_shard_id, &node, &create_req)
                .await
                .map_err(|e| ApiError::Conflict(format!("Failed to create timeline: {e}")))?;

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
                        stripe_size: Some(shard.shard.stripe_size),
                        count: shard.shard.count,
                    });
                }
                Some(params) => {
                    if params.stripe_size != Some(shard.shard.stripe_size) {
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
        let (targets, compute_hook) = {
            let mut locked = self.inner.write().unwrap();

            let pageservers = locked.nodes.clone();

            let mut targets = Vec::new();

            for (tenant_shard_id, shard) in locked
                .tenants
                .range_mut(TenantShardId::tenant_range(tenant_id))
            {
                if policy.is_none() {
                    policy = Some(shard.policy.clone());
                }

                if tenant_shard_id.shard_count == ShardCount(split_req.new_shard_count) {
                    tracing::warn!(
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

                targets.push((*tenant_shard_id, node.clone()));
            }
            (targets, locked.compute_hook.clone())
        };

        let mut replacements = HashMap::new();
        for (tenant_shard_id, node) in targets {
            let client = Client::new();
            let response = client
                .request(
                    Method::PUT,
                    format!("{}/tenant/{}/shard_split", node.base_url(), tenant_shard_id),
                )
                .json(&TenantShardSplitRequest {
                    new_shard_count: split_req.new_shard_count,
                })
                .send()
                .await
                .map_err(|e| {
                    ApiError::Conflict(format!("Failed to split {}: {}", tenant_shard_id, e))
                })?;
            response.error_for_status_ref().map_err(|e| {
                ApiError::Conflict(format!("Failed to split {}: {}", tenant_shard_id, e))
            })?;
            let response: TenantShardSplitResponse = response.json().await.map_err(|e| {
                ApiError::InternalServerError(anyhow::anyhow!(
                    "Malformed response from pageserver: {}",
                    e
                ))
            })?;

            tracing::info!(
                "Split {} into {}",
                tenant_shard_id,
                response
                    .new_shards
                    .iter()
                    .map(|s| format!("{:?}", s))
                    .collect::<Vec<_>>()
                    .join(",")
            );

            replacements.insert(tenant_shard_id, response.new_shards);
        }

        // TODO: concurrency: we're dropping the state lock while issuing split API calls.
        //       We should add some marker to the TenantState that causes any other change
        //       to refuse until the split is complete.  This will be related to a persistent
        //       splitting marker that will ensure resume after crash.

        // Replace all the shards we just split with their children
        let mut response = TenantShardSplitResponse {
            new_shards: Vec::new(),
        };
        let mut child_locations = Vec::new();
        {
            let mut locked = self.inner.write().unwrap();
            for (replaced, children) in replacements.into_iter() {
                let (pageserver, generation, shard_ident, config) = {
                    let old_state = locked
                        .tenants
                        .remove(&replaced)
                        .expect("It was present, we just split it");
                    (
                        old_state.intent.attached.unwrap(),
                        old_state.generation,
                        old_state.shard,
                        old_state.config.clone(),
                    )
                };

                locked.tenants.remove(&replaced);

                for child in children {
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

                    let mut child_state = TenantState::new(
                        child,
                        child_shard,
                        policy
                            .clone()
                            .expect("We set this if any replacements are pushed"),
                    );
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

        for (child_id, child_ps) in child_locations {
            if let Err(e) = compute_hook.notify(child_id, child_ps).await {
                tracing::warn!("Failed to update compute of {}->{} during split, proceeding anyway to complete split ({e})",
                        child_id, child_ps);
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

                shard.intent.attached = Some(migrate_req.node_id);
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
                }

                tracing::info!("Migrating: new intent {:?}", shard.intent);
                shard.sequence = shard.sequence.next();
            }

            shard.maybe_reconcile(result_tx, &pageservers, &compute_hook)
        };

        if let Some(waiter) = waiter {
            waiter
                .wait_timeout(RECONCILE_TIMEOUT)
                .await
                .map_err(|e| ApiError::Timeout(format!("{}", e).into()))?;
        } else {
            tracing::warn!("Migration is a no-op");
        }

        Ok(TenantShardMigrateResponse {})
    }

    pub(crate) fn node_register(&self, register_req: NodeRegisterRequest) {
        let mut locked = self.inner.write().unwrap();

        let mut new_nodes = (*locked.nodes).clone();

        new_nodes.insert(
            register_req.node_id,
            Node {
                id: register_req.node_id,
                listen_http_addr: register_req.listen_http_addr,
                listen_http_port: register_req.listen_http_port,
                listen_pg_addr: register_req.listen_pg_addr,
                listen_pg_port: register_req.listen_pg_port,
                scheduling: NodeSchedulingPolicy::Filling,
                // TODO: we shouldn't really call this Active until we've heartbeated it.
                availability: NodeAvailability::Active,
            },
        );

        locked.nodes = Arc::new(new_nodes);

        tracing::info!(
            "Registered pageserver {}, now have {} pageservers",
            register_req.node_id,
            locked.nodes.len()
        );
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
                        tenant_state.maybe_reconcile(result_tx.clone(), &new_nodes, &compute_hook);
                    }
                }
            }

            // TODO: in the background, we should balance work back onto this pageserver
        }

        locked.nodes = new_nodes;

        Ok(())
    }
}

// TODO: use mgmt_api client
async fn timeline_create(
    tenant_shard_id: &TenantShardId,
    node: &Node,
    req: &TimelineCreateRequest,
) -> anyhow::Result<TimelineInfo> {
    let client = Client::new();
    let response = client
        .request(
            Method::POST,
            format!("{}/tenant/{}/timeline", node.base_url(), tenant_shard_id),
        )
        .json(req)
        .send()
        .await?;
    response.error_for_status_ref()?;

    Ok(response.json().await?)
}

fn ensure_attached(
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

        if let Some(waiter) = shard.maybe_reconcile(result_tx.clone(), &pageservers, &compute_hook)
        {
            waiters.push(waiter);
        }
    }
    Ok(waiters)
}
