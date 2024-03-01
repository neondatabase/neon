use crate::persistence::Persistence;
use crate::service;
use pageserver_api::controller_api::NodeAvailability;
use pageserver_api::models::{
    LocationConfig, LocationConfigMode, LocationConfigSecondary, TenantConfig,
};
use pageserver_api::shard::{ShardIdentity, TenantShardId};
use pageserver_client::mgmt_api;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use utils::generation::Generation;
use utils::id::{NodeId, TimelineId};
use utils::lsn::Lsn;
use utils::sync::gate::GateGuard;

use crate::compute_hook::{ComputeHook, NotifyError};
use crate::node::Node;
use crate::tenant_state::{IntentState, ObservedState, ObservedStateLocation};

/// Object with the lifetime of the background reconcile task that is created
/// for tenants which have a difference between their intent and observed states.
pub(super) struct Reconciler {
    /// See [`crate::tenant_state::TenantState`] for the meanings of these fields: they are a snapshot
    /// of a tenant's state from when we spawned a reconcile task.
    pub(super) tenant_shard_id: TenantShardId,
    pub(crate) shard: ShardIdentity,
    pub(crate) generation: Generation,
    pub(crate) intent: TargetState,
    pub(crate) config: TenantConfig,
    pub(crate) observed: ObservedState,

    pub(crate) service_config: service::Config,

    /// A snapshot of the pageservers as they were when we were asked
    /// to reconcile.
    pub(crate) pageservers: Arc<HashMap<NodeId, Node>>,

    /// A hook to notify the running postgres instances when we change the location
    /// of a tenant.  Use this via [`Self::compute_notify`] to update our failure flag
    /// and guarantee eventual retries.
    pub(crate) compute_hook: Arc<ComputeHook>,

    /// To avoid stalling if the cloud control plane is unavailable, we may proceed
    /// past failures in [`ComputeHook::notify`], but we _must_ remember that we failed
    /// so that we can set [`crate::tenant_state::TenantState::pending_compute_notification`] to ensure a later retry.
    pub(crate) compute_notify_failure: bool,

    /// A means to abort background reconciliation: it is essential to
    /// call this when something changes in the original TenantState that
    /// will make this reconciliation impossible or unnecessary, for
    /// example when a pageserver node goes offline, or the PlacementPolicy for
    /// the tenant is changed.
    pub(crate) cancel: CancellationToken,

    /// Reconcilers are registered with a Gate so that during a graceful shutdown we
    /// can wait for all the reconcilers to respond to their cancellation tokens.
    pub(crate) _gate_guard: GateGuard,

    /// Access to persistent storage for updating generation numbers
    pub(crate) persistence: Arc<Persistence>,
}

/// This is a snapshot of [`crate::tenant_state::IntentState`], but it does not do any
/// reference counting for Scheduler.  The IntentState is what the scheduler works with,
/// and the TargetState is just the instruction for a particular Reconciler run.
#[derive(Debug)]
pub(crate) struct TargetState {
    pub(crate) attached: Option<NodeId>,
    pub(crate) secondary: Vec<NodeId>,
}

impl TargetState {
    pub(crate) fn from_intent(intent: &IntentState) -> Self {
        Self {
            attached: *intent.get_attached(),
            secondary: intent.get_secondary().clone(),
        }
    }

    fn all_pageservers(&self) -> Vec<NodeId> {
        let mut result = self.secondary.clone();
        if let Some(node_id) = &self.attached {
            result.push(*node_id);
        }
        result
    }
}

#[derive(thiserror::Error, Debug)]
pub(crate) enum ReconcileError {
    #[error(transparent)]
    Notify(#[from] NotifyError),
    #[error("Cancelled")]
    Cancel,
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl Reconciler {
    async fn location_config(
        &mut self,
        node_id: NodeId,
        config: LocationConfig,
        flush_ms: Option<Duration>,
    ) -> anyhow::Result<()> {
        let node = self
            .pageservers
            .get(&node_id)
            .expect("Pageserver may not be removed while referenced");

        self.observed
            .locations
            .insert(node.id, ObservedStateLocation { conf: None });

        tracing::info!("location_config({}) calling: {:?}", node_id, config);
        let client =
            mgmt_api::Client::new(node.base_url(), self.service_config.jwt_token.as_deref());
        client
            .location_config(self.tenant_shard_id, config.clone(), flush_ms)
            .await?;
        tracing::info!("location_config({}) complete: {:?}", node_id, config);

        self.observed
            .locations
            .insert(node.id, ObservedStateLocation { conf: Some(config) });

        Ok(())
    }

    async fn maybe_live_migrate(&mut self) -> Result<(), ReconcileError> {
        let destination = if let Some(node_id) = self.intent.attached {
            match self.observed.locations.get(&node_id) {
                Some(conf) => {
                    // We will do a live migration only if the intended destination is not
                    // currently in an attached state.
                    match &conf.conf {
                        Some(conf) if conf.mode == LocationConfigMode::Secondary => {
                            // Fall through to do a live migration
                            node_id
                        }
                        None | Some(_) => {
                            // Attached or uncertain: don't do a live migration, proceed
                            // with a general-case reconciliation
                            tracing::info!("maybe_live_migrate: destination is None or attached");
                            return Ok(());
                        }
                    }
                }
                None => {
                    // Our destination is not attached: maybe live migrate if some other
                    // node is currently attached.  Fall through.
                    node_id
                }
            }
        } else {
            // No intent to be attached
            tracing::info!("maybe_live_migrate: no attached intent");
            return Ok(());
        };

        let mut origin = None;
        for (node_id, state) in &self.observed.locations {
            if let Some(observed_conf) = &state.conf {
                if observed_conf.mode == LocationConfigMode::AttachedSingle {
                    let node = self
                        .pageservers
                        .get(node_id)
                        .expect("Nodes may not be removed while referenced");
                    // We will only attempt live migration if the origin is not offline: this
                    // avoids trying to do it while reconciling after responding to an HA failover.
                    if !matches!(node.availability, NodeAvailability::Offline) {
                        origin = Some(*node_id);
                        break;
                    }
                }
            }
        }

        let Some(origin) = origin else {
            tracing::info!("maybe_live_migrate: no origin found");
            return Ok(());
        };

        // We have an origin and a destination: proceed to do the live migration
        tracing::info!("Live migrating {}->{}", origin, destination);
        self.live_migrate(origin, destination).await?;

        Ok(())
    }

    async fn get_lsns(
        &self,
        tenant_shard_id: TenantShardId,
        node_id: &NodeId,
    ) -> anyhow::Result<HashMap<TimelineId, Lsn>> {
        let node = self
            .pageservers
            .get(node_id)
            .expect("Pageserver may not be removed while referenced");

        let client =
            mgmt_api::Client::new(node.base_url(), self.service_config.jwt_token.as_deref());

        let timelines = client.timeline_list(&tenant_shard_id).await?;
        Ok(timelines
            .into_iter()
            .map(|t| (t.timeline_id, t.last_record_lsn))
            .collect())
    }

    async fn secondary_download(&self, tenant_shard_id: TenantShardId, node_id: &NodeId) {
        let node = self
            .pageservers
            .get(node_id)
            .expect("Pageserver may not be removed while referenced");

        let client =
            mgmt_api::Client::new(node.base_url(), self.service_config.jwt_token.as_deref());

        match client.tenant_secondary_download(tenant_shard_id).await {
            Ok(()) => {}
            Err(_) => {
                tracing::info!("  (skipping, destination wasn't in secondary mode)")
            }
        }
    }

    async fn await_lsn(
        &self,
        tenant_shard_id: TenantShardId,
        pageserver_id: &NodeId,
        baseline: HashMap<TimelineId, Lsn>,
    ) -> anyhow::Result<()> {
        loop {
            let latest = match self.get_lsns(tenant_shard_id, pageserver_id).await {
                Ok(l) => l,
                Err(e) => {
                    println!(
                        "🕑 Can't get LSNs on pageserver {} yet, waiting ({e})",
                        pageserver_id
                    );
                    std::thread::sleep(Duration::from_millis(500));
                    continue;
                }
            };

            let mut any_behind: bool = false;
            for (timeline_id, baseline_lsn) in &baseline {
                match latest.get(timeline_id) {
                    Some(latest_lsn) => {
                        println!("🕑 LSN origin {baseline_lsn} vs destination {latest_lsn}");
                        if latest_lsn < baseline_lsn {
                            any_behind = true;
                        }
                    }
                    None => {
                        // Expected timeline isn't yet visible on migration destination.
                        // (IRL we would have to account for timeline deletion, but this
                        //  is just test helper)
                        any_behind = true;
                    }
                }
            }

            if !any_behind {
                println!("✅ LSN caught up.  Proceeding...");
                break;
            } else {
                std::thread::sleep(Duration::from_millis(500));
            }
        }

        Ok(())
    }

    pub async fn live_migrate(
        &mut self,
        origin_ps_id: NodeId,
        dest_ps_id: NodeId,
    ) -> anyhow::Result<()> {
        // `maybe_live_migrate` is responsibble for sanity of inputs
        assert!(origin_ps_id != dest_ps_id);

        fn build_location_config(
            shard: &ShardIdentity,
            config: &TenantConfig,
            mode: LocationConfigMode,
            generation: Option<Generation>,
            secondary_conf: Option<LocationConfigSecondary>,
        ) -> LocationConfig {
            LocationConfig {
                mode,
                generation: generation.map(|g| g.into().unwrap()),
                secondary_conf,
                tenant_conf: config.clone(),
                shard_number: shard.number.0,
                shard_count: shard.count.literal(),
                shard_stripe_size: shard.stripe_size.0,
            }
        }

        tracing::info!(
            "🔁 Switching origin pageserver {} to stale mode",
            origin_ps_id
        );

        // FIXME: it is incorrect to use self.generation here, we should use the generation
        // from the ObservedState of the origin pageserver (it might be older than self.generation)
        let stale_conf = build_location_config(
            &self.shard,
            &self.config,
            LocationConfigMode::AttachedStale,
            Some(self.generation),
            None,
        );
        self.location_config(origin_ps_id, stale_conf, Some(Duration::from_secs(10)))
            .await?;

        let baseline_lsns = Some(self.get_lsns(self.tenant_shard_id, &origin_ps_id).await?);

        // If we are migrating to a destination that has a secondary location, warm it up first
        if let Some(destination_conf) = self.observed.locations.get(&dest_ps_id) {
            if let Some(destination_conf) = &destination_conf.conf {
                if destination_conf.mode == LocationConfigMode::Secondary {
                    tracing::info!(
                        "🔁 Downloading latest layers to destination pageserver {}",
                        dest_ps_id,
                    );
                    self.secondary_download(self.tenant_shard_id, &dest_ps_id)
                        .await;
                }
            }
        }

        // Increment generation before attaching to new pageserver
        self.generation = self
            .persistence
            .increment_generation(self.tenant_shard_id, dest_ps_id)
            .await?;

        let dest_conf = build_location_config(
            &self.shard,
            &self.config,
            LocationConfigMode::AttachedMulti,
            Some(self.generation),
            None,
        );

        tracing::info!("🔁 Attaching to pageserver {}", dest_ps_id);
        self.location_config(dest_ps_id, dest_conf, None).await?;

        if let Some(baseline) = baseline_lsns {
            tracing::info!("🕑 Waiting for LSN to catch up...");
            self.await_lsn(self.tenant_shard_id, &dest_ps_id, baseline)
                .await?;
        }

        tracing::info!("🔁 Notifying compute to use pageserver {}", dest_ps_id);

        // During a live migration it is unhelpful to proceed if we couldn't notify compute: if we detach
        // the origin without notifying compute, we will render the tenant unavailable.
        while let Err(e) = self.compute_notify().await {
            match e {
                NotifyError::Fatal(_) => return Err(anyhow::anyhow!(e)),
                _ => {
                    tracing::warn!(
                        "Live migration blocked by compute notification error, retrying: {e}"
                    );
                }
            }
        }

        // Downgrade the origin to secondary.  If the tenant's policy is PlacementPolicy::Single, then
        // this location will be deleted in the general case reconciliation that runs after this.
        let origin_secondary_conf = build_location_config(
            &self.shard,
            &self.config,
            LocationConfigMode::Secondary,
            None,
            Some(LocationConfigSecondary { warm: true }),
        );
        self.location_config(origin_ps_id, origin_secondary_conf.clone(), None)
            .await?;
        // TODO: we should also be setting the ObservedState on earlier API calls, in case we fail
        // partway through.  In fact, all location conf API calls should be in a wrapper that sets
        // the observed state to None, then runs, then sets it to what we wrote.
        self.observed.locations.insert(
            origin_ps_id,
            ObservedStateLocation {
                conf: Some(origin_secondary_conf),
            },
        );

        println!(
            "🔁 Switching to AttachedSingle mode on pageserver {}",
            dest_ps_id
        );
        let dest_final_conf = build_location_config(
            &self.shard,
            &self.config,
            LocationConfigMode::AttachedSingle,
            Some(self.generation),
            None,
        );
        self.location_config(dest_ps_id, dest_final_conf.clone(), None)
            .await?;
        self.observed.locations.insert(
            dest_ps_id,
            ObservedStateLocation {
                conf: Some(dest_final_conf),
            },
        );

        println!("✅ Migration complete");

        Ok(())
    }

    /// Reconciling a tenant makes API calls to pageservers until the observed state
    /// matches the intended state.
    ///
    /// First we apply special case handling (e.g. for live migrations), and then a
    /// general case reconciliation where we walk through the intent by pageserver
    /// and call out to the pageserver to apply the desired state.
    pub(crate) async fn reconcile(&mut self) -> Result<(), ReconcileError> {
        // TODO: if any of self.observed is None, call to remote pageservers
        // to learn correct state.

        // Special case: live migration
        self.maybe_live_migrate().await?;

        // If the attached pageserver is not attached, do so now.
        if let Some(node_id) = self.intent.attached {
            let mut wanted_conf =
                attached_location_conf(self.generation, &self.shard, &self.config);
            match self.observed.locations.get(&node_id) {
                Some(conf) if conf.conf.as_ref() == Some(&wanted_conf) => {
                    // Nothing to do
                    tracing::info!(%node_id, "Observed configuration already correct.")
                }
                _ => {
                    // In all cases other than a matching observed configuration, we will
                    // reconcile this location.  This includes locations with different configurations, as well
                    // as locations with unknown (None) observed state.
                    self.generation = self
                        .persistence
                        .increment_generation(self.tenant_shard_id, node_id)
                        .await?;
                    wanted_conf.generation = self.generation.into();
                    tracing::info!(%node_id, "Observed configuration requires update.");
                    self.location_config(node_id, wanted_conf, None).await?;
                    self.compute_notify().await?;
                }
            }
        }

        // Configure secondary locations: if these were previously attached this
        // implicitly downgrades them from attached to secondary.
        let mut changes = Vec::new();
        for node_id in &self.intent.secondary {
            let wanted_conf = secondary_location_conf(&self.shard, &self.config);
            match self.observed.locations.get(node_id) {
                Some(conf) if conf.conf.as_ref() == Some(&wanted_conf) => {
                    // Nothing to do
                    tracing::info!(%node_id, "Observed configuration already correct.")
                }
                _ => {
                    // In all cases other than a matching observed configuration, we will
                    // reconcile this location.
                    tracing::info!(%node_id, "Observed configuration requires update.");
                    changes.push((*node_id, wanted_conf))
                }
            }
        }

        // Detach any extraneous pageservers that are no longer referenced
        // by our intent.
        let all_pageservers = self.intent.all_pageservers();
        for node_id in self.observed.locations.keys() {
            if all_pageservers.contains(node_id) {
                // We are only detaching pageservers that aren't used at all.
                continue;
            }

            changes.push((
                *node_id,
                LocationConfig {
                    mode: LocationConfigMode::Detached,
                    generation: None,
                    secondary_conf: None,
                    shard_number: self.shard.number.0,
                    shard_count: self.shard.count.literal(),
                    shard_stripe_size: self.shard.stripe_size.0,
                    tenant_conf: self.config.clone(),
                },
            ));
        }

        for (node_id, conf) in changes {
            if self.cancel.is_cancelled() {
                return Err(ReconcileError::Cancel);
            }
            self.location_config(node_id, conf, None).await?;
        }

        Ok(())
    }

    pub(crate) async fn compute_notify(&mut self) -> Result<(), NotifyError> {
        // Whenever a particular Reconciler emits a notification, it is always notifying for the intended
        // destination.
        if let Some(node_id) = self.intent.attached {
            let result = self
                .compute_hook
                .notify(self.tenant_shard_id, node_id, &self.cancel)
                .await;
            if let Err(e) = &result {
                // It is up to the caller whether they want to drop out on this error, but they don't have to:
                // in general we should avoid letting unavailability of the cloud control plane stop us from
                // making progress.
                tracing::warn!("Failed to notify compute of attached pageserver {node_id}: {e}");
                // Set this flag so that in our ReconcileResult we will set the flag on the shard that it
                // needs to retry at some point.
                self.compute_notify_failure = true;
            }
            result
        } else {
            Ok(())
        }
    }
}

pub(crate) fn attached_location_conf(
    generation: Generation,
    shard: &ShardIdentity,
    config: &TenantConfig,
) -> LocationConfig {
    LocationConfig {
        mode: LocationConfigMode::AttachedSingle,
        generation: generation.into(),
        secondary_conf: None,
        shard_number: shard.number.0,
        shard_count: shard.count.literal(),
        shard_stripe_size: shard.stripe_size.0,
        tenant_conf: config.clone(),
    }
}

pub(crate) fn secondary_location_conf(
    shard: &ShardIdentity,
    config: &TenantConfig,
) -> LocationConfig {
    LocationConfig {
        mode: LocationConfigMode::Secondary,
        generation: None,
        secondary_conf: Some(LocationConfigSecondary { warm: true }),
        shard_number: shard.number.0,
        shard_count: shard.count.literal(),
        shard_stripe_size: shard.stripe_size.0,
        tenant_conf: config.clone(),
    }
}
