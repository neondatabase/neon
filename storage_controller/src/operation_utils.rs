use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use pageserver_api::controller_api::{NodeSchedulingPolicy, ShardSchedulingPolicy};
use utils::id::NodeId;
use utils::shard::TenantShardId;

use crate::background_node_operations::OperationError;
use crate::node::Node;
use crate::scheduler::Scheduler;
use crate::tenant_shard::TenantShard;

/// Check that the state of the node being drained is as expected:
/// node is present in memory and scheduling policy is set to expected_policy
pub(crate) fn validate_node_state(
    node_id: &NodeId,
    nodes: Arc<HashMap<NodeId, Node>>,
    expected_policy: NodeSchedulingPolicy,
) -> Result<(), OperationError> {
    let node = nodes.get(node_id).ok_or(OperationError::NodeStateChanged(
        format!("node {node_id} was removed").into(),
    ))?;

    let current_policy = node.get_scheduling();
    if current_policy != expected_policy {
        // TODO(vlad): maybe cancel pending reconciles before erroring out. need to think
        // about it
        return Err(OperationError::NodeStateChanged(
            format!("node {node_id} changed state to {current_policy:?}").into(),
        ));
    }

    Ok(())
}

/// Struct that houses a few utility methods for draining pageserver nodes
pub(crate) struct TenantShardDrain {
    pub(crate) drained_node: NodeId,
    pub(crate) tenant_shard_id: TenantShardId,
}

impl TenantShardDrain {
    /// Check if the tenant shard under question is eligible for drainining:
    /// it's primary attachment is on the node being drained
    pub(crate) fn tenant_shard_eligible_for_drain(
        &self,
        tenants: &BTreeMap<TenantShardId, TenantShard>,
        scheduler: &Scheduler,
    ) -> TenantShardDrainAction {
        let Some(tenant_shard) = tenants.get(&self.tenant_shard_id) else {
            return TenantShardDrainAction::Skip;
        };

        if *tenant_shard.intent.get_attached() != Some(self.drained_node) {
            // If the intent attached node is not the drained node, check the observed state
            // of the shard on the drained node. If it is Attached*, it means the shard is
            // beeing migrated from the drained node. The drain loop needs to wait for the
            // reconciliation to complete for a smooth draining.

            use pageserver_api::models::LocationConfigMode::*;

            let attach_mode = tenant_shard
                .observed
                .locations
                .get(&self.drained_node)
                .and_then(|observed| observed.conf.as_ref().map(|conf| conf.mode));

            return match (attach_mode, tenant_shard.intent.get_attached()) {
                (Some(AttachedSingle | AttachedMulti | AttachedStale), Some(intent_node_id)) => {
                    TenantShardDrainAction::Reconcile(*intent_node_id)
                }
                _ => TenantShardDrainAction::Skip,
            };
        }

        // Only tenants with a normal (Active) scheduling policy are proactively moved
        // around during a node drain.  Shards which have been manually configured to a different
        // policy are only rescheduled by manual intervention.
        match tenant_shard.get_scheduling_policy() {
            ShardSchedulingPolicy::Active | ShardSchedulingPolicy::Essential => {
                // A migration during drain is classed as 'essential' because it is required to
                // uphold our availability goals for the tenant: this shard is elegible for migration.
            }
            ShardSchedulingPolicy::Pause | ShardSchedulingPolicy::Stop => {
                // If we have been asked to avoid rescheduling this shard, then do not migrate it during a drain
                return TenantShardDrainAction::Skip;
            }
        }

        match tenant_shard.preferred_secondary(scheduler) {
            Some(node) => TenantShardDrainAction::RescheduleToSecondary(node),
            None => {
                tracing::warn!(
                    tenant_id=%self.tenant_shard_id.tenant_id, shard_id=%self.tenant_shard_id.shard_slug(),
                    "No eligible secondary while draining {}", self.drained_node
                );

                TenantShardDrainAction::Skip
            }
        }
    }

    /// Attempt to reschedule the tenant shard under question to one of its secondary locations
    /// Returns an Err when the operation should be aborted and Ok(None) when the tenant shard
    /// should be skipped.
    pub(crate) fn reschedule_to_secondary<'a>(
        &self,
        destination: NodeId,
        tenants: &'a mut BTreeMap<TenantShardId, TenantShard>,
        scheduler: &mut Scheduler,
        nodes: &Arc<HashMap<NodeId, Node>>,
    ) -> Result<Option<&'a mut TenantShard>, OperationError> {
        let tenant_shard = match tenants.get_mut(&self.tenant_shard_id) {
            Some(some) => some,
            None => {
                // Tenant shard was removed in the meantime.
                // Skip to the next one, but don't fail the overall operation
                return Ok(None);
            }
        };

        if !nodes.contains_key(&destination) {
            return Err(OperationError::NodeStateChanged(
                format!("node {destination} was removed").into(),
            ));
        }

        if !tenant_shard.intent.get_secondary().contains(&destination) {
            tracing::info!(
                tenant_id=%self.tenant_shard_id.tenant_id, shard_id=%self.tenant_shard_id.shard_slug(),
                "Secondary moved away from {destination} during drain"
            );

            return Ok(None);
        }

        match tenant_shard.reschedule_to_secondary(Some(destination), scheduler) {
            Err(e) => {
                tracing::warn!(
                    tenant_id=%self.tenant_shard_id.tenant_id, shard_id=%self.tenant_shard_id.shard_slug(),
                    "Scheduling error when draining pageserver {} : {}", self.drained_node, e
                );

                Ok(None)
            }
            Ok(()) => {
                let scheduled_to = tenant_shard.intent.get_attached();
                tracing::info!(
                    tenant_id=%self.tenant_shard_id.tenant_id, shard_id=%self.tenant_shard_id.shard_slug(),
                    "Rescheduled shard while draining node {}: {} -> {:?}",
                    self.drained_node,
                    self.drained_node,
                    scheduled_to
                );

                Ok(Some(tenant_shard))
            }
        }
    }
}

/// Action to take when draining a tenant shard.
pub(crate) enum TenantShardDrainAction {
    /// The tenant shard is on the draining node.
    /// Reschedule the tenant shard to a secondary location.
    /// Holds a destination node id to reschedule to.
    RescheduleToSecondary(NodeId),
    /// The tenant shard is beeing migrated from the draining node.
    /// Wait for the reconciliation to complete.
    /// Holds the intent attached node id.
    Reconcile(NodeId),
    /// The tenant shard is not eligible for drainining, skip it.
    Skip,
}
