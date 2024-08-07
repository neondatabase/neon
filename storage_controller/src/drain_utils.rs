use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use pageserver_api::controller_api::NodeSchedulingPolicy;
use utils::{id::NodeId, shard::TenantShardId};

use crate::{
    background_node_operations::OperationError, node::Node, scheduler::Scheduler,
    tenant_shard::TenantShard,
};

pub(crate) struct TenantShardIterator<F> {
    tenants_accessor: F,
    inspected_all_shards: bool,
    last_inspected_shard: Option<TenantShardId>,
}

/// A simple iterator which can be used in tandem with [`crate::service::Service`]
/// to iterate over all known tenant shard ids without holding the lock on the
/// service state at all times.
impl<F> TenantShardIterator<F>
where
    F: Fn(Option<TenantShardId>) -> Option<TenantShardId>,
{
    pub(crate) fn new(tenants_accessor: F) -> Self {
        Self {
            tenants_accessor,
            inspected_all_shards: false,
            last_inspected_shard: None,
        }
    }

    /// Returns the next tenant shard id if one exists
    pub(crate) fn next(&mut self) -> Option<TenantShardId> {
        if self.inspected_all_shards {
            return None;
        }

        match (self.tenants_accessor)(self.last_inspected_shard) {
            Some(tid) => {
                self.last_inspected_shard = Some(tid);
                Some(tid)
            }
            None => {
                self.inspected_all_shards = true;
                None
            }
        }
    }

    /// Returns true when the end of the iterator is reached and false otherwise
    pub(crate) fn finished(&self) -> bool {
        self.inspected_all_shards
    }
}

/// Check that the state of the node being drained is as expected:
/// node is present in memory and scheduling policy is set to [`NodeSchedulingPolicy::Draining`]
pub(crate) fn validate_node_state(
    node_id: &NodeId,
    nodes: Arc<HashMap<NodeId, Node>>,
) -> Result<(), OperationError> {
    let node = nodes.get(node_id).ok_or(OperationError::NodeStateChanged(
        format!("node {} was removed", node_id).into(),
    ))?;

    let current_policy = node.get_scheduling();
    if !matches!(current_policy, NodeSchedulingPolicy::Draining) {
        // TODO(vlad): maybe cancel pending reconciles before erroring out. need to think
        // about it
        return Err(OperationError::NodeStateChanged(
            format!("node {} changed state to {:?}", node_id, current_policy).into(),
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
    ) -> Result<Option<NodeId>, OperationError> {
        let tenant_shard = tenants
            .get(&self.tenant_shard_id)
            .ok_or(OperationError::TenantShardRemoved(self.tenant_shard_id))?;

        if *tenant_shard.intent.get_attached() != Some(self.drained_node) {
            return Ok(None);
        }

        match scheduler.node_preferred(tenant_shard.intent.get_secondary()) {
            Some(node) => Ok(Some(node)),
            None => {
                tracing::warn!(
                    tenant_id=%self.tenant_shard_id.tenant_id, shard_id=%self.tenant_shard_id.shard_slug(),
                    "No eligible secondary while draining {}", self.drained_node
                );

                Ok(None)
            }
        }
    }

    /// Attempt to reschedule the tenant shard under question to one of its secondary locations
    pub(crate) fn reschedule_to_secondary<'a>(
        &self,
        destination: NodeId,
        tenants: &'a mut BTreeMap<TenantShardId, TenantShard>,
        scheduler: &mut Scheduler,
        nodes: &Arc<HashMap<NodeId, Node>>,
    ) -> Result<Option<&'a mut TenantShard>, OperationError> {
        let tenant_shard = tenants
            .get_mut(&self.tenant_shard_id)
            .ok_or(OperationError::TenantShardRemoved(self.tenant_shard_id))?;

        if !nodes.contains_key(&destination) {
            return Err(OperationError::NodeStateChanged(
                format!("node {} was removed", destination).into(),
            ));
        }

        if !tenant_shard.intent.get_secondary().contains(&destination) {
            return Err(OperationError::NodeStateChanged(
                format!(
                    "node {} not a secondary for {}",
                    destination, self.tenant_shard_id
                )
                .into(),
            ));
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use utils::{
        id::TenantId,
        shard::{ShardCount, ShardNumber, TenantShardId},
    };

    use super::TenantShardIterator;

    #[test]
    fn test_tenant_shard_iterator() {
        let tenant_id = TenantId::generate();
        let shard_count = ShardCount(8);

        let mut tenant_shards = Vec::default();
        for i in 0..shard_count.0 {
            tenant_shards.push((
                TenantShardId {
                    tenant_id,
                    shard_number: ShardNumber(i),
                    shard_count,
                },
                (),
            ))
        }

        let tenant_shards = Arc::new(tenant_shards);

        let mut tid_iter = TenantShardIterator::new({
            let tenants = tenant_shards.clone();
            move |last_inspected_shard: Option<TenantShardId>| {
                let entry = match last_inspected_shard {
                    Some(skip_past) => {
                        let mut cursor = tenants.iter().skip_while(|(tid, _)| *tid != skip_past);
                        cursor.nth(1)
                    }
                    None => tenants.first(),
                };

                entry.map(|(tid, _)| tid).copied()
            }
        });

        let mut iterated_over = Vec::default();
        while let Some(tid) = tid_iter.next() {
            iterated_over.push((tid, ()));
        }

        assert_eq!(iterated_over, *tenant_shards);
    }
}
