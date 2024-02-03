use crate::node::Node;
use std::collections::HashMap;
use utils::{http::error::ApiError, id::NodeId};

/// Scenarios in which we cannot find a suitable location for a tenant shard
#[derive(thiserror::Error, Debug)]
pub enum ScheduleError {
    #[error("No pageservers found")]
    NoPageservers,
    #[error("No pageserver found matching constraint")]
    ImpossibleConstraint,
}

impl From<ScheduleError> for ApiError {
    fn from(value: ScheduleError) -> Self {
        ApiError::Conflict(format!("Scheduling error: {}", value))
    }
}

struct SchedulerNode {
    /// How many shards are currently scheduled on this node, via their [`crate::tenant_state::IntentState`].
    shard_count: usize,

    /// Whether this node is currently elegible to have new shards scheduled (this is derived
    /// from a node's availability state and scheduling policy).
    may_schedule: bool,
}

pub(crate) struct Scheduler {
    nodes: HashMap<NodeId, SchedulerNode>,
}

impl Scheduler {
    pub(crate) fn new(nodes: &HashMap<NodeId, Node>) -> Self {
        let mut scheduler_nodes = HashMap::new();
        for (node_id, node) in nodes {
            scheduler_nodes.insert(
                *node_id,
                SchedulerNode {
                    shard_count: 0,
                    may_schedule: node.may_schedule(),
                },
            );
        }

        Self {
            nodes: scheduler_nodes,
        }
    }

    pub(crate) fn node_ref(&mut self, node_id: NodeId) {
        let Some(node) = self.nodes.get_mut(&node_id) else {
            debug_assert!(false);
            tracing::error!("Scheduler missing node {node_id}");
            return;
        };

        node.shard_count += 1;
    }

    pub(crate) fn node_deref(&mut self, node_id: NodeId) {
        let Some(node) = self.nodes.get_mut(&node_id) else {
            debug_assert!(false);
            tracing::error!("Scheduler missing node {node_id}");
            return;
        };

        node.shard_count -= 1;
    }

    pub(crate) fn node_upsert(&mut self, node_id: NodeId, may_schedule: bool) {
        use std::collections::hash_map::Entry::*;
        match self.nodes.entry(node_id) {
            Occupied(mut entry) => {
                entry.get_mut().may_schedule = may_schedule;
            }
            Vacant(entry) => {
                entry.insert(SchedulerNode {
                    shard_count: 0,
                    may_schedule,
                });
            }
        }
    }

    pub(crate) fn schedule_shard(
        &mut self,
        hard_exclude: &[NodeId],
    ) -> Result<NodeId, ScheduleError> {
        if self.nodes.is_empty() {
            return Err(ScheduleError::NoPageservers);
        }

        let mut tenant_counts: Vec<(NodeId, usize)> = self
            .nodes
            .iter()
            .filter_map(|(k, v)| {
                if hard_exclude.contains(k) || !v.may_schedule {
                    None
                } else {
                    Some((*k, v.shard_count))
                }
            })
            .collect();

        // Sort by tenant count.  Nodes with the same tenant count are sorted by ID.
        tenant_counts.sort_by_key(|i| (i.1, i.0));

        if tenant_counts.is_empty() {
            // After applying constraints, no pageservers were left
            return Err(ScheduleError::ImpossibleConstraint);
        }

        for (node_id, count) in &tenant_counts {
            tracing::info!("tenant_counts[{node_id}]={count}");
        }

        let node_id = tenant_counts.first().unwrap().0;
        tracing::info!("scheduler selected node {node_id}");

        // Note that we do not update shard count here to reflect the scheduling: that
        // is IntentState's job when the scheduled location is used.

        Ok(node_id)
    }
}
