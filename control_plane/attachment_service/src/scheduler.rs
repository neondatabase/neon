use crate::{node::Node, tenant_state::TenantState};
use serde::Serialize;
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

#[derive(Serialize, Eq, PartialEq)]
struct SchedulerNode {
    /// How many shards are currently scheduled on this node, via their [`crate::tenant_state::IntentState`].
    shard_count: usize,

    /// Whether this node is currently elegible to have new shards scheduled (this is derived
    /// from a node's availability state and scheduling policy).
    may_schedule: bool,
}

/// This type is responsible for selecting which node is used when a tenant shard needs to choose a pageserver
/// on which to run.
///
/// The type has no persistent state of its own: this is all populated at startup.  The Serialize
/// impl is only for debug dumps.
#[derive(Serialize)]
pub(crate) struct Scheduler {
    nodes: HashMap<NodeId, SchedulerNode>,
}

impl Scheduler {
    pub(crate) fn new<'a>(nodes: impl Iterator<Item = &'a Node>) -> Self {
        let mut scheduler_nodes = HashMap::new();
        for node in nodes {
            scheduler_nodes.insert(
                node.id,
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

    /// For debug/support: check that our internal statistics are in sync with the state of
    /// the nodes & tenant shards.
    ///
    /// If anything is inconsistent, log details and return an error.
    pub(crate) fn consistency_check<'a>(
        &self,
        nodes: impl Iterator<Item = &'a Node>,
        shards: impl Iterator<Item = &'a TenantState>,
    ) -> anyhow::Result<()> {
        let mut expect_nodes: HashMap<NodeId, SchedulerNode> = HashMap::new();
        for node in nodes {
            expect_nodes.insert(
                node.id,
                SchedulerNode {
                    shard_count: 0,
                    may_schedule: node.may_schedule(),
                },
            );
        }

        for shard in shards {
            if let Some(node_id) = shard.intent.get_attached() {
                match expect_nodes.get_mut(node_id) {
                    Some(node) => node.shard_count += 1,
                    None => anyhow::bail!(
                        "Tenant {} references nonexistent node {}",
                        shard.tenant_shard_id,
                        node_id
                    ),
                }
            }

            for node_id in shard.intent.get_secondary() {
                match expect_nodes.get_mut(node_id) {
                    Some(node) => node.shard_count += 1,
                    None => anyhow::bail!(
                        "Tenant {} references nonexistent node {}",
                        shard.tenant_shard_id,
                        node_id
                    ),
                }
            }
        }

        for (node_id, expect_node) in &expect_nodes {
            let Some(self_node) = self.nodes.get(node_id) else {
                anyhow::bail!("Node {node_id} not found in Self")
            };

            if self_node != expect_node {
                tracing::error!("Inconsistency detected in scheduling state for node {node_id}");
                tracing::error!("Expected state: {}", serde_json::to_string(expect_node)?);
                tracing::error!("Self state: {}", serde_json::to_string(self_node)?);

                anyhow::bail!("Inconsistent state on {node_id}");
            }
        }

        if expect_nodes.len() != self.nodes.len() {
            // We just checked that all the expected nodes are present.  If the lengths don't match,
            // it means that we have nodes in Self that are unexpected.
            for node_id in self.nodes.keys() {
                if !expect_nodes.contains_key(node_id) {
                    anyhow::bail!("Node {node_id} found in Self but not in expected nodes");
                }
            }
        }

        Ok(())
    }

    /// Increment the reference count of a node.  This reference count is used to guide scheduling
    /// decisions, not for memory management: it represents one tenant shard whose IntentState targets
    /// this node.
    ///
    /// It is an error to call this for a node that is not known to the scheduler (i.e. passed into
    /// [`Self::new`] or [`Self::node_upsert`])
    pub(crate) fn node_inc_ref(&mut self, node_id: NodeId) {
        let Some(node) = self.nodes.get_mut(&node_id) else {
            tracing::error!("Scheduler missing node {node_id}");
            debug_assert!(false);
            return;
        };

        node.shard_count += 1;
    }

    /// Decrement a node's reference count.  Inverse of [`Self::node_inc_ref`].
    pub(crate) fn node_dec_ref(&mut self, node_id: NodeId) {
        let Some(node) = self.nodes.get_mut(&node_id) else {
            debug_assert!(false);
            tracing::error!("Scheduler missing node {node_id}");
            return;
        };

        node.shard_count -= 1;
    }

    pub(crate) fn node_upsert(&mut self, node: &Node) {
        use std::collections::hash_map::Entry::*;
        match self.nodes.entry(node.id) {
            Occupied(mut entry) => {
                entry.get_mut().may_schedule = node.may_schedule();
            }
            Vacant(entry) => {
                entry.insert(SchedulerNode {
                    shard_count: 0,
                    may_schedule: node.may_schedule(),
                });
            }
        }
    }

    pub(crate) fn node_remove(&mut self, node_id: NodeId) {
        if self.nodes.remove(&node_id).is_none() {
            tracing::warn!(node_id=%node_id, "Removed non-existent node from scheduler");
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
            // After applying constraints, no pageservers were left.  We log some detail about
            // the state of nodes to help understand why this happened.  This is not logged as an error because
            // it is legitimately possible for enough nodes to be Offline to prevent scheduling a shard.
            tracing::info!("Scheduling failure, while excluding {hard_exclude:?}, node states:");
            for (node_id, node) in &self.nodes {
                tracing::info!(
                    "Node {node_id}: may_schedule={} shards={}",
                    node.may_schedule,
                    node.shard_count
                );
            }

            return Err(ScheduleError::ImpossibleConstraint);
        }

        let node_id = tenant_counts.first().unwrap().0;
        tracing::info!(
            "scheduler selected node {node_id} (elegible nodes {:?}, exclude: {hard_exclude:?})",
            tenant_counts.iter().map(|i| i.0 .0).collect::<Vec<_>>()
        );

        // Note that we do not update shard count here to reflect the scheduling: that
        // is IntentState's job when the scheduled location is used.

        Ok(node_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    use control_plane::attachment_service::{NodeAvailability, NodeSchedulingPolicy};
    use utils::id::NodeId;

    use crate::{node::Node, tenant_state::IntentState};

    #[test]
    fn scheduler_basic() -> anyhow::Result<()> {
        let mut nodes = HashMap::new();
        nodes.insert(
            NodeId(1),
            Node {
                id: NodeId(1),
                availability: NodeAvailability::Active,
                scheduling: NodeSchedulingPolicy::Active,
                listen_http_addr: String::new(),
                listen_http_port: 0,
                listen_pg_addr: String::new(),
                listen_pg_port: 0,
            },
        );

        nodes.insert(
            NodeId(2),
            Node {
                id: NodeId(2),
                availability: NodeAvailability::Active,
                scheduling: NodeSchedulingPolicy::Active,
                listen_http_addr: String::new(),
                listen_http_port: 0,
                listen_pg_addr: String::new(),
                listen_pg_port: 0,
            },
        );

        let mut scheduler = Scheduler::new(nodes.values());
        let mut t1_intent = IntentState::new();
        let mut t2_intent = IntentState::new();

        let scheduled = scheduler.schedule_shard(&[])?;
        t1_intent.set_attached(&mut scheduler, Some(scheduled));
        let scheduled = scheduler.schedule_shard(&[])?;
        t2_intent.set_attached(&mut scheduler, Some(scheduled));

        assert_eq!(scheduler.nodes.get(&NodeId(1)).unwrap().shard_count, 1);
        assert_eq!(scheduler.nodes.get(&NodeId(2)).unwrap().shard_count, 1);

        let scheduled = scheduler.schedule_shard(&t1_intent.all_pageservers())?;
        t1_intent.push_secondary(&mut scheduler, scheduled);

        assert_eq!(scheduler.nodes.get(&NodeId(1)).unwrap().shard_count, 1);
        assert_eq!(scheduler.nodes.get(&NodeId(2)).unwrap().shard_count, 2);

        t1_intent.clear(&mut scheduler);
        assert_eq!(scheduler.nodes.get(&NodeId(1)).unwrap().shard_count, 0);
        assert_eq!(scheduler.nodes.get(&NodeId(2)).unwrap().shard_count, 1);

        if cfg!(debug_assertions) {
            // Dropping an IntentState without clearing it causes a panic in debug mode,
            // because we have failed to properly update scheduler shard counts.
            let result = std::panic::catch_unwind(move || {
                drop(t2_intent);
            });
            assert!(result.is_err());
        } else {
            t2_intent.clear(&mut scheduler);
            assert_eq!(scheduler.nodes.get(&NodeId(1)).unwrap().shard_count, 0);
            assert_eq!(scheduler.nodes.get(&NodeId(2)).unwrap().shard_count, 0);
        }

        Ok(())
    }
}
