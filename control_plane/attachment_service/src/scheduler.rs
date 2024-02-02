use control_plane::attachment_service::NodeSchedulingPolicy;
use std::{collections::HashMap, rc::Rc};
use utils::{http::error::ApiError, id::NodeId};

use crate::node::Node;

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

struct AttachedTarget(NodeId);
struct SecondaryTarget(NodeId);

/// A reference to a node which has been scheduled for use as an attached location
pub(crate) struct NodeAttached(Rc<AttachedTarget>);
/// A reference to a node which has been scheduled for use as a secondary location
pub(crate) struct NodeSecondary(Rc<SecondaryTarget>);

impl NodeAttached {
    pub(crate) fn id(&self) -> NodeId {
        (*self.0).0
    }
}

impl std::fmt::Debug for NodeAttached {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.id())
    }
}

impl NodeSecondary {
    pub(crate) fn id(&self) -> NodeId {
        (*self.0).0
    }
}

impl std::fmt::Debug for NodeSecondary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.id())
    }
}

/// What's the difference between [`Node`] and [`SchedulerNode`]?
/// - The regular Node type is a snapshot of the configuration of an external node: it holds addresses and ports, so
///   that if you have a Node, you can make pageserver API calls.  Nodes get cloned around into
///   async tasks, and an Arc<> of a map of nodes represents a snapshot of which nodes exist when
///   starting such a task.
/// - Scheduler nodes live inside the Scheduler, which lives inside the big Service lock.  These are not
///   a handle for external calls, they're just for keeping track of internal per-node state, like which
///   shards are intending to attach to which node.
pub(crate) struct SchedulerNode {
    node_id: NodeId,

    policy: NodeSchedulingPolicy,

    attached: Rc<AttachedTarget>,
    secondary: Rc<SecondaryTarget>,
}

impl Drop for SchedulerNode {
    fn drop(&mut self) {
        // Safety check that we will never unhook a node from the scheduler while
        // some tenants are still referencing it.  This is important because tenants
        // assume that the nodes they reference are always still present.
        assert!(Rc::<AttachedTarget>::strong_count(&self.attached) == 1);
        assert!(Rc::<SecondaryTarget>::strong_count(&self.secondary) == 1);
    }
}

/// Parameter for scheduling that expresses which nodes a shard _doesn't_ want to go on.  Typically
/// used to avoid scheduling a secondary location on the same node as an attached location, or vice
/// versa.
pub(crate) struct ShardConstraints {
    anti_affinity: Vec<NodeId>,
}

impl ShardConstraints {
    pub(crate) fn new() -> Self {
        Self {
            anti_affinity: Vec::new(),
        }
    }
    pub(crate) fn push_anti_affinity(&mut self, nodes: Vec<NodeId>) {
        self.anti_affinity.extend(nodes.into_iter())
    }

    pub(crate) fn push_anti_attached(&mut self, n: &NodeAttached) {
        self.anti_affinity.push(n.id())
    }
    pub(crate) fn push_anti_secondary(&mut self, n: &NodeSecondary) {
        self.anti_affinity.push(n.id())
    }
}

pub(crate) struct Scheduler {
    nodes: HashMap<NodeId, SchedulerNode>,
}

impl Scheduler {
    pub(crate) fn new(nodes: &HashMap<NodeId, Node>) -> Self {
        Self {
            nodes: nodes
                .iter()
                .map(|(node_id, node)| {
                    (
                        *node_id,
                        SchedulerNode {
                            node_id: *node_id,
                            attached: Rc::new(AttachedTarget(*node_id)),
                            secondary: Rc::new(SecondaryTarget(*node_id)),
                            policy: node.scheduling,
                        },
                    )
                })
                .collect(),
        }
    }

    pub(crate) fn node_downgrade(&self, node_attached: NodeAttached) -> NodeSecondary {
        // unwrap safety: we never drop a SchedulerNode while it is referenced by tenants: see check
        // in [`SchedulerNode::drop`].
        let node = self.nodes.get(&node_attached.id()).unwrap();
        NodeSecondary(node.secondary.clone())
    }

    /// For use during startup, when we know a node ID and want to increment the scheduler refcounts while
    /// we populate an IntentState
    pub(crate) fn node_reference_attached(&self, node_id: NodeId) -> NodeAttached {
        let node = self.nodes.get(&node_id).unwrap();
        NodeAttached(node.attached.clone())
    }

    pub(crate) fn node_reference_secondary(&self, node_id: NodeId) -> NodeSecondary {
        let node = self.nodes.get(&node_id).unwrap();
        NodeSecondary(node.secondary.clone())
    }

    pub(crate) fn schedule_shard_attached(
        &mut self,
        constraints: &ShardConstraints,
    ) -> Result<NodeAttached, ScheduleError> {
        let node = self.schedule_shard(constraints)?;
        Ok(NodeAttached(node.attached.clone()))
    }

    pub(crate) fn schedule_shard_secondary(
        &mut self,
        constraints: &ShardConstraints,
    ) -> Result<NodeSecondary, ScheduleError> {
        let node = self.schedule_shard(constraints)?;
        Ok(NodeSecondary(node.secondary.clone()))
    }

    fn schedule_shard(
        &mut self,
        constraints: &ShardConstraints,
    ) -> Result<&SchedulerNode, ScheduleError> {
        if self.nodes.is_empty() {
            return Err(ScheduleError::NoPageservers);
        }

        let mut tenant_counts = HashMap::new();
        for node in self.nodes.values() {
            tenant_counts.insert(
                node.node_id,
                Rc::<AttachedTarget>::strong_count(&node.attached)
                    + Rc::<SecondaryTarget>::strong_count(&node.secondary),
            );
        }

        let mut tenant_counts: Vec<(NodeId, usize)> = tenant_counts
            .iter()
            .filter_map(|(k, v)| {
                if constraints.anti_affinity.contains(k) {
                    None
                } else {
                    Some((*k, *v))
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
        *self.tenant_counts.get_mut(&node_id).unwrap() += 1;
        Ok(self.nodes.get(&node_id).unwrap())
    }
}
