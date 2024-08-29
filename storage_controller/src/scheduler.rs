use crate::{node::Node, tenant_shard::TenantShard};
use pageserver_api::controller_api::UtilizationScore;
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
pub enum MaySchedule {
    Yes(UtilizationScore),
    No,
}

#[derive(Serialize)]
struct SchedulerNode {
    /// How many shards are currently scheduled on this node, via their [`crate::tenant_shard::IntentState`].
    shard_count: usize,
    /// How many shards are currently attached on this node, via their [`crate::tenant_shard::IntentState`].
    attached_shard_count: usize,

    /// Whether this node is currently elegible to have new shards scheduled (this is derived
    /// from a node's availability state and scheduling policy).
    may_schedule: MaySchedule,
}

impl PartialEq for SchedulerNode {
    fn eq(&self, other: &Self) -> bool {
        let may_schedule_matches = matches!(
            (&self.may_schedule, &other.may_schedule),
            (MaySchedule::Yes(_), MaySchedule::Yes(_)) | (MaySchedule::No, MaySchedule::No)
        );

        may_schedule_matches
            && self.shard_count == other.shard_count
            && self.attached_shard_count == other.attached_shard_count
    }
}

impl Eq for SchedulerNode {}

/// This type is responsible for selecting which node is used when a tenant shard needs to choose a pageserver
/// on which to run.
///
/// The type has no persistent state of its own: this is all populated at startup.  The Serialize
/// impl is only for debug dumps.
#[derive(Serialize)]
pub(crate) struct Scheduler {
    nodes: HashMap<NodeId, SchedulerNode>,
}

/// Score for soft constraint scheduling: lower scores are preferred to higher scores.
///
/// For example, we may set an affinity score based on the number of shards from the same
/// tenant already on a node, to implicitly prefer to balance out shards.
#[derive(Copy, Clone, Debug, Eq, PartialEq, PartialOrd, Ord)]
pub(crate) struct AffinityScore(pub(crate) usize);

impl AffinityScore {
    /// If we have no anti-affinity at all toward a node, this is its score.  It means
    /// the scheduler has a free choice amongst nodes with this score, and may pick a node
    /// based on other information such as total utilization.
    pub(crate) const FREE: Self = Self(0);

    pub(crate) fn inc(&mut self) {
        self.0 += 1;
    }
}

impl std::ops::Add for AffinityScore {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        Self(self.0 + rhs.0)
    }
}

/// Hint for whether this is a sincere attempt to schedule, or a speculative
/// check for where we _would_ schedule (done during optimization)
#[derive(Debug)]
pub(crate) enum ScheduleMode {
    Normal,
    Speculative,
}

impl Default for ScheduleMode {
    fn default() -> Self {
        Self::Normal
    }
}

// For carrying state between multiple calls to [`TenantShard::schedule`], e.g. when calling
// it for many shards in the same tenant.
#[derive(Debug, Default)]
pub(crate) struct ScheduleContext {
    /// Sparse map of nodes: omitting a node implicitly makes its affinity [`AffinityScore::FREE`]
    pub(crate) nodes: HashMap<NodeId, AffinityScore>,

    /// Specifically how many _attached_ locations are on each node
    pub(crate) attached_nodes: HashMap<NodeId, usize>,

    pub(crate) mode: ScheduleMode,
}

impl ScheduleContext {
    /// Input is a list of nodes we would like to avoid using again within this context.  The more
    /// times a node is passed into this call, the less inclined we are to use it.
    pub(crate) fn avoid(&mut self, nodes: &[NodeId]) {
        for node_id in nodes {
            let entry = self.nodes.entry(*node_id).or_insert(AffinityScore::FREE);
            entry.inc()
        }
    }

    pub(crate) fn push_attached(&mut self, node_id: NodeId) {
        let entry = self.attached_nodes.entry(node_id).or_default();
        *entry += 1;
    }

    pub(crate) fn get_node_affinity(&self, node_id: NodeId) -> AffinityScore {
        self.nodes
            .get(&node_id)
            .copied()
            .unwrap_or(AffinityScore::FREE)
    }

    pub(crate) fn get_node_attachments(&self, node_id: NodeId) -> usize {
        self.attached_nodes.get(&node_id).copied().unwrap_or(0)
    }
}

pub(crate) enum RefCountUpdate {
    PromoteSecondary,
    Attach,
    Detach,
    DemoteAttached,
    AddSecondary,
    RemoveSecondary,
}

impl Scheduler {
    pub(crate) fn new<'a>(nodes: impl Iterator<Item = &'a Node>) -> Self {
        let mut scheduler_nodes = HashMap::new();
        for node in nodes {
            scheduler_nodes.insert(
                node.get_id(),
                SchedulerNode {
                    shard_count: 0,
                    attached_shard_count: 0,
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
        shards: impl Iterator<Item = &'a TenantShard>,
    ) -> anyhow::Result<()> {
        let mut expect_nodes: HashMap<NodeId, SchedulerNode> = HashMap::new();
        for node in nodes {
            expect_nodes.insert(
                node.get_id(),
                SchedulerNode {
                    shard_count: 0,
                    attached_shard_count: 0,
                    may_schedule: node.may_schedule(),
                },
            );
        }

        for shard in shards {
            if let Some(node_id) = shard.intent.get_attached() {
                match expect_nodes.get_mut(node_id) {
                    Some(node) => {
                        node.shard_count += 1;
                        node.attached_shard_count += 1;
                    }
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

    /// Update the reference counts of a node. These reference counts are used to guide scheduling
    /// decisions, not for memory management: they represent the number of tenant shard whose IntentState
    /// targets this node and the number of tenants shars whose IntentState is attached to this
    /// node.
    ///
    /// It is an error to call this for a node that is not known to the scheduler (i.e. passed into
    /// [`Self::new`] or [`Self::node_upsert`])
    pub(crate) fn update_node_ref_counts(&mut self, node_id: NodeId, update: RefCountUpdate) {
        let Some(node) = self.nodes.get_mut(&node_id) else {
            debug_assert!(false);
            tracing::error!("Scheduler missing node {node_id}");
            return;
        };

        match update {
            RefCountUpdate::PromoteSecondary => {
                node.attached_shard_count += 1;
            }
            RefCountUpdate::Attach => {
                node.shard_count += 1;
                node.attached_shard_count += 1;
            }
            RefCountUpdate::Detach => {
                node.shard_count -= 1;
                node.attached_shard_count -= 1;
            }
            RefCountUpdate::DemoteAttached => {
                node.attached_shard_count -= 1;
            }
            RefCountUpdate::AddSecondary => {
                node.shard_count += 1;
            }
            RefCountUpdate::RemoveSecondary => {
                node.shard_count -= 1;
            }
        }
    }

    pub(crate) fn node_upsert(&mut self, node: &Node) {
        use std::collections::hash_map::Entry::*;
        match self.nodes.entry(node.get_id()) {
            Occupied(mut entry) => {
                entry.get_mut().may_schedule = node.may_schedule();
            }
            Vacant(entry) => {
                entry.insert(SchedulerNode {
                    shard_count: 0,
                    attached_shard_count: 0,
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

    /// Where we have several nodes to choose from, for example when picking a secondary location
    /// to promote to an attached location, this method may be used to pick the best choice based
    /// on the scheduler's knowledge of utilization and availability.
    ///
    /// If the input is empty, or all the nodes are not elegible for scheduling, return None: the
    /// caller can pick a node some other way.
    pub(crate) fn node_preferred(&self, nodes: &[NodeId]) -> Option<NodeId> {
        if nodes.is_empty() {
            return None;
        }

        // TODO: When the utilization score returned by the pageserver becomes meaningful,
        // schedule based on that instead of the shard count.
        let node = nodes
            .iter()
            .map(|node_id| {
                let may_schedule = self
                    .nodes
                    .get(node_id)
                    .map(|n| n.may_schedule != MaySchedule::No)
                    .unwrap_or(false);
                (*node_id, may_schedule)
            })
            .max_by_key(|(_n, may_schedule)| *may_schedule);

        // If even the preferred node has may_schedule==false, return None
        node.and_then(|(node_id, may_schedule)| if may_schedule { Some(node_id) } else { None })
    }

    /// hard_exclude: it is forbidden to use nodes in this list, typically becacuse they
    /// are already in use by this shard -- we use this to avoid picking the same node
    /// as both attached and secondary location.  This is a hard constraint: if we cannot
    /// find any nodes that aren't in this list, then we will return a [`ScheduleError::ImpossibleConstraint`].
    ///
    /// context: we prefer to avoid using nodes identified in the context, according
    /// to their anti-affinity score.  We use this to prefeer to avoid placing shards in
    /// the same tenant on the same node.  This is a soft constraint: the context will never
    /// cause us to fail to schedule a shard.
    pub(crate) fn schedule_shard(
        &self,
        hard_exclude: &[NodeId],
        context: &ScheduleContext,
    ) -> Result<NodeId, ScheduleError> {
        if self.nodes.is_empty() {
            return Err(ScheduleError::NoPageservers);
        }

        let mut scores: Vec<(NodeId, AffinityScore, usize)> = self
            .nodes
            .iter()
            .filter_map(|(k, v)| {
                if hard_exclude.contains(k) || v.may_schedule == MaySchedule::No {
                    None
                } else {
                    Some((
                        *k,
                        context.nodes.get(k).copied().unwrap_or(AffinityScore::FREE),
                        v.shard_count,
                    ))
                }
            })
            .collect();

        // Sort by, in order of precedence:
        //  1st: Affinity score.  We should never pick a higher-score node if a lower-score node is available
        //  2nd: Utilization.  Within nodes with the same affinity, use the least loaded nodes.
        //  3rd: Node ID.  This is a convenience to make selection deterministic in tests and empty systems.
        scores.sort_by_key(|i| (i.1, i.2, i.0));

        if scores.is_empty() {
            // After applying constraints, no pageservers were left.
            if !matches!(context.mode, ScheduleMode::Speculative) {
                // If this was not a speculative attempt, log details to understand why we couldn't
                // schedule: this may help an engineer understand if some nodes are marked offline
                // in a way that's preventing progress.
                tracing::info!(
                    "Scheduling failure, while excluding {hard_exclude:?}, node states:"
                );
                for (node_id, node) in &self.nodes {
                    tracing::info!(
                        "Node {node_id}: may_schedule={} shards={}",
                        node.may_schedule != MaySchedule::No,
                        node.shard_count
                    );
                }
            }
            return Err(ScheduleError::ImpossibleConstraint);
        }

        // Lowest score wins
        let node_id = scores.first().unwrap().0;

        if !matches!(context.mode, ScheduleMode::Speculative) {
            tracing::info!(
            "scheduler selected node {node_id} (elegible nodes {:?}, hard exclude: {hard_exclude:?}, soft exclude: {context:?})",
            scores.iter().map(|i| i.0 .0).collect::<Vec<_>>()
        );
        }

        // Note that we do not update shard count here to reflect the scheduling: that
        // is IntentState's job when the scheduled location is used.

        Ok(node_id)
    }

    /// Unit test access to internal state
    #[cfg(test)]
    pub(crate) fn get_node_shard_count(&self, node_id: NodeId) -> usize {
        self.nodes.get(&node_id).unwrap().shard_count
    }

    #[cfg(test)]
    pub(crate) fn get_node_attached_shard_count(&self, node_id: NodeId) -> usize {
        self.nodes.get(&node_id).unwrap().attached_shard_count
    }
}

#[cfg(test)]
pub(crate) mod test_utils {

    use crate::node::Node;
    use pageserver_api::controller_api::{NodeAvailability, UtilizationScore};
    use std::collections::HashMap;
    use utils::id::NodeId;
    /// Test helper: synthesize the requested number of nodes, all in active state.
    ///
    /// Node IDs start at one.
    pub(crate) fn make_test_nodes(n: u64) -> HashMap<NodeId, Node> {
        (1..n + 1)
            .map(|i| {
                (NodeId(i), {
                    let mut node = Node::new(
                        NodeId(i),
                        format!("httphost-{i}"),
                        80 + i as u16,
                        format!("pghost-{i}"),
                        5432 + i as u16,
                    );
                    node.set_availability(NodeAvailability::Active(UtilizationScore::worst()));
                    assert!(node.is_available());
                    node
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::tenant_shard::IntentState;
    #[test]
    fn scheduler_basic() -> anyhow::Result<()> {
        let nodes = test_utils::make_test_nodes(2);

        let mut scheduler = Scheduler::new(nodes.values());
        let mut t1_intent = IntentState::new();
        let mut t2_intent = IntentState::new();

        let context = ScheduleContext::default();

        let scheduled = scheduler.schedule_shard(&[], &context)?;
        t1_intent.set_attached(&mut scheduler, Some(scheduled));
        let scheduled = scheduler.schedule_shard(&[], &context)?;
        t2_intent.set_attached(&mut scheduler, Some(scheduled));

        assert_eq!(scheduler.get_node_shard_count(NodeId(1)), 1);
        assert_eq!(scheduler.get_node_attached_shard_count(NodeId(1)), 1);

        assert_eq!(scheduler.get_node_shard_count(NodeId(2)), 1);
        assert_eq!(scheduler.get_node_attached_shard_count(NodeId(2)), 1);

        let scheduled = scheduler.schedule_shard(&t1_intent.all_pageservers(), &context)?;
        t1_intent.push_secondary(&mut scheduler, scheduled);

        assert_eq!(scheduler.get_node_shard_count(NodeId(1)), 1);
        assert_eq!(scheduler.get_node_attached_shard_count(NodeId(1)), 1);

        assert_eq!(scheduler.get_node_shard_count(NodeId(2)), 2);
        assert_eq!(scheduler.get_node_attached_shard_count(NodeId(2)), 1);

        t1_intent.clear(&mut scheduler);
        assert_eq!(scheduler.get_node_shard_count(NodeId(1)), 0);
        assert_eq!(scheduler.get_node_shard_count(NodeId(2)), 1);

        let total_attached = scheduler.get_node_attached_shard_count(NodeId(1))
            + scheduler.get_node_attached_shard_count(NodeId(2));
        assert_eq!(total_attached, 1);

        if cfg!(debug_assertions) {
            // Dropping an IntentState without clearing it causes a panic in debug mode,
            // because we have failed to properly update scheduler shard counts.
            let result = std::panic::catch_unwind(move || {
                drop(t2_intent);
            });
            assert!(result.is_err());
        } else {
            t2_intent.clear(&mut scheduler);

            assert_eq!(scheduler.get_node_shard_count(NodeId(1)), 0);
            assert_eq!(scheduler.get_node_attached_shard_count(NodeId(1)), 0);

            assert_eq!(scheduler.get_node_shard_count(NodeId(2)), 0);
            assert_eq!(scheduler.get_node_attached_shard_count(NodeId(2)), 0);
        }

        Ok(())
    }
}
