use pageserver_api::shard::TenantShardId;
use std::collections::{BTreeMap, HashMap};
use utils::{http::error::ApiError, id::NodeId};

use crate::{node::Node, tenant_state::TenantState};

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

pub(crate) struct Scheduler {
    tenant_counts: HashMap<NodeId, usize>,
}

impl Scheduler {
    pub(crate) fn new(
        tenants: &BTreeMap<TenantShardId, TenantState>,
        nodes: &HashMap<NodeId, Node>,
    ) -> Self {
        let mut tenant_counts = HashMap::new();
        for node_id in nodes.keys() {
            tenant_counts.insert(*node_id, 0);
        }

        for tenant in tenants.values() {
            if let Some(ps) = tenant.intent.attached {
                let entry = tenant_counts.entry(ps).or_insert(0);
                *entry += 1;
            }
        }

        for (node_id, node) in nodes {
            if !node.may_schedule() {
                tenant_counts.remove(node_id);
            }
        }

        Self { tenant_counts }
    }

    pub(crate) fn schedule_shard(
        &mut self,
        hard_exclude: &[NodeId],
    ) -> Result<NodeId, ScheduleError> {
        if self.tenant_counts.is_empty() {
            return Err(ScheduleError::NoPageservers);
        }

        let mut tenant_counts: Vec<(NodeId, usize)> = self
            .tenant_counts
            .iter()
            .filter_map(|(k, v)| {
                if hard_exclude.contains(k) {
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

        let node_id = tenant_counts.first().unwrap().0;
        tracing::info!(
            "scheduler selected node {node_id} (elegible nodes {:?}, exclude: {hard_exclude:?})",
            tenant_counts.iter().map(|i| i.0 .0).collect::<Vec<_>>()
        );
        *self.tenant_counts.get_mut(&node_id).unwrap() += 1;
        Ok(node_id)
    }
}
