//! Branch mappings for convenience

use std::collections::HashMap;
use std::fs;
use std::path::Path;

use anyhow::{bail, Context};
use serde::{Deserialize, Serialize};

use utils::id::{TenantId, TenantTimelineId, TimelineId};

/// Keep human-readable aliases in memory (and persist them to config XXX), to hide tenant/timeline hex strings from the user.
#[derive(PartialEq, Eq, Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct BranchMappings {
    /// Default tenant ID to use with the 'neon_local' command line utility, when
    /// --tenant_id is not explicitly specified. This comes from the branches.
    pub default_tenant_id: Option<TenantId>,

    // A `HashMap<String, HashMap<TenantId, TimelineId>>` would be more appropriate here,
    // but deserialization into a generic toml object as `toml::Value::try_from` fails with an error.
    // https://toml.io/en/v1.0.0 does not contain a concept of "a table inside another table".
    pub mappings: HashMap<String, Vec<(TenantId, TimelineId)>>,
}

impl BranchMappings {
    pub fn register_branch_mapping(
        &mut self,
        branch_name: String,
        tenant_id: TenantId,
        timeline_id: TimelineId,
    ) -> anyhow::Result<()> {
        let existing_values = self.mappings.entry(branch_name.clone()).or_default();

        let existing_ids = existing_values
            .iter()
            .find(|(existing_tenant_id, _)| existing_tenant_id == &tenant_id);

        if let Some((_, old_timeline_id)) = existing_ids {
            if old_timeline_id == &timeline_id {
                Ok(())
            } else {
                bail!("branch '{branch_name}' is already mapped to timeline {old_timeline_id}, cannot map to another timeline {timeline_id}");
            }
        } else {
            existing_values.push((tenant_id, timeline_id));
            Ok(())
        }
    }

    pub fn get_branch_timeline_id(
        &self,
        branch_name: &str,
        tenant_id: TenantId,
    ) -> Option<TimelineId> {
        // If it looks like a timeline ID, return it as it is
        if let Ok(timeline_id) = branch_name.parse::<TimelineId>() {
            return Some(timeline_id);
        }

        self.mappings
            .get(branch_name)?
            .iter()
            .find(|(mapped_tenant_id, _)| mapped_tenant_id == &tenant_id)
            .map(|&(_, timeline_id)| timeline_id)
            .map(TimelineId::from)
    }

    pub fn timeline_name_mappings(&self) -> HashMap<TenantTimelineId, String> {
        self.mappings
            .iter()
            .flat_map(|(name, tenant_timelines)| {
                tenant_timelines.iter().map(|&(tenant_id, timeline_id)| {
                    (TenantTimelineId::new(tenant_id, timeline_id), name.clone())
                })
            })
            .collect()
    }

    pub fn persist(&self, path: &Path) -> anyhow::Result<()> {
        let content = &toml::to_string_pretty(self)?;
        fs::write(path, content).with_context(|| {
            format!(
                "Failed to write branch information into path '{}'",
                path.display()
            )
        })
    }

    pub fn load(path: &Path) -> anyhow::Result<BranchMappings> {
        let branches_file_contents = fs::read_to_string(path)?;
        Ok(toml::from_str(branches_file_contents.as_str())?)
    }
}
