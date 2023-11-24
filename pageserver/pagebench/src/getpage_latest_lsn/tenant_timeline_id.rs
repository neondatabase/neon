use std::str::FromStr;

use anyhow::Context;
use utils::id::TimelineId;

use utils::id::TenantId;

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub(crate) struct TenantTimelineId {
    pub(crate) tenant_id: TenantId,
    pub(crate) timeline_id: TimelineId,
}

impl FromStr for TenantTimelineId {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (tenant_id, timeline_id) = s
            .split_once("/")
            .context("tenant and timeline id must be separated by `/`")?;
        let tenant_id = TenantId::from_str(&tenant_id)
            .with_context(|| format!("invalid tenant id: {tenant_id:?}"))?;
        let timeline_id = TimelineId::from_str(&timeline_id)
            .with_context(|| format!("invalid timeline id: {timeline_id:?}"))?;
        Ok(Self {
            tenant_id,
            timeline_id,
        })
    }
}

impl std::fmt::Display for TenantTimelineId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.tenant_id, self.timeline_id)
    }
}
