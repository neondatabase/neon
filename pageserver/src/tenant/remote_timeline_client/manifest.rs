use serde::{Deserialize, Serialize};
use utils::{id::TimelineId, lsn::Lsn};

#[derive(Clone, Serialize, Deserialize)]
pub struct TenantManifest {
    /// Debugging aid describing the version of this manifest.
    pub version: usize,

    pub offloaded_timelines: Vec<OffloadedTimelineManifest>,
}

#[derive(Clone, Serialize, Deserialize, Copy)]
pub struct OffloadedTimelineManifest {
    pub timeline_id: TimelineId,
    /// Whether the timeline has a parent it has been branched off from or not
    pub ancestor_timeline_id: Option<TimelineId>,
    /// Whether to retain the branch lsn at the ancestor or not
    pub ancestor_retain_lsn: Option<Lsn>,
}

const LATEST_TENANT_MANIFEST_VERSION: usize = 1;

impl TenantManifest {
    pub fn empty() -> Self {
        Self {
            version: LATEST_TENANT_MANIFEST_VERSION,
            offloaded_timelines: vec![],
        }
    }
    pub fn from_s3_bytes(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice::<Self>(bytes)
    }

    pub fn to_s3_bytes(&self) -> serde_json::Result<Vec<u8>> {
        serde_json::to_vec(self)
    }
}
