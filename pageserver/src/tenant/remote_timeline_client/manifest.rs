use serde::{Deserialize, Serialize};
use utils::{id::TimelineId, lsn::Lsn};

#[derive(Clone, Serialize, Deserialize)]
pub struct TenantManifest {
    /// Debugging aid describing the version of this manifest.
    version: usize,

    offloaded_timelines: Vec<OffloadedTimelineManifest>,
}

#[derive(Clone, Serialize, Deserialize, Copy)]
pub struct OffloadedTimelineManifest {
    pub timeline_id: TimelineId,
    /// Whether the timeline has a parent it has been branched off from or not
    pub ancestor_timeline_id: Option<TimelineId>,
    /// Whether to retain the branch lsn at the ancestor or not
    pub ancestor_retain_lsn: Option<Lsn>,
}
