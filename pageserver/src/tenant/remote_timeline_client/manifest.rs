use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use utils::{id::TimelineId, lsn::Lsn};

/// Tenant-shard scoped manifest
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TenantManifest {
    /// Debugging aid describing the version of this manifest.
    /// Can also be used for distinguishing breaking changes later on.
    pub version: usize,

    /// The list of offloaded timelines together with enough information
    /// to not have to actually load them.
    ///
    /// Note: the timelines mentioned in this list might be deleted, i.e.
    /// we don't hold an invariant that the references aren't dangling.
    /// Existence of index-part.json is the actual indicator of timeline existence.
    pub offloaded_timelines: Vec<OffloadedTimelineManifest>,
}

/// The remote level representation of an offloaded timeline.
///
/// Very similar to [`pageserver_api::models::OffloadedTimelineInfo`],
/// but the two datastructures serve different needs, this is for a persistent disk format
/// that must be backwards compatible, while the other is only for informative purposes.
#[derive(Clone, Serialize, Deserialize, Copy, PartialEq, Eq)]
pub struct OffloadedTimelineManifest {
    pub timeline_id: TimelineId,
    /// Whether the timeline has a parent it has been branched off from or not
    pub ancestor_timeline_id: Option<TimelineId>,
    /// Whether to retain the branch lsn at the ancestor or not
    pub ancestor_retain_lsn: Option<Lsn>,
    /// The time point when the timeline was archived
    pub archived_at: NaiveDateTime,
}

pub const LATEST_TENANT_MANIFEST_VERSION: usize = 1;

impl TenantManifest {
    pub(crate) fn empty() -> Self {
        Self {
            version: LATEST_TENANT_MANIFEST_VERSION,
            offloaded_timelines: vec![],
        }
    }
    pub fn from_json_bytes(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice::<Self>(bytes)
    }

    pub(crate) fn to_json_bytes(&self) -> serde_json::Result<Vec<u8>> {
        serde_json::to_vec(self)
    }
}
