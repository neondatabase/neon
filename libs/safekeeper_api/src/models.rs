use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};

use utils::{
    id::{NodeId, TenantId, TimelineId},
    lsn::Lsn,
};

#[serde_as]
#[derive(Serialize, Deserialize)]
pub struct TimelineCreateRequest {
    #[serde_as(as = "DisplayFromStr")]
    pub tenant_id: TenantId,
    #[serde_as(as = "DisplayFromStr")]
    pub timeline_id: TimelineId,
    pub peer_ids: Option<Vec<NodeId>>,
    pub pg_version: u32,
    pub system_id: Option<u64>,
    pub wal_seg_size: Option<u32>,
    #[serde_as(as = "DisplayFromStr")]
    pub commit_lsn: Lsn,
    // If not passed, it is assigned to the beginning of commit_lsn segment.
    pub local_start_lsn: Option<Lsn>,
}
