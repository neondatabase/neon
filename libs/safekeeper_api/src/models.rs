use serde::{Deserialize, Serialize};
use utils::{
    id::{NodeId, TenantId, TimelineId},
    lsn::Lsn,
};

#[derive(Serialize, Deserialize)]
pub struct TimelineCreateRequest {
    #[serde(with = "serde_with::rust::display_fromstr")]
    pub tenant_id: TenantId,
    #[serde(with = "serde_with::rust::display_fromstr")]
    pub timeline_id: TimelineId,
    pub peer_ids: Option<Vec<NodeId>>,
    pub pg_version: u32,
    pub system_id: Option<u64>,
    pub wal_seg_size: Option<u32>,
    #[serde(with = "serde_with::rust::display_fromstr")]
    pub commit_lsn: Lsn,
    // If not passed, it is assigned to the beginning of commit_lsn segment.
    pub local_start_lsn: Option<Lsn>,
}
