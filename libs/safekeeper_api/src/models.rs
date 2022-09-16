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

fn lsn_invalid() -> Lsn {
    Lsn::INVALID
}

/// Data about safekeeper's timeline, mirrors broker.proto.
#[serde_as]
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SkTimelineInfo {
    /// Term of the last entry.
    pub last_log_term: Option<u64>,
    /// LSN of the last record.
    #[serde_as(as = "DisplayFromStr")]
    #[serde(default = "lsn_invalid")]
    pub flush_lsn: Lsn,
    /// Up to which LSN safekeeper regards its WAL as committed.
    #[serde_as(as = "DisplayFromStr")]
    #[serde(default = "lsn_invalid")]
    pub commit_lsn: Lsn,
    /// LSN up to which safekeeper has backed WAL.
    #[serde_as(as = "DisplayFromStr")]
    #[serde(default = "lsn_invalid")]
    pub backup_lsn: Lsn,
    /// LSN of last checkpoint uploaded by pageserver.
    #[serde_as(as = "DisplayFromStr")]
    #[serde(default = "lsn_invalid")]
    pub remote_consistent_lsn: Lsn,
    #[serde_as(as = "DisplayFromStr")]
    #[serde(default = "lsn_invalid")]
    pub peer_horizon_lsn: Lsn,
    #[serde_as(as = "DisplayFromStr")]
    #[serde(default = "lsn_invalid")]
    pub local_start_lsn: Lsn,
    /// A connection string to use for WAL receiving.
    #[serde(default)]
    pub safekeeper_connstr: Option<String>,
}
