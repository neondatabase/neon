use serde::{Deserialize, Serialize};

use utils::{
    id::{NodeId, TenantId, TimelineId},
    lsn::Lsn,
};

#[derive(Serialize, Deserialize)]
pub struct TimelineCreateRequest {
    pub tenant_id: TenantId,
    pub timeline_id: TimelineId,
    pub peer_ids: Option<Vec<NodeId>>,
    pub pg_version: u32,
    pub system_id: Option<u64>,
    pub wal_seg_size: Option<u32>,
    pub commit_lsn: Lsn,
    // If not passed, it is assigned to the beginning of commit_lsn segment.
    pub local_start_lsn: Option<Lsn>,
}

fn lsn_invalid() -> Lsn {
    Lsn::INVALID
}

/// Data about safekeeper's timeline, mirrors broker.proto.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SkTimelineInfo {
    /// Term.
    pub term: Option<u64>,
    /// Term of the last entry.
    pub last_log_term: Option<u64>,
    /// LSN of the last record.
    #[serde(default = "lsn_invalid")]
    pub flush_lsn: Lsn,
    /// Up to which LSN safekeeper regards its WAL as committed.
    #[serde(default = "lsn_invalid")]
    pub commit_lsn: Lsn,
    /// LSN up to which safekeeper has backed WAL.
    #[serde(default = "lsn_invalid")]
    pub backup_lsn: Lsn,
    /// LSN of last checkpoint uploaded by pageserver.
    #[serde(default = "lsn_invalid")]
    pub remote_consistent_lsn: Lsn,
    #[serde(default = "lsn_invalid")]
    pub peer_horizon_lsn: Lsn,
    #[serde(default = "lsn_invalid")]
    pub local_start_lsn: Lsn,
    /// A connection string to use for WAL receiving.
    #[serde(default)]
    pub safekeeper_connstr: Option<String>,
    #[serde(default)]
    pub http_connstr: Option<String>,
    // Minimum of all active RO replicas flush LSN
    #[serde(default = "lsn_invalid")]
    pub standby_horizon: Lsn,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TimelineCopyRequest {
    pub target_timeline_id: TimelineId,
    pub until_lsn: Lsn,
}
