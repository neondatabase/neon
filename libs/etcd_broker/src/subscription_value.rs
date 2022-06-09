//! Module for the values to put into etcd.

use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};
use utils::lsn::Lsn;

/// Data about safekeeper's timeline. Fields made optional for easy migrations.
#[serde_as]
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SkTimelineInfo {
    /// Term of the last entry.
    pub last_log_term: Option<u64>,
    /// LSN of the last record.
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[serde(default)]
    pub flush_lsn: Option<Lsn>,
    /// Up to which LSN safekeeper regards its WAL as committed.
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[serde(default)]
    pub commit_lsn: Option<Lsn>,
    /// LSN up to which safekeeper has backed WAL.
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[serde(default)]
    pub backup_lsn: Option<Lsn>,
    /// LSN of last checkpoint uploaded by pageserver.
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[serde(default)]
    pub remote_consistent_lsn: Option<Lsn>,
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[serde(default)]
    pub peer_horizon_lsn: Option<Lsn>,
    /// A connection string to use for WAL receiving.
    #[serde(default)]
    pub safekeeper_connstr: Option<String>,
}
