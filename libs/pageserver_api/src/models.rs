use std::num::NonZeroU64;

use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};
use utils::{
    id::{NodeId, TenantId, TimelineId},
    lsn::Lsn,
};

/// A state of a tenant in pageserver's memory.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum TenantState {
    /// Tenant is fully operational, its background jobs might be running or not.
    Active { background_jobs_running: bool },
    /// A tenant is recognized by pageserver, but not yet ready to operate:
    /// e.g. not present locally and being downloaded or being read into memory from the file system.
    Paused,
    /// A tenant is recognized by the pageserver, but no longer used for any operations, as failed to get activated.
    Broken,
}

#[serde_as]
#[derive(Serialize, Deserialize)]
pub struct TimelineCreateRequest {
    #[serde(default)]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub new_timeline_id: Option<TimelineId>,
    #[serde(default)]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub ancestor_timeline_id: Option<TimelineId>,
    #[serde(default)]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub ancestor_start_lsn: Option<Lsn>,
    pub pg_version: Option<u32>,
}

#[serde_as]
#[derive(Serialize, Deserialize, Default)]
pub struct TenantCreateRequest {
    #[serde(default)]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub new_tenant_id: Option<TenantId>,
    pub checkpoint_distance: Option<u64>,
    pub checkpoint_timeout: Option<String>,
    pub compaction_target_size: Option<u64>,
    pub compaction_period: Option<String>,
    pub compaction_threshold: Option<usize>,
    pub gc_horizon: Option<u64>,
    pub gc_period: Option<String>,
    pub image_creation_threshold: Option<usize>,
    pub pitr_interval: Option<String>,
    pub walreceiver_connect_timeout: Option<String>,
    pub lagging_wal_timeout: Option<String>,
    pub max_lsn_wal_lag: Option<NonZeroU64>,
}

#[serde_as]
#[derive(Serialize, Deserialize)]
#[serde(transparent)]
pub struct TenantCreateResponse(#[serde_as(as = "DisplayFromStr")] pub TenantId);

#[derive(Serialize)]
pub struct StatusResponse {
    pub id: NodeId,
}

impl TenantCreateRequest {
    pub fn new(new_tenant_id: Option<TenantId>) -> TenantCreateRequest {
        TenantCreateRequest {
            new_tenant_id,
            ..Default::default()
        }
    }
}

#[serde_as]
#[derive(Serialize, Deserialize)]
pub struct TenantConfigRequest {
    pub tenant_id: TenantId,
    #[serde(default)]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub checkpoint_distance: Option<u64>,
    pub checkpoint_timeout: Option<String>,
    pub compaction_target_size: Option<u64>,
    pub compaction_period: Option<String>,
    pub compaction_threshold: Option<usize>,
    pub gc_horizon: Option<u64>,
    pub gc_period: Option<String>,
    pub image_creation_threshold: Option<usize>,
    pub pitr_interval: Option<String>,
    pub walreceiver_connect_timeout: Option<String>,
    pub lagging_wal_timeout: Option<String>,
    pub max_lsn_wal_lag: Option<NonZeroU64>,
}

impl TenantConfigRequest {
    pub fn new(tenant_id: TenantId) -> TenantConfigRequest {
        TenantConfigRequest {
            tenant_id,
            checkpoint_distance: None,
            checkpoint_timeout: None,
            compaction_target_size: None,
            compaction_period: None,
            compaction_threshold: None,
            gc_horizon: None,
            gc_period: None,
            image_creation_threshold: None,
            pitr_interval: None,
            walreceiver_connect_timeout: None,
            lagging_wal_timeout: None,
            max_lsn_wal_lag: None,
        }
    }
}

#[serde_as]
#[derive(Serialize, Deserialize, Clone)]
pub struct TenantInfo {
    #[serde_as(as = "DisplayFromStr")]
    pub id: TenantId,
    pub state: TenantState,
    pub current_physical_size: Option<u64>, // physical size is only included in `tenant_status` endpoint
    pub has_in_progress_downloads: Option<bool>,
}

#[serde_as]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LocalTimelineInfo {
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub ancestor_timeline_id: Option<TimelineId>,
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub ancestor_lsn: Option<Lsn>,
    #[serde_as(as = "DisplayFromStr")]
    pub last_record_lsn: Lsn,
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub prev_record_lsn: Option<Lsn>,
    #[serde_as(as = "DisplayFromStr")]
    pub latest_gc_cutoff_lsn: Lsn,
    #[serde_as(as = "DisplayFromStr")]
    pub disk_consistent_lsn: Lsn,
    pub current_logical_size: Option<u64>, // is None when timeline is Unloaded
    pub current_physical_size: Option<u64>, // is None when timeline is Unloaded
    pub current_logical_size_non_incremental: Option<u64>,
    pub current_physical_size_non_incremental: Option<u64>,

    pub wal_source_connstr: Option<String>,
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub last_received_msg_lsn: Option<Lsn>,
    /// the timestamp (in microseconds) of the last received message
    pub last_received_msg_ts: Option<u128>,
    pub pg_version: u32,
}

#[serde_as]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RemoteTimelineInfo {
    #[serde_as(as = "DisplayFromStr")]
    pub remote_consistent_lsn: Lsn,
    pub awaits_download: bool,
}

///
/// This represents the output of the "timeline_detail" API call.
///
#[serde_as]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TimelineInfo {
    #[serde_as(as = "DisplayFromStr")]
    pub tenant_id: TenantId,
    #[serde_as(as = "DisplayFromStr")]
    pub timeline_id: TimelineId,
    pub local: Option<LocalTimelineInfo>,
    pub remote: Option<RemoteTimelineInfo>,
}

pub type ConfigureFailpointsRequest = Vec<FailpointConfig>;

/// Information for configuring a single fail point
#[derive(Debug, Serialize, Deserialize)]
pub struct FailpointConfig {
    /// Name of the fail point
    pub name: String,
    /// List of actions to take, using the format described in `fail::cfg`
    ///
    /// We also support `actions = "exit"` to cause the fail point to immediately exit.
    pub actions: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TimelineGcRequest {
    pub gc_horizon: Option<u64>,
}
