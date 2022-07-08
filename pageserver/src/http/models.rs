use std::num::NonZeroU64;

use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};
use utils::{
    lsn::Lsn,
    zid::{NodeId, ZTenantId, ZTimelineId},
};

#[serde_as]
#[derive(Serialize, Deserialize)]
pub struct TimelineCreateRequest {
    #[serde(default)]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub new_timeline_id: Option<ZTimelineId>,
    #[serde(default)]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub ancestor_timeline_id: Option<ZTimelineId>,
    #[serde(default)]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub ancestor_start_lsn: Option<Lsn>,
}

#[serde_as]
#[derive(Serialize, Deserialize, Default)]
pub struct TenantCreateRequest {
    #[serde(default)]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub new_tenant_id: Option<ZTenantId>,
    pub checkpoint_distance: Option<u64>,
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
    pub data_checksums_enabled: Option<bool>,
}

#[serde_as]
#[derive(Serialize, Deserialize)]
#[serde(transparent)]
pub struct TenantCreateResponse(#[serde_as(as = "DisplayFromStr")] pub ZTenantId);

#[derive(Serialize)]
pub struct StatusResponse {
    pub id: NodeId,
}

impl TenantCreateRequest {
    pub fn new(new_tenant_id: Option<ZTenantId>) -> TenantCreateRequest {
        TenantCreateRequest {
            new_tenant_id,
            ..Default::default()
        }
    }
}

#[serde_as]
#[derive(Serialize, Deserialize)]
pub struct TenantConfigRequest {
    pub tenant_id: ZTenantId,
    #[serde(default)]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub checkpoint_distance: Option<u64>,
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
    pub fn new(tenant_id: ZTenantId) -> TenantConfigRequest {
        TenantConfigRequest {
            tenant_id,
            checkpoint_distance: None,
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

/// A WAL receiver's data stored inside the global `WAL_RECEIVERS`.
/// We keep one WAL receiver active per timeline.
#[serde_as]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct WalReceiverEntry {
    pub wal_producer_connstr: Option<String>,
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub last_received_msg_lsn: Option<Lsn>,
    /// the timestamp (in microseconds) of the last received message
    pub last_received_msg_ts: Option<u128>,
}
