use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};
use zenith_utils::{
    lsn::Lsn,
    zid::{ZNodeId, ZTenantId, ZTimelineId},
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
#[derive(Serialize, Deserialize)]
pub struct TenantCreateRequest {
    #[serde(default)]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub new_tenant_id: Option<ZTenantId>,
    pub checkpoint_distance: Option<u64>,
    pub compaction_period: Option<String>,
    pub gc_horizon: Option<u64>,
    pub gc_period: Option<String>,
    pub pitr_interval: Option<String>,
}

#[serde_as]
#[derive(Serialize, Deserialize)]
#[serde(transparent)]
pub struct TenantCreateResponse(#[serde_as(as = "DisplayFromStr")] pub ZTenantId);

#[derive(Serialize)]
pub struct StatusResponse {
    pub id: ZNodeId,
}

impl TenantCreateRequest {
    pub fn new(new_tenant_id: Option<ZTenantId>) -> TenantCreateRequest {
        TenantCreateRequest {
            new_tenant_id,
            checkpoint_distance: None,
            compaction_period: None,
            gc_horizon: None,
            gc_period: None,
            pitr_interval: None,
        }
    }
}
