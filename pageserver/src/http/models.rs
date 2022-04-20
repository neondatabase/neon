use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};
use utils::{
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
}

#[serde_as]
#[derive(Serialize, Deserialize)]
#[serde(transparent)]
pub struct TenantCreateResponse(#[serde_as(as = "DisplayFromStr")] pub ZTenantId);

#[derive(Serialize)]
pub struct StatusResponse {
    pub id: ZNodeId,
}
