use serde::{Deserialize, Serialize};
use zenith_utils::zid::ZNodeId;
use zenith_utils::{
    lsn::Lsn,
    zid::{opt_display_serde, ZTenantId, ZTimelineId},
};

#[derive(Serialize, Deserialize)]
pub struct TimelineCreateRequest {
    #[serde(default)]
    #[serde(with = "opt_display_serde")]
    pub new_timeline_id: Option<ZTimelineId>,
    #[serde(default)]
    #[serde(with = "opt_display_serde")]
    pub ancestor_timeline_id: Option<ZTimelineId>,
    pub ancestor_start_lsn: Option<Lsn>,
}

#[derive(Serialize, Deserialize)]
pub struct TenantCreateRequest {
    #[serde(default)]
    #[serde(with = "opt_display_serde")]
    pub new_tenant_id: Option<ZTenantId>,
    #[serde(default)]
    #[serde(with = "opt_display_serde")]
    pub initial_timeline_id: Option<ZTimelineId>,
}

#[derive(Deserialize, Serialize)]
pub struct TenantCreateResponse {
    #[serde(with = "hex")]
    pub tenant_id: ZTenantId,
    #[serde(with = "hex")]
    pub timeline_id: ZTimelineId,
}

#[derive(Serialize)]
pub struct StatusResponse {
    pub id: ZNodeId,
}
