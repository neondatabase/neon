use serde::{Deserialize, Serialize};
use zenith_utils::zid::ZNodeId;
use zenith_utils::{
    lsn::Lsn,
    zid::{opt_display_serde, ZTenantId, ZTimelineId},
};

#[derive(Serialize, Deserialize)]
pub struct TimelineCreateRequest {
    #[serde(with = "hex")]
    pub tenant_id: ZTenantId,
    #[serde(with = "hex")]
    pub timeline_id: ZTimelineId,
    #[serde(with = "opt_display_serde")]
    pub ancestor_timeline_id: Option<ZTimelineId>,
    pub start_lsn: Option<Lsn>,
}

#[derive(Serialize, Deserialize)]
pub struct TenantCreateRequest {
    #[serde(with = "hex")]
    pub tenant_id: ZTenantId,
}

#[derive(Serialize)]
pub struct StatusResponse {
    pub id: ZNodeId,
}
