use serde::{Deserialize, Serialize};
use zenith_utils::zid::ZTimelineId;

use crate::ZTenantId;
use zenith_utils::zid::ZNodeId;

#[derive(Serialize, Deserialize)]
pub struct TimelineCreateRequest {
    #[serde(with = "hex")]
    pub tenant_id: ZTenantId,
    #[serde(with = "hex")]
    pub timeline_id: ZTimelineId,
    pub start_point: String,
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
