use serde::{Deserialize, Serialize};
use utils::zid::{NodeId, ZTenantId, ZTimelineId};

#[derive(Serialize, Deserialize)]
pub struct TimelineCreateRequest {
    pub tenant_id: ZTenantId,
    pub timeline_id: ZTimelineId,
    pub peer_ids: Vec<NodeId>,
}
