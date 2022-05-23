use serde::{Deserialize, Serialize};
use utils::zid::{ZNodeId, TenantId, ZTimelineId};

#[derive(Serialize, Deserialize)]
pub struct TimelineCreateRequest {
    pub tenant_id: TenantId,
    pub timeline_id: ZTimelineId,
    pub peer_ids: Vec<ZNodeId>,
}
