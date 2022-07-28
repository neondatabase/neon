use serde::{Deserialize, Serialize};
use utils::zid::{NodeId, ZTimelineId};

#[derive(Serialize, Deserialize)]
pub struct TimelineCreateRequest {
    pub timeline_id: ZTimelineId,
    pub peer_ids: Vec<NodeId>,
}
