use serde::{Deserialize, Serialize};
use utils::id::{NodeId, TimelineId};

#[derive(Serialize, Deserialize)]
pub struct TimelineCreateRequest {
    pub timeline_id: TimelineId,
    pub peer_ids: Vec<NodeId>,
}
