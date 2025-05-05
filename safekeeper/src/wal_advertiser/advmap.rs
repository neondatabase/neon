//! The data structure that track advertisement state.

use std::sync::{Arc, RwLock};

pub struct World {
    pageservers: RwLock<HashMap<NodeId, Arc<Pageserver>>>,
}

pub struct Pageserver {
    node_id: NodeId,
    attachments: RwLock<HashMap<TenantShardId, Arc<Attachment>>>,
}

pub struct Attachment {
    pageserver: NodeId,
    tenant_shard_id: TenantShardId,
    generation: Generation,
    remote_consistent_lsn: RwLock<HashMap<TimelineId, Arc<Timeline>>>,
}

pub struct Timeline {
    tenant_shard_id: TenantShardId,
    timeline_id: TimelineId,
    remote_consistent_lsn: RwLock<Lsn>,
}

