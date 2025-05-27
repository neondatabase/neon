use utils::{
    generation::Generation,
    id::{NodeId, TenantId, TimelineId},
    shard::TenantShardId,
};

use crate::persistence::Persistence;

pub struct ActorClient {}

enum Message {
    PageserverAttachmentNew {
        tenant_shard_id: TenantShardId,
        ps_generation: Generation,
        ps_id: NodeId,
    },
    PageserverAttachmentRemoved {
        tenant_shard_id: TenantShardId,
        ps_generation: Generation,
        ps_id: NodeId,
    },
    SafekeeperConfigChange {
        tenant_id: TenantId,
        timeline_id: TimelineId,
        safekeeper_ids: Vec<NodeId>,
    },
    SafekeeperDelete {
        safekeeper_id: NodeId,
    },
}

struct Actor {}

pub async fn spawn(persistence: Arc<Persistence>) -> ActorClient {
    let actor = Actor { persistence };
    tokio::spawn(actor.run());
    ActorClient {}
}

impl ActorClient {}

impl Actor {
    async fn run(self) {
        loop {}
    }
}
