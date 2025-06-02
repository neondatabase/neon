//! The data structure that track advertisement state.

use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use serde::Serialize;
use utils::{
    generation::Generation,
    id::{NodeId, TenantId, TenantTimelineId, TimelineId},
    lsn::Lsn,
    shard::{ShardIndex, TenantShardId},
};

#[derive(Default)]
pub struct World {
    pageservers: RwLock<HashMap<NodeId, Arc<Pageserver>>>,
}

pub struct Pageserver {
    node_id: NodeId,
    attachments: RwLock<HashMap<TenantShardId, Arc<PageserverAttachment>>>,
}

pub struct PageserverAttachment {
    pageserver: NodeId,
    tenant_shard_id: TenantShardId,
    generation: Generation,
    remote_consistent_lsn: RwLock<HashMap<TimelineId, Arc<PageserverTimeline>>>,
}

pub struct PageserverTimeline {
    pageserver: NodeId,
    tenant_shard_id: TenantShardId,
    timeline_id: TimelineId,
    generation: Generation,
    remote_consistent_lsn: RwLock<Lsn>,
}

pub struct SafekeeperTimeline {}

impl World {
    pub fn housekeeping(&self) {}
    pub fn load_timeline(&self, ttid: TenantTimelineId) -> Arc<SafekeeperTimeline> {
        todo!()
    }
    pub fn update_pageserver_attachments(
        &self,
        tenant_id: TenantId,
        arg: safekeeper_api::models::TenantShardPageserverAttachmentChange,
    ) -> anyhow::Result<()> {
        todo!()
    }
}

impl SafekeeperTimeline {
    pub fn get_pageserver_timeline(
        &self,
        ttld: TenantTimelineId,
        shard: ShardIndex,
        pageserver_generation: Generation,
    ) -> Arc<PageserverTimeline> {
        assert!(!pageserver_generation.is_none());
        todo!()
    }
}

impl PageserverTimeline {
    pub fn update_remote_consistent_lsn(&self, lsn: Lsn) -> anyhow::Result<()> {
        todo!()
    }
}
