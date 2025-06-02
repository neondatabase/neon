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
    sync::gate::GateGuard,
};

use crate::timeline::Timeline;

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
    tenant_shard_id: TenantShardId,
    timeline_id: TimelineId,
    generation: Generation,
    remote_consistent_lsn: RwLock<Lsn>,
    pageserver: Arc<Pageserver>,
}

pub struct SafekeeperTimelineHandle {}
struct SafekeeperTimeline {
    tli: Arc<Timeline>,
    pageserver_shards: RwLock<HashMap<TenantShardId, Arc<PageserverTimeline>>>,
}

impl World {
    pub fn housekeeping(&self) {}
    pub fn spawn(&self, tli: Arc<Timeline>) -> SafekeeperTimelineHandle {
        tokio::spawn(async move {
            SafekeeperTimeline {
                tli,
                pageserver_shards: todo!(),
            }
            .run()
            .await;
        });
        SafekeeperTimelineHandle {}
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
    async fn run(self) {
        let Ok(gate_guard) = self.tli.gate.enter() else {
            return;
        };
        let cancel = self.tli.cancel.child_token();
        let mut commit_lsn_rx = self.tli.get_commit_lsn_watch_rx();
        let ttid = self.tli.ttid;
        loop {
            let commit_lsn = *commit_lsn_rx.borrow_and_update();
            {
                let guard = self.pageserver_shards.read().unwrap();
                for (shard, ps_tl) in guard.iter() {
                    if *ps_tl.remote_consistent_lsn.read() < commit_lsn {
                        ps_tl.pageserver.advertise(ps_tl);
                    }
                }
            }

            tokio::select! {
                _ = cancel.cancelled() => { return; }
                // TODO: debounce changed notifications, one every second or so is easily enough.
                commit_lsn = commit_lsn_rx.changed() => { continue; }
            };
        }
        drop(gate_guard);
    }
}

impl Pageserver {
    fn advertise(&self, ps_tl: &Arc<PageserverTimeline>) {
        todo!()
    }
}
