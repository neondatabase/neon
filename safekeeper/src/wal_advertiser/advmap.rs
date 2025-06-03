//! The data structure that track advertisement state.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet, btree_map, hash_map},
    sync::{Arc, RwLock},
};

use safekeeper_api::models::TenantShardPageserverAttachment;
use serde::Serialize;
use tokio::sync::mpsc;
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
    inner: RwLock<WorldInner>,
}

struct WorldInner {
    tenant_shards: HashMap<TenantId, SafekeeperTenant>,
}

struct Pageservers {
    pageservers: HashMap<NodeId, PageserverHandle>,
}

impl Pageservers {
    fn get_handle(&self, ps_id: NodeId) -> PageserverHandle {
        match self.pageservers.entry(ps_id) {
            hash_map::Entry::Occupied(occupied_entry) => occupied_entry.get().clone(),
            hash_map::Entry::Vacant(vacant_entry) => {
                todo!()
            }
        }
    }
}

struct Pageserver {
    // XXX track more fine-grained
    world: Arc<World>,
}

struct CommitLsnAdv {
    ttid: TenantTimelineId,
    commit_lsn: Lsn,
}

struct RemoteConsistentLsnAdv {
    tenant_id: TenantId,
    shard_attachment: ShardAttachmentId,
    timeline_id: TimelineId,
    remote_consistent_lsn: Lsn,
}

#[derive(Clone)]
struct PageserverHandle {
    tx: tokio::sync::mpsc::Sender<CommitLsnAdv>,
}

struct SafekeeperTenant {
    /// Shared among all [`SafekeeperTimeline::shard_attachments`]
    /// in [`Self::timelines`].
    shard_attachments: Arc<RwLock<ShardAttachments>>,
    timelines: HashMap<TimelineId, Arc<SafekeeperTimeline>>,
}

struct SafekeeperTimeline {
    tli: Arc<Timeline>,
    /// Shared among all [`SafekeeperTimeline`] instances of a [`SafekeeperTenant`],
    /// and [`SafekeeperTenant::shard_attachments`].
    shard_attachments: Arc<RwLock<ShardAttachments>>,
}

struct ShardAttachments {
    pageservers: Pageservers,
    precise: BTreeMap<ShardAttachmentId, ShardAttachment>,
    nodes: BTreeMap<NodeId, (usize, PageserverHandle)>, // usize is a refcount from precise
}

#[derive(Default)]
struct ShardAttachment {
    remote_consistent_lsn: Option<Lsn>,
}

impl ShardAttachments {
    pub fn add(&mut self, a: ShardAttachmentId) {
        let (refcount, _) = self
            .nodes
            .entry(a.ps_id)
            .or_insert_with(|| (0, self.pageservers.get_handle(a.ps_id)));
        *refcount += 1;
    }
    pub fn remove(&mut self, a: ShardAttachmentId) -> ShardAttachmentChange {
        let removed = self.precise.remove(&a);
        if !removed {
            return ShardAttachmentChange::None;
        }
        let mut entry = match self.nodes.entry(a.ps_id) {
            btree_map::Entry::Vacant(vacant_entry) => unreachable!("was referenced by precise"),
            btree_map::Entry::Occupied(occupied_entry) => occupied_entry,
        };
        *entry.get_mut() = entry
            .get()
            .checked_sub(1)
            .expect("was referenced by precise, we add refcount in add()");
        if *entry.get() == 0 {
            entry.remove();
        }
        ShardAttachmentChange::Removed(a.ps_id)
    }
    pub fn advance_remote_consistent_lsn(
        &mut self,
        a: ShardAttachmentId,
        remote_consistent_lsn: Lsn,
    ) -> anyhow::Result<()> {
        let Some(attachment) = self.precise.get_mut(&a) else {
            anyhow::bail!(
                "attachment is not known: attachment={a:?} remote_consistent_lsn={remote_consistent_lsn}"
            );
        };
        let current = attachment
            .remote_consistent_lsn
            .get_or_insert(remote_consistent_lsn);
        match current.cmp(remote_consistent_lsn) {
            std::cmp::Ordering::Less => {
                *current = remote_consistent_lsn;
                Ok(())
            }
            std::cmp::Ordering::Equal => {
                warn!(attachment=?a, %remote_consistent_lsn, "update does not advance remote_consistent_lsn");
                Ok(())
            }
            std::cmp::Ordering::Greater => {
                // Does this need to be an error? Can we just ignore? Does anything in this function need to be an error?
                anyhow::bail!("proposed remote_consistent_lsn is lower than current record");
            }
        }
    }
    pub fn nodes(&self) -> impl Iterator<Item = &PageserverHandle> {
        self.nodes.values()
    }
}

#[derive(Debug)]
struct ShardAttachmentId {
    shard_id: ShardIndex,
    generation: Generation,
    ps_id: NodeId,
}

pub struct PageserverAttachment {
    pageserver: NodeId,
    tenant_shard_id: TenantShardId,
    generation: Generation,
    remote_consistent_lsn: RwLock<HashMap<TimelineId, Arc<PageserverTimeline>>>,
}

pub struct SafekeeperTimelineHandle {}

impl World {
    pub fn housekeeping(&self) {}
    pub fn spawn(&self, tli: Arc<Timeline>) -> SafekeeperTimelineHandle {
        tokio::spawn(async move {
            SafekeeperTimeline {
                tli,
                pageserver_timeline_shards: todo!(),
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
        let mut inner = self.inner.write().unwrap();
        let sk_tenant: &mut SafekeeperTenant =
            inner.tenant_shards.entry(tenant_id).or_insert(todo!());
        let mut shard_attachments = sk_tenant.shard_attachments.write().unwrap();
        use safekeeper_api::models::TenantShardPageserverAttachmentChange::*;
        let change = match arg {
            Attach(a) => shard_attachments.add(a.into()),
            Detach(a) => shard_attachments.remove(a.into()),
        };
        Ok(())
    }
    pub fn process_remote_consistent_lsn_adv(&self, adv: RemoteConsistentLsnAdv) {

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
            let shard_attachments = self.shard_attachments.read().unwrap();
            for node in shard_attachments.nodes() {
                node.advertise_commit_lsn(CommitLsnAdv { ttid, commit_lsn });
            }
            tokio::select! {
                _ = cancel.cancelled() => { return; }
                commit_lsn = async {
                    // at most one advertisement per second
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    commit_lsn_rx.changed().await
                } => { continue; }
            };
        }
        drop(gate_guard);
    }
}

impl PageserverHandle {
    pub async fn advertise_commit_lsn(&self, adv: CommitLsnAdv) {
        self.tx
            .send(adv)
            .await
            .expect("Pageserver loop never drops the rx");
    }
}

impl Pageserver {
}

impl From<safekeeper_api::models::TenantShardPageserverAttachment> for ShardAttachmentId {
    fn from(value: safekeeper_api::models::TenantShardPageserverAttachment) {
        let safekeeper_api::models::TenantShardPageserverAttachment {
            shard_id,
            generation,
            ps_id,
        } = value;
        ShardAttachmentId {
            shard_id,
            generation,
            ps_id,
        }
    }
}
