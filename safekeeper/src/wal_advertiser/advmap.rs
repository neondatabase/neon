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

#[derive(Default)]
struct SafekeeperTenant {
    /// Shared with [`SafekeeperTimeline::tenant_shard_attachments`].
    tenant_shard_attachments: Arc<RwLock<TenantShardAttachments>>,
    timelines: HashMap<TimelineId, Arc<SafekeeperTimeline>>,
}

struct SafekeeperTimeline {
    tli: Arc<Timeline>,
    /// Shared with [`SafekeeperTenant::tenant_shard_attachments`].
    tenant_shard_attachments: Arc<RwLock<TenantShardAttachments>>,
    remote_consistent_lsns: HashMap<ShardAttachmentId, Lsn>,
}

#[derive(Default)]
struct TenantShardAttachments {
    pageservers: Pageservers,
    precise: BTreeSet<ShardAttachmentId>,
    /// projection from `precise`
    nodes: BTreeMap<NodeId, (usize, PageserverHandle)>, // usize is a refcount from precise
}

impl TenantShardAttachments {
    pub fn add(&mut self, a: ShardAttachmentId) {
        let (refcount, _) = self
            .nodes
            .entry(a.ps_id)
            .or_insert_with(|| (0, self.pageservers.get_handle(a.ps_id)));
        *refcount += 1;
    }
    pub fn remove(&mut self, a: ShardAttachmentId) {
        let removed = self.precise.remove(&a);
        if !removed {
            return;
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
    }

    pub fn nodes(&self) -> impl Iterator<Item = &PageserverHandle> {
        self.nodes.values()
    }
}

#[derive(Debug, Hash, PartialEq, Eq)]
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
    pub fn spawn(&self, tli: Arc<Timeline>) -> anyhow::Result<SafekeeperTimelineHandle> {
        let ttid = tli.ttid;
        let mut inner = self.inner.write().unwrap();
        let sk_tenant: &mut SafekeeperTenant = inner.tenant_shards.entry(tenant_id).or_default();
        let vacant = match sk_tenant.timelines.entry(ttid.timeline_id) {
            hash_map::Entry::Occupied(occupied_entry) => {
                anyhow::bail!("entry for timeline already exists");
            }
            hash_map::Entry::Vacant(vacant) => vacant,
        };
        tokio::spawn(async move {
            SafekeeperTimeline {
                tli,
                tenant_shard_attachments: Arc::clone(&sk_tenant.tenant_shard_attachments),
                remote_consistent_lsns: HashMap::default(), // TODO: fill from persistence inside .run()?
            }
            .run()
            .await;
        });
        vacant.insert(SafekeeperTimelineHandle {})
    }
    pub fn update_pageserver_attachments(
        &self,
        tenant_id: TenantId,
        arg: safekeeper_api::models::TenantShardPageserverAttachmentChange,
    ) -> anyhow::Result<()> {
        let mut inner = self.inner.write().unwrap();
        let sk_tenant: &mut SafekeeperTenant =
            inner.tenant_shards.entry(tenant_id).or_insert(todo!());
        let mut shard_attachments = sk_tenant.tenant_shard_attachments.write().unwrap();
        use safekeeper_api::models::TenantShardPageserverAttachmentChange::*;
        let change = match arg {
            Attach(a) => shard_attachments.add(a.into()),
            Detach(a) => shard_attachments.remove(a.into()),
        };
        Ok(())
    }
    pub fn handle_remote_consistent_lsn_advertisement(&mut self, adv: RemoteConsistentLsnAdv) {
        debug!(?adv, "processing advertisement");
        let RemoteConsistentLsnAdv {
            tenant_id,
            shard_attachment,
            timeline_id,
            remote_consistent_lsn,
        } = adv;
        let mut inner = self.inner.write().unwrap();
        let Some(sk_tenant) = inner.tenant_shards.get(&tenant_id) else {
            warn!(?adv, "tenant shard attachment is not known");
            return;
        };
        let Some(timeline) = sk_tenant.timelines.get_mut(&timeline_id) else {
            warn!(?adv, "safekeeper timeline is not know");
            return;
        };
        if cfg!(feature = "testing") {
            let commit_lsn = *timeline.tli.get_commit_lsn_watch_rx().borrow();
            if !(remote_consistent_lsn <= commit_lsn) {
                warn!(
                    ?adv,
                    "advertised remote_consistent_lsn is ahead of commit_lsn"
                );
                return;
            }
        }
        let current = match timeline.remote_consistent_lsns.entry(shard_attachment) {
            hash_map::Entry::Occupied(mut occupied_entry) => occupied_entry.get_mut(),
            hash_map::Entry::Vacant(vacant_entry) => {
                info!(?adv, "first time learning about timeline shard attachment");
                vacant_entry.insert(remote_consistent_lsn)
            }
        };
        if !(*current <= remote_consistent_lsn) {
            warn!(current=%*current, ?adv, "advertised remote_consistent_lsn is lower than earlier advertisements, either delayed message or shard is behaving inconsistently");
            return;
        }
        *current = remote_consistent_lsn;
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
            let shard_attachments = self.tenant_shard_attachments.read().unwrap();
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

impl SafekeeperTimelineHandle {
    pub fn ready_for_eviction(&self) -> bool {
        todo!()
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

impl Pageserver {}

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
