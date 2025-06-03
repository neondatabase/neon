//! The data structure that track advertisement state.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet, btree_map, hash_map},
    sync::{Arc, Mutex, RwLock},
    time::Duration,
};

use anyhow::Context;
use safekeeper_api::models::TenantShardPageserverAttachment;
use serde::Serialize;
use tokio::sync::{Notify, mpsc};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
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
    cancel: CancellationToken,
    inner: RwLock<WorldInner>,
    pageservers: Pageservers,
}

#[derive(Default)]
struct WorldInner {
    tenants: HashMap<TenantId, SafekeeperTenant>,
}

#[derive(Default)]
struct Pageservers {
    cancel: CancellationToken,
    pageservers: HashMap<NodeId, PageserverHandle>,
}

impl Pageservers {
    fn get_handle(&mut self, ps_id: NodeId) -> PageserverHandle {
        match self.pageservers.entry(ps_id) {
            hash_map::Entry::Occupied(occupied_entry) => occupied_entry.get().clone(),
            hash_map::Entry::Vacant(vacant_entry) => {
                let notify = Arc::new(Notify::new());
                let cancel = CancellationToken::new();
                let state = Default::default();
                tokio::spawn(
                    PageserverTask {
                        ps_id,
                        state: state.clone(),
                        notify: Arc::clone(&notify),
                        cancel: cancel.clone(),
                    }
                    .run(),
                );
                PageserverHandle { state, cancel }
            }
        }
    }
}

struct PageserverTask {
    ps_id: NodeId,
    state: Arc<Mutex<PageserverSharedState>>,
    notify: Arc<tokio::sync::Notify>,
    cancel: CancellationToken,
    pending_advertisements: HashMap<TenantTimelineId, Lsn>,
}

#[derive(Default)]
struct PageserverSharedState {
    pending_advertisements: HashMap<TenantTimelineId, Lsn>,
    endpoint: tonic::endpoint::Endpoint,
}

struct CommitLsnAdv {
    ttid: TenantTimelineId,
    commit_lsn: Lsn,
}

#[derive(Debug)]
struct RemoteConsistentLsnAdv {
    tenant_id: TenantId,
    shard_attachment: ShardAttachmentId,
    timeline_id: TimelineId,
    remote_consistent_lsn: Lsn,
}

#[derive(Clone)]
struct PageserverHandle {
    state: Arc<RwLock<PageserverSharedState>>,
    cancel: CancellationToken,
}

impl std::ops::Drop for PageserverHandle {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

#[derive(Default)]
struct SafekeeperTenant {
    /// Shared with [`SafekeeperTimeline::tenant_shard_attachments`].
    tenant_shard_attachments: tokio::sync::watch::Sender<TenantShardAttachments>,
    timelines: HashMap<TimelineId, Arc<SafekeeperTimeline>>,
}

struct SafekeeperTimeline {
    tli: Arc<Timeline>,
    /// Shared with [`SafekeeperTenant::tenant_shard_attachments`].
    tenant_shard_attachments: tokio::sync::watch::Receiver<TenantShardAttachments>,
    remote_consistent_lsns: Mutex<HashMap<ShardAttachmentId, Lsn>>,
}

#[derive(Default)]
struct TenantShardAttachments {
    pageservers: Pageservers,
    precise: HashSet<ShardAttachmentId>,
    /// projection from `precise`
    nodes: HashMap<NodeId, (usize, PageserverHandle)>, // usize is a refcount from precise
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
            hash_map::Entry::Vacant(vacant_entry) => unreachable!("was referenced by precise"),
            hash_map::Entry::Occupied(occupied_entry) => occupied_entry,
        };
        let (refcount, _) = entry.get_mut();
        *refcount = refcount
            .checked_sub(1)
            .expect("was referenced by precise, we add refcount in add()");
        if *refcount == 0 {
            entry.remove();
        }
    }

    pub fn nodes(&self) -> impl Iterator<Item = &PageserverHandle> {
        self.nodes.values().map(|(_, h)| h)
    }
}

#[derive(Debug, Hash, PartialEq, Eq)]
struct ShardAttachmentId {
    shard_id: ShardIndex,
    generation: Generation,
    ps_id: NodeId,
}

pub struct SafekeeperTimelineHandle {}
impl SafekeeperTimelineHandle {
    pub fn ready_for_eviction(&self) -> bool {
        todo!()
    }
}

impl World {
    pub fn register_timeline(
        &self,
        tli: Arc<Timeline>,
    ) -> anyhow::Result<SafekeeperTimelineHandle> {
        let ttid = tli.ttid;
        let mut inner = self.inner.write().unwrap();
        let sk_tenant: &mut SafekeeperTenant = inner.tenants.entry(ttid.tenant_id).or_default();
        let vacant = match sk_tenant.timelines.entry(ttid.timeline_id) {
            hash_map::Entry::Occupied(occupied_entry) => {
                anyhow::bail!("entry for timeline already exists");
            }
            hash_map::Entry::Vacant(vacant) => vacant,
        };
        tokio::spawn(async move {
            SafekeeperTimeline {
                tli,
                tenant_shard_attachments: sk_tenant.tenant_shard_attachments.subscribe(),
                remote_consistent_lsns: HashMap::default(), // TODO: fill from persistence inside .run()?
            }
            .run()
            .await;
        });
        Ok(vacant.insert(SafekeeperTimelineHandle {}).clone())
    }
    pub fn update_pageserver_attachments(
        &self,
        tenant_id: TenantId,
        arg: safekeeper_api::models::TenantShardPageserverAttachmentChange,
    ) -> anyhow::Result<()> {
        let mut inner = self.inner.write().unwrap();
        let sk_tenant: &mut SafekeeperTenant = inner.tenants.entry(tenant_id).or_insert(todo!());
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
        } = &adv;
        let mut inner = self.inner.write().unwrap();
        let Some(sk_tenant) = inner.tenants.get(&tenant_id) else {
            warn!(?adv, "tenant shard attachment is not known");
            return;
        };
        let Some(timeline) = sk_tenant.timelines.get_mut(&timeline_id) else {
            warn!(?adv, "safekeeper timeline is not know");
            return;
        };
        if cfg!(feature = "testing") {
            let commit_lsn = *timeline.tli.get_commit_lsn_watch_rx().borrow();
            if !(*remote_consistent_lsn <= commit_lsn) {
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
    async fn run(mut self) {
        let Ok(gate_guard) = self.tli.gate.enter() else {
            return;
        };
        let cancel = self.tli.cancel.child_token();

        let ttid = self.tli.ttid;

        let mut commit_lsn_rx = self.tli.get_commit_lsn_watch_rx();

        // arm for first iteration
        commit_lsn_rx.mark_changed();
        self.tenant_shard_attachments.mark_changed();

        let mut tenant_shard_attachments: Vec<PageserverHandle> = Vec::new();
        loop {
            tokio::select! {
                _ = cancel.cancelled() => { return; }
                _ = async {
                    // at most one advertisement per second
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    commit_lsn_rx.changed().await
                } => { }
                _ = self.tenant_shard_attachments.changed() => {
                    tenant_shard_attachments.clear();
                    tenant_shard_attachments.extend(self.tenant_shard_attachments.borrow_and_update().nodes().cloned().collect());
                    tenant_shard_attachments.shrink_to_fit();
                }
            };
            let commit_lsn = *commit_lsn_rx.borrow_and_update(); // The rhs deref minimizes time we lock the commit_lsn_tx
            for pageserver_handle in tenant_shard_attachments {
                // NB: if this function ever becomes slow / may need an .await, make sure
                // that other nodes continue to recieve advertisements.
                pageserver_handle.advertise_commit_lsn(CommitLsnAdv { ttid, commit_lsn });
            }
        }
        drop(gate_guard);
    }
}

impl PageserverHandle {
    pub fn advertise_commit_lsn(&self, adv: CommitLsnAdv) {
        let CommitLsnAdv { ttid, commit_lsn } = adv;
        let mut state = self.state.write().unwrap();
        state.pending_advertisements.insert(ttid, commit_lsn);
    }
}

impl PageserverTask {
    /// Cancellation: happens through last PageserverHandle being dropped.
    async fn run(mut self) {
        let mut pending_advertisements = HashMap::new();
        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => {
                    return;
                }
                res = self.run0() => {
                    if let Err(err) = res {
                        error!(?err, "pageserver loop failed");
                        // TODO: backoff? + cancellation sensitivity
                        tokio::time::sleep(Duration::from_secs(10)).await;
                    }
                    continue;
                }
            };
        }
    }
    async fn run0(&mut self) -> anyhow::Result<()> {
        use storage_broker::wal_advertisement::pageserver_client::PageserverClient;
        let stream = async_stream::stream! { loop {
            while self.pending_advertisements.is_empty() {
                tokio::select! {
                    _ = self.cancel.cancelled() => {
                        return;
                    }
                    _ = self.notify.notified() => {}
                }
                let mut state = self.state.lock().unwrap();
                std::mem::swap(
                    &mut state.pending_advertisements,
                    &mut self.pending_advertisements,
                );
            }
        } };
        let client: PageserverClient<_> = PageserverClient::connect(todo!())
            .await
            .context("connect")?;
        let publish_stream = client
            .publish_commit_lsn_advertisements(stream)
            .await
            .context("publish stream")?;
    }
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
