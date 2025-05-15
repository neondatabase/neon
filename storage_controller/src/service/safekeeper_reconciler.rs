use std::{
    collections::HashMap,
    str::FromStr,
    sync::{Arc, atomic::AtomicU64},
    time::Duration,
};

use clashmap::{ClashMap, Entry};
use safekeeper_api::models::PullTimelineRequest;
use safekeeper_client::mgmt_api;
use tokio::sync::{
    Semaphore,
    mpsc::{self, UnboundedReceiver, UnboundedSender},
};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;
use utils::{
    id::{NodeId, TenantId, TimelineId},
    logging::SecretString,
};

use crate::{
    persistence::SafekeeperTimelineOpKind, safekeeper::Safekeeper,
    safekeeper_client::SafekeeperClient,
};

use super::Service;

pub(crate) struct SafekeeperReconcilers {
    cancel: CancellationToken,
    reconcilers: HashMap<NodeId, ReconcilerHandle>,
}

impl SafekeeperReconcilers {
    pub fn new(cancel: CancellationToken) -> Self {
        SafekeeperReconcilers {
            cancel,
            reconcilers: HashMap::new(),
        }
    }
    /// Adds a safekeeper-specific reconciler.
    /// Can be called multiple times, but it needs to be called at least once
    /// for every new safekeeper added.
    pub(crate) fn start_reconciler(&mut self, node_id: NodeId, service: &Arc<Service>) {
        self.reconcilers.entry(node_id).or_insert_with(|| {
            SafekeeperReconciler::spawn(self.cancel.child_token(), service.clone())
        });
    }
    /// Stop a safekeeper-specific reconciler.
    /// Stops the reconciler, cancelling all ongoing tasks.
    pub(crate) fn stop_reconciler(&mut self, node_id: NodeId) {
        if let Some(handle) = self.reconcilers.remove(&node_id) {
            handle.cancel.cancel();
        }
    }
    pub(crate) fn schedule_request_vec(&self, reqs: Vec<ScheduleRequest>) {
        tracing::info!(
            "Scheduling {} pending safekeeper ops loaded from db",
            reqs.len()
        );
        for req in reqs {
            self.schedule_request(req);
        }
    }
    pub(crate) fn schedule_request(&self, req: ScheduleRequest) {
        let node_id = req.safekeeper.get_id();
        let reconciler_handle = self.reconcilers.get(&node_id).unwrap();
        reconciler_handle.schedule_reconcile(req);
    }
    /// Cancel ongoing reconciles for the given timeline
    ///
    /// Specifying `None` here only removes reconciles for the tenant-global reconciliation,
    /// instead of doing this for all timelines of the tenant.
    ///
    /// Callers must remove the reconciles from the db manually
    pub(crate) fn cancel_reconciles_for_timeline(
        &mut self,
        node_id: NodeId,
        tenant_id: TenantId,
        timeline_id: Option<TimelineId>,
    ) {
        if let Some(handle) = self.reconcilers.get(&node_id) {
            handle.cancel_reconciliation(tenant_id, timeline_id);
        }
    }
}

/// Initial load of the pending operations from the db
pub(crate) async fn load_schedule_requests(
    service: &Arc<Service>,
    safekeepers: &HashMap<NodeId, Safekeeper>,
) -> anyhow::Result<Vec<ScheduleRequest>> {
    let pending_ops_timelines = service
        .persistence
        .list_pending_ops_with_timelines()
        .await?;
    let mut res = Vec::with_capacity(pending_ops_timelines.len());
    for (op_persist, timeline_persist) in pending_ops_timelines {
        let node_id = NodeId(op_persist.sk_id as u64);
        let Some(sk) = safekeepers.get(&node_id) else {
            // This shouldn't happen, at least the safekeeper should exist as decomissioned.
            tracing::warn!(
                tenant_id = op_persist.tenant_id,
                timeline_id = op_persist.timeline_id,
                "couldn't find safekeeper with pending op id {node_id} in list of stored safekeepers"
            );
            continue;
        };
        let sk = Box::new(sk.clone());
        let tenant_id = TenantId::from_str(&op_persist.tenant_id)?;
        let timeline_id = if !op_persist.timeline_id.is_empty() {
            Some(TimelineId::from_str(&op_persist.timeline_id)?)
        } else {
            None
        };
        let host_list = match op_persist.op_kind {
            SafekeeperTimelineOpKind::Delete => Vec::new(),
            SafekeeperTimelineOpKind::Exclude => Vec::new(),
            SafekeeperTimelineOpKind::Pull => {
                if timeline_id.is_none() {
                    // We only do this extra check (outside of timeline_persist check) to give better error msgs
                    anyhow::bail!(
                        "timeline_id is empty for `pull` schedule request for {tenant_id}"
                    );
                };
                let Some(timeline_persist) = timeline_persist else {
                    // This shouldn't happen, the timeline should still exist
                    tracing::warn!(
                        tenant_id = op_persist.tenant_id,
                        timeline_id = op_persist.timeline_id,
                        "couldn't find timeline for corresponding pull op"
                    );
                    continue;
                };
                timeline_persist
                    .sk_set
                    .iter()
                    .filter_map(|sk_id| {
                        let other_node_id = NodeId(*sk_id as u64);
                        if node_id == other_node_id {
                            // We obviously don't want to pull from ourselves
                            return None;
                        }
                        let Some(sk) = safekeepers.get(&other_node_id) else {
                            tracing::warn!(
                                "couldnt find safekeeper with pending op id {other_node_id}, not pulling from it"
                            );
                            return None;
                        };
                        Some((other_node_id, sk.base_url()))
                    })
                    .collect::<Vec<_>>()
            }
        };
        let req = ScheduleRequest {
            safekeeper: sk,
            host_list,
            tenant_id,
            timeline_id,
            generation: op_persist.generation as u32,
            kind: op_persist.op_kind,
        };
        res.push(req);
    }
    Ok(res)
}

pub(crate) struct ScheduleRequest {
    pub(crate) safekeeper: Box<Safekeeper>,
    pub(crate) host_list: Vec<(NodeId, String)>,
    pub(crate) tenant_id: TenantId,
    pub(crate) timeline_id: Option<TimelineId>,
    pub(crate) generation: u32,
    pub(crate) kind: SafekeeperTimelineOpKind,
}

/// A way to keep ongoing/queued reconcile requests apart
#[derive(Copy, Clone, PartialEq, Eq)]
struct TokenId(u64);

type OngoingTokens = ClashMap<(TenantId, Option<TimelineId>), (CancellationToken, TokenId)>;

/// Handle to per safekeeper reconciler.
struct ReconcilerHandle {
    tx: UnboundedSender<(ScheduleRequest, CancellationToken, TokenId)>,
    ongoing_tokens: Arc<OngoingTokens>,
    token_id_counter: AtomicU64,
    cancel: CancellationToken,
}

impl ReconcilerHandle {
    /// Obtain a new token slot, cancelling any existing reconciliations for
    /// that timeline. It is not useful to have >1 operation per <tenant_id,
    /// timeline_id, safekeeper>, hence scheduling op cancels current one if it
    /// exists.
    fn new_token_slot(
        &self,
        tenant_id: TenantId,
        timeline_id: Option<TimelineId>,
    ) -> (CancellationToken, TokenId) {
        let token_id = self
            .token_id_counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let token_id = TokenId(token_id);
        let entry = self.ongoing_tokens.entry((tenant_id, timeline_id));
        if let Entry::Occupied(entry) = &entry {
            let (cancel, _) = entry.get();
            cancel.cancel();
        }
        entry.insert((self.cancel.child_token(), token_id)).clone()
    }
    /// Cancel an ongoing reconciliation
    fn cancel_reconciliation(&self, tenant_id: TenantId, timeline_id: Option<TimelineId>) {
        if let Some((_, (cancel, _id))) = self.ongoing_tokens.remove(&(tenant_id, timeline_id)) {
            cancel.cancel();
        }
    }
    fn schedule_reconcile(&self, req: ScheduleRequest) {
        let (cancel, token_id) = self.new_token_slot(req.tenant_id, req.timeline_id);
        let hostname = req.safekeeper.skp.host.clone();
        if let Err(err) = self.tx.send((req, cancel, token_id)) {
            tracing::info!("scheduling request onto {hostname} returned error: {err}");
        }
    }
}

pub(crate) struct SafekeeperReconciler {
    inner: SafekeeperReconcilerInner,
    concurrency_limiter: Arc<Semaphore>,
    rx: UnboundedReceiver<(ScheduleRequest, CancellationToken, TokenId)>,
    cancel: CancellationToken,
}

/// Thin wrapper over `Service` to not clutter its inherent functions
#[derive(Clone)]
struct SafekeeperReconcilerInner {
    ongoing_tokens: Arc<OngoingTokens>,
    service: Arc<Service>,
}

impl SafekeeperReconciler {
    fn spawn(cancel: CancellationToken, service: Arc<Service>) -> ReconcilerHandle {
        // We hold the ServiceInner lock so we don't want to make sending to the reconciler channel to be blocking.
        let (tx, rx) = mpsc::unbounded_channel();
        let concurrency = service.config.safekeeper_reconciler_concurrency;
        let ongoing_tokens = Arc::new(ClashMap::new());
        let mut reconciler = SafekeeperReconciler {
            inner: SafekeeperReconcilerInner {
                service,
                ongoing_tokens: ongoing_tokens.clone(),
            },
            rx,
            concurrency_limiter: Arc::new(Semaphore::new(concurrency)),
            cancel: cancel.clone(),
        };
        let handle = ReconcilerHandle {
            tx,
            ongoing_tokens,
            token_id_counter: AtomicU64::new(0),
            cancel,
        };
        tokio::spawn(async move { reconciler.run().await });
        handle
    }
    async fn run(&mut self) {
        loop {
            let req = tokio::select! {
                req = self.rx.recv() => req,
                _ = self.cancel.cancelled() => break,
            };
            let Some((req, req_cancel, req_token_id)) = req else {
                break;
            };

            let permit_res = tokio::select! {
                req = self.concurrency_limiter.clone().acquire_owned() => req,
                _ = self.cancel.cancelled() => break,
            };
            let Ok(_permit) = permit_res else { return };

            let inner = self.inner.clone();
            if req_cancel.is_cancelled() {
                continue;
            }

            tokio::task::spawn(async move {
                let kind = req.kind;
                let tenant_id = req.tenant_id;
                let timeline_id = req.timeline_id;
                let node_id = req.safekeeper.skp.id;
                inner
                    .reconcile_one(req, req_cancel, req_token_id)
                    .instrument(tracing::info_span!(
                        "reconcile_one",
                        ?kind,
                        %tenant_id,
                        ?timeline_id,
                        %node_id,
                    ))
                    .await;
            });
        }
    }
}

impl SafekeeperReconcilerInner {
    async fn reconcile_one(
        &self,
        req: ScheduleRequest,
        req_cancel: CancellationToken,
        req_token_id: TokenId,
    ) {
        let req_host = req.safekeeper.skp.host.clone();
        let success;
        match req.kind {
            SafekeeperTimelineOpKind::Pull => {
                let Some(timeline_id) = req.timeline_id else {
                    tracing::warn!(
                        "ignoring invalid schedule request: timeline_id is empty for `pull`"
                    );
                    return;
                };
                let our_id = req.safekeeper.get_id();
                let http_hosts = req
                    .host_list
                    .iter()
                    .filter(|(node_id, _hostname)| *node_id != our_id)
                    .map(|(_, hostname)| hostname.clone())
                    .collect::<Vec<_>>();
                let pull_req = PullTimelineRequest {
                    http_hosts,
                    tenant_id: req.tenant_id,
                    timeline_id,
                    ignore_tombstone: Some(false),
                };
                success = self
                    .reconcile_inner(
                        &req,
                        async |client| client.pull_timeline(&pull_req).await,
                        |resp| {
                            if let Some(host) = resp.safekeeper_host {
                                tracing::info!("pulled timeline from {host} onto {req_host}");
                            } else {
                                tracing::info!(
                                    "timeline already present on safekeeper on {req_host}"
                                );
                            }
                        },
                        req_cancel,
                    )
                    .await;
            }
            SafekeeperTimelineOpKind::Exclude => {
                // TODO actually exclude instead of delete here
                let tenant_id = req.tenant_id;
                let Some(timeline_id) = req.timeline_id else {
                    tracing::warn!(
                        "ignoring invalid schedule request: timeline_id is empty for `exclude`"
                    );
                    return;
                };
                success = self
                    .reconcile_inner(
                        &req,
                        async |client| client.delete_timeline(tenant_id, timeline_id).await,
                        |_resp| {
                            tracing::info!("deleted timeline from {req_host}");
                        },
                        req_cancel,
                    )
                    .await;
            }
            SafekeeperTimelineOpKind::Delete => {
                let tenant_id = req.tenant_id;
                if let Some(timeline_id) = req.timeline_id {
                    success = self
                        .reconcile_inner(
                            &req,
                            async |client| client.delete_timeline(tenant_id, timeline_id).await,
                            |_resp| {
                                tracing::info!("deleted timeline from {req_host}");
                            },
                            req_cancel,
                        )
                        .await;
                    if success {
                        self.delete_timeline_from_db(tenant_id, timeline_id).await;
                    }
                } else {
                    success = self
                        .reconcile_inner(
                            &req,
                            async |client| client.delete_tenant(tenant_id).await,
                            |_resp| {
                                tracing::info!(%tenant_id, "deleted tenant from {req_host}");
                            },
                            req_cancel,
                        )
                        .await;
                    if success {
                        self.delete_tenant_timelines_from_db(tenant_id).await;
                    }
                }
            }
        }
        if success {
            self.ongoing_tokens.remove_if(
                &(req.tenant_id, req.timeline_id),
                |_ttid, (_cancel, token_id)| {
                    // Ensure that this request is indeed the request we just finished and not a new one
                    req_token_id == *token_id
                },
            );
        }
    }
    async fn delete_timeline_from_db(&self, tenant_id: TenantId, timeline_id: TimelineId) {
        match self
            .service
            .persistence
            .list_pending_ops_for_timeline(tenant_id, timeline_id)
            .await
        {
            Ok(list) => {
                if !list.is_empty() {
                    // duplicate the timeline_id here because it might be None in the reconcile context
                    tracing::info!(%timeline_id, "not deleting timeline from db as there is {} open reconciles", list.len());
                    return;
                }
            }
            Err(e) => {
                tracing::warn!(%timeline_id, "couldn't query pending ops: {e}");
                return;
            }
        }
        tracing::info!(%tenant_id, %timeline_id, "deleting timeline from db after all reconciles succeeded");
        // In theory we could crash right after deleting the op from the db and right before reaching this,
        // but then we'll boot up with a timeline that has deleted_at set, so hopefully we'll issue deletion ops for it again.
        if let Err(err) = self
            .service
            .persistence
            .delete_timeline(tenant_id, timeline_id)
            .await
        {
            tracing::warn!(%tenant_id, %timeline_id, "couldn't delete timeline from db: {err}");
        }
    }
    async fn delete_tenant_timelines_from_db(&self, tenant_id: TenantId) {
        let timeline_list = match self
            .service
            .persistence
            .list_timelines_for_tenant(tenant_id)
            .await
        {
            Ok(timeline_list) => timeline_list,
            Err(e) => {
                tracing::warn!(%tenant_id, "couldn't query timelines: {e}");
                return;
            }
        };
        for timeline in timeline_list {
            let Ok(timeline_id) = TimelineId::from_str(&timeline.timeline_id) else {
                tracing::warn!("Invalid timeline ID in database {}", timeline.timeline_id);
                continue;
            };
            self.delete_timeline_from_db(tenant_id, timeline_id).await;
        }
    }
    /// Returns whether the reconciliation happened successfully (or we got cancelled)
    async fn reconcile_inner<T, F, U>(
        &self,
        req: &ScheduleRequest,
        closure: impl Fn(SafekeeperClient) -> F,
        log_success: impl FnOnce(T) -> U,
        req_cancel: CancellationToken,
    ) -> bool
    where
        F: Future<Output = Result<T, safekeeper_client::mgmt_api::Error>>,
    {
        let jwt = self
            .service
            .config
            .safekeeper_jwt_token
            .clone()
            .map(SecretString::from);
        loop {
            let res = req
                .safekeeper
                .with_client_retries(
                    |client| {
                        let closure = &closure;
                        async move { closure(client).await }
                    },
                    self.service.get_http_client(),
                    &jwt,
                    3,
                    10,
                    Duration::from_secs(10),
                    &req_cancel,
                )
                .await;
            match res {
                Ok(resp) => {
                    log_success(resp);
                    let res = self
                        .service
                        .persistence
                        .remove_pending_op(
                            req.tenant_id,
                            req.timeline_id,
                            req.safekeeper.get_id(),
                            req.generation,
                        )
                        .await;
                    if let Err(err) = res {
                        tracing::info!(
                            "couldn't remove reconciliation request onto {} from persistence: {err:?}",
                            req.safekeeper.skp.host
                        );
                    }
                    return true;
                }
                Err(mgmt_api::Error::Cancelled) => {
                    // On cancellation, the code that issued it will take care of removing db entries (if needed)
                    return false;
                }
                Err(e) => {
                    tracing::info!(
                        "Reconcile attempt for safekeeper {} failed, retrying after sleep: {e:?}",
                        req.safekeeper.skp.host
                    );
                    const SLEEP_TIME: Duration = Duration::from_secs(1);
                    tokio::time::sleep(SLEEP_TIME).await;
                }
            }
        }
    }
}
