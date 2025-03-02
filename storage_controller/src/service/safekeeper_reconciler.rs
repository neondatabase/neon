use std::{collections::HashMap, sync::Arc, time::Duration};

use safekeeper_api::models::PullTimelineRequest;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;
use utils::{
    failpoint_support,
    id::{NodeId, TenantId, TimelineId},
    logging::SecretString,
};

use crate::{
    id_lock_map::trace_shared_lock,
    persistence::{SafekeeperPersistence, SafekeeperTimelineOpKind},
    safekeeper::Safekeeper,
    safekeeper_client::SafekeeperClient,
    service::TenantOperations,
};

use super::{Service, TimelinePersistence};

pub(crate) struct SafekeeperReconcilers {
    reconcilers: HashMap<NodeId, ReconcilerHandle>,
}

impl SafekeeperReconcilers {
    pub fn new() -> Self {
        SafekeeperReconcilers {
            reconcilers: HashMap::new(),
        }
    }
    pub(crate) fn schedule_request(&mut self, service: &Arc<Service>, req: ScheduleRequest) {
        let node_id = req.safekeeper.get_id();
        let hostname = req.safekeeper.skp.host.clone();
        let reconciler_handle = self
            .reconcilers
            .entry(node_id)
            .or_insert_with(|| SafekeeperReconciler::spawn(service.clone()));
        if let Err(err) = reconciler_handle.tx.send(req) {
            tracing::info!("scheduling request onto {hostname} returned error: {err}",);
        }
    }
    pub(crate) fn cancel_safekeeper(&mut self, node_id: NodeId) {
        if let Some(handle) = self.reconcilers.remove(&node_id) {
            handle.cancel.cancel();
        }
    }
}

pub(crate) struct ScheduleRequest {
    pub(crate) safekeeper: Box<Safekeeper>,
    pub(crate) host_list: Vec<(NodeId, String)>,
    pub(crate) tenant_id: TenantId,
    pub(crate) timeline_id: TimelineId,
    pub(crate) generation: u32,
    pub(crate) kind: SafekeeperTimelineOpKind,
}

struct ReconcilerHandle {
    tx: UnboundedSender<ScheduleRequest>,
    cancel: CancellationToken,
}
pub(crate) struct SafekeeperReconciler {
    service: Arc<Service>,
    rx: UnboundedReceiver<ScheduleRequest>,
    cancel: CancellationToken,
}

impl SafekeeperReconciler {
    fn spawn(service: Arc<Service>) -> ReconcilerHandle {
        let cancel = CancellationToken::new();
        // We hold the ServiceInner lock so we don't want to make sending to the reconciler channel to be blocking.
        let (tx, rx) = mpsc::unbounded_channel();
        let mut reconciler = SafekeeperReconciler {
            service,
            rx,
            cancel: cancel.clone(),
        };
        let handle = ReconcilerHandle { tx, cancel };
        tokio::spawn(async move { reconciler.run().await });
        handle
    }
    async fn run(&mut self) {
        loop {
            // TODO add parallelism with semaphore here
            let req = tokio::select! {
                req = self.rx.recv() => req,
                _ = self.cancel.cancelled() => break,
            };
            let Some(req) = req else { break };

            let kind = req.kind;
            let tenant_id = req.tenant_id;
            let timeline_id = req.timeline_id;
            self.reconcile_one(req)
                .instrument(tracing::info_span!(
                    "reconcile_one",
                    ?kind,
                    %tenant_id,
                    %timeline_id
                ))
                .await;
        }
    }
    async fn reconcile_one(&self, req: ScheduleRequest) {
        let req_host = req.safekeeper.skp.host.clone();
        match req.kind {
            SafekeeperTimelineOpKind::Pull => {
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
                    timeline_id: req.timeline_id,
                };
                self.reconcile_inner(
                    req,
                    async |client| client.pull_timeline(&pull_req).await,
                    |resp| {
                        tracing::info!(
                            "pulled timeline from {} onto {req_host}",
                            resp.safekeeper_host,
                        );
                    },
                )
                .await;
            }
            SafekeeperTimelineOpKind::Exclude => {
                // TODO actually exclude instead of delete here
                let tenant_id = req.tenant_id;
                let timeline_id = req.timeline_id;
                self.reconcile_inner(
                    req,
                    async |client| client.delete_timeline(tenant_id, timeline_id).await,
                    |_resp| {
                        tracing::info!("deleted timeline from {req_host}");
                    },
                )
                .await;
            }
            SafekeeperTimelineOpKind::Delete => {
                let tenant_id = req.tenant_id;
                let timeline_id = req.timeline_id;
                self.reconcile_inner(
                    req,
                    async |client| client.delete_timeline(tenant_id, timeline_id).await,
                    |_resp| {
                        tracing::info!("deleted timeline from {req_host}");
                    },
                )
                .await;
            }
        }
    }
    async fn reconcile_inner<T, F, U>(
        &self,
        req: ScheduleRequest,
        closure: impl Fn(SafekeeperClient) -> F,
        log_success: impl FnOnce(T) -> U,
    ) where
        F: Future<Output = Result<T, safekeeper_client::mgmt_api::Error>>,
    {
        let jwt = self
            .service
            .config
            .safekeeper_jwt_token
            .clone()
            .map(SecretString::from);
        let res = req
            .safekeeper
            .with_client_retries(
                |client| {
                    let closure = &closure;
                    async move { closure(client).await }
                },
                &jwt,
                3,
                10,
                Duration::from_secs(10),
                &self.cancel,
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
            }
            Err(e) => {
                tracing::info!(
                    "Reconcile attempt for safekeeper {} failed: {e:?}",
                    req.safekeeper.skp.host
                );
                // TODO we should probably automatically retry on error here
            }
        }
    }
}
