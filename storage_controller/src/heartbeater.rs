use futures::{stream::FuturesUnordered, StreamExt};
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;

use pageserver_api::{controller_api::NodeAvailability, models::PageserverUtilization};

use thiserror::Error;
use utils::id::NodeId;

use crate::node::Node;

struct HeartbeaterTask {
    receiver: tokio::sync::mpsc::UnboundedReceiver<HeartbeatRequest>,
    cancel: CancellationToken,

    state: HashMap<NodeId, PageserverState>,

    max_offline_interval: Duration,
    max_warming_up_interval: Duration,
    jwt_token: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) enum PageserverState {
    Available {
        last_seen_at: Instant,
        utilization: PageserverUtilization,
    },
    WarmingUp {
        started_at: Instant,
    },
    Offline,
}

#[derive(Debug)]
pub(crate) struct AvailablityDeltas(pub Vec<(NodeId, PageserverState)>);

#[derive(Debug, Error)]
pub(crate) enum HeartbeaterError {
    #[error("Cancelled")]
    Cancel,
}

struct HeartbeatRequest {
    pageservers: Arc<HashMap<NodeId, Node>>,
    reply: tokio::sync::oneshot::Sender<Result<AvailablityDeltas, HeartbeaterError>>,
}

pub(crate) struct Heartbeater {
    sender: tokio::sync::mpsc::UnboundedSender<HeartbeatRequest>,
}

impl Heartbeater {
    pub(crate) fn new(
        jwt_token: Option<String>,
        max_offline_interval: Duration,
        max_warming_up_interval: Duration,
        cancel: CancellationToken,
    ) -> Self {
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel::<HeartbeatRequest>();
        let mut heartbeater = HeartbeaterTask::new(
            receiver,
            jwt_token,
            max_offline_interval,
            max_warming_up_interval,
            cancel,
        );
        tokio::task::spawn(async move { heartbeater.run().await });

        Self { sender }
    }

    pub(crate) async fn heartbeat(
        &self,
        pageservers: Arc<HashMap<NodeId, Node>>,
    ) -> Result<AvailablityDeltas, HeartbeaterError> {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        self.sender
            .send(HeartbeatRequest {
                pageservers,
                reply: sender,
            })
            .map_err(|_| HeartbeaterError::Cancel)?;

        receiver
            .await
            .map_err(|_| HeartbeaterError::Cancel)
            .and_then(|x| x)
    }
}

impl HeartbeaterTask {
    fn new(
        receiver: tokio::sync::mpsc::UnboundedReceiver<HeartbeatRequest>,
        jwt_token: Option<String>,
        max_offline_interval: Duration,
        max_warming_up_interval: Duration,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            receiver,
            cancel,
            state: HashMap::new(),
            max_offline_interval,
            max_warming_up_interval,
            jwt_token,
        }
    }

    async fn run(&mut self) {
        loop {
            tokio::select! {
                request = self.receiver.recv() => {
                    match request {
                        Some(req) => {
                            let res = self.heartbeat(req.pageservers).await;
                            req.reply.send(res).unwrap();
                        },
                        None => { return; }
                    }
                },
                _ = self.cancel.cancelled() => return
            }
        }
    }

    async fn heartbeat(
        &mut self,
        pageservers: Arc<HashMap<NodeId, Node>>,
    ) -> Result<AvailablityDeltas, HeartbeaterError> {
        let mut new_state = HashMap::new();

        let mut heartbeat_futs = FuturesUnordered::new();
        for (node_id, node) in &*pageservers {
            heartbeat_futs.push({
                let jwt_token = self.jwt_token.clone();
                let cancel = self.cancel.clone();

                // Clone the node and mark it as available such that the request
                // goes through to the pageserver even when the node is marked offline.
                // This doesn't impact the availability observed by [`crate::service::Service`].
                let mut node_clone = node.clone();
                node_clone
                    .set_availability(NodeAvailability::Active(PageserverUtilization::full()));

                async move {
                    let response = node_clone
                        .with_client_retries(
                            |client| async move { client.get_utilization().await },
                            &jwt_token,
                            3,
                            3,
                            Duration::from_secs(1),
                            &cancel,
                        )
                        .await;

                    let response = match response {
                        Some(r) => r,
                        None => {
                            // This indicates cancellation of the request.
                            // We ignore the node in this case.
                            return None;
                        }
                    };

                    let status = if let Ok(utilization) = response {
                        PageserverState::Available {
                            last_seen_at: Instant::now(),
                            utilization,
                        }
                    } else if let NodeAvailability::WarmingUp(last_seen_at) =
                        node.get_availability()
                    {
                        PageserverState::WarmingUp {
                            started_at: *last_seen_at,
                        }
                    } else {
                        PageserverState::Offline
                    };

                    Some((*node_id, status))
                }
            });

            loop {
                let maybe_status = tokio::select! {
                    next = heartbeat_futs.next() => {
                        match next {
                            Some(result) => result,
                            None => { break; }
                        }
                    },
                    _ = self.cancel.cancelled() => { return Err(HeartbeaterError::Cancel); }
                };

                if let Some((node_id, status)) = maybe_status {
                    new_state.insert(node_id, status);
                }
            }
        }

        let mut warming_up = 0;
        let mut offline = 0;
        for state in new_state.values() {
            match state {
                PageserverState::WarmingUp { .. } => {
                    warming_up += 1;
                }
                PageserverState::Offline { .. } => offline += 1,
                PageserverState::Available { .. } => {}
            }
        }

        tracing::info!(
            "Heartbeat round complete for {} nodes, {} warming-up, {} offline",
            new_state.len(),
            warming_up,
            offline
        );

        let mut deltas = Vec::new();
        let now = Instant::now();
        for (node_id, ps_state) in new_state.iter_mut() {
            use std::collections::hash_map::Entry::*;
            let entry = self.state.entry(*node_id);

            let mut needs_update = false;
            match entry {
                Occupied(ref occ) => match (occ.get(), &ps_state) {
                    (PageserverState::Offline, PageserverState::Offline) => {}
                    (PageserverState::Available { last_seen_at, .. }, PageserverState::Offline) => {
                        if now - *last_seen_at >= self.max_offline_interval {
                            deltas.push((*node_id, ps_state.clone()));
                            needs_update = true;
                        }
                    }
                    (_, PageserverState::WarmingUp { started_at }) => {
                        if now - *started_at >= self.max_warming_up_interval {
                            *ps_state = PageserverState::Offline;
                        }

                        deltas.push((*node_id, ps_state.clone()));
                        needs_update = true;
                    }
                    _ => {
                        deltas.push((*node_id, ps_state.clone()));
                        needs_update = true;
                    }
                },
                Vacant(_) => {
                    // This is a new node. Don't generate a delta for it.
                    deltas.push((*node_id, ps_state.clone()));
                }
            }

            match entry {
                Occupied(mut occ) if needs_update => {
                    (*occ.get_mut()) = ps_state.clone();
                }
                Vacant(vac) => {
                    vac.insert(ps_state.clone());
                }
                _ => {}
            }
        }

        Ok(AvailablityDeltas(deltas))
    }
}
