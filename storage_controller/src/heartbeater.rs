use futures::{stream::FuturesUnordered, StreamExt};
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;

use pageserver_api::{
    controller_api::{NodeAvailability, UtilizationScore},
    models::PageserverUtilization,
};

use thiserror::Error;
use utils::id::NodeId;

use crate::node::Node;

struct HeartbeaterTask {
    receiver: tokio::sync::mpsc::UnboundedReceiver<HeartbeatRequest>,
    cancel: CancellationToken,

    state: HashMap<NodeId, PageserverState>,

    max_unavailable_interval: Duration,
    jwt_token: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) enum PageserverState {
    Available {
        last_seen_at: Instant,
        utilization: PageserverUtilization,
        new: bool,
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
        max_unavailable_interval: Duration,
        cancel: CancellationToken,
    ) -> Self {
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel::<HeartbeatRequest>();
        let mut heartbeater =
            HeartbeaterTask::new(receiver, jwt_token, max_unavailable_interval, cancel);
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
            .unwrap();

        receiver.await.unwrap()
    }
}

impl HeartbeaterTask {
    fn new(
        receiver: tokio::sync::mpsc::UnboundedReceiver<HeartbeatRequest>,
        jwt_token: Option<String>,
        max_unavailable_interval: Duration,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            receiver,
            cancel,
            state: HashMap::new(),
            max_unavailable_interval,
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
                let new_node = !self.state.contains_key(node_id);

                // Clone the node and mark it as available such that the request
                // goes through to the pageserver even when the node is marked offline.
                // This doesn't impact the availability observed by [`crate::service::Service`].
                let mut node = node.clone();
                node.set_availability(NodeAvailability::Active(UtilizationScore::worst()));

                async move {
                    let response = node
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
                            new: new_node,
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
        tracing::info!(
            "Heartbeat round complete for {} nodes, {} offline",
            new_state.len(),
            new_state
                .values()
                .filter(|s| match s {
                    PageserverState::Available { .. } => {
                        false
                    }
                    PageserverState::Offline => true,
                })
                .count()
        );

        let mut deltas = Vec::new();
        let now = Instant::now();
        for (node_id, ps_state) in new_state {
            use std::collections::hash_map::Entry::*;
            let entry = self.state.entry(node_id);

            let mut needs_update = false;
            match entry {
                Occupied(ref occ) => match (occ.get(), &ps_state) {
                    (PageserverState::Offline, PageserverState::Offline) => {}
                    (PageserverState::Available { last_seen_at, .. }, PageserverState::Offline) => {
                        if now - *last_seen_at >= self.max_unavailable_interval {
                            deltas.push((node_id, ps_state.clone()));
                            needs_update = true;
                        }
                    }
                    _ => {
                        deltas.push((node_id, ps_state.clone()));
                        needs_update = true;
                    }
                },
                Vacant(_) => {
                    // This is a new node. Don't generate a delta for it.
                    deltas.push((node_id, ps_state.clone()));
                }
            }

            match entry {
                Occupied(mut occ) if needs_update => {
                    (*occ.get_mut()) = ps_state;
                }
                Vacant(vac) => {
                    vac.insert(ps_state);
                }
                _ => {}
            }
        }

        Ok(AvailablityDeltas(deltas))
    }
}
