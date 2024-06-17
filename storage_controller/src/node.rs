use std::{str::FromStr, time::Duration};

use pageserver_api::{
    controller_api::{
        NodeAvailability, NodeDescribeResponse, NodeRegisterRequest, NodeSchedulingPolicy,
        TenantLocateResponseShard, UtilizationScore,
    },
    shard::TenantShardId,
};
use pageserver_client::mgmt_api;
use reqwest::StatusCode;
use serde::Serialize;
use tokio_util::sync::CancellationToken;
use utils::{backoff, id::NodeId};

use crate::{
    pageserver_client::PageserverClient, persistence::NodePersistence, scheduler::MaySchedule,
};

/// Represents the in-memory description of a Node.
///
/// Scheduling statistics are maintened separately in [`crate::scheduler`].
///
/// The persistent subset of the Node is defined in [`crate::persistence::NodePersistence`]: the
/// implementation of serialization on this type is only for debug dumps.
#[derive(Clone, Serialize)]
pub(crate) struct Node {
    id: NodeId,

    availability: NodeAvailability,
    scheduling: NodeSchedulingPolicy,

    listen_http_addr: String,
    listen_http_port: u16,

    listen_pg_addr: String,
    listen_pg_port: u16,

    // This cancellation token means "stop any RPCs in flight to this node, and don't start
    // any more". It is not related to process shutdown.
    #[serde(skip)]
    cancel: CancellationToken,
}

/// When updating [`Node::availability`] we use this type to indicate to the caller
/// whether/how they changed it.
pub(crate) enum AvailabilityTransition {
    ToActive,
    ToOffline,
    Unchanged,
}

impl Node {
    pub(crate) fn base_url(&self) -> String {
        format!("http://{}:{}", self.listen_http_addr, self.listen_http_port)
    }

    pub(crate) fn get_id(&self) -> NodeId {
        self.id
    }

    pub(crate) fn set_scheduling(&mut self, scheduling: NodeSchedulingPolicy) {
        self.scheduling = scheduling
    }

    /// Does this registration request match `self`?  This is used when deciding whether a registration
    /// request should be allowed to update an existing record with the same node ID.
    pub(crate) fn registration_match(&self, register_req: &NodeRegisterRequest) -> bool {
        self.id == register_req.node_id
            && self.listen_http_addr == register_req.listen_http_addr
            && self.listen_http_port == register_req.listen_http_port
            && self.listen_pg_addr == register_req.listen_pg_addr
            && self.listen_pg_port == register_req.listen_pg_port
    }

    /// For a shard located on this node, populate a response object
    /// with this node's address information.
    pub(crate) fn shard_location(&self, shard_id: TenantShardId) -> TenantLocateResponseShard {
        TenantLocateResponseShard {
            shard_id,
            node_id: self.id,
            listen_http_addr: self.listen_http_addr.clone(),
            listen_http_port: self.listen_http_port,
            listen_pg_addr: self.listen_pg_addr.clone(),
            listen_pg_port: self.listen_pg_port,
        }
    }

    pub(crate) fn set_availability(&mut self, availability: NodeAvailability) {
        match self.get_availability_transition(availability) {
            AvailabilityTransition::ToActive => {
                // Give the node a new cancellation token, effectively resetting it to un-cancelled.  Any
                // users of previously-cloned copies of the node will still see the old cancellation
                // state.  For example, Reconcilers in flight will have to complete and be spawned
                // again to realize that the node has become available.
                self.cancel = CancellationToken::new();
            }
            AvailabilityTransition::ToOffline => {
                // Fire the node's cancellation token to cancel any in-flight API requests to it
                self.cancel.cancel();
            }
            AvailabilityTransition::Unchanged => {}
        }
        self.availability = availability;
    }

    /// Without modifying the availability of the node, convert the intended availability
    /// into a description of the transition.
    pub(crate) fn get_availability_transition(
        &self,
        availability: NodeAvailability,
    ) -> AvailabilityTransition {
        use AvailabilityTransition::*;
        use NodeAvailability::*;

        match (self.availability, availability) {
            (Offline, Active(_)) => ToActive,
            (Active(_), Offline) => ToOffline,
            // Consider the case when the storage controller handles the re-attach of a node
            // before the heartbeats detect that the node is back online. We still need
            // [`Service::node_configure`] to attempt reconciliations for shards with an
            // unknown observed location.
            // The unsavoury match arm below handles this situation.
            (Active(lhs), Active(rhs))
                if lhs == UtilizationScore::worst() && rhs < UtilizationScore::worst() =>
            {
                ToActive
            }
            _ => Unchanged,
        }
    }

    /// Whether we may send API requests to this node.
    pub(crate) fn is_available(&self) -> bool {
        // When we clone a node, [`Self::availability`] is a snapshot, but [`Self::cancel`] holds
        // a reference to the original Node's cancellation status.  Checking both of these results
        // in a "pessimistic" check where we will consider a Node instance unavailable if it was unavailable
        // when we cloned it, or if the original Node instance's cancellation token was fired.
        matches!(self.availability, NodeAvailability::Active(_)) && !self.cancel.is_cancelled()
    }

    /// Is this node elegible to have work scheduled onto it?
    pub(crate) fn may_schedule(&self) -> MaySchedule {
        let score = match self.availability {
            NodeAvailability::Active(score) => score,
            NodeAvailability::Offline => return MaySchedule::No,
        };

        match self.scheduling {
            NodeSchedulingPolicy::Active => MaySchedule::Yes(score),
            NodeSchedulingPolicy::Draining => MaySchedule::No,
            NodeSchedulingPolicy::Filling => MaySchedule::Yes(score),
            NodeSchedulingPolicy::Pause => MaySchedule::No,
        }
    }

    pub(crate) fn new(
        id: NodeId,
        listen_http_addr: String,
        listen_http_port: u16,
        listen_pg_addr: String,
        listen_pg_port: u16,
    ) -> Self {
        Self {
            id,
            listen_http_addr,
            listen_http_port,
            listen_pg_addr,
            listen_pg_port,
            scheduling: NodeSchedulingPolicy::Filling,
            availability: NodeAvailability::Offline,
            cancel: CancellationToken::new(),
        }
    }

    pub(crate) fn to_persistent(&self) -> NodePersistence {
        NodePersistence {
            node_id: self.id.0 as i64,
            scheduling_policy: self.scheduling.into(),
            listen_http_addr: self.listen_http_addr.clone(),
            listen_http_port: self.listen_http_port as i32,
            listen_pg_addr: self.listen_pg_addr.clone(),
            listen_pg_port: self.listen_pg_port as i32,
        }
    }

    pub(crate) fn from_persistent(np: NodePersistence) -> Self {
        Self {
            id: NodeId(np.node_id as u64),
            // At startup we consider a node offline until proven otherwise.
            availability: NodeAvailability::Offline,
            scheduling: NodeSchedulingPolicy::from_str(&np.scheduling_policy)
                .expect("Bad scheduling policy in DB"),
            listen_http_addr: np.listen_http_addr,
            listen_http_port: np.listen_http_port as u16,
            listen_pg_addr: np.listen_pg_addr,
            listen_pg_port: np.listen_pg_port as u16,
            cancel: CancellationToken::new(),
        }
    }

    /// Wrapper for issuing requests to pageserver management API: takes care of generic
    /// retry/backoff for retryable HTTP status codes.
    ///
    /// This will return None to indicate cancellation.  Cancellation may happen from
    /// the cancellation token passed in, or from Self's cancellation token (i.e. node
    /// going offline).
    pub(crate) async fn with_client_retries<T, O, F>(
        &self,
        mut op: O,
        jwt: &Option<String>,
        warn_threshold: u32,
        max_retries: u32,
        timeout: Duration,
        cancel: &CancellationToken,
    ) -> Option<mgmt_api::Result<T>>
    where
        O: FnMut(PageserverClient) -> F,
        F: std::future::Future<Output = mgmt_api::Result<T>>,
    {
        fn is_fatal(e: &mgmt_api::Error) -> bool {
            use mgmt_api::Error::*;
            match e {
                ReceiveBody(_) | ReceiveErrorBody(_) => false,
                ApiError(StatusCode::SERVICE_UNAVAILABLE, _)
                | ApiError(StatusCode::GATEWAY_TIMEOUT, _)
                | ApiError(StatusCode::REQUEST_TIMEOUT, _) => false,
                ApiError(_, _) => true,
                Cancelled => true,
            }
        }

        backoff::retry(
            || {
                let http_client = reqwest::ClientBuilder::new()
                    .timeout(timeout)
                    .build()
                    .expect("Failed to construct HTTP client");

                let client = PageserverClient::from_client(
                    self.get_id(),
                    http_client,
                    self.base_url(),
                    jwt.as_deref(),
                );

                let node_cancel_fut = self.cancel.cancelled();

                let op_fut = op(client);

                async {
                    tokio::select! {
                        r = op_fut=> {r},
                        _ = node_cancel_fut => {
                        Err(mgmt_api::Error::Cancelled)
                    }}
                }
            },
            is_fatal,
            warn_threshold,
            max_retries,
            &format!(
                "Call to node {} ({}:{}) management API",
                self.id, self.listen_http_addr, self.listen_http_port
            ),
            cancel,
        )
        .await
    }

    /// Generate the simplified API-friendly description of a node's state
    pub(crate) fn describe(&self) -> NodeDescribeResponse {
        NodeDescribeResponse {
            id: self.id,
            availability: self.availability.into(),
            scheduling: self.scheduling,
            listen_http_addr: self.listen_http_addr.clone(),
            listen_http_port: self.listen_http_port,
            listen_pg_addr: self.listen_pg_addr.clone(),
            listen_pg_port: self.listen_pg_port,
        }
    }
}

impl std::fmt::Display for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({})", self.id, self.listen_http_addr)
    }
}

impl std::fmt::Debug for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({})", self.id, self.listen_http_addr)
    }
}
