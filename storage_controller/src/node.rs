use std::str::FromStr;
use std::time::Duration;

use pageserver_api::controller_api::{
    AvailabilityZone, NodeAvailability, NodeDescribeResponse, NodeLifecycle, NodeRegisterRequest,
    NodeSchedulingPolicy, TenantLocateResponseShard,
};
use pageserver_api::shard::TenantShardId;
use pageserver_client::mgmt_api;
use reqwest::StatusCode;
use serde::Serialize;
use tokio_util::sync::CancellationToken;
use utils::backoff;
use utils::id::NodeId;

use crate::pageserver_client::PageserverClient;
use crate::persistence::NodePersistence;
use crate::scheduler::MaySchedule;

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
    lifecycle: NodeLifecycle,

    listen_http_addr: String,
    listen_http_port: u16,
    listen_https_port: Option<u16>,

    listen_pg_addr: String,
    listen_pg_port: u16,
    listen_grpc_addr: Option<String>,
    listen_grpc_port: Option<u16>,

    availability_zone_id: AvailabilityZone,

    // Flag from storcon's config to use https for pageserver admin API.
    // Invariant: if |true|, listen_https_port should contain a value.
    use_https: bool,
    // This cancellation token means "stop any RPCs in flight to this node, and don't start
    // any more". It is not related to process shutdown.
    #[serde(skip)]
    cancel: CancellationToken,
}

/// When updating [`Node::availability`] we use this type to indicate to the caller
/// whether/how they changed it.
pub(crate) enum AvailabilityTransition {
    ToActive,
    ToWarmingUpFromActive,
    ToWarmingUpFromOffline,
    ToOffline,
    Unchanged,
}

impl Node {
    pub(crate) fn base_url(&self) -> String {
        if self.use_https {
            format!(
                "https://{}:{}",
                self.listen_http_addr,
                self.listen_https_port
                    .expect("https port should be specified if use_https is on")
            )
        } else {
            format!("http://{}:{}", self.listen_http_addr, self.listen_http_port)
        }
    }

    pub(crate) fn get_id(&self) -> NodeId {
        self.id
    }

    #[allow(unused)]
    pub(crate) fn get_availability_zone_id(&self) -> &AvailabilityZone {
        &self.availability_zone_id
    }

    pub(crate) fn get_scheduling(&self) -> NodeSchedulingPolicy {
        self.scheduling
    }

    pub(crate) fn set_scheduling(&mut self, scheduling: NodeSchedulingPolicy) {
        self.scheduling = scheduling
    }

    pub(crate) fn has_https_port(&self) -> bool {
        self.listen_https_port.is_some()
    }

    /// Does this registration request match `self`?  This is used when deciding whether a registration
    /// request should be allowed to update an existing record with the same node ID.
    pub(crate) fn registration_match(&self, register_req: &NodeRegisterRequest) -> bool {
        self.id == register_req.node_id
            && self.listen_http_addr == register_req.listen_http_addr
            && self.listen_http_port == register_req.listen_http_port
            // Note: HTTPS and gRPC addresses may change, to allow for migrations. See
            // [`Self::need_update`] for more details.
            && self.listen_pg_addr == register_req.listen_pg_addr
            && self.listen_pg_port == register_req.listen_pg_port
            && self.availability_zone_id == register_req.availability_zone_id
    }

    // Do we need to update an existing record in DB on this registration request?
    pub(crate) fn need_update(&self, register_req: &NodeRegisterRequest) -> bool {
        // These are checked here, since they may change before we're fully migrated.
        self.listen_https_port != register_req.listen_https_port
            || self.listen_grpc_addr != register_req.listen_grpc_addr
            || self.listen_grpc_port != register_req.listen_grpc_port
    }

    /// For a shard located on this node, populate a response object
    /// with this node's address information.
    pub(crate) fn shard_location(&self, shard_id: TenantShardId) -> TenantLocateResponseShard {
        TenantLocateResponseShard {
            shard_id,
            node_id: self.id,
            listen_http_addr: self.listen_http_addr.clone(),
            listen_http_port: self.listen_http_port,
            listen_https_port: self.listen_https_port,
            listen_pg_addr: self.listen_pg_addr.clone(),
            listen_pg_port: self.listen_pg_port,
            listen_grpc_addr: self.listen_grpc_addr.clone(),
            listen_grpc_port: self.listen_grpc_port,
        }
    }

    pub(crate) fn get_availability(&self) -> &NodeAvailability {
        &self.availability
    }

    pub(crate) fn set_availability(&mut self, availability: NodeAvailability) {
        use AvailabilityTransition::*;
        use NodeAvailability::WarmingUp;

        match self.get_availability_transition(&availability) {
            ToActive => {
                // Give the node a new cancellation token, effectively resetting it to un-cancelled.  Any
                // users of previously-cloned copies of the node will still see the old cancellation
                // state.  For example, Reconcilers in flight will have to complete and be spawned
                // again to realize that the node has become available.
                self.cancel = CancellationToken::new();
            }
            ToOffline | ToWarmingUpFromActive => {
                // Fire the node's cancellation token to cancel any in-flight API requests to it
                self.cancel.cancel();
            }
            Unchanged | ToWarmingUpFromOffline => {}
        }

        if let (WarmingUp(crnt), WarmingUp(proposed)) = (&self.availability, &availability) {
            self.availability = WarmingUp(std::cmp::max(*crnt, *proposed));
        } else {
            self.availability = availability;
        }
    }

    /// Without modifying the availability of the node, convert the intended availability
    /// into a description of the transition.
    pub(crate) fn get_availability_transition(
        &self,
        availability: &NodeAvailability,
    ) -> AvailabilityTransition {
        use AvailabilityTransition::*;
        use NodeAvailability::*;

        match (&self.availability, availability) {
            (Offline, Active(_)) => ToActive,
            (Active(_), Offline) => ToOffline,
            (Active(_), WarmingUp(_)) => ToWarmingUpFromActive,
            (WarmingUp(_), Offline) => ToOffline,
            (WarmingUp(_), Active(_)) => ToActive,
            (Offline, WarmingUp(_)) => ToWarmingUpFromOffline,
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
        let utilization = match &self.availability {
            NodeAvailability::Active(u) => u.clone(),
            NodeAvailability::Offline | NodeAvailability::WarmingUp(_) => return MaySchedule::No,
        };

        match self.scheduling {
            NodeSchedulingPolicy::Active => MaySchedule::Yes(utilization),
            NodeSchedulingPolicy::Draining => MaySchedule::No,
            NodeSchedulingPolicy::Filling => MaySchedule::Yes(utilization),
            NodeSchedulingPolicy::Pause => MaySchedule::No,
            NodeSchedulingPolicy::PauseForRestart => MaySchedule::No,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        id: NodeId,
        listen_http_addr: String,
        listen_http_port: u16,
        listen_https_port: Option<u16>,
        listen_pg_addr: String,
        listen_pg_port: u16,
        listen_grpc_addr: Option<String>,
        listen_grpc_port: Option<u16>,
        availability_zone_id: AvailabilityZone,
        use_https: bool,
    ) -> anyhow::Result<Self> {
        if use_https && listen_https_port.is_none() {
            anyhow::bail!(
                "cannot create node {id}: \
                https is enabled, but https port is not specified"
            );
        }

        if listen_grpc_addr.is_some() != listen_grpc_port.is_some() {
            anyhow::bail!("cannot create node {id}: must specify both gRPC address and port");
        }

        Ok(Self {
            id,
            listen_http_addr,
            listen_http_port,
            listen_https_port,
            listen_pg_addr,
            listen_pg_port,
            listen_grpc_addr,
            listen_grpc_port,
            scheduling: NodeSchedulingPolicy::Active,
            lifecycle: NodeLifecycle::Active,
            availability: NodeAvailability::Offline,
            availability_zone_id,
            use_https,
            cancel: CancellationToken::new(),
        })
    }

    pub(crate) fn to_persistent(&self) -> NodePersistence {
        NodePersistence {
            node_id: self.id.0 as i64,
            scheduling_policy: self.scheduling.into(),
            lifecycle: self.lifecycle.into(),
            listen_http_addr: self.listen_http_addr.clone(),
            listen_http_port: self.listen_http_port as i32,
            listen_https_port: self.listen_https_port.map(|x| x as i32),
            listen_pg_addr: self.listen_pg_addr.clone(),
            listen_pg_port: self.listen_pg_port as i32,
            listen_grpc_addr: self.listen_grpc_addr.clone(),
            listen_grpc_port: self.listen_grpc_port.map(|port| port as i32),
            availability_zone_id: self.availability_zone_id.0.clone(),
        }
    }

    pub(crate) fn from_persistent(np: NodePersistence, use_https: bool) -> anyhow::Result<Self> {
        if use_https && np.listen_https_port.is_none() {
            anyhow::bail!(
                "cannot load node {} from persistent: \
                https is enabled, but https port is not specified",
                np.node_id,
            );
        }

        if np.listen_grpc_addr.is_some() != np.listen_grpc_port.is_some() {
            anyhow::bail!(
                "can't load node {}: must specify both gRPC address and port",
                np.node_id
            );
        }

        Ok(Self {
            id: NodeId(np.node_id as u64),
            // At startup we consider a node offline until proven otherwise.
            availability: NodeAvailability::Offline,
            scheduling: NodeSchedulingPolicy::from_str(&np.scheduling_policy)
                .expect("Bad scheduling policy in DB"),
            lifecycle: NodeLifecycle::from_str(&np.lifecycle).expect("Bad lifecycle in DB"),
            listen_http_addr: np.listen_http_addr,
            listen_http_port: np.listen_http_port as u16,
            listen_https_port: np.listen_https_port.map(|x| x as u16),
            listen_pg_addr: np.listen_pg_addr,
            listen_pg_port: np.listen_pg_port as u16,
            listen_grpc_addr: np.listen_grpc_addr,
            listen_grpc_port: np.listen_grpc_port.map(|port| port as u16),
            availability_zone_id: AvailabilityZone(np.availability_zone_id),
            use_https,
            cancel: CancellationToken::new(),
        })
    }

    /// Wrapper for issuing requests to pageserver management API: takes care of generic
    /// retry/backoff for retryable HTTP status codes.
    ///
    /// This will return None to indicate cancellation.  Cancellation may happen from
    /// the cancellation token passed in, or from Self's cancellation token (i.e. node
    /// going offline).
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn with_client_retries<T, O, F>(
        &self,
        mut op: O,
        http_client: &reqwest::Client,
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
                SendRequest(_) | ReceiveBody(_) | ReceiveErrorBody(_) => false,
                ApiError(StatusCode::SERVICE_UNAVAILABLE, _)
                | ApiError(StatusCode::GATEWAY_TIMEOUT, _)
                | ApiError(StatusCode::REQUEST_TIMEOUT, _) => false,
                ApiError(_, _) => true,
                Cancelled => true,
                Timeout(_) => false,
            }
        }

        backoff::retry(
            || {
                let client = PageserverClient::new(
                    self.get_id(),
                    http_client.clone(),
                    self.base_url(),
                    jwt.as_deref(),
                );

                let node_cancel_fut = self.cancel.cancelled();

                let op_fut = tokio::time::timeout(timeout, op(client));

                async {
                    tokio::select! {
                        r = op_fut => match r {
                            Ok(r) => r,
                            Err(e) => Err(mgmt_api::Error::Timeout(format!("{e}"))),
                        },
                        _ = node_cancel_fut => {
                        Err(mgmt_api::Error::Cancelled)
                    }}
                }
            },
            is_fatal,
            warn_threshold,
            max_retries,
            &format!(
                "Call to node {} ({}) management API",
                self.id,
                self.base_url(),
            ),
            cancel,
        )
        .await
    }

    /// Generate the simplified API-friendly description of a node's state
    pub(crate) fn describe(&self) -> NodeDescribeResponse {
        NodeDescribeResponse {
            id: self.id,
            availability: self.availability.clone().into(),
            scheduling: self.scheduling,
            availability_zone_id: self.availability_zone_id.0.clone(),
            listen_http_addr: self.listen_http_addr.clone(),
            listen_http_port: self.listen_http_port,
            listen_https_port: self.listen_https_port,
            listen_pg_addr: self.listen_pg_addr.clone(),
            listen_pg_port: self.listen_pg_port,
            listen_grpc_addr: self.listen_grpc_addr.clone(),
            listen_grpc_port: self.listen_grpc_port,
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
