use std::time::Duration;

use hyper::StatusCode;
use pageserver_api::controller_api::{NodeAvailability, NodeSchedulingPolicy};
use pageserver_client::mgmt_api;
use serde::Serialize;
use tokio_util::sync::CancellationToken;
use utils::{backoff, id::NodeId};

use crate::persistence::NodePersistence;

/// Represents the in-memory description of a Node.
///
/// Scheduling statistics are maintened separately in [`crate::scheduler`].
///
/// The persistent subset of the Node is defined in [`crate::persistence::NodePersistence`]: the
/// implementation of serialization on this type is only for debug dumps.
#[derive(Clone, Serialize)]
pub(crate) struct Node {
    pub(crate) id: NodeId,

    pub(crate) availability: NodeAvailability,
    pub(crate) scheduling: NodeSchedulingPolicy,

    pub(crate) listen_http_addr: String,
    pub(crate) listen_http_port: u16,

    pub(crate) listen_pg_addr: String,
    pub(crate) listen_pg_port: u16,
}

impl Node {
    pub(crate) fn base_url(&self) -> String {
        format!("http://{}:{}", self.listen_http_addr, self.listen_http_port)
    }

    /// Is this node elegible to have work scheduled onto it?
    pub(crate) fn may_schedule(&self) -> bool {
        match self.availability {
            NodeAvailability::Active => {}
            NodeAvailability::Offline => return false,
        }

        match self.scheduling {
            NodeSchedulingPolicy::Active => true,
            NodeSchedulingPolicy::Draining => false,
            NodeSchedulingPolicy::Filling => true,
            NodeSchedulingPolicy::Pause => false,
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

    /// Wrapper for issuing requests to pageserver management API: takes care of generic
    /// retry/backoff for retryable HTTP status codes.
    ///
    /// TODO: hook this up to our knowledge of a pageserver's Active/Offline status so that
    /// we pre-emptively fail requests that would go to an offline pageserver.
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
        O: FnMut(mgmt_api::Client) -> F,
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
            }
        }

        backoff::retry(
            || {
                let http_client = reqwest::ClientBuilder::new()
                    .timeout(timeout)
                    .build()
                    .expect("Failed to construct HTTP client");

                let client =
                    mgmt_api::Client::from_client(http_client, self.base_url(), jwt.as_deref());

                op(client)
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
}
