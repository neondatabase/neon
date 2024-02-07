use std::{collections::HashMap, time::Duration};

use control_plane::endpoint::{ComputeControlPlane, EndpointStatus};
use control_plane::local_env::LocalEnv;
use hyper::{Method, StatusCode};
use pageserver_api::shard::{ShardCount, ShardIndex, ShardNumber, TenantShardId};
use postgres_connection::parse_host_port;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;
use utils::{
    backoff::{self},
    id::{NodeId, TenantId},
};

use crate::service::Config;

const BUSY_DELAY: Duration = Duration::from_secs(1);
const SLOWDOWN_DELAY: Duration = Duration::from_secs(5);

pub(crate) const API_CONCURRENCY: usize = 32;

pub(super) struct ComputeHookTenant {
    shards: Vec<(ShardIndex, NodeId)>,
}

#[derive(Serialize, Deserialize, Debug)]
struct ComputeHookNotifyRequestShard {
    node_id: NodeId,
    shard_number: ShardNumber,
}

/// Request body that we send to the control plane to notify it of where a tenant is attached
#[derive(Serialize, Deserialize, Debug)]
struct ComputeHookNotifyRequest {
    tenant_id: TenantId,
    shards: Vec<ComputeHookNotifyRequestShard>,
}

/// Error type for attempts to call into the control plane compute notification hook
#[derive(thiserror::Error, Debug)]
pub(crate) enum NotifyError {
    // Request was not send successfully, e.g. transport error
    #[error("Sending request: {0}")]
    Request(#[from] reqwest::Error),
    // Request could not be serviced right now due to ongoing Operation in control plane, but should be possible soon.
    #[error("Control plane tenant busy")]
    Busy,
    // Explicit 429 response asking us to retry less frequently
    #[error("Control plane overloaded")]
    SlowDown,
    // A 503 response indicates the control plane can't handle the request right now
    #[error("Control plane unavailable (status {0})")]
    Unavailable(StatusCode),
    // API returned unexpected non-success status.  We will retry, but log a warning.
    #[error("Control plane returned unexpected status {0}")]
    Unexpected(StatusCode),
    // We shutdown while sending
    #[error("Shutting down")]
    ShuttingDown,
    // A response indicates we will never succeed, such as 400 or 404
    #[error("Non-retryable error {0}")]
    Fatal(StatusCode),
}

impl ComputeHookTenant {
    async fn maybe_reconfigure(&mut self, tenant_id: TenantId) -> Option<ComputeHookNotifyRequest> {
        // Find the highest shard count and drop any shards that aren't
        // for that shard count.
        let shard_count = self.shards.iter().map(|(k, _v)| k.shard_count).max();
        let Some(shard_count) = shard_count else {
            // No shards, nothing to do.
            tracing::info!("ComputeHookTenant::maybe_reconfigure: no shards");
            return None;
        };

        self.shards.retain(|(k, _v)| k.shard_count == shard_count);
        self.shards
            .sort_by_key(|(shard, _node_id)| shard.shard_number);

        if self.shards.len() == shard_count.0 as usize || shard_count == ShardCount(0) {
            // We have pageservers for all the shards: emit a configuration update
            return Some(ComputeHookNotifyRequest {
                tenant_id,
                shards: self
                    .shards
                    .iter()
                    .map(|(shard, node_id)| ComputeHookNotifyRequestShard {
                        shard_number: shard.shard_number,
                        node_id: *node_id,
                    })
                    .collect(),
            });
        } else {
            tracing::info!(
                "ComputeHookTenant::maybe_reconfigure: not enough shards ({}/{})",
                self.shards.len(),
                shard_count.0
            );
        }

        None
    }
}

/// The compute hook is a destination for notifications about changes to tenant:pageserver
/// mapping.  It aggregates updates for the shards in a tenant, and when appropriate reconfigures
/// the compute connection string.
pub(super) struct ComputeHook {
    config: Config,
    state: tokio::sync::Mutex<HashMap<TenantId, ComputeHookTenant>>,
    authorization_header: Option<String>,
}

impl ComputeHook {
    pub(super) fn new(config: Config) -> Self {
        let authorization_header = config
            .control_plane_jwt_token
            .clone()
            .map(|jwt| format!("Bearer {}", jwt));

        Self {
            state: Default::default(),
            config,
            authorization_header,
        }
    }

    /// For test environments: use neon_local's LocalEnv to update compute
    async fn do_notify_local(
        &self,
        reconfigure_request: ComputeHookNotifyRequest,
    ) -> anyhow::Result<()> {
        let env = match LocalEnv::load_config() {
            Ok(e) => e,
            Err(e) => {
                tracing::warn!("Couldn't load neon_local config, skipping compute update ({e})");
                return Ok(());
            }
        };
        let cplane =
            ComputeControlPlane::load(env.clone()).expect("Error loading compute control plane");
        let ComputeHookNotifyRequest { tenant_id, shards } = reconfigure_request;

        let compute_pageservers = shards
            .into_iter()
            .map(|shard| {
                let ps_conf = env
                    .get_pageserver_conf(shard.node_id)
                    .expect("Unknown pageserver");
                let (pg_host, pg_port) = parse_host_port(&ps_conf.listen_pg_addr)
                    .expect("Unable to parse listen_pg_addr");
                (pg_host, pg_port.unwrap_or(5432))
            })
            .collect::<Vec<_>>();

        for (endpoint_name, endpoint) in &cplane.endpoints {
            if endpoint.tenant_id == tenant_id && endpoint.status() == EndpointStatus::Running {
                tracing::info!("🔁 Reconfiguring endpoint {}", endpoint_name,);
                endpoint.reconfigure(compute_pageservers.clone()).await?;
            }
        }

        Ok(())
    }

    async fn do_notify_iteration(
        &self,
        client: &reqwest::Client,
        url: &String,
        reconfigure_request: &ComputeHookNotifyRequest,
        cancel: &CancellationToken,
    ) -> Result<(), NotifyError> {
        let req = client.request(Method::PUT, url);
        let req = if let Some(value) = &self.authorization_header {
            req.header(reqwest::header::AUTHORIZATION, value)
        } else {
            req
        };

        tracing::debug!(
            "Sending notify request to {} ({:?})",
            url,
            reconfigure_request
        );
        let send_result = req.json(&reconfigure_request).send().await;
        let response = match send_result {
            Ok(r) => r,
            Err(e) => return Err(e.into()),
        };

        // Treat all 2xx responses as success
        if response.status() >= StatusCode::OK && response.status() < StatusCode::MULTIPLE_CHOICES {
            if response.status() != StatusCode::OK {
                // Non-200 2xx response: it doesn't make sense to retry, but this is unexpected, so
                // log a warning.
                tracing::warn!(
                    "Unexpected 2xx response code {} from control plane",
                    response.status()
                );
            }

            return Ok(());
        }

        // Error response codes
        match response.status() {
            StatusCode::TOO_MANY_REQUESTS => {
                // TODO: 429 handling should be global: set some state visible to other requests
                // so that they will delay before starting, rather than all notifications trying
                // once before backing off.
                tokio::time::timeout(SLOWDOWN_DELAY, cancel.cancelled())
                    .await
                    .ok();
                Err(NotifyError::SlowDown)
            }
            StatusCode::LOCKED => {
                // Delay our retry if busy: the usual fast exponential backoff in backoff::retry
                // is not appropriate
                tokio::time::timeout(BUSY_DELAY, cancel.cancelled())
                    .await
                    .ok();
                Err(NotifyError::Busy)
            }
            StatusCode::SERVICE_UNAVAILABLE
            | StatusCode::GATEWAY_TIMEOUT
            | StatusCode::BAD_GATEWAY => Err(NotifyError::Unavailable(response.status())),
            StatusCode::BAD_REQUEST | StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => {
                Err(NotifyError::Fatal(response.status()))
            }
            _ => Err(NotifyError::Unexpected(response.status())),
        }
    }

    async fn do_notify(
        &self,
        url: &String,
        reconfigure_request: ComputeHookNotifyRequest,
        cancel: &CancellationToken,
    ) -> Result<(), NotifyError> {
        let client = reqwest::Client::new();
        backoff::retry(
            || self.do_notify_iteration(&client, url, &reconfigure_request, cancel),
            |e| matches!(e, NotifyError::Fatal(_)),
            3,
            10,
            "Send compute notification",
            cancel,
        )
        .await
        .ok_or_else(|| NotifyError::ShuttingDown)
        .and_then(|x| x)
    }

    /// Call this to notify the compute (postgres) tier of new pageservers to use
    /// for a tenant.  notify() is called by each shard individually, and this function
    /// will decide whether an update to the tenant is sent.  An update is sent on the
    /// condition that:
    /// - We know a pageserver for every shard.
    /// - All the shards have the same shard_count (i.e. we are not mid-split)
    ///
    /// Cancellation token enables callers to drop out, e.g. if calling from a Reconciler
    /// that is cancelled.
    ///
    /// This function is fallible, including in the case that the control plane is transiently
    /// unavailable.  A limited number of retries are done internally to efficiently hide short unavailability
    /// periods, but we don't retry forever.  The **caller** is responsible for handling failures and
    /// ensuring that they eventually call again to ensure that the compute is eventually notified of
    /// the proper pageserver nodes for a tenant.
    #[tracing::instrument(skip_all, fields(tenant_shard_id, node_id))]
    pub(super) async fn notify(
        &self,
        tenant_shard_id: TenantShardId,
        node_id: NodeId,
        cancel: &CancellationToken,
    ) -> Result<(), NotifyError> {
        let mut locked = self.state.lock().await;
        let entry = locked
            .entry(tenant_shard_id.tenant_id)
            .or_insert_with(|| ComputeHookTenant { shards: Vec::new() });

        let shard_index = ShardIndex {
            shard_count: tenant_shard_id.shard_count,
            shard_number: tenant_shard_id.shard_number,
        };

        let mut set = false;
        for (existing_shard, existing_node) in &mut entry.shards {
            if *existing_shard == shard_index {
                *existing_node = node_id;
                set = true;
            }
        }
        if !set {
            entry.shards.push((shard_index, node_id));
        }

        let reconfigure_request = entry.maybe_reconfigure(tenant_shard_id.tenant_id).await;
        let Some(reconfigure_request) = reconfigure_request else {
            // The tenant doesn't yet have pageservers for all its shards: we won't notify anything
            // until it does.
            tracing::debug!("Tenant isn't yet ready to emit a notification",);
            return Ok(());
        };

        if let Some(notify_url) = &self.config.compute_hook_url {
            self.do_notify(notify_url, reconfigure_request, cancel)
                .await
        } else {
            self.do_notify_local(reconfigure_request)
                .await
                .map_err(|e| {
                    // This path is for testing only, so munge the error into our prod-style error type.
                    tracing::error!("Local notification hook failed: {e}");
                    NotifyError::Fatal(StatusCode::INTERNAL_SERVER_ERROR)
                })
        }
    }
}
