use std::sync::Arc;
use std::{collections::HashMap, time::Duration};

use control_plane::endpoint::{ComputeControlPlane, EndpointStatus};
use control_plane::local_env::LocalEnv;
use hyper::{Method, StatusCode};
use pageserver_api::shard::{ShardCount, ShardNumber, ShardStripeSize, TenantShardId};
use postgres_connection::parse_host_port;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;
use utils::{
    backoff::{self},
    id::{NodeId, TenantId},
};

use crate::service::Config;

const SLOWDOWN_DELAY: Duration = Duration::from_secs(5);

pub(crate) const API_CONCURRENCY: usize = 32;

struct ShardedComputeHookTenant {
    stripe_size: ShardStripeSize,
    shard_count: ShardCount,
    shards: Vec<(ShardNumber, NodeId)>,

    // Must hold this lock to send a notification.
    send_lock: Arc<tokio::sync::Mutex<ComputeHookNotifyRequest>>,
}

enum ComputeHookTenant {
    Unsharded((NodeId, Arc<tokio::sync::Mutex<ComputeHookNotifyRequest>>)),
    Sharded(ShardedComputeHookTenant),
}

impl ComputeHookTenant {
    /// Construct with at least one shard's information
    fn new(tenant_shard_id: TenantShardId, stripe_size: ShardStripeSize, node_id: NodeId) -> Self {
        if tenant_shard_id.shard_count.count() > 1 {
            Self::Sharded(ShardedComputeHookTenant {
                shards: vec![(tenant_shard_id.shard_number, node_id)],
                stripe_size,
                shard_count: tenant_shard_id.shard_count,
                send_lock: Arc::default(),
            })
        } else {
            Self::Unsharded((node_id, Arc::default()))
        }
    }

    fn get_lock(&self) -> &Arc<tokio::sync::Mutex<ComputeHookNotifyRequest>> {
        match self {
            Self::Unsharded((_node_id, lock)) => lock,
            Self::Sharded(sharded_tenant) => &sharded_tenant.send_lock,
        }
    }

    /// Set one shard's location.  If stripe size or shard count have changed, Self is reset
    /// and drops existing content.
    fn update(
        &mut self,
        tenant_shard_id: TenantShardId,
        stripe_size: ShardStripeSize,
        node_id: NodeId,
    ) {
        match self {
            Self::Unsharded((existing_node_id, _lock))
                if tenant_shard_id.shard_count.count() == 1 =>
            {
                *existing_node_id = node_id
            }
            Self::Sharded(sharded_tenant)
                if sharded_tenant.stripe_size == stripe_size
                    && sharded_tenant.shard_count == tenant_shard_id.shard_count =>
            {
                if let Some(existing) = sharded_tenant
                    .shards
                    .iter()
                    .position(|s| s.0 == tenant_shard_id.shard_number)
                {
                    sharded_tenant.shards.get_mut(existing).unwrap().1 = node_id;
                } else {
                    sharded_tenant
                        .shards
                        .push((tenant_shard_id.shard_number, node_id));
                    sharded_tenant.shards.sort_by_key(|s| s.0)
                }
            }
            _ => {
                // Shard count changed: reset struct.
                *self = Self::new(tenant_shard_id, stripe_size, node_id);
            }
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
struct ComputeHookNotifyRequestShard {
    node_id: NodeId,
    shard_number: ShardNumber,
}

/// Request body that we send to the control plane to notify it of where a tenant is attached
#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
struct ComputeHookNotifyRequest {
    tenant_id: TenantId,
    stripe_size: Option<ShardStripeSize>,
    shards: Vec<ComputeHookNotifyRequestShard>,
}

impl Default for ComputeHookNotifyRequest {
    fn default() -> Self {
        Self {
            tenant_id: TenantId::from_array([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]),
            stripe_size: None,
            shards: Vec::new(),
        }
    }
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

enum MaybeSendResult {
    // Please send this request while holding the lock, and if you succeed then write
    // the request into the lock.
    Transmit(
        (
            ComputeHookNotifyRequest,
            tokio::sync::OwnedMutexGuard<ComputeHookNotifyRequest>,
        ),
    ),
    // Something requires sending, but you must wait for a current sender then call again
    AwaitLock(Arc<tokio::sync::Mutex<ComputeHookNotifyRequest>>),
    // Nothing requires sending
    Noop,
}

impl ComputeHookTenant {
    fn maybe_send(
        &self,
        tenant_id: TenantId,
        lock: Option<tokio::sync::OwnedMutexGuard<ComputeHookNotifyRequest>>,
    ) -> MaybeSendResult {
        let locked = match lock {
            Some(already_locked) => already_locked,
            None => {
                let Ok(locked) = self.get_lock().clone().try_lock_owned() else {
                    return MaybeSendResult::AwaitLock(self.get_lock().clone());
                };
                locked
            }
        };

        let request = match self {
            Self::Unsharded((node_id, _lock)) => Some(ComputeHookNotifyRequest {
                tenant_id,
                shards: vec![ComputeHookNotifyRequestShard {
                    shard_number: ShardNumber(0),
                    node_id: *node_id,
                }],
                stripe_size: None,
            }),
            Self::Sharded(sharded_tenant)
                if sharded_tenant.shards.len() == sharded_tenant.shard_count.count() as usize =>
            {
                Some(ComputeHookNotifyRequest {
                    tenant_id,
                    shards: sharded_tenant
                        .shards
                        .iter()
                        .map(|(shard_number, node_id)| ComputeHookNotifyRequestShard {
                            shard_number: *shard_number,
                            node_id: *node_id,
                        })
                        .collect(),
                    stripe_size: Some(sharded_tenant.stripe_size),
                })
            }
            Self::Sharded(sharded_tenant) => {
                // Sharded tenant doesn't yet have information for all its shards

                tracing::info!(
                    "ComputeHookTenant::maybe_send: not enough shards ({}/{})",
                    sharded_tenant.shards.len(),
                    sharded_tenant.shard_count.count()
                );
                None
            }
        };

        match request {
            None => {
                // Not yet ready to emit a notification
                tracing::info!("Tenant isn't yet ready to emit a notification");
                MaybeSendResult::Noop
            }
            Some(request) if request == *locked => {
                // No change from the last value successfully sent
                MaybeSendResult::Noop
            }
            Some(request) => MaybeSendResult::Transmit((request, locked)),
        }
    }
}

/// The compute hook is a destination for notifications about changes to tenant:pageserver
/// mapping.  It aggregates updates for the shards in a tenant, and when appropriate reconfigures
/// the compute connection string.
pub(super) struct ComputeHook {
    config: Config,
    state: std::sync::Mutex<HashMap<TenantId, ComputeHookTenant>>,
    authorization_header: Option<String>,

    // Concurrency limiter, so that we do not overload the cloud control plane when updating
    // large numbers of tenants (e.g. when failing over after a node failure)
    api_concurrency: tokio::sync::Semaphore,

    // This lock is only used in testing enviroments, to serialize calls into neon_lock
    neon_local_lock: tokio::sync::Mutex<()>,
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
            neon_local_lock: Default::default(),
            api_concurrency: tokio::sync::Semaphore::new(API_CONCURRENCY),
        }
    }

    /// For test environments: use neon_local's LocalEnv to update compute
    async fn do_notify_local(
        &self,
        reconfigure_request: &ComputeHookNotifyRequest,
    ) -> anyhow::Result<()> {
        // neon_local updates are not safe to call concurrently, use a lock to serialize
        // all calls to this function
        let _locked = self.neon_local_lock.lock().await;

        let env = match LocalEnv::load_config() {
            Ok(e) => e,
            Err(e) => {
                tracing::warn!("Couldn't load neon_local config, skipping compute update ({e})");
                return Ok(());
            }
        };
        let cplane =
            ComputeControlPlane::load(env.clone()).expect("Error loading compute control plane");
        let ComputeHookNotifyRequest {
            tenant_id,
            shards,
            stripe_size,
        } = reconfigure_request;

        let compute_pageservers = shards
            .iter()
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
            if endpoint.tenant_id == *tenant_id && endpoint.status() == EndpointStatus::Running {
                tracing::info!("Reconfiguring endpoint {}", endpoint_name,);
                endpoint
                    .reconfigure(compute_pageservers.clone(), *stripe_size)
                    .await?;
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

        tracing::info!(
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
                // We consider this fatal, because it's possible that the operation blocking the control one is
                // also the one that is waiting for this reconcile.  We should let the reconciler calling
                // this hook fail, to give control plane a chance to un-lock.
                tracing::info!("Control plane reports tenant is locked, dropping out of notify");
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
        reconfigure_request: &ComputeHookNotifyRequest,
        cancel: &CancellationToken,
    ) -> Result<(), NotifyError> {
        let client = reqwest::Client::new();

        // We hold these semaphore units across all retries, rather than only across each
        // HTTP request: this is to preserve fairness and avoid a situation where a retry might
        // time out waiting for a semaphore.
        let _units = self
            .api_concurrency
            .acquire()
            .await
            // Interpret closed semaphore as shutdown
            .map_err(|_| NotifyError::ShuttingDown)?;

        backoff::retry(
            || self.do_notify_iteration(&client, url, reconfigure_request, cancel),
            |e| {
                matches!(
                    e,
                    NotifyError::Fatal(_) | NotifyError::Unexpected(_) | NotifyError::Busy
                )
            },
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
    #[tracing::instrument(skip_all, fields(tenant_id=%tenant_shard_id.tenant_id, shard_id=%tenant_shard_id.shard_slug(), node_id))]
    pub(super) async fn notify(
        &self,
        tenant_shard_id: TenantShardId,
        node_id: NodeId,
        stripe_size: ShardStripeSize,
        cancel: &CancellationToken,
    ) -> Result<(), NotifyError> {
        let maybe_send_result = {
            let mut locked = self.state.lock().unwrap();

            use std::collections::hash_map::Entry;
            let tenant = match locked.entry(tenant_shard_id.tenant_id) {
                Entry::Vacant(e) => e.insert(ComputeHookTenant::new(
                    tenant_shard_id,
                    stripe_size,
                    node_id,
                )),
                Entry::Occupied(e) => {
                    let tenant = e.into_mut();
                    tenant.update(tenant_shard_id, stripe_size, node_id);
                    tenant
                }
            };
            tenant.maybe_send(tenant_shard_id.tenant_id, None)
        };

        // Process result: we may get an update to send, or we may have to wait for a lock
        // before trying again.
        let (request, mut guard) = match maybe_send_result {
            MaybeSendResult::Noop => {
                return Ok(());
            }
            MaybeSendResult::AwaitLock(lock) => {
                let send_locked = lock.lock_owned().await;

                let locked = self.state.lock().unwrap();
                let Some(tenant) = locked.get(&tenant_shard_id.tenant_id) else {
                    return Ok(());
                };
                match tenant.maybe_send(tenant_shard_id.tenant_id, Some(send_locked)) {
                    MaybeSendResult::AwaitLock(_) => {
                        unreachable!("We supplied lock guard")
                    }
                    MaybeSendResult::Noop => {
                        return Ok(());
                    }
                    MaybeSendResult::Transmit((request, lock)) => (request, lock),
                }
            }
            MaybeSendResult::Transmit((request, lock)) => (request, lock),
        };

        let result = if let Some(notify_url) = &self.config.compute_hook_url {
            self.do_notify(notify_url, &request, cancel).await
        } else {
            self.do_notify_local(&request).await.map_err(|e| {
                // This path is for testing only, so munge the error into our prod-style error type.
                tracing::error!("Local notification hook failed: {e}");
                NotifyError::Fatal(StatusCode::INTERNAL_SERVER_ERROR)
            })
        };

        if result.is_ok() {
            *guard = request;
        }
        result
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use pageserver_api::shard::{ShardCount, ShardNumber};
    use utils::id::TenantId;

    use super::*;

    #[test]
    fn tenant_updates() -> anyhow::Result<()> {
        let tenant_id = TenantId::generate();
        let mut tenant_state = ComputeHookTenant::new(
            TenantShardId {
                tenant_id,
                shard_count: ShardCount::new(0),
                shard_number: ShardNumber(0),
            },
            ShardStripeSize(12345),
            NodeId(1),
        );

        // An unsharded tenant is always ready to emit a notification, but won't
        // send the same one twice
        let send_result = tenant_state.maybe_send(tenant_id, None);
        let MaybeSendResult::Transmit((request, mut guard)) = send_result else {
            anyhow::bail!("Wrong send result");
        };
        assert_eq!(request.shards.len(), 1);
        assert!(request.stripe_size.is_none());

        // Simulate successful send
        *guard = request;
        drop(guard);

        // Try asking again: this should be a no-op
        let send_result = tenant_state.maybe_send(tenant_id, None);
        assert!(matches!(send_result, MaybeSendResult::Noop));

        // Writing the first shard of a multi-sharded situation (i.e. in a split)
        // resets the tenant state and puts it in an non-notifying state (need to
        // see all shards)
        tenant_state.update(
            TenantShardId {
                tenant_id,
                shard_count: ShardCount::new(2),
                shard_number: ShardNumber(1),
            },
            ShardStripeSize(32768),
            NodeId(1),
        );
        assert!(matches!(
            tenant_state.maybe_send(tenant_id, None),
            MaybeSendResult::Noop
        ));

        // Writing the second shard makes it ready to notify
        tenant_state.update(
            TenantShardId {
                tenant_id,
                shard_count: ShardCount::new(2),
                shard_number: ShardNumber(0),
            },
            ShardStripeSize(32768),
            NodeId(1),
        );

        let send_result = tenant_state.maybe_send(tenant_id, None);
        let MaybeSendResult::Transmit((request, mut guard)) = send_result else {
            anyhow::bail!("Wrong send result");
        };
        assert_eq!(request.shards.len(), 2);
        assert_eq!(request.stripe_size, Some(ShardStripeSize(32768)));

        // Simulate successful send
        *guard = request;
        drop(guard);

        Ok(())
    }
}
