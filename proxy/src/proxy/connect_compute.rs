use crate::{
    auth,
    compute::{self, PostgresConnection},
    console::{self, errors::WakeComputeError},
    context::RequestMonitoring,
    metrics::NUM_CONNECTION_FAILURES,
    proxy::{
        retry::{retry_after, ShouldRetry},
        wake_compute::wake_compute,
    },
};
use async_trait::async_trait;
use pq_proto::StartupMessageParams;
use tokio::time;
use tracing::{error, info, warn};

const CONNECT_TIMEOUT: time::Duration = time::Duration::from_secs(2);

/// If we couldn't connect, a cached connection info might be to blame
/// (e.g. the compute node's address might've changed at the wrong time).
/// Invalidate the cache entry (if any) to prevent subsequent errors.
#[tracing::instrument(name = "invalidate_cache", skip_all)]
pub fn invalidate_cache(node_info: console::CachedNodeInfo) -> compute::ConnCfg {
    let is_cached = node_info.cached();
    if is_cached {
        warn!("invalidating stalled compute node info cache entry");
    }
    let label = match is_cached {
        true => "compute_cached",
        false => "compute_uncached",
    };
    NUM_CONNECTION_FAILURES.with_label_values(&[label]).inc();

    node_info.invalidate().config
}

/// Try to connect to the compute node once.
#[tracing::instrument(name = "connect_once", fields(pid = tracing::field::Empty), skip_all)]
async fn connect_to_compute_once(
    ctx: &mut RequestMonitoring,
    node_info: &console::CachedNodeInfo,
    timeout: time::Duration,
) -> Result<PostgresConnection, compute::ConnectionError> {
    let allow_self_signed_compute = node_info.allow_self_signed_compute;

    node_info
        .config
        .connect(ctx, allow_self_signed_compute, timeout)
        .await
}

#[async_trait]
pub trait ConnectMechanism {
    type Connection;
    type ConnectError;
    type Error: From<Self::ConnectError>;
    async fn connect_once(
        &self,
        ctx: &mut RequestMonitoring,
        node_info: &console::CachedNodeInfo,
        timeout: time::Duration,
    ) -> Result<Self::Connection, Self::ConnectError>;

    fn update_connect_config(&self, conf: &mut compute::ConnCfg);
}

pub struct TcpMechanism<'a> {
    /// KV-dictionary with PostgreSQL connection params.
    pub params: &'a StartupMessageParams,
}

#[async_trait]
impl ConnectMechanism for TcpMechanism<'_> {
    type Connection = PostgresConnection;
    type ConnectError = compute::ConnectionError;
    type Error = compute::ConnectionError;

    async fn connect_once(
        &self,
        ctx: &mut RequestMonitoring,
        node_info: &console::CachedNodeInfo,
        timeout: time::Duration,
    ) -> Result<PostgresConnection, Self::Error> {
        connect_to_compute_once(ctx, node_info, timeout).await
    }

    fn update_connect_config(&self, config: &mut compute::ConnCfg) {
        config.set_startup_params(self.params);
    }
}

/// Try to connect to the compute node, retrying if necessary.
/// This function might update `node_info`, so we take it by `&mut`.
#[tracing::instrument(skip_all)]
pub async fn connect_to_compute<M: ConnectMechanism>(
    ctx: &mut RequestMonitoring,
    mechanism: &M,
    mut node_info: console::CachedNodeInfo,
    user_info: &auth::BackendType<'_, auth::backend::ComputeUserInfo>,
) -> Result<M::Connection, M::Error>
where
    M::ConnectError: ShouldRetry + std::fmt::Debug,
    M::Error: From<WakeComputeError>,
{
    mechanism.update_connect_config(&mut node_info.config);

    // try once
    let (config, err) = match mechanism
        .connect_once(ctx, &node_info, CONNECT_TIMEOUT)
        .await
    {
        Ok(res) => {
            ctx.latency_timer.success();
            return Ok(res);
        }
        Err(e) => {
            error!(error = ?e, "could not connect to compute node");
            (invalidate_cache(node_info), e)
        }
    };

    ctx.latency_timer.cache_miss();

    let mut num_retries = 1;

    // if we failed to connect, it's likely that the compute node was suspended, wake a new compute node
    info!("compute node's state has likely changed; requesting a wake-up");
    let mut node_info = match user_info {
        auth::BackendType::Console(api, info) => {
            wake_compute(&mut num_retries, ctx, api, info).await?
        }
        // nothing to do?
        auth::BackendType::Link(_) => return Err(err.into()),
    };

    node_info.config.reuse_password(&config);
    mechanism.update_connect_config(&mut node_info.config);

    // now that we have a new node, try connect to it repeatedly.
    // this can error for a few reasons, for instance:
    // * DNS connection settings haven't quite propagated yet
    info!("wake_compute success. attempting to connect");
    loop {
        match mechanism
            .connect_once(ctx, &node_info, CONNECT_TIMEOUT)
            .await
        {
            Ok(res) => {
                ctx.latency_timer.success();
                return Ok(res);
            }
            Err(e) => {
                let retriable = e.should_retry(num_retries);
                if !retriable {
                    error!(error = ?e, num_retries, retriable, "couldn't connect to compute node");
                    return Err(e.into());
                }
                warn!(error = ?e, num_retries, retriable, "couldn't connect to compute node");
            }
        }

        let wait_duration = retry_after(num_retries);
        num_retries += 1;

        time::sleep(wait_duration).await;
    }
}
