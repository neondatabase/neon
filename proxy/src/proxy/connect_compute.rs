use crate::{
    auth::backend::ComputeCredentialKeys,
    compute::{self, PostgresConnection},
    console::{self, errors::WakeComputeError, CachedNodeInfo, NodeInfo},
    context::RequestMonitoring,
    error::ReportableError,
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
pub fn invalidate_cache(node_info: console::CachedNodeInfo) -> NodeInfo {
    let is_cached = node_info.cached();
    if is_cached {
        warn!("invalidating stalled compute node info cache entry");
    }
    let label = match is_cached {
        true => "compute_cached",
        false => "compute_uncached",
    };
    NUM_CONNECTION_FAILURES.with_label_values(&[label]).inc();

    node_info.invalidate()
}

#[async_trait]
pub trait ConnectMechanism {
    type Connection;
    type ConnectError: ReportableError;
    type Error: From<Self::ConnectError>;
    async fn connect_once(
        &self,
        ctx: &mut RequestMonitoring,
        node_info: &console::CachedNodeInfo,
        timeout: time::Duration,
    ) -> Result<Self::Connection, Self::ConnectError>;

    fn update_connect_config(&self, conf: &mut compute::ConnCfg);
}

#[async_trait]
pub trait ComputeConnectBackend {
    async fn wake_compute(
        &self,
        ctx: &mut RequestMonitoring,
    ) -> Result<CachedNodeInfo, console::errors::WakeComputeError>;

    fn get_keys(&self) -> Option<&ComputeCredentialKeys>;
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

    #[tracing::instrument(fields(pid = tracing::field::Empty), skip_all)]
    async fn connect_once(
        &self,
        ctx: &mut RequestMonitoring,
        node_info: &console::CachedNodeInfo,
        timeout: time::Duration,
    ) -> Result<PostgresConnection, Self::Error> {
        node_info.connect(ctx, timeout).await
    }

    fn update_connect_config(&self, config: &mut compute::ConnCfg) {
        config.set_startup_params(self.params);
    }
}

/// Try to connect to the compute node, retrying if necessary.
/// This function might update `node_info`, so we take it by `&mut`.
#[tracing::instrument(skip_all)]
pub async fn connect_to_compute<M: ConnectMechanism, B: ComputeConnectBackend>(
    ctx: &mut RequestMonitoring,
    mechanism: &M,
    user_info: &B,
    allow_self_signed_compute: bool,
) -> Result<M::Connection, M::Error>
where
    M::ConnectError: ShouldRetry + std::fmt::Debug,
    M::Error: From<WakeComputeError>,
{
    let mut num_retries = 0;
    let mut node_info = wake_compute(&mut num_retries, ctx, user_info).await?;
    if let Some(keys) = user_info.get_keys() {
        node_info.set_keys(keys);
    }
    node_info.allow_self_signed_compute = allow_self_signed_compute;
    // let mut node_info = credentials.get_node_info(ctx, user_info).await?;
    mechanism.update_connect_config(&mut node_info.config);

    // try once
    let err = match mechanism
        .connect_once(ctx, &node_info, CONNECT_TIMEOUT)
        .await
    {
        Ok(res) => {
            ctx.latency_timer.success();
            return Ok(res);
        }
        Err(e) => e,
    };

    error!(error = ?err, "could not connect to compute node");

    let node_info = if !node_info.cached() {
        // If the error is Postgres, that means that we managed to connect to the compute node, but there was an error.
        // Do not need to retrieve a new node_info, just return the old one.
        if !err.should_retry(num_retries) {
            return Err(err.into());
        }
        node_info
    } else {
        // if we failed to connect, it's likely that the compute node was suspended, wake a new compute node
        info!("compute node's state has likely changed; requesting a wake-up");
        ctx.latency_timer.cache_miss();
        let old_node_info = invalidate_cache(node_info);
        let mut node_info = wake_compute(&mut num_retries, ctx, user_info).await?;
        node_info.reuse_settings(old_node_info);

        mechanism.update_connect_config(&mut node_info.config);
        node_info
    };

    // now that we have a new node, try connect to it repeatedly.
    // this can error for a few reasons, for instance:
    // * DNS connection settings haven't quite propagated yet
    info!("wake_compute success. attempting to connect");
    num_retries = 1;
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
