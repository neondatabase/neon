use std::{
    convert::Infallible,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use dashmap::DashSet;
use redis::{
    streams::{StreamReadOptions, StreamReadReply},
    AsyncCommands, FromRedisValue, Value,
};
use serde::Deserialize;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::{
    config::EndpointCacheConfig,
    context::RequestMonitoring,
    intern::{BranchIdInt, EndpointIdInt, ProjectIdInt},
    metrics::{Metrics, RedisErrors, RedisEventsCount},
    rate_limiter::GlobalRateLimiter,
    redis::connection_with_credentials_provider::ConnectionWithCredentialsProvider,
    EndpointId,
};

#[derive(Deserialize, Debug, Clone)]
pub(crate) struct ControlPlaneEventKey {
    endpoint_created: Option<EndpointCreated>,
    branch_created: Option<BranchCreated>,
    project_created: Option<ProjectCreated>,
}
#[derive(Deserialize, Debug, Clone)]
struct EndpointCreated {
    endpoint_id: String,
}
#[derive(Deserialize, Debug, Clone)]
struct BranchCreated {
    branch_id: String,
}
#[derive(Deserialize, Debug, Clone)]
struct ProjectCreated {
    project_id: String,
}

pub struct EndpointsCache {
    config: EndpointCacheConfig,
    endpoints: DashSet<EndpointIdInt>,
    branches: DashSet<BranchIdInt>,
    projects: DashSet<ProjectIdInt>,
    ready: AtomicBool,
    limiter: Arc<Mutex<GlobalRateLimiter>>,
}

impl EndpointsCache {
    pub(crate) fn new(config: EndpointCacheConfig) -> Self {
        Self {
            limiter: Arc::new(Mutex::new(GlobalRateLimiter::new(
                config.limiter_info.clone(),
            ))),
            config,
            endpoints: DashSet::new(),
            branches: DashSet::new(),
            projects: DashSet::new(),
            ready: AtomicBool::new(false),
        }
    }
    pub(crate) async fn is_valid(&self, ctx: &RequestMonitoring, endpoint: &EndpointId) -> bool {
        if !self.ready.load(Ordering::Acquire) {
            return true;
        }
        let rejected = self.should_reject(endpoint);
        ctx.set_rejected(rejected);
        info!(?rejected, "check endpoint is valid, disabled cache");
        // If cache is disabled, just collect the metrics and return or
        // If the limiter allows, we don't need to check the cache.
        if self.config.disable_cache || self.limiter.lock().await.check() {
            return true;
        }
        !rejected
    }
    fn should_reject(&self, endpoint: &EndpointId) -> bool {
        if endpoint.is_endpoint() {
            !self.endpoints.contains(&EndpointIdInt::from(endpoint))
        } else if endpoint.is_branch() {
            !self
                .branches
                .contains(&BranchIdInt::from(&endpoint.as_branch()))
        } else {
            !self
                .projects
                .contains(&ProjectIdInt::from(&endpoint.as_project()))
        }
    }
    fn insert_event(&self, key: ControlPlaneEventKey) {
        // Do not do normalization here, we expect the events to be normalized.
        if let Some(endpoint_created) = key.endpoint_created {
            self.endpoints
                .insert(EndpointIdInt::from(&endpoint_created.endpoint_id.into()));
            Metrics::get()
                .proxy
                .redis_events_count
                .inc(RedisEventsCount::EndpointCreated);
        }
        if let Some(branch_created) = key.branch_created {
            self.branches
                .insert(BranchIdInt::from(&branch_created.branch_id.into()));
            Metrics::get()
                .proxy
                .redis_events_count
                .inc(RedisEventsCount::BranchCreated);
        }
        if let Some(project_created) = key.project_created {
            self.projects
                .insert(ProjectIdInt::from(&project_created.project_id.into()));
            Metrics::get()
                .proxy
                .redis_events_count
                .inc(RedisEventsCount::ProjectCreated);
        }
    }
    pub async fn do_read(
        &self,
        mut con: ConnectionWithCredentialsProvider,
        cancellation_token: CancellationToken,
    ) -> anyhow::Result<Infallible> {
        let mut last_id = "0-0".to_string();
        loop {
            if let Err(e) = con.connect().await {
                tracing::error!("error connecting to redis: {:?}", e);
                self.ready.store(false, Ordering::Release);
            }
            if let Err(e) = self.read_from_stream(&mut con, &mut last_id).await {
                tracing::error!("error reading from redis: {:?}", e);
                self.ready.store(false, Ordering::Release);
            }
            if cancellation_token.is_cancelled() {
                info!("cancellation token is cancelled, exiting");
                tokio::time::sleep(Duration::from_secs(60 * 60 * 24 * 7)).await;
                // 1 week.
            }
            tokio::time::sleep(self.config.retry_interval).await;
        }
    }
    async fn read_from_stream(
        &self,
        con: &mut ConnectionWithCredentialsProvider,
        last_id: &mut String,
    ) -> anyhow::Result<()> {
        tracing::info!("reading endpoints/branches/projects from redis");
        self.batch_read(
            con,
            StreamReadOptions::default().count(self.config.initial_batch_size),
            last_id,
            true,
        )
        .await?;
        tracing::info!("ready to filter user requests");
        self.ready.store(true, Ordering::Release);
        self.batch_read(
            con,
            StreamReadOptions::default()
                .count(self.config.default_batch_size)
                .block(self.config.xread_timeout.as_millis() as usize),
            last_id,
            false,
        )
        .await
    }
    fn parse_key_value(value: &Value) -> anyhow::Result<ControlPlaneEventKey> {
        let s: String = FromRedisValue::from_redis_value(value)?;
        Ok(serde_json::from_str(&s)?)
    }
    async fn batch_read(
        &self,
        conn: &mut ConnectionWithCredentialsProvider,
        opts: StreamReadOptions,
        last_id: &mut String,
        return_when_finish: bool,
    ) -> anyhow::Result<()> {
        let mut total: usize = 0;
        loop {
            let mut res: StreamReadReply = conn
                .xread_options(&[&self.config.stream_name], &[last_id.as_str()], &opts)
                .await?;

            if res.keys.is_empty() {
                if return_when_finish {
                    if total != 0 {
                        break;
                    }
                    anyhow::bail!(
                        "Redis stream {} is empty, cannot be used to filter endpoints",
                        self.config.stream_name
                    );
                }
                // If we are not returning when finish, we should wait for more data.
                continue;
            }
            if res.keys.len() != 1 {
                anyhow::bail!("Cannot read from redis stream {}", self.config.stream_name);
            }

            let res = res.keys.pop().expect("Checked length above");
            let len = res.ids.len();
            for x in res.ids {
                total += 1;
                for (_, v) in x.map {
                    let key = match Self::parse_key_value(&v) {
                        Ok(x) => x,
                        Err(e) => {
                            Metrics::get().proxy.redis_errors_total.inc(RedisErrors {
                                channel: &self.config.stream_name,
                            });
                            tracing::error!("error parsing value {v:?}: {e:?}");
                            continue;
                        }
                    };
                    self.insert_event(key);
                }
                if total.is_power_of_two() {
                    tracing::debug!("endpoints read {}", total);
                }
                *last_id = x.id;
            }
            if return_when_finish && len <= self.config.default_batch_size {
                break;
            }
        }
        tracing::info!("read {} endpoints/branches/projects from redis", total);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::ControlPlaneEventKey;

    #[test]
    fn test() {
        let s = "{\"branch_created\":null,\"endpoint_created\":{\"endpoint_id\":\"ep-rapid-thunder-w0qqw2q9\"},\"project_created\":null,\"type\":\"endpoint_created\"}";
        let _: ControlPlaneEventKey = serde_json::from_str(s).unwrap();
    }
}
