use std::convert::Infallible;
use std::future::pending;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use dashmap::DashSet;
use redis::streams::{StreamReadOptions, StreamReadReply};
use redis::{AsyncCommands, FromRedisValue, Value};
use serde::Deserialize;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::config::EndpointCacheConfig;
use crate::context::RequestMonitoring;
use crate::intern::{BranchIdInt, EndpointIdInt, ProjectIdInt};
use crate::metrics::{Metrics, RedisErrors, RedisEventsCount};
use crate::rate_limiter::GlobalRateLimiter;
use crate::redis::connection_with_credentials_provider::ConnectionWithCredentialsProvider;
use crate::types::EndpointId;

#[allow(clippy::enum_variant_names)]
#[derive(Deserialize, Debug, Clone)]
#[serde(tag = "type", rename_all(deserialize = "snake_case"))]
enum ControlPlaneEvent {
    EndpointCreated { endpoint_created: EndpointCreated },
    BranchCreated { branch_created: BranchCreated },
    ProjectCreated { project_created: ProjectCreated },
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

impl TryFrom<&Value> for ControlPlaneEvent {
    type Error = anyhow::Error;
    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        let json = String::from_redis_value(value)?;
        Ok(serde_json::from_str(&json)?)
    }
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

    fn insert_event(&self, event: ControlPlaneEvent) {
        let counter = match event {
            ControlPlaneEvent::EndpointCreated { endpoint_created } => {
                self.endpoints
                    .insert(EndpointIdInt::from(&endpoint_created.endpoint_id.into()));
                RedisEventsCount::EndpointCreated
            }
            ControlPlaneEvent::BranchCreated { branch_created } => {
                self.branches
                    .insert(BranchIdInt::from(&branch_created.branch_id.into()));
                RedisEventsCount::BranchCreated
            }
            ControlPlaneEvent::ProjectCreated { project_created } => {
                self.projects
                    .insert(ProjectIdInt::from(&project_created.project_id.into()));
                RedisEventsCount::ProjectCreated
            }
        };
        Metrics::get().proxy.redis_events_count.inc(counter);
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
                // Maintenance tasks run forever. Sleep forever when canceled.
                pending::<()>().await;
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

            let key = res.keys.pop().expect("Checked length above");
            let len = key.ids.len();
            for stream_id in key.ids {
                total += 1;
                for value in stream_id.map.values() {
                    match value.try_into() {
                        Ok(event) => self.insert_event(event),
                        Err(err) => {
                            Metrics::get().proxy.redis_errors_total.inc(RedisErrors {
                                channel: &self.config.stream_name,
                            });
                            tracing::error!("error parsing value {value:?}: {err:?}");
                        }
                    };
                }
                if total.is_power_of_two() {
                    tracing::debug!("endpoints read {}", total);
                }
                *last_id = stream_id.id;
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
    use super::ControlPlaneEvent;

    #[test]
    fn test_parse_control_plane_event() {
        let s = r#"{"branch_created":null,"endpoint_created":{"endpoint_id":"ep-rapid-thunder-w0qqw2q9"},"project_created":null,"type":"endpoint_created"}"#;
        serde_json::from_str::<ControlPlaneEvent>(s).unwrap();
    }
}
