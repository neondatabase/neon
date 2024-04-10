use std::{
    convert::Infallible,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use dashmap::DashSet;
use redis::{
    streams::{StreamReadOptions, StreamReadReply},
    AsyncCommands, FromRedisValue, Value,
};
use serde::Deserialize;
use tokio::sync::Mutex;

use crate::{
    config::EndpointCacheConfig,
    context::RequestMonitoring,
    intern::{BranchIdInt, EndpointIdInt, ProjectIdInt},
    metrics::REDIS_BROKEN_MESSAGES,
    rate_limiter::GlobalRateLimiter,
    redis::connection_with_credentials_provider::ConnectionWithCredentialsProvider,
    EndpointId,
};

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all(deserialize = "snake_case"))]
pub enum ControlPlaneEventKey {
    EndpointCreated { endpoint_id: String },
    BranchCreated { branch_id: String },
    ProjectCreated { project_id: String },
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
    pub fn new(config: EndpointCacheConfig) -> Self {
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
    pub async fn is_valid(&self, ctx: &mut RequestMonitoring, endpoint: &EndpointId) -> bool {
        if !self.ready.load(Ordering::Acquire) {
            return true;
        }
        // If cache is disabled, just collect the metrics and return.
        if self.config.disable_cache {
            ctx.set_rejected(self.should_reject(endpoint));
            return true;
        }
        // If the limiter allows, we don't need to check the cache.
        if self.limiter.lock().await.check() {
            return true;
        }
        let rejected = self.should_reject(endpoint);
        ctx.set_rejected(rejected);
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
        match key {
            ControlPlaneEventKey::EndpointCreated { endpoint_id } => {
                self.endpoints
                    .insert(EndpointIdInt::from(&endpoint_id.into()));
            }
            ControlPlaneEventKey::BranchCreated { branch_id } => {
                self.branches.insert(BranchIdInt::from(&branch_id.into()));
            }
            ControlPlaneEventKey::ProjectCreated { project_id } => {
                self.projects.insert(ProjectIdInt::from(&project_id.into()));
            }
        }
    }
    pub async fn do_read(
        &self,
        mut con: ConnectionWithCredentialsProvider,
    ) -> anyhow::Result<Infallible> {
        let mut last_id = "0-0".to_string();
        loop {
            self.ready.store(false, Ordering::Release);
            if let Err(e) = con.connect().await {
                tracing::error!("error connecting to redis: {:?}", e);
                continue;
            }
            if let Err(e) = self.read_from_stream(&mut con, &mut last_id).await {
                tracing::error!("error reading from redis: {:?}", e);
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
    fn parse_key_value(_: &str, value: &Value) -> anyhow::Result<ControlPlaneEventKey> {
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
            if res.keys.len() != 1 {
                anyhow::bail!("Cannot read from redis stream {}", self.config.stream_name);
            }

            let res = res.keys.pop().expect("Checked length above");

            if return_when_finish && res.ids.len() <= self.config.default_batch_size {
                break;
            }
            for x in res.ids {
                total += 1;
                for (k, v) in x.map {
                    let key = match Self::parse_key_value(&k, &v) {
                        Ok(x) => x,
                        Err(e) => {
                            REDIS_BROKEN_MESSAGES
                                .with_label_values(&[&self.config.stream_name])
                                .inc();
                            tracing::error!("error parsing key-value {k}: {e:?}");
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
        }
        tracing::info!("read {} endpoints/branches/projects from redis", total);
        Ok(())
    }
}
