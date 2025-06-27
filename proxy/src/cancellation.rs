use std::convert::Infallible;
use std::net::{IpAddr, SocketAddr};
use std::pin::pin;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use anyhow::anyhow;
use futures::FutureExt;
use ipnet::{IpNet, Ipv4Net, Ipv6Net};
use postgres_client::RawCancelToken;
use postgres_client::tls::MakeTlsConnect;
use redis::{Cmd, FromRedisValue, Value};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::net::TcpStream;
use tokio::time::timeout;
use tracing::{debug, error, info};

use crate::auth::AuthError;
use crate::auth::backend::ComputeUserInfo;
use crate::batch::{BatchQueue, QueueProcessing};
use crate::config::ComputeConfig;
use crate::context::RequestContext;
use crate::control_plane::ControlPlaneApi;
use crate::error::ReportableError;
use crate::ext::LockExt;
use crate::metrics::{CancelChannelSizeGuard, CancellationRequest, Metrics, RedisMsgKind};
use crate::pqproto::CancelKeyData;
use crate::rate_limiter::LeakyBucketRateLimiter;
use crate::redis::keys::KeyPrefix;
use crate::redis::kv_ops::RedisKVClient;

type IpSubnetKey = IpNet;

const CANCEL_KEY_TTL: std::time::Duration = std::time::Duration::from_secs(600);
const CANCEL_KEY_REFRESH: std::time::Duration = std::time::Duration::from_secs(570);

// Message types for sending through mpsc channel
pub enum CancelKeyOp {
    StoreCancelKey {
        key: CancelKeyData,
        value: Box<str>,
        expire: std::time::Duration,
    },
    GetCancelData {
        key: CancelKeyData,
    },
}

pub struct Pipeline {
    inner: redis::Pipeline,
    replies: usize,
}

impl Pipeline {
    fn with_capacity(n: usize) -> Self {
        Self {
            inner: redis::Pipeline::with_capacity(n),
            replies: 0,
        }
    }

    async fn execute(self, client: &mut RedisKVClient) -> Vec<anyhow::Result<Value>> {
        let responses = self.replies;
        let batch_size = self.inner.len();

        match client.query(&self.inner).await {
            // for each reply, we expect that many values.
            Ok(Value::Array(values)) if values.len() == responses => {
                debug!(
                    batch_size,
                    responses, "successfully completed cancellation jobs",
                );
                values.into_iter().map(Ok).collect()
            }
            Ok(value) => {
                error!(batch_size, ?value, "unexpected redis return value");
                std::iter::repeat_with(|| Err(anyhow!("incorrect response type from redis")))
                    .take(responses)
                    .collect()
            }
            Err(err) => {
                std::iter::repeat_with(|| Err(anyhow!("could not send cmd to redis: {err}")))
                    .take(responses)
                    .collect()
            }
        }
    }

    fn add_command_with_reply(&mut self, cmd: Cmd) {
        self.inner.add_command(cmd);
        self.replies += 1;
    }

    fn add_command_no_reply(&mut self, cmd: Cmd) {
        self.inner.add_command(cmd).ignore();
    }
}

impl CancelKeyOp {
    fn register(&self, pipe: &mut Pipeline) {
        match self {
            CancelKeyOp::StoreCancelKey { key, value, expire } => {
                let key = KeyPrefix::Cancel(*key).build_redis_key();
                pipe.add_command_with_reply(Cmd::hset(&key, "data", &**value));
                pipe.add_command_no_reply(Cmd::expire(&key, expire.as_secs() as i64));
            }
            CancelKeyOp::GetCancelData { key } => {
                let key = KeyPrefix::Cancel(*key).build_redis_key();
                pipe.add_command_with_reply(Cmd::hget(key, "data"));
            }
        }
    }
}

pub struct CancellationProcessor {
    pub client: RedisKVClient,
    pub batch_size: usize,
}

impl QueueProcessing for CancellationProcessor {
    type Req = (CancelChannelSizeGuard<'static>, CancelKeyOp);
    type Res = anyhow::Result<redis::Value>;

    fn batch_size(&self, _queue_size: usize) -> usize {
        self.batch_size
    }

    async fn apply(&mut self, batch: Vec<Self::Req>) -> Vec<Self::Res> {
        let mut pipeline = Pipeline::with_capacity(batch.len());

        let batch_size = batch.len();
        debug!(batch_size, "running cancellation jobs");

        for (_, op) in &batch {
            op.register(&mut pipeline);
        }

        pipeline.execute(&mut self.client).await
    }
}

/// Enables serving `CancelRequest`s.
///
/// If `CancellationPublisher` is available, cancel request will be used to publish the cancellation key to other proxy instances.
pub struct CancellationHandler {
    compute_config: &'static ComputeConfig,
    // rate limiter of cancellation requests
    limiter: Arc<std::sync::Mutex<LeakyBucketRateLimiter<IpSubnetKey>>>,
    tx: OnceLock<BatchQueue<CancellationProcessor>>, // send messages to the redis KV client task
}

#[derive(Debug, Error)]
pub(crate) enum CancelError {
    #[error("{0}")]
    IO(#[from] std::io::Error),

    #[error("{0}")]
    Postgres(#[from] postgres_client::Error),

    #[error("rate limit exceeded")]
    RateLimit,

    #[error("Authentication error")]
    AuthError(#[from] AuthError),

    #[error("key not found")]
    NotFound,

    #[error("proxy service error")]
    InternalError,
}

impl ReportableError for CancelError {
    fn get_error_kind(&self) -> crate::error::ErrorKind {
        match self {
            CancelError::IO(_) => crate::error::ErrorKind::Compute,
            CancelError::Postgres(e) if e.as_db_error().is_some() => {
                crate::error::ErrorKind::Postgres
            }
            CancelError::Postgres(_) => crate::error::ErrorKind::Compute,
            CancelError::RateLimit => crate::error::ErrorKind::RateLimit,
            CancelError::NotFound | CancelError::AuthError(_) => crate::error::ErrorKind::User,
            CancelError::InternalError => crate::error::ErrorKind::Service,
        }
    }
}

impl CancellationHandler {
    pub fn new(compute_config: &'static ComputeConfig) -> Self {
        Self {
            compute_config,
            tx: OnceLock::new(),
            limiter: Arc::new(std::sync::Mutex::new(
                LeakyBucketRateLimiter::<IpSubnetKey>::new_with_shards(
                    LeakyBucketRateLimiter::<IpSubnetKey>::DEFAULT,
                    64,
                ),
            )),
        }
    }

    pub fn init_tx(&self, queue: BatchQueue<CancellationProcessor>) {
        self.tx
            .set(queue)
            .map_err(|_| {})
            .expect("cancellation queue should be registered once");
    }

    pub(crate) fn get_key(self: Arc<Self>) -> Session {
        // we intentionally generate a random "backend pid" and "secret key" here.
        // we use the corresponding u64 as an identifier for the
        // actual endpoint+pid+secret for postgres/pgbouncer.
        //
        // if we forwarded the backend_pid from postgres to the client, there would be a lot
        // of overlap between our computes as most pids are small (~100).

        let key: CancelKeyData = rand::random();

        debug!("registered new query cancellation key {key}");
        Session {
            key,
            cancellation_handler: self,
        }
    }

    /// This is not cancel safe
    async fn get_cancel_key(
        &self,
        key: CancelKeyData,
    ) -> Result<Option<CancelClosure>, CancelError> {
        let guard = Metrics::get()
            .proxy
            .cancel_channel_size
            .guard(RedisMsgKind::HGet);
        let op = CancelKeyOp::GetCancelData { key };

        let Some(tx) = self.tx.get() else {
            tracing::warn!("cancellation handler is not available");
            return Err(CancelError::InternalError);
        };

        const TIMEOUT: Duration = Duration::from_secs(5);
        let result = timeout(
            TIMEOUT,
            tx.call((guard, op), std::future::pending::<Infallible>()),
        )
        .await
        .map_err(|_| {
            tracing::warn!("timed out waiting to receive GetCancelData response");
            CancelError::RateLimit
        })?
        // cannot be cancelled
        .unwrap_or_else(|x| match x {})
        .map_err(|e| {
            tracing::warn!("failed to receive GetCancelData response: {e}");
            CancelError::InternalError
        })?;

        let cancel_state_str = String::from_owned_redis_value(result).map_err(|e| {
            tracing::warn!("failed to receive GetCancelData response: {e}");
            CancelError::InternalError
        })?;

        let cancel_closure: CancelClosure =
            serde_json::from_str(&cancel_state_str).map_err(|e| {
                tracing::warn!("failed to deserialize cancel state: {e}");
                CancelError::InternalError
            })?;

        Ok(Some(cancel_closure))
    }

    /// Try to cancel a running query for the corresponding connection.
    /// If the cancellation key is not found, it will be published to Redis.
    /// check_allowed - if true, check if the IP is allowed to cancel the query.
    /// Will fetch IP allowlist internally.
    ///
    /// return Result primarily for tests
    ///
    /// This is not cancel safe
    pub(crate) async fn cancel_session<T: ControlPlaneApi>(
        &self,
        key: CancelKeyData,
        ctx: RequestContext,
        check_ip_allowed: bool,
        check_vpc_allowed: bool,
        auth_backend: &T,
    ) -> Result<(), CancelError> {
        let subnet_key = match ctx.peer_addr() {
            IpAddr::V4(ip) => IpNet::V4(Ipv4Net::new_assert(ip, 24).trunc()), // use defaut mask here
            IpAddr::V6(ip) => IpNet::V6(Ipv6Net::new_assert(ip, 64).trunc()),
        };

        let allowed = {
            let rate_limit_config = None;
            let limiter = self.limiter.lock_propagate_poison();
            limiter.check(subnet_key, rate_limit_config, 1)
        };
        if !allowed {
            // log only the subnet part of the IP address to know which subnet is rate limited
            tracing::warn!("Rate limit exceeded. Skipping cancellation message, {subnet_key}");
            Metrics::get()
                .proxy
                .cancellation_requests_total
                .inc(CancellationRequest {
                    kind: crate::metrics::CancellationOutcome::RateLimitExceeded,
                });
            return Err(CancelError::RateLimit);
        }

        let cancel_state = self.get_cancel_key(key).await.map_err(|e| {
            tracing::warn!("failed to receive RedisOp response: {e}");
            CancelError::InternalError
        })?;

        let Some(cancel_closure) = cancel_state else {
            tracing::warn!("query cancellation key not found: {key}");
            Metrics::get()
                .proxy
                .cancellation_requests_total
                .inc(CancellationRequest {
                    kind: crate::metrics::CancellationOutcome::NotFound,
                });
            return Err(CancelError::NotFound);
        };

        let info = &cancel_closure.user_info;
        let access_controls = auth_backend
            .get_endpoint_access_control(&ctx, &info.endpoint, &info.user)
            .await
            .map_err(|e| CancelError::AuthError(e.into()))?;

        access_controls.check(&ctx, check_ip_allowed, check_vpc_allowed)?;

        Metrics::get()
            .proxy
            .cancellation_requests_total
            .inc(CancellationRequest {
                kind: crate::metrics::CancellationOutcome::Found,
            });
        info!("cancelling query per user's request using key {key}");
        cancel_closure.try_cancel_query(self.compute_config).await
    }
}

/// This should've been a [`std::future::Future`], but
/// it's impossible to name a type of an unboxed future
/// (we'd need something like `#![feature(type_alias_impl_trait)]`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CancelClosure {
    socket_addr: SocketAddr,
    cancel_token: RawCancelToken,
    hostname: String, // for pg_sni router
    user_info: ComputeUserInfo,
}

impl CancelClosure {
    pub(crate) fn new(
        socket_addr: SocketAddr,
        cancel_token: RawCancelToken,
        hostname: String,
        user_info: ComputeUserInfo,
    ) -> Self {
        Self {
            socket_addr,
            cancel_token,
            hostname,
            user_info,
        }
    }
    /// Cancels the query running on user's compute node.
    pub(crate) async fn try_cancel_query(
        &self,
        compute_config: &ComputeConfig,
    ) -> Result<(), CancelError> {
        let socket = TcpStream::connect(self.socket_addr).await?;

        let tls = <_ as MakeTlsConnect<tokio::net::TcpStream>>::make_tls_connect(
            compute_config,
            &self.hostname,
        )
        .map_err(|e| CancelError::IO(std::io::Error::other(e.to_string())))?;

        self.cancel_token.cancel_query_raw(socket, tls).await?;
        debug!("query was cancelled");
        Ok(())
    }
}

/// Helper for registering query cancellation tokens.
pub(crate) struct Session {
    /// The user-facing key identifying this session.
    key: CancelKeyData,
    cancellation_handler: Arc<CancellationHandler>,
}

impl Session {
    pub(crate) fn key(&self) -> &CancelKeyData {
        &self.key
    }

    /// Ensure the cancel key is continously refreshed,
    /// but stop when the channel is dropped.
    ///
    /// This is not cancel safe
    pub(crate) async fn maintain_cancel_key(
        &self,
        session_id: uuid::Uuid,
        cancel: tokio::sync::oneshot::Receiver<Infallible>,
        cancel_closure: &CancelClosure,
        compute_config: &ComputeConfig,
    ) {
        let Some(tx) = self.cancellation_handler.tx.get() else {
            tracing::warn!("cancellation handler is not available");
            // don't exit, as we only want to exit if cancelled externally.
            std::future::pending().await
        };

        let closure_json = serde_json::to_string(&cancel_closure)
            .expect("serialising to json string should not fail")
            .into_boxed_str();

        let mut cancel = pin!(cancel);

        loop {
            let guard = Metrics::get()
                .proxy
                .cancel_channel_size
                .guard(RedisMsgKind::HSet);
            let op = CancelKeyOp::StoreCancelKey {
                key: self.key,
                value: closure_json.clone(),
                expire: CANCEL_KEY_TTL,
            };

            tracing::debug!(
                src=%self.key,
                dest=?cancel_closure.cancel_token,
                "registering cancellation key"
            );

            match tx.call((guard, op), cancel.as_mut()).await {
                Ok(Ok(_)) => {
                    tracing::debug!(
                        src=%self.key,
                        dest=?cancel_closure.cancel_token,
                        "registered cancellation key"
                    );

                    // wait before continuing.
                    tokio::time::sleep(CANCEL_KEY_REFRESH).await;
                }
                // retry immediately.
                Ok(Err(error)) => {
                    tracing::warn!(?error, "error registering cancellation key");
                }
                Err(Err(_cancelled)) => break,
            }
        }

        if let Err(err) = cancel_closure
            .try_cancel_query(compute_config)
            .boxed()
            .await
        {
            tracing::warn!(
                ?session_id,
                ?err,
                "could not cancel the query in the database"
            );
        }
    }
}
