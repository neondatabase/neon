use std::convert::Infallible;
use std::net::{IpAddr, SocketAddr};
use std::pin::pin;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use futures::FutureExt;
use ipnet::{IpNet, Ipv4Net, Ipv6Net};
use metrics::MeasuredCounterPairGuard;
use postgres_client::RawCancelToken;
use postgres_client::tls::MakeTlsConnect;
use redis::{Cmd, FromRedisValue, SetExpiry, SetOptions, Value};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::net::TcpStream;
use tokio::time::timeout;
use tracing::{debug, error, info};

use crate::auth::AuthError;
use crate::auth::backend::ComputeUserInfo;
use crate::batch::{BatchQueue, BatchQueueError, QueueProcessing};
use crate::config::ComputeConfig;
use crate::context::RequestContext;
use crate::control_plane::ControlPlaneApi;
use crate::error::ReportableError;
use crate::ext::LockExt;
use crate::metrics::{
    CancelChannelSizeGauge, CancelChannelSizeGuard, CancellationRequest, Metrics, RedisMsgKind,
};
use crate::pqproto::CancelKeyData;
use crate::rate_limiter::LeakyBucketRateLimiter;
use crate::redis::keys::KeyPrefix;
use crate::redis::kv_ops::{RedisKVClient, RedisKVClientError};
use crate::util::run_until;

type IpSubnetKey = IpNet;

const CANCEL_KEY_TTL: Duration = Duration::from_secs(600);
const CANCEL_KEY_REFRESH: Duration = Duration::from_secs(570);

// Message types for sending through mpsc channel
pub enum CancelKeyOp {
    Store {
        key: CancelKeyData,
        value: Box<str>,
        expire: Duration,
    },
    Refresh {
        key: CancelKeyData,
        expire: Duration,
    },
    Get {
        key: CancelKeyData,
    },
    GetOld {
        key: CancelKeyData,
    },
}

impl CancelKeyOp {
    fn redis_msg_kind(&self) -> RedisMsgKind {
        match self {
            CancelKeyOp::Store { .. } => RedisMsgKind::Set,
            CancelKeyOp::Refresh { .. } => RedisMsgKind::Expire,
            CancelKeyOp::Get { .. } => RedisMsgKind::Get,
            CancelKeyOp::GetOld { .. } => RedisMsgKind::HGet,
        }
    }

    fn metric_guard(&self) -> MeasuredCounterPairGuard<'static, CancelChannelSizeGauge> {
        Metrics::get()
            .proxy
            .cancel_channel_size
            .guard(self.redis_msg_kind())
    }
}

#[derive(thiserror::Error, Debug, Clone)]
pub enum PipelineError {
    #[error("could not send cmd to redis: {0}")]
    RedisKVClient(Arc<RedisKVClientError>),
    #[error("incorrect number of responses from redis")]
    IncorrectNumberOfResponses,
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

    async fn execute(self, client: &mut RedisKVClient) -> Result<Vec<Value>, PipelineError> {
        let responses = self.replies;
        let batch_size = self.inner.len();

        if !client.credentials_refreshed() {
            tracing::debug!(
                "Redis credentials are not refreshed. Sleeping for 5 seconds before retrying..."
            );
            tokio::time::sleep(Duration::from_secs(5)).await;
        }

        match client.query(&self.inner).await {
            // for each reply, we expect that many values.
            Ok(Value::Array(values)) if values.len() == responses => {
                debug!(
                    batch_size,
                    responses, "successfully completed cancellation jobs",
                );
                Ok(values.into_iter().collect())
            }
            Ok(value) => {
                error!(batch_size, ?value, "unexpected redis return value");
                Err(PipelineError::IncorrectNumberOfResponses)
            }
            Err(err) => Err(PipelineError::RedisKVClient(Arc::new(err))),
        }
    }

    fn add_command(&mut self, cmd: Cmd) {
        self.inner.add_command(cmd);
        self.replies += 1;
    }
}

impl CancelKeyOp {
    fn register(&self, pipe: &mut Pipeline) {
        match self {
            CancelKeyOp::Store { key, value, expire } => {
                let key = KeyPrefix::Cancel(*key).build_redis_key();
                pipe.add_command(Cmd::set_options(
                    &key,
                    &**value,
                    SetOptions::default().with_expiration(SetExpiry::EX(expire.as_secs())),
                ));
            }
            CancelKeyOp::Refresh { key, expire } => {
                let key = KeyPrefix::Cancel(*key).build_redis_key();
                pipe.add_command(Cmd::expire(&key, expire.as_secs() as i64));
            }
            CancelKeyOp::GetOld { key } => {
                let key = KeyPrefix::Cancel(*key).build_redis_key();
                pipe.add_command(Cmd::hget(key, "data"));
            }
            CancelKeyOp::Get { key } => {
                let key = KeyPrefix::Cancel(*key).build_redis_key();
                pipe.add_command(Cmd::get(key));
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
    type Res = redis::Value;
    type Err = PipelineError;

    fn batch_size(&self, _queue_size: usize) -> usize {
        self.batch_size
    }

    async fn apply(&mut self, batch: Vec<Self::Req>) -> Result<Vec<Self::Res>, Self::Err> {
        if !self.client.credentials_refreshed() {
            // this will cause a timeout for cancellation operations
            tracing::debug!(
                "Redis credentials are not refreshed. Sleeping for 5 seconds before retrying..."
            );
            tokio::time::sleep(Duration::from_secs(5)).await;
        }

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
        const TIMEOUT: Duration = Duration::from_secs(5);

        let Some(tx) = self.tx.get() else {
            tracing::warn!("cancellation handler is not available");
            return Err(CancelError::InternalError);
        };

        let op = CancelKeyOp::Get { key };
        let (id, rx) = tx.enqueue((op.metric_guard(), op));
        let result = timeout(
            TIMEOUT,
            tx.call(id, rx, std::future::pending::<Infallible>()),
        )
        .await
        .map_err(|_| {
            tracing::warn!("timed out waiting to receive GetCancelData response");
            CancelError::RateLimit
        })?;

        // We may still have cancel keys set with HSET <key> "data".
        // Check error type and retry with HGET.
        // TODO: remove code after HSET is not used anymore.
        let result = if let Err(err) = result.as_ref()
            && let BatchQueueError::Result(err) = err
            && let PipelineError::RedisKVClient(err) = err
            && let RedisKVClientError::Redis(err) = &**err
            && let Some(errcode) = err.code()
            && errcode == "WRONGTYPE"
        {
            let op = CancelKeyOp::GetOld { key };
            let (id, rx) = tx.enqueue((op.metric_guard(), op));
            timeout(
                TIMEOUT,
                tx.call(id, rx, std::future::pending::<Infallible>()),
            )
            .await
            .map_err(|_| {
                tracing::warn!("timed out waiting to receive GetCancelData response");
                CancelError::RateLimit
            })?
        } else {
            result
        };

        let result = result.map_err(|e| {
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

        enum State {
            Set,
            Refresh,
        }
        let mut state = State::Set;

        loop {
            let op = match state {
                State::Set => {
                    tracing::debug!(
                        src=%self.key,
                        dest=?cancel_closure.cancel_token,
                        "registering cancellation key"
                    );
                    CancelKeyOp::Store {
                        key: self.key,
                        value: closure_json.clone(),
                        expire: CANCEL_KEY_TTL,
                    }
                }

                State::Refresh => {
                    tracing::debug!(
                        src=%self.key,
                        dest=?cancel_closure.cancel_token,
                        "refreshing cancellation key"
                    );
                    CancelKeyOp::Refresh {
                        key: self.key,
                        expire: CANCEL_KEY_TTL,
                    }
                }
            };

            let (id, rx) = tx.enqueue((op.metric_guard(), op));

            match tx.call(id, rx, cancel.as_mut()).await {
                // SET returns OK
                Ok(Value::Okay) => {
                    tracing::debug!(
                        src=%self.key,
                        dest=?cancel_closure.cancel_token,
                        "registered cancellation key"
                    );
                    state = State::Refresh;
                }

                // EXPIRE returns 1
                Ok(Value::Int(1)) => {
                    tracing::debug!(
                        src=%self.key,
                        dest=?cancel_closure.cancel_token,
                        "refreshed cancellation key"
                    );
                }

                Ok(_) => {
                    // Any other response likely means the key expired.
                    tracing::warn!(src=%self.key, "refreshing cancellation key failed");
                    // Re-enter the SET loop to repush full data.
                    state = State::Set;
                }

                // retry immediately.
                Err(BatchQueueError::Result(error)) => {
                    tracing::warn!(?error, "error refreshing cancellation key");
                    // Small delay to prevent busy loop with high cpu and logging.
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    continue;
                }

                Err(BatchQueueError::Cancelled(Err(_cancelled))) => break,
            }

            // wait before continuing. break immediately if cancelled.
            if run_until(tokio::time::sleep(CANCEL_KEY_REFRESH), cancel.as_mut())
                .await
                .is_err()
            {
                break;
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
