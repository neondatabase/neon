use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bb8::{ManageConnection, Pool, PooledConnection};
use futures::TryStreamExt;
use once_cell::sync::Lazy;
use postgres_client::connect_raw::StartupStream;
use postgres_protocol::message::backend::Message;
use rand::Rng;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tracing::debug;

use crate::auth::Backend;
use crate::auth::backend::{ComputeUserInfo, MaybeOwned};
use crate::compute::{AuthInfo, ComputeConnection, MaybeRustlsStream};
use crate::config::{ProxyConfig, TcpPoolConfig};
use crate::context::RequestContext;
use crate::error::{ErrorKind, ReportableError, UserFacingError};
use crate::pqproto;
use crate::proxy::connect_auth::{self, AuthError};
use crate::proxy::connect_compute;
use crate::types::{DbName, EndpointCacheKey, RoleName};

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub(crate) struct TcpPoolKey {
    endpoint: EndpointCacheKey,
    dbname: DbName,
    role: RoleName,
}

impl TcpPoolKey {
    pub(crate) fn new(endpoint: EndpointCacheKey, dbname: DbName, role: RoleName) -> Self {
        Self {
            endpoint,
            dbname,
            role,
        }
    }
}

impl std::fmt::Display for TcpPoolKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}@{}", self.endpoint, self.role, self.dbname)
    }
}

struct PooledCompute {
    conn: Option<ComputeConnection>,
    fresh: bool,
}

#[derive(Clone)]
struct ComputeConnectionManager {
    ctx: RequestContext,
    config: &'static ProxyConfig,
    backend: Arc<Backend<'static, ComputeUserInfo>>,
    auth_info: AuthInfo,
}

#[async_trait]
impl ManageConnection for ComputeConnectionManager {
    type Connection = PooledCompute;
    type Error = AuthError;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        let conn = connect_auth::connect_to_compute_and_auth(
            &self.ctx,
            self.config,
            &self.backend,
            self.auth_info.clone(),
            connect_compute::TlsNegotiation::Postgres,
        )
        .await?;

        Ok(PooledCompute {
            conn: Some(conn),
            fresh: true,
        })
    }

    async fn is_valid(&self, _conn: &mut Self::Connection) -> Result<(), Self::Error> {
        Ok(())
    }

    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        conn.conn.is_none()
    }
}

type KeyedPool = Pool<ComputeConnectionManager>;

#[derive(Default)]
struct Inner {
    pools: clashmap::ClashMap<TcpPoolKey, KeyedPool>,
    startup_params: clashmap::ClashMap<TcpPoolKey, Arc<Vec<(Box<str>, Box<str>)>>>,
}

pub(crate) struct TcpPoolCheckout {
    key: TcpPoolKey,
    reset_query: Arc<str>,
    pooled: Option<PooledConnection<'static, ComputeConnectionManager>>,
}

#[derive(Clone)]
pub(crate) struct TcpPoolReacquire {
    key: TcpPoolKey,
    reset_query: Arc<str>,
}

impl TcpPoolReacquire {
    pub(crate) fn key(&self) -> &TcpPoolKey {
        &self.key
    }

    pub(crate) fn reset_query(&self) -> Arc<str> {
        self.reset_query.clone()
    }
}

impl TcpPoolCheckout {
    pub(crate) fn reset_query(&self) -> Arc<str> {
        self.reset_query.clone()
    }

    pub(crate) fn reacquire_info(&self) -> TcpPoolReacquire {
        TcpPoolReacquire {
            key: self.key.clone(),
            reset_query: self.reset_query.clone(),
        }
    }

    pub(crate) fn release(mut self, conn: ComputeConnection, reusable: bool) {
        if !reusable {
            self.pooled.take();
            return;
        }

        if let Some(mut pooled) = self.pooled.take() {
            pooled.conn = Some(conn);
            pooled.fresh = false;
        }
    }
}

#[derive(Debug, Error)]
pub(crate) enum AcquireError {
    #[error("{0}")]
    Connect(#[from] AuthError),
    #[error("{0}")]
    Startup(#[from] postgres_client::Error),
}

impl UserFacingError for AcquireError {
    fn to_string_client(&self) -> String {
        match self {
            AcquireError::Connect(e) => e.to_string_client(),
            AcquireError::Startup(e) => e.to_string(),
        }
    }
}

impl ReportableError for AcquireError {
    fn get_error_kind(&self) -> ErrorKind {
        match self {
            AcquireError::Connect(e) => e.get_error_kind(),
            AcquireError::Startup(_) => ErrorKind::Postgres,
        }
    }
}

async fn drain_fresh_startup(conn: &mut ComputeConnection) -> Result<(), postgres_client::Error> {
    loop {
        let msg = conn
            .stream
            .try_next()
            .await
            .map_err(postgres_client::Error::io)?;

        match msg {
            Some(Message::ParameterStatus(_))
            | Some(Message::BackendKeyData(_))
            | Some(Message::NoticeResponse(_)) => {}
            Some(Message::ReadyForQuery(_)) => return Ok(()),
            Some(Message::ErrorResponse(body)) => return Err(postgres_client::Error::db(body)),
            Some(_) => return Err(postgres_client::Error::unexpected_message()),
            None => return Err(postgres_client::Error::closed()),
        }
    }
}

async fn write_simple_query<S>(stream: &mut S, query: &str) -> std::io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    stream.write_u8(b'Q').await?;
    stream.write_u32((query.len() + 5) as u32).await?;
    stream.write_all(query.as_bytes()).await?;
    stream.write_u8(0).await?;
    stream.flush().await
}

async fn drain_simple_query<S>(stream: &mut S) -> Result<u8, postgres_client::Error>
where
    S: AsyncRead + Unpin,
{
    let mut buf = Vec::new();
    loop {
        let (tag, body) = pqproto::read_message(stream, &mut buf, 65536)
            .await
            .map_err(postgres_client::Error::io)?;

        match tag {
            b'Z' if body.len() == 1 => return Ok(body[0]),
            b'Z' => return Err(postgres_client::Error::unexpected_message()),
            b'C' | b'D' | b'I' | b'N' | b'S' | b'T' => {}
            b'E' => return Err(postgres_client::Error::unexpected_message()),
            _ => return Err(postgres_client::Error::unexpected_message()),
        }
    }
}

async fn reset_raw_session(
    stream: &mut MaybeRustlsStream,
    reset_query: &str,
) -> Result<(), postgres_client::Error> {
    write_simple_query(stream, reset_query)
        .await
        .map_err(postgres_client::Error::io)?;
    let status = drain_simple_query(stream).await?;
    if status == b'I' {
        Ok(())
    } else {
        Err(postgres_client::Error::unexpected_message())
    }
}

pub(crate) async fn reset_session(
    conn: ComputeConnection,
    reset_query: &str,
) -> Result<ComputeConnection, postgres_client::Error> {
    let ComputeConnection {
        stream,
        aux,
        hostname,
        ssl_mode,
        socket_addr,
        guage,
    } = conn;

    let mut raw_stream = stream.into_framed().into_inner();
    reset_raw_session(&mut raw_stream, reset_query).await?;

    Ok(ComputeConnection {
        stream: StartupStream::new(raw_stream),
        aux,
        hostname,
        ssl_mode,
        socket_addr,
        guage,
    })
}

pub(crate) struct TcpPoolManager {
    inner: Arc<Inner>,
}

impl TcpPoolManager {
    pub(crate) fn set_startup_params(&self, key: &TcpPoolKey, params: Vec<(Box<str>, Box<str>)>) {
        self.inner
            .startup_params
            .insert(key.clone(), Arc::new(params));
    }

    pub(crate) fn get_startup_params(
        &self,
        key: &TcpPoolKey,
    ) -> Option<Arc<Vec<(Box<str>, Box<str>)>>> {
        self.inner.startup_params.get(key).map(|v| v.clone())
    }

    pub(crate) async fn gc_worker(&self) -> anyhow::Result<Infallible> {
        let mut interval = tokio::time::interval(Duration::from_secs(30));
        loop {
            interval.tick().await;
            self.gc_one_shard();
        }
    }

    fn gc_one_shard(&self) {
        let shards = self.inner.pools.shards();
        if shards.is_empty() {
            return;
        }

        let shard_idx = rand::rng().random_range(0..shards.len());
        let mut shard = shards[shard_idx].write();
        shard.retain(|(_, pool)| {
            let state = pool.state();
            let keep = state.connections > 0;
            if !keep {
                debug!("tcp pool: dropping empty keyed pool");
            }
            keep
        });
    }

    async fn get_or_create_pool(
        &self,
        key: TcpPoolKey,
        mgr: ComputeConnectionManager,
        cfg: TcpPoolConfig,
    ) -> KeyedPool {
        if let Some(pool) = self.inner.pools.get(&key).map(|p| p.clone()) {
            return pool;
        }

        let pool = Pool::builder()
            .max_size(cfg.max_conns_per_key as u32)
            .idle_timeout(Some(cfg.idle_timeout))
            .connection_timeout(Duration::from_secs(365 * 24 * 60 * 60))
            .build_unchecked(mgr);

        self.inner
            .pools
            .entry(key)
            .or_insert_with(|| pool.clone())
            .clone()
    }

    pub(crate) async fn prepare_reacquire(
        &self,
        config: &TcpPoolConfig,
        key: TcpPoolKey,
        ctx: RequestContext,
        proxy_config: &'static ProxyConfig,
        cplane: crate::control_plane::client::ControlPlaneClient,
        user_info: ComputeUserInfo,
        auth_info: AuthInfo,
    ) -> TcpPoolReacquire {
        let reset_query = Arc::<str>::from(auth_info.tcp_pool_session_reset_query());
        let backend = Arc::new(Backend::ControlPlane(MaybeOwned::Owned(cplane), user_info));
        let mgr = ComputeConnectionManager {
            ctx,
            config: proxy_config,
            backend,
            auth_info,
        };

        self.get_or_create_pool(key.clone(), mgr, *config).await;

        TcpPoolReacquire { key, reset_query }
    }

    pub(crate) async fn acquire_or_connect(
        &self,
        config: &TcpPoolConfig,
        key: TcpPoolKey,
        ctx: RequestContext,
        proxy_config: &'static ProxyConfig,
        cplane: crate::control_plane::client::ControlPlaneClient,
        user_info: ComputeUserInfo,
        auth_info: AuthInfo,
    ) -> Result<(ComputeConnection, Option<TcpPoolCheckout>, bool), AcquireError> {
        if !config.enabled {
            let backend = Backend::ControlPlane(MaybeOwned::Owned(cplane), user_info);
            let conn = connect_auth::connect_to_compute_and_auth(
                &ctx,
                proxy_config,
                &backend,
                auth_info,
                connect_compute::TlsNegotiation::Postgres,
            )
            .await?;
            return Ok((conn, None, false));
        }

        let reset_query = Arc::<str>::from(auth_info.tcp_pool_session_reset_query());
        let backend = Arc::new(Backend::ControlPlane(MaybeOwned::Owned(cplane), user_info));
        let mgr = ComputeConnectionManager {
            ctx,
            config: proxy_config,
            backend,
            auth_info,
        };

        let pool = self.get_or_create_pool(key.clone(), mgr, *config).await;
        let mut pooled = pool.get_owned().await.map_err(|e| match e {
            bb8::RunError::User(e) => AcquireError::Connect(e),
            bb8::RunError::TimedOut => unreachable!("connection_timeout is effectively disabled"),
        })?;

        let was_reused = !pooled.fresh;
        let conn = pooled
            .conn
            .take()
            .expect("pooled slot must hold a connection");
        Ok((
            conn,
            Some(TcpPoolCheckout {
                key,
                reset_query,
                pooled: Some(pooled),
            }),
            was_reused,
        ))
    }

    pub(crate) async fn reacquire(
        &self,
        key: TcpPoolKey,
        reset_query: Arc<str>,
    ) -> Result<(ComputeConnection, TcpPoolCheckout), AcquireError> {
        let pool = self
            .inner
            .pools
            .get(&key)
            .map(|p| p.clone())
            .expect("pool key must exist before re-acquire");

        let mut pooled = pool.get_owned().await.map_err(|e| match e {
            bb8::RunError::User(e) => AcquireError::Connect(e),
            bb8::RunError::TimedOut => unreachable!("connection_timeout is effectively disabled"),
        })?;

        let mut conn = pooled
            .conn
            .take()
            .expect("pooled slot must hold a connection");
        if pooled.fresh {
            drain_fresh_startup(&mut conn).await?;
            pooled.fresh = false;
        }
        let conn = reset_session(conn, &reset_query).await?;
        Ok((
            conn,
            TcpPoolCheckout {
                key,
                reset_query,
                pooled: Some(pooled),
            },
        ))
    }
}

static MANAGER: Lazy<TcpPoolManager> = Lazy::new(|| TcpPoolManager {
    inner: Arc::new(Inner::default()),
});

pub(crate) fn manager() -> &'static TcpPoolManager {
    &MANAGER
}
