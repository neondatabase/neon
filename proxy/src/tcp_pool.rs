use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bb8::{ManageConnection, Pool, PooledConnection};
use once_cell::sync::Lazy;
use thiserror::Error;

use crate::auth::Backend;
use crate::auth::backend::{ComputeUserInfo, MaybeOwned};
use crate::compute::{AuthInfo, ComputeConnection};
use crate::config::{ProxyConfig, TcpPoolConfig};
use crate::context::RequestContext;
use crate::error::{ErrorKind, ReportableError, UserFacingError};
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
}

pub(crate) struct TcpPoolCheckout {
    key: TcpPoolKey,
    pooled: Option<PooledConnection<'static, ComputeConnectionManager>>,
}

impl TcpPoolCheckout {
    pub(crate) fn key(&self) -> &TcpPoolKey {
        &self.key
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
}

impl UserFacingError for AcquireError {
    fn to_string_client(&self) -> String {
        match self {
            AcquireError::Connect(e) => e.to_string_client(),
        }
    }
}

impl ReportableError for AcquireError {
    fn get_error_kind(&self) -> ErrorKind {
        match self {
            AcquireError::Connect(e) => e.get_error_kind(),
        }
    }
}

pub(crate) struct TcpPoolManager {
    inner: Arc<Inner>,
}

impl TcpPoolManager {
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
        println!("\n\n\n\nwas_reused = {}\n\n\n\n", was_reused);
        let conn = pooled.conn.take().expect("pooled slot must hold a connection");
        Ok((
            conn,
            Some(TcpPoolCheckout {
                key,
                pooled: Some(pooled),
            }),
            was_reused,
        ))
    }

    pub(crate) async fn reacquire(
        &self,
        key: TcpPoolKey,
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

        let conn = pooled.conn.take().expect("pooled slot must hold a connection");
        Ok((
            conn,
            TcpPoolCheckout {
                key,
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
