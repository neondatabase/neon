use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use once_cell::sync::Lazy;
use parking_lot::Mutex;
use thiserror::Error;
use tracing::{debug, info, warn};

use crate::compute::ComputeConnection;
use crate::config::TcpPoolConfig;
use crate::error::{ErrorKind, ReportableError, UserFacingError};
use crate::proxy::connect_auth::AuthError;
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

struct IdleConn {
    conn: ComputeConnection,
    returned_at: Instant,
}

impl std::fmt::Debug for IdleConn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IdleConn")
            .field("returned_at", &self.returned_at)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Default)]
struct KeyPool {
    open: usize,
    idle: Vec<IdleConn>,
}

#[derive(Default)]
struct Inner {
    pools: clashmap::ClashMap<TcpPoolKey, Arc<Mutex<KeyPool>>>,
    open_total: AtomicUsize,
}

#[derive(Clone)]
pub(crate) struct TcpPoolCheckout {
    key: TcpPoolKey,
    manager: Arc<Inner>,
}

impl TcpPoolCheckout {
    pub(crate) fn release(self, config: &TcpPoolConfig, conn: ComputeConnection, reusable: bool) {
        release(&self.manager, config, self.key, conn, reusable);
    }
}

#[derive(Debug, Error)]
pub(crate) enum AcquireError {
    #[error("pool exhausted for key `{0}`")]
    PoolExhausted(TcpPoolKey),
    #[error("{0}")]
    Connect(#[from] AuthError),
}

impl UserFacingError for AcquireError {
    fn to_string_client(&self) -> String {
        match self {
            AcquireError::PoolExhausted(_) => {
                "Connection pool is exhausted for this endpoint/user/database".to_string()
            }
            AcquireError::Connect(e) => e.to_string_client(),
        }
    }
}

impl ReportableError for AcquireError {
    fn get_error_kind(&self) -> ErrorKind {
        match self {
            AcquireError::PoolExhausted(_) => ErrorKind::RateLimit,
            AcquireError::Connect(e) => e.get_error_kind(),
        }
    }
}

pub(crate) struct TcpPoolManager {
    inner: Arc<Inner>,
}

impl TcpPoolManager {
    pub(crate) async fn acquire_or_connect<F, Fut>(
        &self,
        config: &TcpPoolConfig,
        key: TcpPoolKey,
        connect: F,
    ) -> Result<(ComputeConnection, Option<TcpPoolCheckout>,bool), AcquireError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<ComputeConnection, AuthError>>,
    {
        if !config.enabled {
            return Ok((connect().await?, None, false));
        }

        let pool = self
            .inner
            .pools
            .entry(key.clone())
            .or_insert_with(|| Arc::new(Mutex::new(KeyPool::default())))
            .clone();

        {
            let mut pool = pool.lock();
            let dropped = gc_idle(&mut pool, config);
            if dropped > 0 {
                self.inner.open_total.fetch_sub(dropped, Ordering::Relaxed);
            }
            if let Some(idle) = pool.idle.pop() {
                debug!(pool_key = %key, "tcp pool: reuse connection");
                let checkout = TcpPoolCheckout {
                    key,
                    manager: self.inner.clone(),
                };
                return Ok((idle.conn, Some(checkout), true));
            }

            let open_total = self.inner.open_total.load(Ordering::Relaxed);
            if pool.open >= config.max_conns_per_key || open_total >= config.max_total_conns {
                return Err(AcquireError::PoolExhausted(key));
            }
            pool.open += 1;
            self.inner.open_total.fetch_add(1, Ordering::Relaxed);
        }

        match connect().await {
            Ok(conn) => {
                info!(pool_key = %key, "tcp pool: opened new connection");
                let checkout = TcpPoolCheckout {
                    key,
                    manager: self.inner.clone(),
                };
                Ok((conn, Some(checkout), false))
            }
            Err(e) => {
                decrement_open_counts(&self.inner, &key);
                Err(AcquireError::Connect(e))
            }
        }
    }
}

fn decrement_open_counts(inner: &Inner, key: &TcpPoolKey) {
    if let Some(pool) = inner.pools.get(key) {
        let mut pool = pool.lock();
        if pool.open > 0 {
            pool.open -= 1;
            inner.open_total.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

fn release(
    inner: &Arc<Inner>,
    config: &TcpPoolConfig,
    key: TcpPoolKey,
    conn: ComputeConnection,
    reusable: bool,
) {
    let Some(pool) = inner.pools.get(&key).map(|p| p.clone()) else {
        return;
    };

    let mut pool = pool.lock();
    let dropped = gc_idle(&mut pool, config);
    if dropped > 0 {
        inner.open_total.fetch_sub(dropped, Ordering::Relaxed);
    }

    if reusable {
        debug!(pool_key = %key, "tcp pool: returning connection");
        pool.idle.push(IdleConn {
            conn,
            returned_at: Instant::now(),
        });
        return;
    }

    warn!(pool_key = %key, "tcp pool: discarding connection");
    if pool.open > 0 {
        pool.open -= 1;
        inner.open_total.fetch_sub(1, Ordering::Relaxed);
    }
}

fn gc_idle(pool: &mut KeyPool, config: &TcpPoolConfig) -> usize {
    if config.idle_timeout.is_zero() {
        return 0;
    }

    let now = Instant::now();
    let old = pool.idle.len();
    pool.idle
        .retain(|idle| now.duration_since(idle.returned_at) <= config.idle_timeout);
    let dropped = old.saturating_sub(pool.idle.len());
    if dropped > 0 {
        pool.open = pool.open.saturating_sub(dropped);
    }
    dropped
}

static MANAGER: Lazy<TcpPoolManager> = Lazy::new(|| TcpPoolManager {
    inner: Arc::new(Inner::default()),
});

pub(crate) fn manager() -> &'static TcpPoolManager {
    &MANAGER
}
