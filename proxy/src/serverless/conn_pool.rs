use dashmap::DashMap;
use futures::{future::poll_fn, Future};
use metrics::IntCounterPairGuard;
use parking_lot::RwLock;
use rand::Rng;
use smol_str::SmolStr;
use std::{collections::HashMap, pin::pin, sync::Arc, sync::Weak, time::Duration};
use std::{
    fmt,
    task::{ready, Poll},
};
use std::{
    ops::Deref,
    sync::atomic::{self, AtomicUsize},
};
use tokio::time::Instant;
use tokio_postgres::tls::NoTlsStream;
use tokio_postgres::{AsyncMessage, ReadyForQueryStatus, Socket};

use crate::console::messages::MetricsAuxInfo;
use crate::metrics::{ENDPOINT_POOLS, GC_LATENCY};
use crate::usage_metrics::{Ids, MetricCounter, USAGE_METRICS};
use crate::{
    auth::backend::ComputeUserInfo, context::RequestMonitoring, metrics::NUM_DB_CONNECTIONS_GAUGE,
    DbName, EndpointCacheKey, RoleName,
};

use tracing::{debug, error, warn, Span};
use tracing::{info, info_span, Instrument};

pub const APP_NAME: SmolStr = SmolStr::new_inline("/sql_over_http");

#[derive(Debug, Clone)]
pub struct ConnInfo {
    pub user_info: ComputeUserInfo,
    pub dbname: DbName,
    pub password: SmolStr,
}

impl ConnInfo {
    // hm, change to hasher to avoid cloning?
    pub fn db_and_user(&self) -> (DbName, RoleName) {
        (self.dbname.clone(), self.user_info.user.clone())
    }

    pub fn endpoint_cache_key(&self) -> EndpointCacheKey {
        self.user_info.endpoint_cache_key()
    }
}

impl fmt::Display for ConnInfo {
    // use custom display to avoid logging password
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}@{}/{}?{}",
            self.user_info.user,
            self.user_info.endpoint,
            self.dbname,
            self.user_info.options.get_cache_key("")
        )
    }
}

struct ConnPoolEntry {
    conn: ClientInner,
    _last_access: std::time::Instant,
}

// Per-endpoint connection pool, (dbname, username) -> DbUserConnPool
// Number of open connections is limited by the `max_conns_per_endpoint`.
pub struct EndpointConnPool {
    pools: HashMap<(DbName, RoleName), DbUserConnPool>,
    total_conns: usize,
    max_conns: usize,
    _guard: IntCounterPairGuard,
    global_pool_size: Arc<AtomicUsize>,
    global_pool_size_max_conns: usize,
}

impl EndpointConnPool {
    fn get_conn_entry(&mut self, db_user: (DbName, RoleName)) -> Option<ConnPoolEntry> {
        let Self {
            pools, total_conns, ..
        } = self;
        pools
            .get_mut(&db_user)
            .and_then(|pool_entries| pool_entries.get_conn_entry(total_conns))
    }

    fn remove_client(&mut self, db_user: (DbName, RoleName), conn_id: uuid::Uuid) -> bool {
        let Self {
            pools, total_conns, ..
        } = self;
        if let Some(pool) = pools.get_mut(&db_user) {
            let old_len = pool.conns.len();
            pool.conns.retain(|conn| conn.conn.conn_id != conn_id);
            let new_len = pool.conns.len();
            let removed = old_len - new_len;
            if removed > 0 {
                self.global_pool_size
                    .fetch_sub(removed, atomic::Ordering::Relaxed);
            }
            *total_conns -= removed;
            removed > 0
        } else {
            false
        }
    }

    fn put(pool: &RwLock<Self>, conn_info: &ConnInfo, client: ClientInner) -> anyhow::Result<()> {
        let conn_id = client.conn_id;

        if client.postgres_client.is_closed() {
            info!(%conn_id, "pool: throwing away connection '{conn_info}' because connection is closed");
            return Ok(());
        }
        let global_max_conn = pool.read().global_pool_size_max_conns;
        if pool.read().global_pool_size.load(atomic::Ordering::Relaxed) >= global_max_conn {
            info!(%conn_id, "pool: throwing away connection '{conn_info}' because pool is full");
            return Ok(());
        }

        // return connection to the pool
        let mut returned = false;
        let mut per_db_size = 0;
        let total_conns = {
            let mut pool = pool.write();

            if pool.total_conns < pool.max_conns {
                let pool_entries = pool.pools.entry(conn_info.db_and_user()).or_default();
                pool_entries.conns.push(ConnPoolEntry {
                    conn: client,
                    _last_access: std::time::Instant::now(),
                });

                returned = true;
                per_db_size = pool_entries.conns.len();

                pool.total_conns += 1;
                pool.global_pool_size
                    .fetch_add(1, atomic::Ordering::Relaxed);
            }

            pool.total_conns
        };

        // do logging outside of the mutex
        if returned {
            info!(%conn_id, "pool: returning connection '{conn_info}' back to the pool, total_conns={total_conns}, for this (db, user)={per_db_size}");
        } else {
            info!(%conn_id, "pool: throwing away connection '{conn_info}' because pool is full, total_conns={total_conns}");
        }

        Ok(())
    }
}

impl Drop for EndpointConnPool {
    fn drop(&mut self) {
        self.global_pool_size
            .fetch_sub(self.total_conns, atomic::Ordering::Relaxed);
    }
}

#[derive(Default)]
pub struct DbUserConnPool {
    conns: Vec<ConnPoolEntry>,
}

impl DbUserConnPool {
    fn clear_closed_clients(&mut self, conns: &mut usize) {
        let old_len = self.conns.len();

        self.conns
            .retain(|conn| !conn.conn.postgres_client.is_closed());

        let new_len = self.conns.len();
        let removed = old_len - new_len;
        *conns -= removed;
    }

    fn get_conn_entry(&mut self, conns: &mut usize) -> Option<ConnPoolEntry> {
        self.clear_closed_clients(conns);
        let conn = self.conns.pop();
        if conn.is_some() {
            *conns -= 1;
        }
        conn
    }
}

pub struct GlobalConnPool {
    // endpoint -> per-endpoint connection pool
    //
    // That should be a fairly conteded map, so return reference to the per-endpoint
    // pool as early as possible and release the lock.
    global_pool: DashMap<EndpointCacheKey, Arc<RwLock<EndpointConnPool>>>,

    /// Number of endpoint-connection pools
    ///
    /// [`DashMap::len`] iterates over all inner pools and acquires a read lock on each.
    /// That seems like far too much effort, so we're using a relaxed increment counter instead.
    /// It's only used for diagnostics.
    global_pool_size: AtomicUsize,

    // total number of connections in the pool
    total_conns: Arc<AtomicUsize>,

    config: &'static crate::config::HttpConfig,
}

#[derive(Debug, Clone, Copy)]
pub struct GlobalConnPoolOptions {
    // Maximum number of connections per one endpoint.
    // Can mix different (dbname, username) connections.
    // When running out of free slots for a particular endpoint,
    // falls back to opening a new connection for each request.
    pub max_conns_per_endpoint: usize,

    pub gc_epoch: Duration,

    pub pool_shards: usize,

    pub idle_timeout: Duration,

    pub opt_in: bool,

    // Total number of connections in the pool.
    pub max_total_conns: usize,
}

impl GlobalConnPool {
    pub fn new(config: &'static crate::config::HttpConfig) -> Arc<Self> {
        let shards = config.pool_options.pool_shards;
        Arc::new(Self {
            global_pool: DashMap::with_shard_amount(shards),
            global_pool_size: AtomicUsize::new(0),
            config,
            total_conns: Arc::new(AtomicUsize::new(0)),
        })
    }

    pub fn shutdown(&self) {
        // drops all strong references to endpoint-pools
        self.global_pool.clear();
    }

    pub async fn gc_worker(&self, mut rng: impl Rng) {
        let epoch = self.config.pool_options.gc_epoch;
        let mut interval = tokio::time::interval(epoch / (self.global_pool.shards().len()) as u32);
        loop {
            interval.tick().await;

            let shard = rng.gen_range(0..self.global_pool.shards().len());
            self.gc(shard);
        }
    }

    fn gc(&self, shard: usize) {
        debug!(shard, "pool: performing epoch reclamation");

        // acquire a random shard lock
        let mut shard = self.global_pool.shards()[shard].write();

        let timer = GC_LATENCY.start_timer();
        let current_len = shard.len();
        shard.retain(|endpoint, x| {
            // if the current endpoint pool is unique (no other strong or weak references)
            // then it is currently not in use by any connections.
            if let Some(pool) = Arc::get_mut(x.get_mut()) {
                let EndpointConnPool {
                    pools, total_conns, ..
                } = pool.get_mut();

                // ensure that closed clients are removed
                pools
                    .iter_mut()
                    .for_each(|(_, db_pool)| db_pool.clear_closed_clients(total_conns));

                // we only remove this pool if it has no active connections
                if *total_conns == 0 {
                    info!("pool: discarding pool for endpoint {endpoint}");
                    return false;
                }
            }

            true
        });
        let new_len = shard.len();
        drop(shard);
        timer.observe_duration();

        let removed = current_len - new_len;

        if removed > 0 {
            let global_pool_size = self
                .global_pool_size
                .fetch_sub(removed, atomic::Ordering::Relaxed)
                - removed;
            info!("pool: performed global pool gc. size now {global_pool_size}");
        }
    }

    pub async fn get(
        self: &Arc<Self>,
        ctx: &mut RequestMonitoring,
        conn_info: &ConnInfo,
    ) -> anyhow::Result<Option<Client>> {
        let mut client: Option<ClientInner> = None;

        let endpoint_pool = self.get_or_create_endpoint_pool(&conn_info.endpoint_cache_key());
        if let Some(entry) = endpoint_pool
            .write()
            .get_conn_entry(conn_info.db_and_user())
        {
            client = Some(entry.conn)
        }
        let endpoint_pool = Arc::downgrade(&endpoint_pool);

        // ok return cached connection if found and establish a new one otherwise
        if let Some(client) = client {
            if client.postgres_client.is_closed() {
                info!("pool: cached connection '{conn_info}' is closed, opening a new one");
                return Ok(None);
            } else {
                info!("pool: reusing connection '{conn_info}'");
                client.session.send(ctx.session_id)?;
                tracing::Span::current().record(
                    "pid",
                    &tracing::field::display(client.postgres_client.get_process_id()),
                );
                ctx.latency_timer.pool_hit();
                ctx.latency_timer.success();
                return Ok(Some(Client::new(client, conn_info.clone(), endpoint_pool)));
            }
        }
        Ok(None)
    }

    fn get_or_create_endpoint_pool(
        self: &Arc<Self>,
        endpoint: &EndpointCacheKey,
    ) -> Arc<RwLock<EndpointConnPool>> {
        // fast path
        if let Some(pool) = self.global_pool.get(endpoint) {
            return pool.clone();
        }

        // slow path
        let new_pool = Arc::new(RwLock::new(EndpointConnPool {
            pools: HashMap::new(),
            total_conns: 0,
            max_conns: self.config.pool_options.max_conns_per_endpoint,
            _guard: ENDPOINT_POOLS.guard(),
            global_pool_size: self.total_conns.clone(),
            global_pool_size_max_conns: self.config.pool_options.max_total_conns,
        }));

        // find or create a pool for this endpoint
        let mut created = false;
        let pool = self
            .global_pool
            .entry(endpoint.clone())
            .or_insert_with(|| {
                created = true;
                new_pool
            })
            .clone();

        // log new global pool size
        if created {
            let global_pool_size = self
                .global_pool_size
                .fetch_add(1, atomic::Ordering::Relaxed)
                + 1;
            info!(
                "pool: created new pool for '{endpoint}', global pool size now {global_pool_size}"
            );
        }

        pool
    }

    pub fn poll_client(
        self: Arc<Self>,
        ctx: &mut RequestMonitoring,
        conn_info: ConnInfo,
        client: tokio_postgres::Client,
        mut connection: tokio_postgres::Connection<Socket, NoTlsStream>,
        conn_id: uuid::Uuid,
        aux: MetricsAuxInfo,
    ) -> Client {
        let conn_gauge = NUM_DB_CONNECTIONS_GAUGE
            .with_label_values(&[ctx.protocol])
            .guard();
        let mut session_id = ctx.session_id;
        let (tx, mut rx) = tokio::sync::watch::channel(session_id);

        let span = info_span!(parent: None, "connection", %conn_id);
        span.in_scope(|| {
            info!(%conn_info, %session_id, "new connection");
        });
        let pool =
            Arc::downgrade(&self.get_or_create_endpoint_pool(&conn_info.endpoint_cache_key()));
        let pool_clone = pool.clone();

        let db_user = conn_info.db_and_user();
        let idle = self.config.pool_options.idle_timeout;
        tokio::spawn(
        async move {
            let _conn_gauge = conn_gauge;
            let mut idle_timeout = pin!(tokio::time::sleep(idle));
            poll_fn(move |cx| {
                if matches!(rx.has_changed(), Ok(true)) {
                    session_id = *rx.borrow_and_update();
                    info!(%session_id, "changed session");
                    idle_timeout.as_mut().reset(Instant::now() + idle);
                }

                // 5 minute idle connection timeout
                if idle_timeout.as_mut().poll(cx).is_ready() {
                    idle_timeout.as_mut().reset(Instant::now() + idle);
                    info!("connection idle");
                    if let Some(pool) = pool.clone().upgrade() {
                        // remove client from pool - should close the connection if it's idle.
                        // does nothing if the client is currently checked-out and in-use
                        if pool.write().remove_client(db_user.clone(), conn_id) {
                            info!("idle connection removed");
                        }
                    }
                }

                loop {
                    let message = ready!(connection.poll_message(cx));

                    match message {
                        Some(Ok(AsyncMessage::Notice(notice))) => {
                            info!(%session_id, "notice: {}", notice);
                        }
                        Some(Ok(AsyncMessage::Notification(notif))) => {
                            warn!(%session_id, pid = notif.process_id(), channel = notif.channel(), "notification received");
                        }
                        Some(Ok(_)) => {
                            warn!(%session_id, "unknown message");
                        }
                        Some(Err(e)) => {
                            error!(%session_id, "connection error: {}", e);
                            break
                        }
                        None => {
                            info!("connection closed");
                            break
                        }
                    }
                }

                // remove from connection pool
                if let Some(pool) = pool.clone().upgrade() {
                    if pool.write().remove_client(db_user.clone(), conn_id) {
                        info!("closed connection removed");
                    }
                }

                Poll::Ready(())
            }).await;

        }
        .instrument(span));
        let inner = ClientInner {
            postgres_client: client,
            session: tx,
            aux,
            conn_id,
        };
        Client::new(inner, conn_info, pool_clone)
    }
}

struct ClientInner {
    postgres_client: tokio_postgres::Client,
    session: tokio::sync::watch::Sender<uuid::Uuid>,
    aux: MetricsAuxInfo,
    conn_id: uuid::Uuid,
}

impl Client {
    pub fn metrics(&self) -> Arc<MetricCounter> {
        let aux = &self.inner.as_ref().unwrap().aux;
        USAGE_METRICS.register(Ids {
            endpoint_id: aux.endpoint_id.clone(),
            branch_id: aux.branch_id.clone(),
        })
    }
}

pub struct Client {
    span: Span,
    inner: Option<ClientInner>,
    conn_info: ConnInfo,
    pool: Weak<RwLock<EndpointConnPool>>,
}

pub struct Discard<'a> {
    conn_id: uuid::Uuid,
    conn_info: &'a ConnInfo,
    pool: &'a mut Weak<RwLock<EndpointConnPool>>,
}

impl Client {
    pub(self) fn new(
        inner: ClientInner,
        conn_info: ConnInfo,
        pool: Weak<RwLock<EndpointConnPool>>,
    ) -> Self {
        Self {
            inner: Some(inner),
            span: Span::current(),
            conn_info,
            pool,
        }
    }
    pub fn inner(&mut self) -> (&mut tokio_postgres::Client, Discard<'_>) {
        let Self {
            inner,
            pool,
            conn_info,
            span: _,
        } = self;
        let inner = inner.as_mut().expect("client inner should not be removed");
        (
            &mut inner.postgres_client,
            Discard {
                pool,
                conn_info,
                conn_id: inner.conn_id,
            },
        )
    }

    pub fn check_idle(&mut self, status: ReadyForQueryStatus) {
        self.inner().1.check_idle(status)
    }
    pub fn discard(&mut self) {
        self.inner().1.discard()
    }
}

impl Discard<'_> {
    pub fn check_idle(&mut self, status: ReadyForQueryStatus) {
        let conn_info = &self.conn_info;
        if status != ReadyForQueryStatus::Idle && std::mem::take(self.pool).strong_count() > 0 {
            info!(conn_id = %self.conn_id, "pool: throwing away connection '{conn_info}' because connection is not idle")
        }
    }
    pub fn discard(&mut self) {
        let conn_info = &self.conn_info;
        if std::mem::take(self.pool).strong_count() > 0 {
            info!(conn_id = %self.conn_id, "pool: throwing away connection '{conn_info}' because connection is potentially in a broken state")
        }
    }
}

impl Deref for Client {
    type Target = tokio_postgres::Client;

    fn deref(&self) -> &Self::Target {
        &self
            .inner
            .as_ref()
            .expect("client inner should not be removed")
            .postgres_client
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        let conn_info = self.conn_info.clone();
        let client = self
            .inner
            .take()
            .expect("client inner should not be removed");
        if let Some(conn_pool) = std::mem::take(&mut self.pool).upgrade() {
            let current_span = self.span.clone();
            // return connection to the pool
            tokio::task::spawn_blocking(move || {
                let _span = current_span.enter();
                let _ = EndpointConnPool::put(&conn_pool, &conn_info, client);
            });
        }
    }
}
