use dashmap::DashMap;
use futures::Future;
use parking_lot::RwLock;
use pin_list::Node;
use pin_project_lite::pin_project;
use rand::Rng;
use smallvec::SmallVec;
use std::pin::Pin;
use std::sync::Weak;
use std::{collections::HashMap, sync::Arc, time::Duration};
use std::{
    fmt,
    task::{ready, Poll},
};
use std::{
    ops::Deref,
    sync::atomic::{self, AtomicUsize},
};
use tokio::sync::mpsc::error::TrySendError;
use tokio::time::{Instant, Sleep};
use tokio_postgres::tls::NoTlsStream;
use tokio_postgres::{AsyncMessage, ReadyForQueryStatus, Socket};
use tokio_util::sync::{CancellationToken, WaitForCancellationFutureOwned};

use crate::console::messages::{ColdStartInfo, MetricsAuxInfo};
use crate::metrics::{HttpEndpointPoolsGuard, Metrics, NumDbConnectionsGuard};
use crate::usage_metrics::{Ids, MetricCounter, USAGE_METRICS};
use crate::{
    auth::backend::ComputeUserInfo, context::RequestMonitoring, DbName, EndpointCacheKey, RoleName,
};

use tracing::{debug, error, warn, Span};
use tracing::{info, info_span, Instrument};

use super::backend::HttpConnError;

#[derive(Debug, Clone)]
pub struct ConnInfo {
    pub user_info: ComputeUserInfo,
    pub dbname: DbName,
    pub password: SmallVec<[u8; 16]>,
}

impl ConnInfo {
    // hm, change to hasher to avoid cloning?
    pub fn db_and_user(&self) -> (DbName, RoleName) {
        (self.dbname.clone(), self.user_info.user.clone())
    }

    pub fn endpoint_cache_key(&self) -> Option<EndpointCacheKey> {
        // We don't want to cache http connections for ephemeral endpoints.
        if self.user_info.options.is_ephemeral() {
            None
        } else {
            Some(self.user_info.endpoint_cache_key())
        }
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

struct ConnPoolEntry<C: ClientInnerExt> {
    conn: ClientInner<C>,
    _last_access: std::time::Instant,
}

// Per-endpoint connection pool, (dbname, username) -> DbUserConnPool
// Number of open connections is limited by the `max_conns_per_endpoint`.
pub struct EndpointConnPool<C: ClientInnerExt> {
    pools: HashMap<(DbName, RoleName), DbUserConnPool<C>>,
    total_conns: usize,
    max_conns: usize,
    _guard: HttpEndpointPoolsGuard<'static>,
    global_connections_count: Arc<AtomicUsize>,
    global_pool_size_max_conns: usize,
}

impl<C: ClientInnerExt> EndpointConnPool<C> {
    fn get_conn_entry(
        &mut self,
        db_user: (DbName, RoleName),
        session_id: uuid::Uuid,
    ) -> Option<ConnPoolEntry<C>> {
        let Self {
            pools,
            total_conns,
            global_connections_count,
            ..
        } = self;
        pools.get_mut(&db_user).and_then(|pool_entries| {
            pool_entries.get_conn_entry(total_conns, global_connections_count, session_id)
        })
    }

    fn remove_client(&mut self, db_user: (DbName, RoleName), conn_id: uuid::Uuid) -> bool {
        let Self {
            pools,
            total_conns,
            global_connections_count,
            ..
        } = self;
        if let Some(pool) = pools.get_mut(&db_user) {
            let mut removed = 0;
            let mut cursor = pool.conns.cursor_front_mut();
            while let Some(client) = cursor.protected() {
                if client.conn.conn_id != conn_id {
                    let _ = cursor.remove_current(uuid::Uuid::nil());
                    removed += 1;
                } else {
                    cursor.move_next()
                }
            }

            if removed > 0 {
                global_connections_count.fetch_sub(removed, atomic::Ordering::Relaxed);
                Metrics::get()
                    .proxy
                    .http_pool_opened_connections
                    .get_metric()
                    .dec_by(removed as i64);
            }
            *total_conns -= removed;
            removed > 0
        } else {
            false
        }
    }

    fn put(
        pool: &RwLock<Self>,
        mut node: Pin<&mut Node<ConnTypes<C>>>,
        db_user: &(DbName, RoleName),
        client: ClientInner<C>,
        conn_info: ConnInfo,
    ) -> bool {
        let conn_id = client.conn_id;

        {
            let pool = pool.read();
            if pool
                .global_connections_count
                .load(atomic::Ordering::Relaxed)
                >= pool.global_pool_size_max_conns
            {
                info!(%conn_id, "pool: throwing away connection '{conn_info}' because pool is full");
                return false;
            }
        }

        // return connection to the pool
        let mut returned = false;
        let mut per_db_size = 0;
        let total_conns = {
            let mut pool = pool.write();

            if pool.total_conns < pool.max_conns {
                let pool_entries = pool.pools.entry(db_user.clone()).or_default();

                if let Some(node) = node.as_mut().initialized_mut() {
                    if node.take_removed(&pool_entries.conns).is_err() {
                        panic!("client is already in the pool")
                    };
                }
                pool_entries.conns.cursor_front_mut().insert_after(
                    node,
                    ConnPoolEntry {
                        conn: client,
                        _last_access: std::time::Instant::now(),
                    },
                    (),
                );

                returned = true;
                per_db_size = pool_entries.len;

                pool.total_conns += 1;
                pool.global_connections_count
                    .fetch_add(1, atomic::Ordering::Relaxed);
                Metrics::get()
                    .proxy
                    .http_pool_opened_connections
                    .get_metric()
                    .inc();
            }

            pool.total_conns
        };

        // do logging outside of the mutex
        if returned {
            info!(%conn_id, "pool: returning connection '{conn_info}' back to the pool, total_conns={total_conns}, for this (db, user)={per_db_size}");
        } else {
            info!(%conn_id, "pool: throwing away connection '{conn_info}' because pool is full, total_conns={total_conns}");
        }

        returned
    }
}

impl<C: ClientInnerExt> Drop for EndpointConnPool<C> {
    fn drop(&mut self) {
        if self.total_conns > 0 {
            self.global_connections_count
                .fetch_sub(self.total_conns, atomic::Ordering::Relaxed);
            Metrics::get()
                .proxy
                .http_pool_opened_connections
                .get_metric()
                .dec_by(self.total_conns as i64);
        }
    }
}

pub struct DbUserConnPool<C: ClientInnerExt> {
    conns: pin_list::PinList<ConnTypes<C>>,
    len: usize,
}

impl<C: ClientInnerExt> Default for DbUserConnPool<C> {
    fn default() -> Self {
        Self {
            conns: pin_list::PinList::new(pin_list::id::Checked::new()),
            len: 0,
        }
    }
}

impl<C: ClientInnerExt> DbUserConnPool<C> {
    fn clear_closed_clients(&mut self, conns: &mut usize) -> usize {
        let mut removed = 0;

        let mut cursor = self.conns.cursor_front_mut();
        while let Some(client) = cursor.protected() {
            if client.conn.is_closed() {
                let _ = cursor.remove_current(uuid::Uuid::nil());
                removed += 1;
            } else {
                cursor.move_next()
            }
        }

        *conns -= removed;
        removed
    }

    fn get_conn_entry(
        &mut self,
        conns: &mut usize,
        global_connections_count: &AtomicUsize,
        session_id: uuid::Uuid,
    ) -> Option<ConnPoolEntry<C>> {
        let mut removed = self.clear_closed_clients(conns);

        let conn = self
            .conns
            .cursor_front_mut()
            .remove_current(session_id)
            .ok();

        if conn.is_some() {
            *conns -= 1;
            removed += 1;
        }
        global_connections_count.fetch_sub(removed, atomic::Ordering::Relaxed);
        Metrics::get()
            .proxy
            .http_pool_opened_connections
            .get_metric()
            .dec_by(removed as i64);
        conn
    }
}

pub struct GlobalConnPool<C: ClientInnerExt> {
    // endpoint -> per-endpoint connection pool
    //
    // That should be a fairly conteded map, so return reference to the per-endpoint
    // pool as early as possible and release the lock.
    global_pool: DashMap<EndpointCacheKey, Arc<RwLock<EndpointConnPool<C>>>>,

    /// Number of endpoint-connection pools
    ///
    /// [`DashMap::len`] iterates over all inner pools and acquires a read lock on each.
    /// That seems like far too much effort, so we're using a relaxed increment counter instead.
    /// It's only used for diagnostics.
    global_pool_size: AtomicUsize,

    /// Total number of connections in the pool
    global_connections_count: Arc<AtomicUsize>,

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

impl<C: ClientInnerExt> GlobalConnPool<C> {
    pub fn new(config: &'static crate::config::HttpConfig) -> Arc<Self> {
        let shards = config.pool_options.pool_shards;
        Arc::new(Self {
            global_pool: DashMap::with_shard_amount(shards),
            global_pool_size: AtomicUsize::new(0),
            config,
            global_connections_count: Arc::new(AtomicUsize::new(0)),
        })
    }

    #[cfg(test)]
    pub fn get_global_connections_count(&self) -> usize {
        self.global_connections_count
            .load(atomic::Ordering::Relaxed)
    }

    pub fn get_idle_timeout(&self) -> Duration {
        self.config.pool_options.idle_timeout
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

        let timer = Metrics::get()
            .proxy
            .http_pool_reclaimation_lag_seconds
            .start_timer();
        let current_len = shard.len();
        let mut clients_removed = 0;
        shard.retain(|endpoint, x| {
            // if the current endpoint pool is unique (no other strong or weak references)
            // then it is currently not in use by any connections.
            if let Some(pool) = Arc::get_mut(x.get_mut()) {
                let EndpointConnPool {
                    pools, total_conns, ..
                } = pool.get_mut();

                // ensure that closed clients are removed
                pools.iter_mut().for_each(|(_, db_pool)| {
                    clients_removed += db_pool.clear_closed_clients(total_conns);
                });

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
        timer.observe();

        // Do logging outside of the lock.
        if clients_removed > 0 {
            let size = self
                .global_connections_count
                .fetch_sub(clients_removed, atomic::Ordering::Relaxed)
                - clients_removed;
            Metrics::get()
                .proxy
                .http_pool_opened_connections
                .get_metric()
                .dec_by(clients_removed as i64);
            info!("pool: performed global pool gc. removed {clients_removed} clients, total number of clients in pool is {size}");
        }
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
    ) -> Result<Option<Client<C>>, HttpConnError> {
        let mut client: Option<ClientInner<C>> = None;
        let Some(endpoint) = conn_info.endpoint_cache_key() else {
            return Ok(None);
        };

        let endpoint_pool = self.get_or_create_endpoint_pool(&endpoint);
        if let Some(entry) = endpoint_pool
            .write()
            .get_conn_entry(conn_info.db_and_user(), ctx.session_id)
        {
            client = Some(entry.conn)
        }
        let endpoint_pool = Arc::downgrade(&endpoint_pool);

        // ok return cached connection if found and establish a new one otherwise
        if let Some(client) = client {
            if client.is_closed() {
                info!("pool: cached connection '{conn_info}' is closed, opening a new one");
                return Ok(None);
            } else {
                tracing::Span::current().record("conn_id", tracing::field::display(client.conn_id));
                tracing::Span::current().record(
                    "pid",
                    &tracing::field::display(client.inner.get_process_id()),
                );
                info!(
                    cold_start_info = ColdStartInfo::HttpPoolHit.as_str(),
                    "pool: reusing connection '{conn_info}'"
                );
                client.session.send(ctx.session_id)?;
                ctx.set_cold_start_info(ColdStartInfo::HttpPoolHit);
                ctx.latency_timer.success();
                return Ok(Some(Client::new(client, conn_info.clone(), endpoint_pool)));
            }
        }
        Ok(None)
    }

    fn get_or_create_endpoint_pool(
        self: &Arc<Self>,
        endpoint: &EndpointCacheKey,
    ) -> Arc<RwLock<EndpointConnPool<C>>> {
        // fast path
        if let Some(pool) = self.global_pool.get(endpoint) {
            return pool.clone();
        }

        // slow path
        let new_pool = Arc::new(RwLock::new(EndpointConnPool {
            pools: HashMap::new(),
            total_conns: 0,
            max_conns: self.config.pool_options.max_conns_per_endpoint,
            _guard: Metrics::get().proxy.http_endpoint_pools.guard(),
            global_connections_count: self.global_connections_count.clone(),
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
}

type ConnTypes<C> = dyn pin_list::Types<
    Id = pin_list::id::Checked,
    Protected = ConnPoolEntry<C>,
    // session ID
    Removed = uuid::Uuid,
    // conn ID
    Unprotected = (),
>;

pub async fn poll_tokio_client(
    global_pool: Arc<GlobalConnPool<tokio_postgres::Client>>,
    ctx: &mut RequestMonitoring,
    conn_info: ConnInfo,
    client: tokio_postgres::Client,
    mut connection: tokio_postgres::Connection<Socket, NoTlsStream>,
    conn_id: uuid::Uuid,
    aux: MetricsAuxInfo,
) -> Client<tokio_postgres::Client> {
    let connection = std::future::poll_fn(move |cx| {
        loop {
            let message = ready!(connection.poll_message(cx));
            match message {
                Some(Ok(AsyncMessage::Notice(notice))) => {
                    info!("notice: {}", notice);
                }
                Some(Ok(AsyncMessage::Notification(notif))) => {
                    warn!(
                        pid = notif.process_id(),
                        channel = notif.channel(),
                        "notification received"
                    );
                }
                Some(Ok(_)) => {
                    warn!("unknown message");
                }
                Some(Err(e)) => {
                    error!("connection error: {}", e);
                    break;
                }
                None => {
                    info!("connection closed");
                    break;
                }
            }
        }
        Poll::Ready(())
    });
    poll_client(
        global_pool,
        ctx,
        conn_info,
        client,
        connection,
        conn_id,
        aux,
    )
}

pub fn poll_client<C: ClientInnerExt, I: Future<Output = ()> + Send + 'static>(
    global_pool: Arc<GlobalConnPool<C>>,
    ctx: &mut RequestMonitoring,
    conn_info: ConnInfo,
    client: C,
    connection: I,
    conn_id: uuid::Uuid,
    aux: MetricsAuxInfo,
) -> Client<C> {
    let conn_gauge = Metrics::get().proxy.db_connections.guard(ctx.protocol);
    let session_id = ctx.session_id;
    let (tx, rx) = tokio::sync::watch::channel(session_id);

    let span = info_span!(parent: None, "connection", %conn_id);
    let cold_start_info = ctx.cold_start_info;
    let session_span = info_span!(parent: span.clone(), "", %session_id);
    session_span.in_scope(|| {
        info!(cold_start_info = cold_start_info.as_str(), %conn_info, "new connection");
    });
    let pool = match conn_info.endpoint_cache_key() {
        Some(endpoint) => Arc::downgrade(&global_pool.get_or_create_endpoint_pool(&endpoint)),
        None => Weak::new(),
    };

    let idle = global_pool.get_idle_timeout();
    let cancel = CancellationToken::new();

    let (send_client, recv_client) = tokio::sync::mpsc::channel(1);
    let db_conn = DbConnection {
        cancelled: cancel.clone().cancelled_owned(),

        idle_timeout: tokio::time::sleep(idle),
        idle,

        node: Node::<ConnTypes<C>>::new(),
        recv_client,
        db_user: conn_info.db_and_user(),
        pool: pool.clone(),

        session_span,
        session_rx: rx,

        conn_gauge,
        conn_id,
        connection,
    };

    tokio::spawn(db_conn.instrument(span));

    let inner = ClientInner {
        inner: client,
        session: tx,
        pool: send_client,
        cancel,
        aux,
        conn_id,
    };
    Client::new(inner, conn_info, pool)
}

pin_project! {
    struct DbConnection<C: ClientInnerExt, Inner> {
        // Used to close the current conn if the client is dropped
        #[pin]
        cancelled: WaitForCancellationFutureOwned,

        // Used to close the current conn if it's idle
        #[pin]
        idle_timeout: Sleep,
        idle: tokio::time::Duration,

        // Used to add/remove conn from the conn pool
        #[pin]
        node: Node<ConnTypes<C>>,
        recv_client: tokio::sync::mpsc::Receiver<(tracing::Span, ClientInner<C>, ConnInfo)>,
        db_user: (DbName, RoleName),
        pool: Weak<RwLock<EndpointConnPool<C>>>,

        // Used for reporting the current session the conn is attached to
        session_span: tracing::Span,
        session_rx: tokio::sync::watch::Receiver<uuid::Uuid>,

        // Static connection state
        conn_gauge: NumDbConnectionsGuard<'static>,
        conn_id: uuid::Uuid,
        #[pin]
        connection: Inner,
    }
}

impl<C: ClientInnerExt, I: Future<Output = ()>> Future for DbConnection<C, I> {
    type Output = ();

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        if this.cancelled.as_mut().poll(cx).is_ready() {
            let _span = this.session_span.enter();
            info!("connection dropped");
            return Poll::Ready(());
        }

        if let Poll::Ready(client) = this.recv_client.poll_recv(cx) {
            // if the send_client is dropped, then the client is dropped
            let Some((span, client, conn_info)) = client else {
                info!("connection dropped");
                return Poll::Ready(());
            };
            // if there's no pool, then this client will be closed.
            let Some(pool) = this.pool.upgrade() else {
                info!("connection dropped");
                return Poll::Ready(());
            };

            let _span = span.enter();
            if !EndpointConnPool::put(&*pool, this.node.as_mut(), this.db_user, client, conn_info) {
                return Poll::Ready(());
            }
        }

        match this.session_rx.has_changed() {
            Ok(true) => {
                let session_id = *this.session_rx.borrow_and_update();
                *this.session_span = info_span!("", %session_id);
                let _span = this.session_span.enter();
                info!("changed session");
                this.idle_timeout
                    .as_mut()
                    .reset(Instant::now() + *this.idle);
            }
            Err(_) => {
                let _span = this.session_span.enter();
                info!("connection dropped");
                return Poll::Ready(());
            }
            _ => {}
        }

        let _span = this.session_span.enter();

        // 5 minute idle connection timeout
        if this.idle_timeout.as_mut().poll(cx).is_ready() {
            this.idle_timeout
                .as_mut()
                .reset(Instant::now() + *this.idle);
            info!("connection idle");
            if let Some(pool) = this.pool.upgrade() {
                // remove client from pool - should close the connection if it's idle.
                // does nothing if the client is currently checked-out and in-use
                if pool
                    .write()
                    .remove_client(this.db_user.clone(), *this.conn_id)
                {
                    info!("idle connection removed");
                }
            }
        }

        ready!(this.connection.poll(cx));

        // remove from connection pool
        if let Some(pool) = this.pool.upgrade() {
            if pool
                .write()
                .remove_client(this.db_user.clone(), *this.conn_id)
            {
                info!("closed connection removed");
            }
        }

        Poll::Ready(())
    }
}

struct ClientInner<C: ClientInnerExt> {
    inner: C,
    session: tokio::sync::watch::Sender<uuid::Uuid>,
    pool: tokio::sync::mpsc::Sender<(tracing::Span, ClientInner<C>, ConnInfo)>,
    cancel: CancellationToken,
    aux: MetricsAuxInfo,
    conn_id: uuid::Uuid,
}

impl<C: ClientInnerExt> Drop for ClientInner<C> {
    fn drop(&mut self) {
        // on client drop, tell the conn to shut down
        self.cancel.cancel();
    }
}

pub trait ClientInnerExt: Sync + Send + 'static {
    fn is_closed(&self) -> bool;
    fn get_process_id(&self) -> i32;
}

impl ClientInnerExt for tokio_postgres::Client {
    fn is_closed(&self) -> bool {
        self.is_closed()
    }
    fn get_process_id(&self) -> i32 {
        self.get_process_id()
    }
}

impl<C: ClientInnerExt> ClientInner<C> {
    pub fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }
}

impl<C: ClientInnerExt> Client<C> {
    pub fn metrics(&self) -> Arc<MetricCounter> {
        let aux = &self.inner.as_ref().unwrap().aux;
        USAGE_METRICS.register(Ids {
            endpoint_id: aux.endpoint_id,
            branch_id: aux.branch_id,
        })
    }
}

pub struct Client<C: ClientInnerExt> {
    span: Span,
    inner: Option<ClientInner<C>>,
    conn_info: ConnInfo,
    pool: Weak<RwLock<EndpointConnPool<C>>>,
}

pub struct Discard<'a, C: ClientInnerExt> {
    conn_info: &'a ConnInfo,
    pool: &'a mut Weak<RwLock<EndpointConnPool<C>>>,
}

impl<C: ClientInnerExt> Client<C> {
    pub(self) fn new(
        inner: ClientInner<C>,
        conn_info: ConnInfo,
        pool: Weak<RwLock<EndpointConnPool<C>>>,
    ) -> Self {
        Self {
            inner: Some(inner),
            span: Span::current(),
            conn_info,
            pool,
        }
    }
    pub fn inner(&mut self) -> (&mut C, Discard<'_, C>) {
        let Self {
            inner,
            pool,
            conn_info,
            span: _,
        } = self;
        let inner = inner.as_mut().expect("client inner should not be removed");
        (&mut inner.inner, Discard { pool, conn_info })
    }
}

impl<C: ClientInnerExt> Discard<'_, C> {
    pub fn check_idle(&mut self, status: ReadyForQueryStatus) {
        let conn_info = &self.conn_info;
        if status != ReadyForQueryStatus::Idle && std::mem::take(self.pool).strong_count() > 0 {
            info!("pool: throwing away connection '{conn_info}' because connection is not idle")
        }
    }
    pub fn discard(&mut self) {
        let conn_info = &self.conn_info;
        if std::mem::take(self.pool).strong_count() > 0 {
            info!("pool: throwing away connection '{conn_info}' because connection is potentially in a broken state")
        }
    }
}

impl<C: ClientInnerExt> Deref for Client<C> {
    type Target = C;

    fn deref(&self) -> &Self::Target {
        &self
            .inner
            .as_ref()
            .expect("client inner should not be removed")
            .inner
    }
}

impl<C: ClientInnerExt> Client<C> {
    fn do_drop(&mut self) {
        let conn_info = self.conn_info.clone();
        let client = self
            .inner
            .take()
            .expect("client inner should not be removed");

        let conn_id = client.conn_id;

        if client.is_closed() {
            info!(%conn_id, "pool: throwing away connection '{conn_info}' because connection is closed");
            return;
        }

        let tx = client.pool.clone();
        match tx.try_send((self.span.clone(), client, self.conn_info.clone())) {
            Ok(_) => {}
            Err(TrySendError::Closed(_)) => {}
            Err(TrySendError::Full(_)) => {
                error!("client channel should not be full")
            }
        }
    }
}

impl<C: ClientInnerExt> Drop for Client<C> {
    fn drop(&mut self) {
        self.do_drop()
    }
}

#[cfg(test)]
mod tests {
    use tokio::task::yield_now;

    use crate::{BranchId, EndpointId, ProjectId};

    use super::*;

    struct MockClient(CancellationToken);
    impl ClientInnerExt for MockClient {
        fn is_closed(&self) -> bool {
            self.0.is_cancelled()
        }
        fn get_process_id(&self) -> i32 {
            0
        }
    }

    fn create_inner(
        global_pool: Arc<GlobalConnPool<MockClient>>,
        conn_info: ConnInfo,
    ) -> (Client<MockClient>, CancellationToken) {
        let cancelled = CancellationToken::new();
        let client = poll_client(
            global_pool,
            &mut RequestMonitoring::test(),
            conn_info,
            MockClient(cancelled.clone()),
            cancelled.clone().cancelled_owned(),
            uuid::Uuid::new_v4(),
            MetricsAuxInfo {
                endpoint_id: (&EndpointId::from("endpoint")).into(),
                project_id: (&ProjectId::from("project")).into(),
                branch_id: (&BranchId::from("branch")).into(),
                cold_start_info: crate::console::messages::ColdStartInfo::Warm,
            },
        );
        (client, cancelled)
    }

    #[tokio::test]
    async fn test_pool() {
        let _ = env_logger::try_init();
        let config = Box::leak(Box::new(crate::config::HttpConfig {
            pool_options: GlobalConnPoolOptions {
                max_conns_per_endpoint: 2,
                gc_epoch: Duration::from_secs(1),
                pool_shards: 2,
                idle_timeout: Duration::from_secs(1),
                opt_in: false,
                max_total_conns: 3,
            },
            request_timeout: Duration::from_secs(1),
        }));
        let pool = GlobalConnPool::new(config);
        let conn_info = ConnInfo {
            user_info: ComputeUserInfo {
                user: "user".into(),
                endpoint: "endpoint".into(),
                options: Default::default(),
            },
            dbname: "dbname".into(),
            password: "password".as_bytes().into(),
        };
        {
            let (mut client, _) = create_inner(pool.clone(), conn_info.clone());
            assert_eq!(0, pool.get_global_connections_count());
            client.inner().1.discard();
            drop(client);
            yield_now().await;
            // Discard should not add the connection from the pool.
            assert_eq!(0, pool.get_global_connections_count());
        }
        {
            let (client, _) = create_inner(pool.clone(), conn_info.clone());
            drop(client);
            yield_now().await;
            assert_eq!(1, pool.get_global_connections_count());
        }
        {
            let (client, cancel) = create_inner(pool.clone(), conn_info.clone());
            cancel.cancel();
            drop(client);
            yield_now().await;
            // The closed client shouldn't be added to the pool.
            assert_eq!(1, pool.get_global_connections_count());
        }
        let cancel = {
            let (client, cancel) = create_inner(pool.clone(), conn_info.clone());
            drop(client);
            yield_now().await;
            // The client should be added to the pool.
            assert_eq!(2, pool.get_global_connections_count());
            cancel
        };
        {
            let client = create_inner(pool.clone(), conn_info.clone());
            drop(client);
            yield_now().await;
            // The client shouldn't be added to the pool. Because the ep-pool is full.
            assert_eq!(2, pool.get_global_connections_count());
        }

        let conn_info = ConnInfo {
            user_info: ComputeUserInfo {
                user: "user".into(),
                endpoint: "endpoint-2".into(),
                options: Default::default(),
            },
            dbname: "dbname".into(),
            password: "password".as_bytes().into(),
        };
        {
            let client = create_inner(pool.clone(), conn_info.clone());
            drop(client);
            yield_now().await;
            assert_eq!(3, pool.get_global_connections_count());
        }
        {
            let client = create_inner(pool.clone(), conn_info.clone());
            drop(client);
            yield_now().await;
            // The client shouldn't be added to the pool. Because the global pool is full.
            assert_eq!(3, pool.get_global_connections_count());
        }

        cancel.cancel();
        // Do gc for all shards.
        pool.gc(0);
        pool.gc(1);
        // Closed client should be removed from the pool.
        assert_eq!(2, pool.get_global_connections_count());
    }
}
