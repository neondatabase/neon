use dashmap::DashMap;
use futures::Future;
use parking_lot::RwLock;
use pin_list::{InitializedNode, Node};
use pin_project_lite::pin_project;
use rand::Rng;
use smallvec::SmallVec;
use std::pin::Pin;
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
use tokio::time::Sleep;
use tokio_postgres::tls::NoTlsStream;
use tokio_postgres::{AsyncMessage, ReadyForQueryStatus, Socket};

use crate::console::messages::{ColdStartInfo, MetricsAuxInfo};
use crate::metrics::{HttpEndpointPoolsGuard, Metrics, NumDbConnectionsGuard};
use crate::usage_metrics::{Ids, MetricCounter, USAGE_METRICS};
use crate::{
    auth::backend::ComputeUserInfo, context::RequestMonitoring, DbName, EndpointCacheKey, RoleName,
};

use tracing::{debug, error, warn};
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

    fn remove_client<'a>(
        &mut self,
        db_user: (DbName, RoleName),
        node: Pin<&'a mut InitializedNode<'a, ConnTypes<C>>>,
    ) -> bool {
        let Self {
            pools,
            total_conns,
            global_connections_count,
            ..
        } = self;
        if let Some(pool) = pools.get_mut(&db_user) {
            if node.unlink(&mut pool.conns).is_ok() {
                global_connections_count.fetch_sub(1, atomic::Ordering::Relaxed);
                Metrics::get()
                    .proxy
                    .http_pool_opened_connections
                    .get_metric()
                    .dec_by(1);
                *total_conns -= 1;
                true
            } else {
                false
            }
        } else {
            false
        }
    }

    fn put(
        pool: &RwLock<Self>,
        node: Pin<&mut Node<ConnTypes<C>>>,
        db_user: &(DbName, RoleName),
        client: ClientInner<C>,
    ) -> bool {
        {
            let pool = pool.read();
            if pool
                .global_connections_count
                .load(atomic::Ordering::Relaxed)
                >= pool.global_pool_size_max_conns
            {
                info!("pool: throwing away connection because pool is full");
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
            info!("pool: returning connection back to the pool, total_conns={total_conns}, for this (db, user)={per_db_size}");
        } else {
            info!("pool: throwing away connection because pool is full, total_conns={total_conns}");
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
    fn get_conn_entry(
        &mut self,
        conns: &mut usize,
        global_connections_count: &AtomicUsize,
        session_id: uuid::Uuid,
    ) -> Option<ConnPoolEntry<C>> {
        let conn = self
            .conns
            .cursor_front_mut()
            .remove_current(session_id)
            .ok()?;

        *conns -= 1;
        global_connections_count.fetch_sub(1, atomic::Ordering::Relaxed);
        Metrics::get().proxy.http_pool_opened_connections.dec_by(1);

        Some(conn)
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
        shard.retain(|endpoint, x| {
            // if the current endpoint pool is unique (no other strong or weak references)
            // then it is currently not in use by any connections.
            if let Some(pool) = Arc::get_mut(x.get_mut()) {
                let EndpointConnPool { total_conns, .. } = pool.get_mut();

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

        // ok return cached connection if found and establish a new one otherwise
        if let Some(client) = client {
            tracing::Span::current().record("conn_id", tracing::field::display(client.conn_id));
            tracing::Span::current().record(
                "pid",
                &tracing::field::display(client.inner.get_process_id()),
            );
            info!(
                cold_start_info = ColdStartInfo::HttpPoolHit.as_str(),
                "pool: reusing connection '{conn_info}'"
            );
            ctx.set_cold_start_info(ColdStartInfo::HttpPoolHit);
            ctx.latency_timer.success();
            return Ok(Some(Client::new(client)));
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
    Unprotected = (),
>;

pub fn poll_tokio_client(
    global_pool: Arc<GlobalConnPool<tokio_postgres::Client>>,
    ctx: &mut RequestMonitoring,
    conn_info: &ConnInfo,
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
    conn_info: &ConnInfo,
    client: C,
    connection: I,
    conn_id: uuid::Uuid,
    aux: MetricsAuxInfo,
) -> Client<C> {
    let conn_gauge = Metrics::get().proxy.db_connections.guard(ctx.protocol);
    let session_id = ctx.session_id;

    let span = info_span!(parent: None, "connection", %conn_id);
    let cold_start_info = ctx.cold_start_info;
    let session_span = info_span!(parent: span.clone(), "", %session_id);
    session_span.in_scope(|| {
        info!(cold_start_info = cold_start_info.as_str(), %conn_info, "new connection");
    });

    let pool = conn_info
        .endpoint_cache_key()
        .map(|endpoint| global_pool.get_or_create_endpoint_pool(&endpoint));

    let idle = global_pool.get_idle_timeout();

    let (send_client, recv_client) = tokio::sync::mpsc::channel(1);
    let db_conn = DbConnection {
        idle_timeout: None,
        idle,

        node: Node::<ConnTypes<C>>::new(),
        recv_client,
        db_user: conn_info.db_and_user(),
        pool,

        session_span,

        conn_gauge,
        connection,
    };

    tokio::spawn(db_conn.instrument(span));

    let inner = ClientInner {
        inner: client,
        pool: send_client,
        aux,
        conn_id,
    };
    Client::new(inner)
}

pin_project! {
    struct DbConnection<C: ClientInnerExt, Inner> {
        // Used to close the current conn if it's idle
        #[pin]
        idle_timeout: Option<Sleep>,
        idle: tokio::time::Duration,

        // Used to add/remove conn from the conn pool
        #[pin]
        node: Node<ConnTypes<C>>,
        recv_client: tokio::sync::mpsc::Receiver<ClientInner<C>>,
        db_user: (DbName, RoleName),
        pool: Option<Arc<RwLock<EndpointConnPool<C>>>>,

        // Used for reporting the current session the conn is attached to
        session_span: tracing::Span,

        // Static connection state
        conn_gauge: NumDbConnectionsGuard<'static>,
        #[pin]
        connection: Inner,
    }

    impl<C: ClientInnerExt, I> PinnedDrop for DbConnection<C, I> {
        fn drop(this: Pin<&mut Self>) {
            let mut this = this.project();
            let Some(init) = this.node.as_mut().initialized_mut() else {  return };
            let pool = this.pool.as_ref().expect("pool must be set if the node is initialsed in the pool");
            if pool.write().remove_client(this.db_user.clone(), init) {
                info!("closed connection removed");
            }
        }
    }
}

impl<C: ClientInnerExt, I: Future<Output = ()>> Future for DbConnection<C, I> {
    type Output = ();

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();

        // Update the session span.
        // If the node is initialised, then it is either
        // 1. Waiting in the idle pool
        // 2. Just removed from the idle pool and this is our first wake up.
        //
        // In the event of 1, nothing happens. (should not have many wakeups while idle)
        // In the event of 2, we remove the session_id that was left in it's place.
        if let Some(init) = this.node.as_mut().initialized_mut() {
            // node is initiated via EndpointConnPool::put.
            // this is only called in the if statement below.
            // this can only occur if pool is set (and pool is never removed).
            // when this occurs, it guarantees that the DbUserConnPool is created (it is never removed).
            let pool = this
                .pool
                .as_ref()
                .expect("node cannot be init without pool");

            let mut pool_lock = pool.write();
            let db_pool = pool_lock
                .pools
                .get(this.db_user)
                .expect("node cannot be init without pool");

            match init.take_removed(&db_pool.conns) {
                Ok((session_id, _)) => {
                    *this.session_span = info_span!("", %session_id);
                    let _span = this.session_span.enter();
                    info!("changed session");

                    // this connection is no longer idle
                    this.idle_timeout.set(None);
                }
                Err(init) => {
                    let idle = this
                        .idle_timeout
                        .as_mut()
                        .as_pin_mut()
                        .expect("timer must be set if node is init");

                    if idle.poll(cx).is_ready() {
                        info!("connection idle");

                        // remove client from pool - should close the connection if it's idle.
                        // does nothing if the client is currently checked-out and in-use
                        if pool_lock.remove_client(this.db_user.clone(), init) {
                            info!("closed connection removed");
                        }
                    }
                }
            }
        }

        let _span = this.session_span.enter();

        // The client has been returned. We will insert it into the linked list for this database.
        if let Poll::Ready(client) = this.recv_client.poll_recv(cx) {
            // if the send_client is dropped, then the client is dropped
            let Some(client) = client else {
                info!("connection dropped");
                return Poll::Ready(());
            };
            // if there's no pool, then this client will be closed.
            let Some(pool) = &this.pool else {
                info!("connection dropped");
                return Poll::Ready(());
            };

            if !EndpointConnPool::put(pool, this.node.as_mut(), this.db_user, client) {
                return Poll::Ready(());
            }

            // this connection is now idle
            this.idle_timeout.set(Some(tokio::time::sleep(*this.idle)));
        }

        this.connection.poll(cx)
    }
}

struct ClientInner<C: ClientInnerExt> {
    inner: C,
    pool: tokio::sync::mpsc::Sender<ClientInner<C>>,
    aux: MetricsAuxInfo,
    conn_id: uuid::Uuid,
}

pub trait ClientInnerExt: Sync + Send + 'static {
    fn get_process_id(&self) -> i32;
}

impl ClientInnerExt for tokio_postgres::Client {
    fn get_process_id(&self) -> i32 {
        self.get_process_id()
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
    inner: Option<ClientInner<C>>,
    discarded: bool,
}

pub struct Discard<'a> {
    conn_id: uuid::Uuid,
    discarded: &'a mut bool,
}

impl<C: ClientInnerExt> Client<C> {
    pub(self) fn new(inner: ClientInner<C>) -> Self {
        Self {
            inner: Some(inner),
            discarded: false,
        }
    }
    pub fn inner(&mut self) -> (&mut C, Discard<'_>) {
        let Self { inner, discarded } = self;
        let inner = inner.as_mut().expect("client inner should not be removed");
        let conn_id = inner.conn_id;
        (&mut inner.inner, Discard { discarded, conn_id })
    }
}

impl Discard<'_> {
    pub fn check_idle(&mut self, status: ReadyForQueryStatus) {
        let conn_id = &self.conn_id;
        if status != ReadyForQueryStatus::Idle && !*self.discarded {
            *self.discarded = true;
            info!(%conn_id, "pool: throwing away connection because connection is not idle")
        }
    }
    pub fn discard(&mut self) {
        let conn_id = &self.conn_id;
        *self.discarded = true;
        info!(%conn_id, "pool: throwing away connection because connection is potentially in a broken state")
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

impl<C: ClientInnerExt> Drop for Client<C> {
    fn drop(&mut self) {
        let client = self
            .inner
            .take()
            .expect("client inner should not be removed");

        if self.discarded {
            return;
        }

        let conn_id = client.conn_id;

        let tx = client.pool.clone();
        match tx.try_send(client) {
            Ok(_) => {}
            Err(TrySendError::Closed(_)) => {
                info!(%conn_id, "pool: throwing away connection because connection is closed");
            }
            Err(TrySendError::Full(_)) => {
                error!("client channel should not be full")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use tokio::task::yield_now;
    use tokio_util::sync::CancellationToken;

    use crate::{BranchId, EndpointId, ProjectId};

    use super::*;

    struct MockClient;
    impl ClientInnerExt for MockClient {
        fn get_process_id(&self) -> i32 {
            0
        }
    }

    fn create_inner(
        global_pool: Arc<GlobalConnPool<MockClient>>,
        conn_info: &ConnInfo,
    ) -> (Client<MockClient>, CancellationToken) {
        let cancelled = CancellationToken::new();
        let client = poll_client(
            global_pool,
            &mut RequestMonitoring::test(),
            conn_info,
            MockClient,
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
            let (mut client, _) = create_inner(pool.clone(), &conn_info);
            assert_eq!(0, pool.get_global_connections_count());
            client.inner().1.discard();
            drop(client);
            yield_now().await;
            // Discard should not add the connection from the pool.
            assert_eq!(0, pool.get_global_connections_count());
        }
        {
            let (client, _) = create_inner(pool.clone(), &conn_info);
            drop(client);
            yield_now().await;
            assert_eq!(1, pool.get_global_connections_count());
        }
        {
            let (client, cancel) = create_inner(pool.clone(), &conn_info);
            cancel.cancel();
            drop(client);
            yield_now().await;
            // The closed client shouldn't be added to the pool.
            assert_eq!(1, pool.get_global_connections_count());
        }
        let cancel = {
            let (client, cancel) = create_inner(pool.clone(), &conn_info);
            drop(client);
            yield_now().await;
            // The client should be added to the pool.
            assert_eq!(2, pool.get_global_connections_count());
            cancel
        };
        {
            let client = create_inner(pool.clone(), &conn_info);
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
            let client = create_inner(pool.clone(), &conn_info);
            drop(client);
            yield_now().await;
            assert_eq!(3, pool.get_global_connections_count());
        }
        {
            let client = create_inner(pool.clone(), &conn_info);
            drop(client);
            yield_now().await;
            // The client shouldn't be added to the pool. Because the global pool is full.
            assert_eq!(3, pool.get_global_connections_count());
        }

        cancel.cancel();
        yield_now().await;
        // Do gc for all shards.
        pool.gc(0);
        pool.gc(1);
        // Closed client should be removed from the pool.
        assert_eq!(2, pool.get_global_connections_count());
    }
}
