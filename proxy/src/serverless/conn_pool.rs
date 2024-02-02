use anyhow::Context;
use async_trait::async_trait;
use dashmap::DashMap;
use futures::{future::poll_fn, Future};
use metrics::{register_int_counter_pair, IntCounterPair, IntCounterPairGuard};
use once_cell::sync::Lazy;
use parking_lot::RwLock;
use pbkdf2::{
    password_hash::{PasswordHashString, PasswordHasher, PasswordVerifier, SaltString},
    Params, Pbkdf2,
};
use prometheus::{exponential_buckets, register_histogram, Histogram};
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
use tokio::time::{self, Instant};
use tokio_postgres::{AsyncMessage, ReadyForQueryStatus};

use crate::{
    auth::{self, backend::ComputeUserInfo, check_peer_addr_is_in_list},
    console::{self, messages::MetricsAuxInfo},
    context::RequestMonitoring,
    metrics::NUM_DB_CONNECTIONS_GAUGE,
    proxy::connect_compute::ConnectMechanism,
    usage_metrics::{Ids, MetricCounter, USAGE_METRICS},
    DbName, EndpointCacheKey, RoleName,
};
use crate::{compute, config};

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
            *total_conns -= removed;
            removed > 0
        } else {
            false
        }
    }

    fn put(pool: &RwLock<Self>, conn_info: &ConnInfo, client: ClientInner) -> anyhow::Result<()> {
        let conn_id = client.conn_id;

        if client.inner.is_closed() {
            info!(%conn_id, "pool: throwing away connection '{conn_info}' because connection is closed");
            return Ok(());
        }

        // return connection to the pool
        let mut returned = false;
        let mut per_db_size = 0;
        let total_conns = {
            let mut pool = pool.write();

            if pool.total_conns < pool.max_conns {
                // we create this db-user entry in get, so it should not be None
                if let Some(pool_entries) = pool.pools.get_mut(&conn_info.db_and_user()) {
                    pool_entries.conns.push(ConnPoolEntry {
                        conn: client,
                        _last_access: std::time::Instant::now(),
                    });

                    returned = true;
                    per_db_size = pool_entries.conns.len();

                    pool.total_conns += 1;
                }
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

/// 4096 is the number of rounds that SCRAM-SHA-256 recommends.
/// It's not the 600,000 that OWASP recommends... but our passwords are high entropy anyway.
///
/// Still takes 1.4ms to hash on my hardware.
/// We don't want to ruin the latency improvements of using the pool by making password verification take too long
const PARAMS: Params = Params {
    rounds: 4096,
    output_length: 32,
};

#[derive(Default)]
pub struct DbUserConnPool {
    conns: Vec<ConnPoolEntry>,
    password_hash: Option<PasswordHashString>,
}

impl DbUserConnPool {
    fn clear_closed_clients(&mut self, conns: &mut usize) {
        let old_len = self.conns.len();

        self.conns.retain(|conn| !conn.conn.inner.is_closed());

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

    proxy_config: &'static crate::config::ProxyConfig,
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
}

pub static GC_LATENCY: Lazy<Histogram> = Lazy::new(|| {
    register_histogram!(
        "proxy_http_pool_reclaimation_lag_seconds",
        "Time it takes to reclaim unused connection pools",
        // 1us -> 65ms
        exponential_buckets(1e-6, 2.0, 16).unwrap(),
    )
    .unwrap()
});

pub static ENDPOINT_POOLS: Lazy<IntCounterPair> = Lazy::new(|| {
    register_int_counter_pair!(
        "proxy_http_pool_endpoints_registered_total",
        "Number of endpoints we have registered pools for",
        "proxy_http_pool_endpoints_unregistered_total",
        "Number of endpoints we have unregistered pools for",
    )
    .unwrap()
});

impl GlobalConnPool {
    pub fn new(config: &'static crate::config::ProxyConfig) -> Arc<Self> {
        let shards = config.http_config.pool_options.pool_shards;
        Arc::new(Self {
            global_pool: DashMap::with_shard_amount(shards),
            global_pool_size: AtomicUsize::new(0),
            proxy_config: config,
        })
    }

    pub fn shutdown(&self) {
        // drops all strong references to endpoint-pools
        self.global_pool.clear();
    }

    pub async fn gc_worker(&self, mut rng: impl Rng) {
        let epoch = self.proxy_config.http_config.pool_options.gc_epoch;
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
        conn_info: ConnInfo,
        force_new: bool,
    ) -> anyhow::Result<Client> {
        let mut client: Option<ClientInner> = None;

        let mut hash_valid = false;
        let mut endpoint_pool = Weak::new();
        if !force_new {
            let pool = self.get_or_create_endpoint_pool(&conn_info.endpoint_cache_key());
            endpoint_pool = Arc::downgrade(&pool);
            let mut hash = None;

            // find a pool entry by (dbname, username) if exists
            {
                let pool = pool.read();
                if let Some(pool_entries) = pool.pools.get(&conn_info.db_and_user()) {
                    if !pool_entries.conns.is_empty() {
                        hash = pool_entries.password_hash.clone();
                    }
                }
            }

            // a connection exists in the pool, verify the password hash
            if let Some(hash) = hash {
                let pw = conn_info.password.clone();
                let validate = tokio::task::spawn_blocking(move || {
                    Pbkdf2.verify_password(pw.as_bytes(), &hash.password_hash())
                })
                .await?;

                // if the hash is invalid, don't error
                // we will continue with the regular connection flow
                if validate.is_ok() {
                    hash_valid = true;
                    if let Some(entry) = pool.write().get_conn_entry(conn_info.db_and_user()) {
                        client = Some(entry.conn)
                    }
                }
            }
        }

        // ok return cached connection if found and establish a new one otherwise
        let new_client = if let Some(client) = client {
            ctx.set_project(client.aux.clone());
            if client.inner.is_closed() {
                let conn_id = uuid::Uuid::new_v4();
                info!(%conn_id, "pool: cached connection '{conn_info}' is closed, opening a new one");
                connect_to_compute(
                    self.proxy_config,
                    ctx,
                    &conn_info,
                    conn_id,
                    endpoint_pool.clone(),
                )
                .await
            } else {
                info!("pool: reusing connection '{conn_info}'");
                client.session.send(ctx.session_id)?;
                tracing::Span::current().record(
                    "pid",
                    &tracing::field::display(client.inner.get_process_id()),
                );
                ctx.latency_timer.pool_hit();
                ctx.latency_timer.success();
                return Ok(Client::new(client, conn_info, endpoint_pool).await);
            }
        } else {
            let conn_id = uuid::Uuid::new_v4();
            info!(%conn_id, "pool: opening a new connection '{conn_info}'");
            connect_to_compute(
                self.proxy_config,
                ctx,
                &conn_info,
                conn_id,
                endpoint_pool.clone(),
            )
            .await
        };
        if let Ok(client) = &new_client {
            tracing::Span::current().record(
                "pid",
                &tracing::field::display(client.inner.get_process_id()),
            );
        }

        match &new_client {
            // clear the hash. it's no longer valid
            // TODO: update tokio-postgres fork to allow access to this error kind directly
            Err(err)
                if hash_valid && err.to_string().contains("password authentication failed") =>
            {
                let pool = self.get_or_create_endpoint_pool(&conn_info.endpoint_cache_key());
                let mut pool = pool.write();
                if let Some(entry) = pool.pools.get_mut(&conn_info.db_and_user()) {
                    entry.password_hash = None;
                }
            }
            // new password is valid and we should insert/update it
            Ok(_) if !force_new && !hash_valid => {
                let pw = conn_info.password.clone();
                let new_hash = tokio::task::spawn_blocking(move || {
                    let salt = SaltString::generate(rand::rngs::OsRng);
                    Pbkdf2
                        .hash_password_customized(pw.as_bytes(), None, None, PARAMS, &salt)
                        .map(|s| s.serialize())
                })
                .await??;

                let pool = self.get_or_create_endpoint_pool(&conn_info.endpoint_cache_key());
                let mut pool = pool.write();
                pool.pools
                    .entry(conn_info.db_and_user())
                    .or_default()
                    .password_hash = Some(new_hash);
            }
            _ => {}
        }
        let new_client = new_client?;
        Ok(Client::new(new_client, conn_info, endpoint_pool).await)
    }

    fn get_or_create_endpoint_pool(
        &self,
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
            max_conns: self
                .proxy_config
                .http_config
                .pool_options
                .max_conns_per_endpoint,
            _guard: ENDPOINT_POOLS.guard(),
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

struct TokioMechanism<'a> {
    pool: Weak<RwLock<EndpointConnPool>>,
    conn_info: &'a ConnInfo,
    conn_id: uuid::Uuid,
    idle: Duration,
}

#[async_trait]
impl ConnectMechanism for TokioMechanism<'_> {
    type Connection = ClientInner;
    type ConnectError = tokio_postgres::Error;
    type Error = anyhow::Error;

    async fn connect_once(
        &self,
        ctx: &mut RequestMonitoring,
        node_info: &console::CachedNodeInfo,
        timeout: time::Duration,
    ) -> Result<Self::Connection, Self::ConnectError> {
        connect_to_compute_once(
            ctx,
            node_info,
            self.conn_info,
            timeout,
            self.conn_id,
            self.pool.clone(),
            self.idle,
        )
        .await
    }

    fn update_connect_config(&self, _config: &mut compute::ConnCfg) {}
}

// Wake up the destination if needed. Code here is a bit involved because
// we reuse the code from the usual proxy and we need to prepare few structures
// that this code expects.
#[tracing::instrument(fields(pid = tracing::field::Empty), skip_all)]
async fn connect_to_compute(
    config: &config::ProxyConfig,
    ctx: &mut RequestMonitoring,
    conn_info: &ConnInfo,
    conn_id: uuid::Uuid,
    pool: Weak<RwLock<EndpointConnPool>>,
) -> anyhow::Result<ClientInner> {
    ctx.set_application(Some(APP_NAME));
    let backend = config
        .auth_backend
        .as_ref()
        .map(|_| conn_info.user_info.clone());

    if !config.disable_ip_check_for_http {
        let (allowed_ips, _) = backend.get_allowed_ips_and_secret(ctx).await?;
        if !check_peer_addr_is_in_list(&ctx.peer_addr, &allowed_ips) {
            return Err(auth::AuthError::ip_address_not_allowed().into());
        }
    }
    let node_info = backend
        .wake_compute(ctx)
        .await?
        .context("missing cache entry from wake_compute")?;

    ctx.set_project(node_info.aux.clone());

    crate::proxy::connect_compute::connect_to_compute(
        ctx,
        &TokioMechanism {
            conn_id,
            conn_info,
            pool,
            idle: config.http_config.pool_options.idle_timeout,
        },
        node_info,
        &backend,
    )
    .await
}

async fn connect_to_compute_once(
    ctx: &mut RequestMonitoring,
    node_info: &console::CachedNodeInfo,
    conn_info: &ConnInfo,
    timeout: time::Duration,
    conn_id: uuid::Uuid,
    pool: Weak<RwLock<EndpointConnPool>>,
    idle: Duration,
) -> Result<ClientInner, tokio_postgres::Error> {
    let mut config = (*node_info.config).clone();
    let mut session = ctx.session_id;

    let (client, mut connection) = config
        .user(&conn_info.user_info.user)
        .password(&*conn_info.password)
        .dbname(&conn_info.dbname)
        .connect_timeout(timeout)
        .connect(tokio_postgres::NoTls)
        .await?;

    let conn_gauge = NUM_DB_CONNECTIONS_GAUGE
        .with_label_values(&[ctx.protocol])
        .guard();

    tracing::Span::current().record("pid", &tracing::field::display(client.get_process_id()));

    let (tx, mut rx) = tokio::sync::watch::channel(session);

    let span = info_span!(parent: None, "connection", %conn_id);
    span.in_scope(|| {
        info!(%conn_info, %session, "new connection");
    });

    let db_user = conn_info.db_and_user();
    tokio::spawn(
        async move {
            let _conn_gauge = conn_gauge;
            let mut idle_timeout = pin!(tokio::time::sleep(idle));
            poll_fn(move |cx| {
                if matches!(rx.has_changed(), Ok(true)) {
                    session = *rx.borrow_and_update();
                    info!(%session, "changed session");
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
                            info!(%session, "notice: {}", notice);
                        }
                        Some(Ok(AsyncMessage::Notification(notif))) => {
                            warn!(%session, pid = notif.process_id(), channel = notif.channel(), "notification received");
                        }
                        Some(Ok(_)) => {
                            warn!(%session, "unknown message");
                        }
                        Some(Err(e)) => {
                            error!(%session, "connection error: {}", e);
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
        .instrument(span)
    );

    Ok(ClientInner {
        inner: client,
        session: tx,
        aux: node_info.aux.clone(),
        conn_id,
    })
}

struct ClientInner {
    inner: tokio_postgres::Client,
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
    conn_id: uuid::Uuid,
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
    pub(self) async fn new(
        inner: ClientInner,
        conn_info: ConnInfo,
        pool: Weak<RwLock<EndpointConnPool>>,
    ) -> Self {
        Self {
            conn_id: inner.conn_id,
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
            conn_id,
            conn_info,
            span: _,
        } = self;
        (
            &mut inner
                .as_mut()
                .expect("client inner should not be removed")
                .inner,
            Discard {
                pool,
                conn_info,
                conn_id: *conn_id,
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
            .inner
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
