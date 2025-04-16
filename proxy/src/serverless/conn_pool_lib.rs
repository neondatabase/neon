use std::collections::HashMap;
use std::ops::Deref;
use std::sync::atomic::{self, AtomicUsize};
use std::sync::{Arc, Weak};
use std::time::Duration;

use clashmap::ClashMap;
use parking_lot::RwLock;
use postgres_client::ReadyForQueryStatus;
use rand::Rng;
use smol_str::ToSmolStr;
use tracing::{Span, debug, info};

use super::conn_pool::ClientDataRemote;
use super::http_conn_pool::ClientDataHttp;
use super::local_conn_pool::ClientDataLocal;
use crate::auth::backend::ComputeUserInfo;
use crate::config::HttpConfig;
use crate::context::RequestContext;
use crate::control_plane::messages::{ColdStartInfo, MetricsAuxInfo};
use crate::metrics::{HttpEndpointPoolsGuard, Metrics};
use crate::protocol2::ConnectionInfoExtra;
use crate::types::{DbName, EndpointCacheKey, RoleName};
use crate::usage_metrics::{Ids, MetricCounter, USAGE_METRICS};

#[derive(Debug, Clone)]
pub(crate) struct ConnInfo {
    pub(crate) user_info: ComputeUserInfo,
    pub(crate) dbname: DbName,
}

impl ConnInfo {
    // hm, change to hasher to avoid cloning?
    pub(crate) fn db_and_user(&self) -> (DbName, RoleName) {
        (self.dbname.clone(), self.user_info.user.clone())
    }

    pub(crate) fn endpoint_cache_key(&self) -> Option<EndpointCacheKey> {
        // We don't want to cache http connections for ephemeral endpoints.
        if self.user_info.options.is_ephemeral() {
            None
        } else {
            Some(self.user_info.endpoint_cache_key())
        }
    }
}

#[derive(Clone)]
#[allow(clippy::large_enum_variant, reason = "TODO")]
pub(crate) enum ClientDataEnum {
    Remote(ClientDataRemote),
    Local(ClientDataLocal),
    Http(ClientDataHttp),
}

#[derive(Clone)]
pub(crate) struct ClientInnerCommon<C: ClientInnerExt> {
    pub(crate) inner: C,
    pub(crate) aux: MetricsAuxInfo,
    pub(crate) conn_id: uuid::Uuid,
    pub(crate) data: ClientDataEnum, // custom client data like session, key, jti
}

impl<C: ClientInnerExt> Drop for ClientInnerCommon<C> {
    fn drop(&mut self) {
        match &mut self.data {
            ClientDataEnum::Remote(remote_data) => {
                remote_data.cancel();
            }
            ClientDataEnum::Local(local_data) => {
                local_data.cancel();
            }
            ClientDataEnum::Http(_http_data) => (),
        }
    }
}

impl<C: ClientInnerExt> ClientInnerCommon<C> {
    pub(crate) fn get_conn_id(&self) -> uuid::Uuid {
        self.conn_id
    }

    pub(crate) fn get_data(&self) -> &ClientDataEnum {
        &self.data
    }
}

pub(crate) struct ConnPoolEntry<C: ClientInnerExt> {
    pub(crate) conn: ClientInnerCommon<C>,
    pub(crate) _last_access: std::time::Instant,
}

// Per-endpoint connection pool, (dbname, username) -> DbUserConnPool
// Number of open connections is limited by the `max_conns_per_endpoint`.
pub(crate) struct EndpointConnPool<C: ClientInnerExt> {
    pools: HashMap<(DbName, RoleName), DbUserConnPool<C>>,
    total_conns: usize,
    /// max # connections per endpoint
    max_conns: usize,
    _guard: HttpEndpointPoolsGuard<'static>,
    global_connections_count: Arc<AtomicUsize>,
    global_pool_size_max_conns: usize,
    pool_name: String,
}

impl<C: ClientInnerExt> EndpointConnPool<C> {
    pub(crate) fn new(
        hmap: HashMap<(DbName, RoleName), DbUserConnPool<C>>,
        tconns: usize,
        max_conns_per_endpoint: usize,
        global_connections_count: Arc<AtomicUsize>,
        max_total_conns: usize,
        pname: String,
    ) -> Self {
        Self {
            pools: hmap,
            total_conns: tconns,
            max_conns: max_conns_per_endpoint,
            _guard: Metrics::get().proxy.http_endpoint_pools.guard(),
            global_connections_count,
            global_pool_size_max_conns: max_total_conns,
            pool_name: pname,
        }
    }

    pub(crate) fn get_conn_entry(
        &mut self,
        db_user: (DbName, RoleName),
    ) -> Option<ConnPoolEntry<C>> {
        let Self {
            pools,
            total_conns,
            global_connections_count,
            ..
        } = self;
        pools.get_mut(&db_user).and_then(|pool_entries| {
            let (entry, removed) = pool_entries.get_conn_entry(total_conns);
            global_connections_count.fetch_sub(removed, atomic::Ordering::Relaxed);
            entry
        })
    }

    pub(crate) fn remove_client(
        &mut self,
        db_user: (DbName, RoleName),
        conn_id: uuid::Uuid,
    ) -> bool {
        let Self {
            pools,
            total_conns,
            global_connections_count,
            ..
        } = self;
        if let Some(pool) = pools.get_mut(&db_user) {
            let old_len = pool.get_conns().len();
            pool.get_conns()
                .retain(|conn| conn.conn.get_conn_id() != conn_id);
            let new_len = pool.get_conns().len();
            let removed = old_len - new_len;
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

    pub(crate) fn get_name(&self) -> &str {
        &self.pool_name
    }

    pub(crate) fn get_pool(&self, db_user: (DbName, RoleName)) -> Option<&DbUserConnPool<C>> {
        self.pools.get(&db_user)
    }

    pub(crate) fn get_pool_mut(
        &mut self,
        db_user: (DbName, RoleName),
    ) -> Option<&mut DbUserConnPool<C>> {
        self.pools.get_mut(&db_user)
    }

    pub(crate) fn put(pool: &RwLock<Self>, conn_info: &ConnInfo, client: ClientInnerCommon<C>) {
        let conn_id = client.get_conn_id();
        let (max_conn, conn_count, pool_name) = {
            let pool = pool.read();
            (
                pool.global_pool_size_max_conns,
                pool.global_connections_count
                    .load(atomic::Ordering::Relaxed),
                pool.get_name().to_string(),
            )
        };

        if client.inner.is_closed() {
            info!(%conn_id, "{}: throwing away connection '{conn_info}' because connection is closed", pool_name);
            return;
        }

        if conn_count >= max_conn {
            info!(%conn_id, "{}: throwing away connection '{conn_info}' because pool is full", pool_name);
            return;
        }

        // return connection to the pool
        let mut returned = false;
        let mut per_db_size = 0;
        let total_conns = {
            let mut pool = pool.write();

            if pool.total_conns < pool.max_conns {
                let pool_entries = pool.pools.entry(conn_info.db_and_user()).or_default();
                pool_entries.get_conns().push(ConnPoolEntry {
                    conn: client,
                    _last_access: std::time::Instant::now(),
                });

                returned = true;
                per_db_size = pool_entries.get_conns().len();

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
            debug!(%conn_id, "{pool_name}: returning connection '{conn_info}' back to the pool, total_conns={total_conns}, for this (db, user)={per_db_size}");
        } else {
            info!(%conn_id, "{pool_name}: throwing away connection '{conn_info}' because pool is full, total_conns={total_conns}");
        }
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

pub(crate) struct DbUserConnPool<C: ClientInnerExt> {
    pub(crate) conns: Vec<ConnPoolEntry<C>>,
    pub(crate) initialized: Option<bool>, // a bit ugly, exists only for local pools
}

impl<C: ClientInnerExt> Default for DbUserConnPool<C> {
    fn default() -> Self {
        Self {
            conns: Vec::new(),
            initialized: None,
        }
    }
}

pub(crate) trait DbUserConn<C: ClientInnerExt>: Default {
    fn set_initialized(&mut self);
    fn is_initialized(&self) -> bool;
    fn clear_closed_clients(&mut self, conns: &mut usize) -> usize;
    fn get_conn_entry(&mut self, conns: &mut usize) -> (Option<ConnPoolEntry<C>>, usize);
    fn get_conns(&mut self) -> &mut Vec<ConnPoolEntry<C>>;
}

impl<C: ClientInnerExt> DbUserConn<C> for DbUserConnPool<C> {
    fn set_initialized(&mut self) {
        self.initialized = Some(true);
    }

    fn is_initialized(&self) -> bool {
        self.initialized.unwrap_or(false)
    }

    fn clear_closed_clients(&mut self, conns: &mut usize) -> usize {
        let old_len = self.conns.len();

        self.conns.retain(|conn| !conn.conn.inner.is_closed());

        let new_len = self.conns.len();
        let removed = old_len - new_len;
        *conns -= removed;
        removed
    }

    fn get_conn_entry(&mut self, conns: &mut usize) -> (Option<ConnPoolEntry<C>>, usize) {
        let mut removed = self.clear_closed_clients(conns);
        let conn = self.conns.pop();
        if conn.is_some() {
            *conns -= 1;
            removed += 1;
        }

        Metrics::get()
            .proxy
            .http_pool_opened_connections
            .get_metric()
            .dec_by(removed as i64);

        (conn, removed)
    }

    fn get_conns(&mut self) -> &mut Vec<ConnPoolEntry<C>> {
        &mut self.conns
    }
}

pub(crate) trait EndpointConnPoolExt {
    type Client;
    type ClientInner: ClientInnerExt;

    fn create(config: &HttpConfig, global_connections_count: Arc<AtomicUsize>) -> Self;

    fn clear_closed(&mut self) -> usize;
    fn total_conns(&self) -> usize;
}

impl<C: ClientInnerExt> EndpointConnPoolExt for EndpointConnPool<C> {
    type Client = Client<C>;
    type ClientInner = C;

    fn create(config: &HttpConfig, global_connections_count: Arc<AtomicUsize>) -> Self {
        EndpointConnPool {
            pools: HashMap::new(),
            total_conns: 0,
            max_conns: config.pool_options.max_conns_per_endpoint,
            _guard: Metrics::get().proxy.http_endpoint_pools.guard(),
            global_connections_count,
            global_pool_size_max_conns: config.pool_options.max_total_conns,
            pool_name: String::from("remote"),
        }
    }

    fn clear_closed(&mut self) -> usize {
        let mut clients_removed: usize = 0;
        for db_pool in self.pools.values_mut() {
            clients_removed += db_pool.clear_closed_clients(&mut self.total_conns);
        }
        clients_removed
    }

    fn total_conns(&self) -> usize {
        self.total_conns
    }
}

pub(crate) struct GlobalConnPool<P>
where
    P: EndpointConnPoolExt,
{
    // endpoint -> per-endpoint connection pool
    //
    // That should be a fairly conteded map, so return reference to the per-endpoint
    // pool as early as possible and release the lock.
    pub(crate) global_pool: ClashMap<EndpointCacheKey, Arc<RwLock<P>>>,

    /// Number of endpoint-connection pools
    ///
    /// [`ClashMap::len`] iterates over all inner pools and acquires a read lock on each.
    /// That seems like far too much effort, so we're using a relaxed increment counter instead.
    /// It's only used for diagnostics.
    pub(crate) global_pool_size: AtomicUsize,

    /// Total number of connections in the pool
    pub(crate) global_connections_count: Arc<AtomicUsize>,

    pub(crate) config: &'static crate::config::HttpConfig,
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

impl<P> GlobalConnPool<P>
where
    P: EndpointConnPoolExt,
{
    pub(crate) fn new(config: &'static crate::config::HttpConfig) -> Arc<Self> {
        let shards = config.pool_options.pool_shards;
        Arc::new(Self {
            global_pool: ClashMap::with_shard_amount(shards),
            global_pool_size: AtomicUsize::new(0),
            config,
            global_connections_count: Arc::new(AtomicUsize::new(0)),
        })
    }

    #[cfg(test)]
    pub(crate) fn get_global_connections_count(&self) -> usize {
        self.global_connections_count
            .load(atomic::Ordering::Relaxed)
    }

    pub(crate) fn get_idle_timeout(&self) -> Duration {
        self.config.pool_options.idle_timeout
    }

    pub(crate) fn shutdown(&self) {
        // drops all strong references to endpoint-pools
        self.global_pool.clear();
    }

    pub(crate) async fn gc_worker(&self, mut rng: impl Rng) {
        let epoch = self.config.pool_options.gc_epoch;
        let mut interval = tokio::time::interval(epoch / (self.global_pool.shards().len()) as u32);
        loop {
            interval.tick().await;

            let shard = rng.gen_range(0..self.global_pool.shards().len());
            self.gc(shard);
        }
    }

    pub(crate) fn gc(&self, shard: usize) {
        debug!(shard, "pool: performing epoch reclamation");

        // acquire a random shard lock
        let mut shard = self.global_pool.shards()[shard].write();

        let timer = Metrics::get()
            .proxy
            .http_pool_reclaimation_lag_seconds
            .start_timer();
        let current_len = shard.len();
        let mut clients_removed = 0;
        shard.retain(|(endpoint, x)| {
            // if the current endpoint pool is unique (no other strong or weak references)
            // then it is currently not in use by any connections.
            if let Some(pool) = Arc::get_mut(x) {
                let endpoints = pool.get_mut();
                clients_removed = endpoints.clear_closed();

                if endpoints.total_conns() == 0 {
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
            info!(
                "pool: performed global pool gc. removed {clients_removed} clients, total number of clients in pool is {size}"
            );
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
}

impl<C: ClientInnerExt> GlobalConnPool<EndpointConnPool<C>> {
    pub(crate) fn get(
        self: &Arc<Self>,
        ctx: &RequestContext,
        conn_info: &ConnInfo,
    ) -> Option<Client<C>> {
        let endpoint = conn_info.endpoint_cache_key()?;

        let endpoint_pool = self.get_endpoint_pool(&endpoint)?;
        let client = endpoint_pool
            .write()
            .get_conn_entry(conn_info.db_and_user())?
            .conn;

        let endpoint_pool = Arc::downgrade(&endpoint_pool);

        if client.inner.is_closed() {
            info!("pool: cached connection '{conn_info}' is closed, opening a new one");
            return None;
        }

        tracing::Span::current().record("conn_id", tracing::field::display(client.get_conn_id()));
        tracing::Span::current().record(
            "pid",
            tracing::field::display(client.inner.get_process_id()),
        );
        debug!(
            cold_start_info = ColdStartInfo::HttpPoolHit.as_str(),
            "pool: reusing connection '{conn_info}'"
        );

        match client.get_data() {
            ClientDataEnum::Local(data) => {
                data.session().send(ctx.session_id()).ok()?;
            }
            ClientDataEnum::Remote(data) => {
                data.session().send(ctx.session_id()).ok()?;
            }
            ClientDataEnum::Http(_) => (),
        }

        ctx.set_cold_start_info(ColdStartInfo::HttpPoolHit);
        Some(Client::new(client, conn_info.clone(), endpoint_pool))
    }
}

impl<P: EndpointConnPoolExt> GlobalConnPool<P> {
    pub(crate) fn get_endpoint_pool(
        self: &Arc<Self>,
        endpoint: &EndpointCacheKey,
    ) -> Option<Arc<RwLock<P>>> {
        Some(self.global_pool.get(endpoint)?.clone())
    }

    pub(crate) fn get_or_create_endpoint_pool(
        self: &Arc<Self>,
        endpoint: &EndpointCacheKey,
    ) -> Arc<RwLock<P>> {
        // fast path
        if let Some(pool) = self.global_pool.get(endpoint) {
            return pool.clone();
        }

        // slow path
        let new_pool = Arc::new(RwLock::new(P::create(
            self.config,
            self.global_connections_count.clone(),
        )));

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

pub(crate) struct Client<C: ClientInnerExt> {
    span: Span,
    inner: Option<ClientInnerCommon<C>>,
    conn_info: ConnInfo,
    pool: Weak<RwLock<EndpointConnPool<C>>>,
}

pub(crate) struct Discard<'a, C: ClientInnerExt> {
    conn_info: &'a ConnInfo,
    pool: &'a mut Weak<RwLock<EndpointConnPool<C>>>,
}

impl<C: ClientInnerExt> Client<C> {
    pub(crate) fn new(
        inner: ClientInnerCommon<C>,
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

    pub(crate) fn client_inner(&mut self) -> (&mut ClientInnerCommon<C>, Discard<'_, C>) {
        let Self {
            inner,
            pool,
            conn_info,
            span: _,
        } = self;
        let inner_m = inner.as_mut().expect("client inner should not be removed");
        (inner_m, Discard { conn_info, pool })
    }

    pub(crate) fn inner(&mut self) -> (&mut C, Discard<'_, C>) {
        let Self {
            inner,
            pool,
            conn_info,
            span: _,
        } = self;
        let inner = inner.as_mut().expect("client inner should not be removed");
        (&mut inner.inner, Discard { conn_info, pool })
    }

    pub(crate) fn metrics(&self, ctx: &RequestContext) -> Arc<MetricCounter> {
        let aux = &self
            .inner
            .as_ref()
            .expect("client inner should not be removed")
            .aux;

        let private_link_id = match ctx.extra() {
            None => None,
            Some(ConnectionInfoExtra::Aws { vpce_id }) => Some(vpce_id.clone()),
            Some(ConnectionInfoExtra::Azure { link_id }) => Some(link_id.to_smolstr()),
        };

        USAGE_METRICS.register(Ids {
            endpoint_id: aux.endpoint_id,
            branch_id: aux.branch_id,
            private_link_id,
        })
    }
}

impl<C: ClientInnerExt> Drop for Client<C> {
    fn drop(&mut self) {
        let conn_info = self.conn_info.clone();
        let client = self
            .inner
            .take()
            .expect("client inner should not be removed");
        if let Some(conn_pool) = std::mem::take(&mut self.pool).upgrade() {
            let _current_span = self.span.enter();
            // return connection to the pool
            EndpointConnPool::put(&conn_pool, &conn_info, client);
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

pub(crate) trait ClientInnerExt: Sync + Send + 'static {
    fn is_closed(&self) -> bool;
    fn get_process_id(&self) -> i32;
}

impl ClientInnerExt for postgres_client::Client {
    fn is_closed(&self) -> bool {
        self.is_closed()
    }

    fn get_process_id(&self) -> i32 {
        self.get_process_id()
    }
}

impl<C: ClientInnerExt> Discard<'_, C> {
    pub(crate) fn check_idle(&mut self, status: ReadyForQueryStatus) {
        let conn_info = &self.conn_info;
        if status != ReadyForQueryStatus::Idle && std::mem::take(self.pool).strong_count() > 0 {
            info!("pool: throwing away connection '{conn_info}' because connection is not idle");
        }
    }
    pub(crate) fn discard(&mut self) {
        let conn_info = &self.conn_info;
        if std::mem::take(self.pool).strong_count() > 0 {
            info!(
                "pool: throwing away connection '{conn_info}' because connection is potentially in a broken state"
            );
        }
    }
}
