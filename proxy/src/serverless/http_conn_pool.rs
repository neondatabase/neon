use std::collections::VecDeque;
use std::sync::atomic::{self, AtomicUsize};
use std::sync::{Arc, Weak};

use hyper::client::conn::http2;
use hyper_util::rt::{TokioExecutor, TokioIo};
use smol_str::ToSmolStr;
use tracing::{Instrument, error, info, info_span};

use super::AsyncRW;
use super::conn_pool_lib::{
    ClientDataEnum, ClientInnerCommon, ClientInnerExt, ConnInfo, ConnPoolEntry,
    EndpointConnPoolExt, GlobalConnPool,
};
use crate::config::HttpConfig;
use crate::context::RequestContext;
use crate::control_plane::messages::MetricsAuxInfo;
use crate::metrics::{HttpEndpointPoolsGuard, Metrics};
use crate::protocol2::ConnectionInfoExtra;
use crate::usage_metrics::{Ids, MetricCounter, USAGE_METRICS};

pub(crate) type Send = http2::SendRequest<hyper::body::Incoming>;
pub(crate) type Connect = http2::Connection<TokioIo<AsyncRW>, hyper::body::Incoming, TokioExecutor>;

#[derive(Clone)]
pub(crate) struct ClientDataHttp();

// Per-endpoint connection pool
// Number of open connections is limited by the `max_conns_per_endpoint`.
pub(crate) struct HttpConnPool {
    // TODO(conrad):
    // either we should open more connections depending on stream count
    // (not exposed by hyper, need our own counter)
    // or we can change this to an Option rather than a VecDeque.
    //
    // Opening more connections to the same db because we run out of streams
    // seems somewhat redundant though.
    //
    // Probably we should run a semaphore and just the single conn. TBD.
    conns: VecDeque<ConnPoolEntry<Send>>,
    _guard: HttpEndpointPoolsGuard<'static>,
    global_connections_count: Arc<AtomicUsize>,
}

impl HttpConnPool {
    fn get_conn_entry(&mut self) -> Option<ConnPoolEntry<Send>> {
        let Self { conns, .. } = self;

        loop {
            let conn = conns.pop_front()?;
            if !conn.conn.inner.is_closed() {
                let new_conn = ConnPoolEntry {
                    conn: conn.conn.clone(),
                    _last_access: std::time::Instant::now(),
                };

                conns.push_back(new_conn);
                return Some(conn);
            }
        }
    }

    fn remove_conn(&mut self, conn_id: uuid::Uuid) -> bool {
        let Self {
            conns,
            global_connections_count,
            ..
        } = self;

        let old_len = conns.len();
        conns.retain(|entry| entry.conn.conn_id != conn_id);
        let new_len = conns.len();
        let removed = old_len - new_len;
        if removed > 0 {
            global_connections_count.fetch_sub(removed, atomic::Ordering::Relaxed);
            Metrics::get()
                .proxy
                .http_pool_opened_connections
                .get_metric()
                .dec_by(removed as i64);
        }
        removed > 0
    }
}

impl EndpointConnPoolExt for HttpConnPool {
    type Client = Client<Send>;
    type ClientInner = Send;

    fn create(_config: &HttpConfig, global_connections_count: Arc<AtomicUsize>) -> Self {
        HttpConnPool {
            conns: VecDeque::new(),
            _guard: Metrics::get().proxy.http_endpoint_pools.guard(),
            global_connections_count,
        }
    }

    fn wrap_client(
        inner: ClientInnerCommon<Self::ClientInner>,
        _conn_info: ConnInfo,
        _pool: Weak<parking_lot::RwLock<Self>>,
    ) -> Self::Client {
        Client::new(inner)
    }

    fn get_conn_entry(
        &mut self,
        _db_user: (crate::types::DbName, crate::types::RoleName),
    ) -> Option<ClientInnerCommon<Self::ClientInner>> {
        Some(self.get_conn_entry()?.conn)
    }

    fn clear_closed(&mut self) -> usize {
        let Self { conns, .. } = self;
        let old_len = conns.len();
        conns.retain(|entry| !entry.conn.inner.is_closed());

        let new_len = conns.len();
        old_len - new_len
    }

    fn total_conns(&self) -> usize {
        self.conns.len()
    }
}

impl Drop for HttpConnPool {
    fn drop(&mut self) {
        if !self.conns.is_empty() {
            self.global_connections_count
                .fetch_sub(self.conns.len(), atomic::Ordering::Relaxed);
            Metrics::get()
                .proxy
                .http_pool_opened_connections
                .get_metric()
                .dec_by(self.conns.len() as i64);
        }
    }
}

pub(crate) fn poll_http2_client(
    global_pool: Arc<GlobalConnPool<HttpConnPool>>,
    ctx: &RequestContext,
    conn_info: &ConnInfo,
    client: Send,
    connection: Connect,
    conn_id: uuid::Uuid,
    aux: MetricsAuxInfo,
) -> Client<Send> {
    let conn_gauge = Metrics::get().proxy.db_connections.guard(ctx.protocol());
    let session_id = ctx.session_id();

    let span = info_span!(parent: None, "connection", %conn_id);
    let cold_start_info = ctx.cold_start_info();
    span.in_scope(|| {
        info!(cold_start_info = cold_start_info.as_str(), %conn_info, %session_id, "new connection");
    });

    let pool = match conn_info.endpoint_cache_key() {
        Some(endpoint) => {
            let pool = global_pool.get_or_create_endpoint_pool(&endpoint);
            let client = ClientInnerCommon {
                inner: client.clone(),
                aux: aux.clone(),
                conn_id,
                data: ClientDataEnum::Http(ClientDataHttp()),
            };
            pool.write().conns.push_back(ConnPoolEntry {
                conn: client,
                _last_access: std::time::Instant::now(),
            });
            Metrics::get()
                .proxy
                .http_pool_opened_connections
                .get_metric()
                .inc();

            Arc::downgrade(&pool)
        }
        None => Weak::new(),
    };

    tokio::spawn(
        async move {
            let _conn_gauge = conn_gauge;
            let res = connection.await;
            match res {
                Ok(()) => info!("connection closed"),
                Err(e) => error!(%session_id, "connection error: {e:?}"),
            }

            // remove from connection pool
            if let Some(pool) = pool.clone().upgrade() {
                if pool.write().remove_conn(conn_id) {
                    info!("closed connection removed");
                }
            }
        }
        .instrument(span),
    );

    let client = ClientInnerCommon {
        inner: client,
        aux,
        conn_id,
        data: ClientDataEnum::Http(ClientDataHttp()),
    };

    Client::new(client)
}

pub(crate) struct Client<C: ClientInnerExt + Clone> {
    pub(crate) inner: ClientInnerCommon<C>,
}

impl<C: ClientInnerExt + Clone> Client<C> {
    pub(self) fn new(inner: ClientInnerCommon<C>) -> Self {
        Self { inner }
    }

    pub(crate) fn metrics(&self, ctx: &RequestContext) -> Arc<MetricCounter> {
        let aux = &self.inner.aux;

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

impl ClientInnerExt for Send {
    fn is_closed(&self) -> bool {
        self.is_closed()
    }

    fn get_process_id(&self) -> i32 {
        // ideally throw something meaningful
        -1
    }
}
