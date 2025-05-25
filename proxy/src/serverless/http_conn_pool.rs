use std::collections::VecDeque;
use std::sync::atomic::{self, AtomicUsize};
use std::sync::{Arc, Weak};

use hyper::client::conn::http2;
use hyper_util::rt::{TokioExecutor, TokioIo};
use smol_str::ToSmolStr;
use tracing::{error, info};

use super::AsyncRW;
use super::conn_pool_lib::{
    ClientInnerCommon, ClientInnerExt, ConnInfo, ConnPoolEntry, EndpointConnPoolExt,
};
use crate::config::HttpConfig;
use crate::context::RequestContext;
use crate::metrics::{HttpEndpointPoolsGuard, Metrics};
use crate::protocol2::ConnectionInfoExtra;
use crate::usage_metrics::{Ids, MetricCounter, USAGE_METRICS};

pub(crate) type Send = http2::SendRequest<hyper::body::Incoming>;
pub(crate) type Connect = http2::Connection<TokioIo<AsyncRW>, hyper::body::Incoming, TokioExecutor>;

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

    pub fn register(&mut self, client: &Client) {
        self.conns.push_back(ConnPoolEntry {
            conn: client.inner.clone(),
            _last_access: std::time::Instant::now(),
        });
    }
}

impl EndpointConnPoolExt for HttpConnPool {
    type Client = Client;
    type ClientInner = Send;
    type Connection = Connect;

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

    fn remove_conn(
        &mut self,
        _db_user: (crate::types::DbName, crate::types::RoleName),
        conn_id: uuid::Uuid,
    ) -> bool {
        self.remove_conn(conn_id)
    }

    async fn spawn_conn(conn: Self::Connection) {
        let res = conn.await;
        match res {
            Ok(()) => info!("connection closed"),
            Err(e) => error!("connection error: {e:?}"),
        }
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

pub(crate) struct Client {
    pub(crate) inner: ClientInnerCommon<Send>,
}

impl Client {
    pub(self) fn new(inner: ClientInnerCommon<Send>) -> Self {
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
