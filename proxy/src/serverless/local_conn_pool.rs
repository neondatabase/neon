use futures::{future::poll_fn, Future};
use jose_jwk::jose_b64::base64ct::{Base64UrlUnpadded, Encoding};
use p256::ecdsa::{Signature, SigningKey};
use parking_lot::RwLock;
use rand::rngs::OsRng;
use serde_json::Value;
use signature::Signer;
use std::ops::Deref;
use std::task::{ready, Poll};
use std::{collections::HashMap, pin::pin, sync::Arc, sync::Weak, time::Duration};
use tokio::time::Instant;
use tokio_postgres::tls::NoTlsStream;
use tokio_postgres::types::ToSql;
use tokio_postgres::{AsyncMessage, ReadyForQueryStatus, Socket};
use tokio_util::sync::CancellationToken;
use typed_json::json;

use crate::console::messages::{ColdStartInfo, MetricsAuxInfo};
use crate::metrics::Metrics;
use crate::usage_metrics::{Ids, MetricCounter, USAGE_METRICS};
use crate::{context::RequestMonitoring, DbName, RoleName};

use tracing::{error, warn, Span};
use tracing::{info, info_span, Instrument};

use super::backend::HttpConnError;
use super::conn_pool::{ClientInnerExt, ConnInfo};

struct ConnPoolEntry<C: ClientInnerExt> {
    conn: ClientInner<C>,
    _last_access: std::time::Instant,
}

// /// key id for the pg_session_jwt state
// static PG_SESSION_JWT_KID: AtomicU64 = AtomicU64::new(1);

// Per-endpoint connection pool, (dbname, username) -> DbUserConnPool
// Number of open connections is limited by the `max_conns_per_endpoint`.
pub(crate) struct EndpointConnPool<C: ClientInnerExt> {
    pools: HashMap<(DbName, RoleName), DbUserConnPool<C>>,
    total_conns: usize,
    max_conns: usize,
    global_pool_size_max_conns: usize,
}

impl<C: ClientInnerExt> EndpointConnPool<C> {
    fn get_conn_entry(&mut self, db_user: (DbName, RoleName)) -> Option<ConnPoolEntry<C>> {
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

    fn put(pool: &RwLock<Self>, conn_info: &ConnInfo, client: ClientInner<C>) {
        let conn_id = client.conn_id;

        if client.is_closed() {
            info!(%conn_id, "pool: throwing away connection '{conn_info}' because connection is closed");
            return;
        }
        let global_max_conn = pool.read().global_pool_size_max_conns;
        if pool.read().total_conns >= global_max_conn {
            info!(%conn_id, "pool: throwing away connection '{conn_info}' because pool is full");
            return;
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
    }
}

impl<C: ClientInnerExt> Drop for EndpointConnPool<C> {
    fn drop(&mut self) {
        if self.total_conns > 0 {
            Metrics::get()
                .proxy
                .http_pool_opened_connections
                .get_metric()
                .dec_by(self.total_conns as i64);
        }
    }
}

pub(crate) struct DbUserConnPool<C: ClientInnerExt> {
    conns: Vec<ConnPoolEntry<C>>,
}

impl<C: ClientInnerExt> Default for DbUserConnPool<C> {
    fn default() -> Self {
        Self { conns: Vec::new() }
    }
}

impl<C: ClientInnerExt> DbUserConnPool<C> {
    fn clear_closed_clients(&mut self, conns: &mut usize) -> usize {
        let old_len = self.conns.len();

        self.conns.retain(|conn| !conn.conn.is_closed());

        let new_len = self.conns.len();
        let removed = old_len - new_len;
        *conns -= removed;
        removed
    }

    fn get_conn_entry(&mut self, conns: &mut usize) -> Option<ConnPoolEntry<C>> {
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
        conn
    }
}

pub(crate) struct LocalConnPool<C: ClientInnerExt> {
    global_pool: RwLock<EndpointConnPool<C>>,

    config: &'static crate::config::HttpConfig,
}

impl<C: ClientInnerExt> LocalConnPool<C> {
    pub(crate) fn new(config: &'static crate::config::HttpConfig) -> Arc<Self> {
        Arc::new(Self {
            global_pool: RwLock::new(EndpointConnPool {
                pools: HashMap::new(),
                total_conns: 0,
                max_conns: config.pool_options.max_conns_per_endpoint,
                global_pool_size_max_conns: config.pool_options.max_total_conns,
            }),
            config,
        })
    }

    pub(crate) fn get_idle_timeout(&self) -> Duration {
        self.config.pool_options.idle_timeout
    }

    // pub(crate) fn shutdown(&self) {
    //     let mut pool = self.global_pool.write();
    //     pool.pools.clear();
    //     pool.total_conns = 0;
    // }

    pub(crate) fn get(
        self: &Arc<Self>,
        ctx: &RequestMonitoring,
        conn_info: &ConnInfo,
    ) -> Result<Option<LocalClient<C>>, HttpConnError> {
        let mut client: Option<ClientInner<C>> = None;
        if let Some(entry) = self
            .global_pool
            .write()
            .get_conn_entry(conn_info.db_and_user())
        {
            client = Some(entry.conn);
        }

        // ok return cached connection if found and establish a new one otherwise
        if let Some(client) = client {
            if client.is_closed() {
                info!("pool: cached connection '{conn_info}' is closed, opening a new one");
                return Ok(None);
            }
            tracing::Span::current().record("conn_id", tracing::field::display(client.conn_id));
            tracing::Span::current().record(
                "pid",
                tracing::field::display(client.inner.get_process_id()),
            );
            info!(
                cold_start_info = ColdStartInfo::HttpPoolHit.as_str(),
                "pool: reusing connection '{conn_info}'"
            );
            client.session.send(ctx.session_id())?;
            ctx.set_cold_start_info(ColdStartInfo::HttpPoolHit);
            ctx.success();
            return Ok(Some(LocalClient::new(
                client,
                conn_info.clone(),
                Arc::downgrade(self),
            )));
        }
        Ok(None)
    }
}

pub(crate) fn poll_client(
    global_pool: Arc<LocalConnPool<tokio_postgres::Client>>,
    ctx: &RequestMonitoring,
    conn_info: ConnInfo,
    client: tokio_postgres::Client,
    mut connection: tokio_postgres::Connection<Socket, NoTlsStream>,
    conn_id: uuid::Uuid,
    aux: MetricsAuxInfo,
) -> LocalClient<tokio_postgres::Client> {
    let conn_gauge = Metrics::get().proxy.db_connections.guard(ctx.protocol());
    let mut session_id = ctx.session_id();
    let (tx, mut rx) = tokio::sync::watch::channel(session_id);

    let span = info_span!(parent: None, "connection", %conn_id);
    let cold_start_info = ctx.cold_start_info();
    span.in_scope(|| {
        info!(cold_start_info = cold_start_info.as_str(), %conn_info, %session_id, "new connection");
    });
    let pool = Arc::downgrade(&global_pool);
    let pool_clone = pool.clone();

    let db_user = conn_info.db_and_user();
    let idle = global_pool.get_idle_timeout();
    let cancel = CancellationToken::new();
    let cancelled = cancel.clone().cancelled_owned();

    tokio::spawn(
    async move {
        let _conn_gauge = conn_gauge;
        let mut idle_timeout = pin!(tokio::time::sleep(idle));
        let mut cancelled = pin!(cancelled);

        poll_fn(move |cx| {
            if cancelled.as_mut().poll(cx).is_ready() {
                info!("connection dropped");
                return Poll::Ready(())
            }

            match rx.has_changed() {
                Ok(true) => {
                    session_id = *rx.borrow_and_update();
                    info!(%session_id, "changed session");
                    idle_timeout.as_mut().reset(Instant::now() + idle);
                }
                Err(_) => {
                    info!("connection dropped");
                    return Poll::Ready(())
                }
                _ => {}
            }

            // 5 minute idle connection timeout
            if idle_timeout.as_mut().poll(cx).is_ready() {
                idle_timeout.as_mut().reset(Instant::now() + idle);
                info!("connection idle");
                if let Some(pool) = pool.clone().upgrade() {
                    // remove client from pool - should close the connection if it's idle.
                    // does nothing if the client is currently checked-out and in-use
                    if pool.global_pool.write().remove_client(db_user.clone(), conn_id) {
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
                if pool.global_pool.write().remove_client(db_user.clone(), conn_id) {
                    info!("closed connection removed");
                }
            }

            Poll::Ready(())
        }).await;

    }
    .instrument(span));

    let key = SigningKey::random(&mut OsRng);

    let inner = ClientInner {
        inner: client,
        session: tx,
        cancel,
        aux,
        conn_id,
        key,
        jti: 0,
    };
    LocalClient::new(inner, conn_info, pool_clone)
}

struct ClientInner<C: ClientInnerExt> {
    inner: C,
    session: tokio::sync::watch::Sender<uuid::Uuid>,
    cancel: CancellationToken,
    aux: MetricsAuxInfo,
    conn_id: uuid::Uuid,

    // needed for pg_session_jwt state
    key: SigningKey,
    jti: u64,
}

impl<C: ClientInnerExt> Drop for ClientInner<C> {
    fn drop(&mut self) {
        // on client drop, tell the conn to shut down
        self.cancel.cancel();
    }
}

impl<C: ClientInnerExt> ClientInner<C> {
    pub(crate) fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }
}

impl<C: ClientInnerExt> LocalClient<C> {
    pub(crate) fn metrics(&self) -> Arc<MetricCounter> {
        let aux = &self.inner.as_ref().unwrap().aux;
        USAGE_METRICS.register(Ids {
            endpoint_id: aux.endpoint_id,
            branch_id: aux.branch_id,
        })
    }
}

pub(crate) struct LocalClient<C: ClientInnerExt> {
    span: Span,
    inner: Option<ClientInner<C>>,
    conn_info: ConnInfo,
    pool: Weak<LocalConnPool<C>>,
}

pub(crate) struct Discard<'a, C: ClientInnerExt> {
    conn_info: &'a ConnInfo,
    pool: &'a mut Weak<LocalConnPool<C>>,
}

impl<C: ClientInnerExt> LocalClient<C> {
    pub(self) fn new(
        inner: ClientInner<C>,
        conn_info: ConnInfo,
        pool: Weak<LocalConnPool<C>>,
    ) -> Self {
        Self {
            inner: Some(inner),
            span: Span::current(),
            conn_info,
            pool,
        }
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
    pub(crate) fn key(&self) -> &SigningKey {
        let inner = &self
            .inner
            .as_ref()
            .expect("client inner should not be removed");
        &inner.key
    }
}
impl LocalClient<tokio_postgres::Client> {
    pub(crate) async fn set_jwt_session(&mut self, payload: &[u8]) -> Result<(), HttpConnError> {
        let inner = self
            .inner
            .as_mut()
            .expect("client inner should not be removed");
        inner.jti += 1;

        let kid = inner.inner.get_process_id();
        let header = json!({"kid":kid.to_string()}).to_string();

        let mut payload = serde_json::from_slice::<serde_json::Map<String, Value>>(payload)
            .map_err(HttpConnError::JwtPayloadError)?;
        payload.insert("jti".to_string(), Value::Number(inner.jti.into()));
        let payload = Value::Object(payload).to_string();

        let token = sign_jwt(&inner.key, header, payload);

        // initiates the auth session
        inner.inner.simple_query("discard all").await?;
        inner
            .inner
            .query(
                "select auth.jwt_session_init($1)",
                &[&token as &(dyn ToSql + Sync)],
            )
            .await?;

        Ok(())
    }

    // pub(crate) fn jti(&mut self) -> u64 {
    //     let jti = &mut self
    //         .inner
    //         .as_mut()
    //         .expect("client inner should not be removed")
    //         .jti;
    //     *jti += 1;
    //     *jti
    // }
}

fn sign_jwt(sk: &SigningKey, header: String, payload: String) -> String {
    let header = Base64UrlUnpadded::encode_string(header.as_bytes());
    let payload = Base64UrlUnpadded::encode_string(payload.as_bytes());

    let message = format!("{header}.{payload}");
    let sig: Signature = sk.sign(message.as_bytes());
    let base64_sig = Base64UrlUnpadded::encode_string(&sig.to_bytes());
    format!("{message}.{base64_sig}")
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
            info!("pool: throwing away connection '{conn_info}' because connection is potentially in a broken state");
        }
    }
}

impl<C: ClientInnerExt> Deref for LocalClient<C> {
    type Target = C;

    fn deref(&self) -> &Self::Target {
        &self
            .inner
            .as_ref()
            .expect("client inner should not be removed")
            .inner
    }
}

impl<C: ClientInnerExt> LocalClient<C> {
    fn do_drop(&mut self) -> Option<impl FnOnce()> {
        let conn_info = self.conn_info.clone();
        let client = self
            .inner
            .take()
            .expect("client inner should not be removed");
        if let Some(conn_pool) = std::mem::take(&mut self.pool).upgrade() {
            let current_span = self.span.clone();
            // return connection to the pool
            return Some(move || {
                let _span = current_span.enter();
                EndpointConnPool::put(&conn_pool.global_pool, &conn_info, client);
            });
        }
        None
    }
}

impl<C: ClientInnerExt> Drop for LocalClient<C> {
    fn drop(&mut self) {
        if let Some(drop) = self.do_drop() {
            tokio::task::spawn_blocking(drop);
        }
    }
}
