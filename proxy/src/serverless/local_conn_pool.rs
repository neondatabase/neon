use futures::{future::poll_fn, Future};
use jose_jwk::jose_b64::base64ct::{Base64UrlUnpadded, Encoding};
use p256::ecdsa::{Signature, SigningKey};
use parking_lot::RwLock;
use rand::rngs::OsRng;
use serde_json::Value;
use signature::Signer;
use std::task::{ready, Poll};
use std::{
    collections::HashMap,
    pin::pin,
    sync::atomic::{self, AtomicUsize},
    sync::Arc,
    sync::Weak,
    time::Duration,
};

use tokio::time::Instant;
use tokio_postgres::tls::NoTlsStream;
use tokio_postgres::types::ToSql;
use tokio_postgres::{AsyncMessage, ReadyForQueryStatus, Socket};
use tokio_util::sync::CancellationToken;
use typed_json::json;

use crate::control_plane::messages::{ColdStartInfo, MetricsAuxInfo};
use crate::metrics::{HttpEndpointPoolsGuard, Metrics};
use crate::usage_metrics::{Ids, MetricCounter, USAGE_METRICS};
use crate::{context::RequestMonitoring, DbName, RoleName};

use tracing::{debug, error, warn, Span};
use tracing::{info, info_span, Instrument};

use super::backend::HttpConnError;
use super::conn_pool::{ClientInnerExt, ConnInfo, ConnPool, EndpointConnPool};

pub(crate) fn poll_client<C: ClientInnerExt>(
    local_pool: Arc<ConnPool<C>>,
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
    let pool = Arc::downgrade(&local_pool);
    let pool_clone = pool.clone();

    let db_user = conn_info.db_and_user();
    let idle = local_pool.get_idle_timeout();
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
                if pool.write().remove_client(db_user.clone(), conn_id) {
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

pub(crate) struct LocalClient<C: ClientInnerExt> {
    span: Span,
    inner: Option<ClientInner<C>>,
    conn_info: ConnInfo,
    pool: Weak<ConnPool<C>>,
}

impl<C: ClientInnerExt> LocalClient<C> {
    pub(self) fn new(inner: ClientInner<C>, conn_info: ConnInfo, pool: Weak<ConnPool<C>>) -> Self {
        Self {
            inner: Some(inner),
            span: Span::current(),
            conn_info,
            pool,
        }
    }

    pub(crate) fn metrics(&self) -> Arc<MetricCounter> {
        let aux = &self.inner.as_ref().unwrap().aux;
        USAGE_METRICS.register(Ids {
            endpoint_id: aux.endpoint_id,
            branch_id: aux.branch_id,
        })
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

    pub fn get_client(&self) -> &C {
        &self
            .inner
            .as_ref()
            .expect("client inner should not be removed")
            .inner
    }

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
                EndpointConnPool::put(&conn_pool.local_pool, &conn_info, client);
            });
        }
        None
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
        let header = json!({"kid":kid}).to_string();

        let mut payload = serde_json::from_slice::<serde_json::Map<String, Value>>(payload)
            .map_err(HttpConnError::JwtPayloadError)?;
        payload.insert("jti".to_string(), Value::Number(inner.jti.into()));
        let payload = Value::Object(payload).to_string();

        debug!(
            kid,
            jti = inner.jti,
            ?header,
            ?payload,
            "signing new ephemeral JWT"
        );

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

        info!(kid, jti = inner.jti, "user session state init");

        Ok(())
    }
}

fn sign_jwt(sk: &SigningKey, header: String, payload: String) -> String {
    let header = Base64UrlUnpadded::encode_string(header.as_bytes());
    let payload = Base64UrlUnpadded::encode_string(payload.as_bytes());

    let message = format!("{header}.{payload}");
    let sig: Signature = sk.sign(message.as_bytes());
    let base64_sig = Base64UrlUnpadded::encode_string(&sig.to_bytes());
    format!("{message}.{base64_sig}")
}

pub(crate) struct Discard<'a, C: ClientInnerExt> {
    conn_info: &'a ConnInfo,
    pool: &'a mut Weak<ConnPool<C>>,
}

impl<C: ClientInnerExt> Discard<'_, C> {
    pub(crate) fn check_idle(&mut self, status: ReadyForQueryStatus) {
        let conn_info = &self.conn_info;
        if status != ReadyForQueryStatus::Idle && std::mem::take(self.pool).strong_count() > 0 {
            info!(
                "local_pool: throwing away connection '{conn_info}' because connection is not idle"
            );
        }
    }
    pub(crate) fn discard(&mut self) {
        let conn_info = &self.conn_info;
        if std::mem::take(self.pool).strong_count() > 0 {
            info!(
                "local_pool: throwing away connection '{conn_info}'
                   because connection is potentially in a broken state"
            );
        }
    }
}

impl<C: ClientInnerExt> Drop for LocalClient<C> {
    fn drop(&mut self) {
        if let Some(drop) = self.do_drop() {
            tokio::task::spawn_blocking(drop);
        }
    }
}
