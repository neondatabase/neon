use dashmap::DashMap;
use pq_proto::CancelKeyData;
use std::{net::SocketAddr, sync::Arc};
use thiserror::Error;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio_postgres::{CancelToken, NoTls};
use tracing::info;
use uuid::Uuid;

use crate::{
    error::ReportableError,
    metrics::NUM_CANCELLATION_REQUESTS,
    redis::cancellation_publisher::{
        CancellationPublisher, CancellationPublisherMut, RedisPublisherClient,
    },
};

pub type CancelMap = Arc<DashMap<CancelKeyData, Option<CancelClosure>>>;
pub type CancellationHandlerMain = CancellationHandler<Option<Arc<Mutex<RedisPublisherClient>>>>;
pub type CancellationHandlerMainInternal = Option<Arc<Mutex<RedisPublisherClient>>>;

/// Enables serving `CancelRequest`s.
///
/// If `CancellationPublisher` is available, cancel request will be used to publish the cancellation key to other proxy instances.
pub struct CancellationHandler<P> {
    map: CancelMap,
    client: P,
    /// This field used for the monitoring purposes.
    /// Represents the source of the cancellation request.
    from: &'static str,
}

#[derive(Debug, Error)]
pub enum CancelError {
    #[error("{0}")]
    IO(#[from] std::io::Error),
    #[error("{0}")]
    Postgres(#[from] tokio_postgres::Error),
}

impl ReportableError for CancelError {
    fn get_error_kind(&self) -> crate::error::ErrorKind {
        match self {
            CancelError::IO(_) => crate::error::ErrorKind::Compute,
            CancelError::Postgres(e) if e.as_db_error().is_some() => {
                crate::error::ErrorKind::Postgres
            }
            CancelError::Postgres(_) => crate::error::ErrorKind::Compute,
        }
    }
}

impl<P: CancellationPublisher> CancellationHandler<P> {
    /// Run async action within an ephemeral session identified by [`CancelKeyData`].
    pub fn get_session(self: Arc<Self>) -> Session<P> {
        // HACK: We'd rather get the real backend_pid but tokio_postgres doesn't
        // expose it and we don't want to do another roundtrip to query
        // for it. The client will be able to notice that this is not the
        // actual backend_pid, but backend_pid is not used for anything
        // so it doesn't matter.
        let key = loop {
            let key = rand::random();

            // Random key collisions are unlikely to happen here, but they're still possible,
            // which is why we have to take care not to rewrite an existing key.
            match self.map.entry(key) {
                dashmap::mapref::entry::Entry::Occupied(_) => continue,
                dashmap::mapref::entry::Entry::Vacant(e) => {
                    e.insert(None);
                }
            }
            break key;
        };

        info!("registered new query cancellation key {key}");
        Session {
            key,
            cancellation_handler: self,
        }
    }
    /// Try to cancel a running query for the corresponding connection.
    /// If the cancellation key is not found, it will be published to Redis.
    pub async fn cancel_session(
        &self,
        key: CancelKeyData,
        session_id: Uuid,
    ) -> Result<(), CancelError> {
        // NB: we should immediately release the lock after cloning the token.
        let Some(cancel_closure) = self.map.get(&key).and_then(|x| x.clone()) else {
            tracing::warn!("query cancellation key not found: {key}");
            NUM_CANCELLATION_REQUESTS
                .with_label_values(&[self.from, "not_found"])
                .inc();
            match self.client.try_publish(key, session_id).await {
                Ok(()) => {} // do nothing
                Err(e) => {
                    return Err(CancelError::IO(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        e.to_string(),
                    )));
                }
            }
            return Ok(());
        };
        NUM_CANCELLATION_REQUESTS
            .with_label_values(&[self.from, "found"])
            .inc();
        info!("cancelling query per user's request using key {key}");
        cancel_closure.try_cancel_query().await
    }

    #[cfg(test)]
    fn contains(&self, session: &Session<P>) -> bool {
        self.map.contains_key(&session.key)
    }

    #[cfg(test)]
    fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

impl CancellationHandler<()> {
    pub fn new(map: CancelMap, from: &'static str) -> Self {
        Self {
            map,
            client: (),
            from,
        }
    }
}

impl<P: CancellationPublisherMut> CancellationHandler<Option<Arc<Mutex<P>>>> {
    pub fn new(map: CancelMap, client: Option<Arc<Mutex<P>>>, from: &'static str) -> Self {
        Self { map, client, from }
    }
}

/// This should've been a [`std::future::Future`], but
/// it's impossible to name a type of an unboxed future
/// (we'd need something like `#![feature(type_alias_impl_trait)]`).
#[derive(Clone)]
pub struct CancelClosure {
    socket_addr: SocketAddr,
    cancel_token: CancelToken,
}

impl CancelClosure {
    pub fn new(socket_addr: SocketAddr, cancel_token: CancelToken) -> Self {
        Self {
            socket_addr,
            cancel_token,
        }
    }
    /// Cancels the query running on user's compute node.
    pub async fn try_cancel_query(self) -> Result<(), CancelError> {
        let socket = TcpStream::connect(self.socket_addr).await?;
        self.cancel_token.cancel_query_raw(socket, NoTls).await?;
        info!("query was cancelled");
        Ok(())
    }
}

/// Helper for registering query cancellation tokens.
pub struct Session<P> {
    /// The user-facing key identifying this session.
    key: CancelKeyData,
    /// The [`CancelMap`] this session belongs to.
    cancellation_handler: Arc<CancellationHandler<P>>,
}

impl<P> Session<P> {
    /// Store the cancel token for the given session.
    /// This enables query cancellation in `crate::proxy::prepare_client_connection`.
    pub fn enable_query_cancellation(&self, cancel_closure: CancelClosure) -> CancelKeyData {
        info!("enabling query cancellation for this session");
        self.cancellation_handler
            .map
            .insert(self.key, Some(cancel_closure));

        self.key
    }
}

impl<P> Drop for Session<P> {
    fn drop(&mut self) {
        self.cancellation_handler.map.remove(&self.key);
        info!("dropped query cancellation key {}", &self.key);
    }
}

#[cfg(test)]
mod tests {
    use crate::metrics::NUM_CANCELLATION_REQUESTS_SOURCE_FROM_REDIS;

    use super::*;

    #[tokio::test]
    async fn check_session_drop() -> anyhow::Result<()> {
        let cancellation_handler = Arc::new(CancellationHandler::<()>::new(
            CancelMap::default(),
            NUM_CANCELLATION_REQUESTS_SOURCE_FROM_REDIS,
        ));

        let session = cancellation_handler.clone().get_session();
        assert!(cancellation_handler.contains(&session));
        drop(session);
        // Check that the session has been dropped.
        assert!(cancellation_handler.is_empty());

        Ok(())
    }
}
