use anyhow::{anyhow, Context};
use hashbrown::HashMap;
use lazy_static::lazy_static;
use parking_lot::Mutex;
use std::net::SocketAddr;
use tokio::net::TcpStream;
use tokio_postgres::{CancelToken, NoTls};
use zenith_utils::pq_proto::CancelKeyData;

lazy_static! {
    /// Enables serving CancelRequests.
    static ref CANCEL_MAP: Mutex<HashMap<CancelKeyData, Option<CancelClosure>>> = Default::default();
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
    pub async fn try_cancel_query(self) -> anyhow::Result<()> {
        let socket = TcpStream::connect(self.socket_addr).await?;
        self.cancel_token.cancel_query_raw(socket, NoTls).await?;

        Ok(())
    }
}

/// Cancel a running query for the corresponding connection.
pub async fn cancel_session(key: CancelKeyData) -> anyhow::Result<()> {
    let cancel_closure = CANCEL_MAP
        .lock()
        .get(&key)
        .and_then(|x| x.clone())
        .with_context(|| format!("unknown session: {:?}", key))?;

    cancel_closure.try_cancel_query().await
}

/// Helper for registering query cancellation tokens.
pub struct Session(CancelKeyData);

impl Session {
    /// Store the cancel token for the given session.
    pub fn enable_cancellation(self, cancel_closure: CancelClosure) -> CancelKeyData {
        CANCEL_MAP.lock().insert(self.0, Some(cancel_closure));
        self.0
    }
}

/// Run async action within an ephemeral session identified by [`CancelKeyData`].
pub async fn with_session<F, R, V>(f: F) -> anyhow::Result<V>
where
    F: FnOnce(Session) -> R,
    R: std::future::Future<Output = anyhow::Result<V>>,
{
    // HACK: We'd rather get the real backend_pid but tokio_postgres doesn't
    // expose it and we don't want to do another roundtrip to query
    // for it. The client will be able to notice that this is not the
    // actual backend_pid, but backend_pid is not used for anything
    // so it doesn't matter.
    let key = rand::random();

    // Random key collisions are unlikely to happen here, but they're still possible,
    // which is why we have to take care not to rewrite an existing key.
    CANCEL_MAP
        .lock()
        .try_insert(key, None)
        .map_err(|_| anyhow!("session already exists: {:?}", key))?;

    // This will guarantee that the session gets dropped
    // as soon as the future is finished.
    scopeguard::defer! {
        CANCEL_MAP.lock().remove(&key);
    }

    let session = Session(key);
    f(session).await
}
