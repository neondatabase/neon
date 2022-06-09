use anyhow::{anyhow, Context};
use hashbrown::HashMap;
use parking_lot::Mutex;
use std::net::SocketAddr;
use tokio::net::TcpStream;
use tokio_postgres::{CancelToken, NoTls};
use utils::pq_proto::CancelKeyData;

/// Enables serving `CancelRequest`s.
#[derive(Default)]
pub struct CancelMap(Mutex<HashMap<CancelKeyData, Option<CancelClosure>>>);

impl CancelMap {
    /// Cancel a running query for the corresponding connection.
    pub async fn cancel_session(&self, key: CancelKeyData) -> anyhow::Result<()> {
        let cancel_closure = self
            .0
            .lock()
            .get(&key)
            .and_then(|x| x.clone())
            .with_context(|| format!("unknown session: {:?}", key))?;

        cancel_closure.try_cancel_query().await
    }

    /// Run async action within an ephemeral session identified by [`CancelKeyData`].
    pub async fn with_session<'a, F, R, V>(&'a self, f: F) -> anyhow::Result<V>
    where
        F: FnOnce(Session<'a>) -> R,
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
        self.0
            .lock()
            .try_insert(key, None)
            .map_err(|_| anyhow!("session already exists: {:?}", key))?;

        // This will guarantee that the session gets dropped
        // as soon as the future is finished.
        scopeguard::defer! {
            self.0.lock().remove(&key);
        }

        let session = Session::new(key, self);
        f(session).await
    }

    #[cfg(test)]
    fn contains(&self, session: &Session) -> bool {
        self.0.lock().contains_key(&session.key)
    }

    #[cfg(test)]
    fn is_empty(&self) -> bool {
        self.0.lock().is_empty()
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
    pub async fn try_cancel_query(self) -> anyhow::Result<()> {
        let socket = TcpStream::connect(self.socket_addr).await?;
        self.cancel_token.cancel_query_raw(socket, NoTls).await?;

        Ok(())
    }
}

/// Helper for registering query cancellation tokens.
pub struct Session<'a> {
    /// The user-facing key identifying this session.
    key: CancelKeyData,
    /// The [`CancelMap`] this session belongs to.
    cancel_map: &'a CancelMap,
}

impl<'a> Session<'a> {
    fn new(key: CancelKeyData, cancel_map: &'a CancelMap) -> Self {
        Self { key, cancel_map }
    }

    /// Store the cancel token for the given session.
    /// This enables query cancellation in [`crate::proxy::handshake`].
    pub fn enable_cancellation(self, cancel_closure: CancelClosure) -> CancelKeyData {
        self.cancel_map
            .0
            .lock()
            .insert(self.key, Some(cancel_closure));

        self.key
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use once_cell::sync::Lazy;

    #[tokio::test]
    async fn check_session_drop() -> anyhow::Result<()> {
        static CANCEL_MAP: Lazy<CancelMap> = Lazy::new(Default::default);

        let (tx, rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(CANCEL_MAP.with_session(|session| async move {
            assert!(CANCEL_MAP.contains(&session));

            tx.send(()).expect("failed to send");
            Ok(futures::future::pending::<()>().await)
        }));

        // Wait until the task has been spawned.
        let () = rx.await.context("failed to hear from the task")?;

        // Drop the session's entry by cancelling the task.
        task.abort();
        let error = task.await.expect_err("task should have failed");
        if !error.is_cancelled() {
            anyhow::bail!(error);
        }

        // Check that the session has been dropped.
        assert!(CANCEL_MAP.is_empty());

        Ok(())
    }
}
