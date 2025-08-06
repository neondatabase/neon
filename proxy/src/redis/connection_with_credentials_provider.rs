use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use futures::FutureExt;
use redis::aio::{ConnectionLike, MultiplexedConnection};
use redis::{ConnectionInfo, IntoConnectionInfo, RedisConnectionInfo, RedisError, RedisResult};
use tokio::task::AbortHandle;
use tracing::{error, info, warn};

use super::elasticache::CredentialsProvider;
use crate::redis::elasticache::CredentialsProviderError;

enum Credentials {
    Static(ConnectionInfo),
    Dynamic(Arc<CredentialsProvider>, redis::ConnectionAddr),
}

impl Clone for Credentials {
    fn clone(&self) -> Self {
        match self {
            Credentials::Static(info) => Credentials::Static(info.clone()),
            Credentials::Dynamic(provider, addr) => {
                Credentials::Dynamic(Arc::clone(provider), addr.clone())
            }
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum ConnectionProviderError {
    #[error(transparent)]
    Redis(#[from] RedisError),
    #[error(transparent)]
    CredentialsProvider(#[from] CredentialsProviderError),
}

/// A wrapper around `redis::MultiplexedConnection` that automatically refreshes the token.
/// Provides PubSub connection without credentials refresh.
pub struct ConnectionWithCredentialsProvider {
    credentials: Credentials,
    // TODO: with more load on the connection, we should consider using a connection pool
    con: Option<MultiplexedConnection>,
    refresh_token_task: Option<AbortHandle>,
    mutex: tokio::sync::Mutex<()>,
    credentials_refreshed: Arc<AtomicBool>,
}

impl Clone for ConnectionWithCredentialsProvider {
    fn clone(&self) -> Self {
        Self {
            credentials: self.credentials.clone(),
            con: None,
            refresh_token_task: None,
            mutex: tokio::sync::Mutex::new(()),
            credentials_refreshed: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl ConnectionWithCredentialsProvider {
    pub fn new_with_credentials_provider(
        host: String,
        port: u16,
        credentials_provider: Arc<CredentialsProvider>,
    ) -> Self {
        Self {
            credentials: Credentials::Dynamic(
                credentials_provider,
                redis::ConnectionAddr::TcpTls {
                    host,
                    port,
                    insecure: false,
                    tls_params: None,
                },
            ),
            con: None,
            refresh_token_task: None,
            mutex: tokio::sync::Mutex::new(()),
            credentials_refreshed: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn new_with_static_credentials<T: IntoConnectionInfo>(params: T) -> Self {
        Self {
            credentials: Credentials::Static(
                params
                    .into_connection_info()
                    .expect("static configured redis credentials should be a valid format"),
            ),
            con: None,
            refresh_token_task: None,
            mutex: tokio::sync::Mutex::new(()),
            credentials_refreshed: Arc::new(AtomicBool::new(true)),
        }
    }

    async fn ping(con: &mut MultiplexedConnection) -> Result<(), ConnectionProviderError> {
        redis::cmd("PING")
            .query_async(con)
            .await
            .map_err(Into::into)
    }

    pub(crate) fn credentials_refreshed(&self) -> bool {
        self.credentials_refreshed.load(Ordering::Relaxed)
    }

    pub(crate) async fn connect(&mut self) -> Result<(), ConnectionProviderError> {
        let _guard = self.mutex.lock().await;
        if let Some(con) = self.con.as_mut() {
            match Self::ping(con).await {
                Ok(()) => {
                    return Ok(());
                }
                Err(e) => {
                    warn!("Error during PING: {e:?}");
                }
            }
        } else {
            info!("Connection is not established");
        }
        info!("Establishing a new connection...");
        self.con = None;
        if let Some(f) = self.refresh_token_task.take() {
            f.abort();
        }
        let mut con = self
            .get_client()
            .await?
            .get_multiplexed_tokio_connection()
            .await?;
        if let Credentials::Dynamic(credentials_provider, _) = &self.credentials {
            let credentials_provider = credentials_provider.clone();
            let con2 = con.clone();
            let credentials_refreshed = self.credentials_refreshed.clone();
            let f = tokio::spawn(Self::keep_connection(
                con2,
                credentials_provider,
                credentials_refreshed,
            ));
            self.refresh_token_task = Some(f.abort_handle());
        }
        match Self::ping(&mut con).await {
            Ok(()) => {
                info!("Connection succesfully established");
            }
            Err(e) => {
                warn!("Connection is broken. Error during PING: {e:?}");
            }
        }
        self.con = Some(con);
        Ok(())
    }

    async fn get_connection_info(&self) -> Result<ConnectionInfo, ConnectionProviderError> {
        match &self.credentials {
            Credentials::Static(info) => Ok(info.clone()),
            Credentials::Dynamic(provider, addr) => {
                let (username, password) = provider.provide_credentials().await?;
                Ok(ConnectionInfo {
                    addr: addr.clone(),
                    redis: RedisConnectionInfo {
                        db: 0,
                        username: Some(username),
                        password: Some(password.clone()),
                        // TODO: switch to RESP3 after testing new client version.
                        protocol: redis::ProtocolVersion::RESP2,
                    },
                })
            }
        }
    }

    async fn get_client(&self) -> Result<redis::Client, ConnectionProviderError> {
        let client = redis::Client::open(self.get_connection_info().await?)?;
        self.credentials_refreshed.store(true, Ordering::Relaxed);
        Ok(client)
    }

    // PubSub does not support credentials refresh.
    // Requires manual reconnection every 12h.
    pub(crate) async fn get_async_pubsub(&self) -> anyhow::Result<redis::aio::PubSub> {
        Ok(self.get_client().await?.get_async_pubsub().await?)
    }

    // The connection lives for 12h.
    // It can be prolonged with sending `AUTH` commands with the refreshed token.
    // https://docs.aws.amazon.com/AmazonElastiCache/latest/red-ug/auth-iam.html#auth-iam-limits
    async fn keep_connection(
        mut con: MultiplexedConnection,
        credentials_provider: Arc<CredentialsProvider>,
        credentials_refreshed: Arc<AtomicBool>,
    ) -> ! {
        loop {
            // The connection lives for 12h, for the sanity check we refresh it every hour.
            tokio::time::sleep(Duration::from_secs(60 * 60)).await;
            match Self::refresh_token(&mut con, credentials_provider.clone()).await {
                Ok(()) => {
                    info!("Token refreshed");
                    credentials_refreshed.store(true, Ordering::Relaxed);
                }
                Err(e) => {
                    error!("Error during token refresh: {e:?}");
                    credentials_refreshed.store(false, Ordering::Relaxed);
                }
            }
        }
    }
    async fn refresh_token(
        con: &mut MultiplexedConnection,
        credentials_provider: Arc<CredentialsProvider>,
    ) -> anyhow::Result<()> {
        let (user, password) = credentials_provider.provide_credentials().await?;
        let _: () = redis::cmd("AUTH")
            .arg(user)
            .arg(password)
            .query_async(con)
            .await?;
        Ok(())
    }
    /// Sends an already encoded (packed) command into the TCP socket and
    /// reads the single response from it.
    pub(crate) async fn send_packed_command(
        &mut self,
        cmd: &redis::Cmd,
    ) -> RedisResult<redis::Value> {
        // Clone connection to avoid having to lock the ArcSwap in write mode
        let con = self.con.as_mut().ok_or(redis::RedisError::from((
            redis::ErrorKind::IoError,
            "Connection not established",
        )))?;
        con.send_packed_command(cmd).await
    }

    /// Sends multiple already encoded (packed) command into the TCP socket
    /// and reads `count` responses from it.  This is used to implement
    /// pipelining.
    pub(crate) async fn send_packed_commands(
        &mut self,
        cmd: &redis::Pipeline,
        offset: usize,
        count: usize,
    ) -> RedisResult<Vec<redis::Value>> {
        // Clone shared connection future to avoid having to lock the ArcSwap in write mode
        let con = self.con.as_mut().ok_or(redis::RedisError::from((
            redis::ErrorKind::IoError,
            "Connection not established",
        )))?;
        con.send_packed_commands(cmd, offset, count).await
    }
}

impl ConnectionLike for ConnectionWithCredentialsProvider {
    fn req_packed_command<'a>(
        &'a mut self,
        cmd: &'a redis::Cmd,
    ) -> redis::RedisFuture<'a, redis::Value> {
        self.send_packed_command(cmd).boxed()
    }

    fn req_packed_commands<'a>(
        &'a mut self,
        cmd: &'a redis::Pipeline,
        offset: usize,
        count: usize,
    ) -> redis::RedisFuture<'a, Vec<redis::Value>> {
        self.send_packed_commands(cmd, offset, count).boxed()
    }

    fn get_db(&self) -> i64 {
        self.con.as_ref().map_or(0, |c| c.get_db())
    }
}
