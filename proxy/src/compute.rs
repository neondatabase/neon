use crate::auth::DatabaseInfo;
use crate::cancellation::CancelClosure;
use crate::error::UserFacingError;
use std::io;
use std::net::SocketAddr;
use thiserror::Error;
use tokio::net::TcpStream;
use tokio_postgres::NoTls;

#[derive(Debug, Error)]
pub enum ConnectionError {
    /// This error doesn't seem to reveal any secrets; for instance,
    /// [`tokio_postgres::error::Kind`] doesn't contain ip addresses and such.
    #[error("Failed to connect to the compute node: {0}")]
    Postgres(#[from] tokio_postgres::Error),

    #[error("Failed to connect to the compute node")]
    FailedToConnectToCompute,

    #[error("Failed to fetch compute node version")]
    FailedToFetchPgVersion,
}

impl UserFacingError for ConnectionError {}

/// PostgreSQL version as [`String`].
pub type Version = String;

/// A pair of `ClientKey` & `ServerKey` for `SCRAM-SHA-256`.
pub type ScramKeys = tokio_postgres::config::ScramKeys<32>;

/// Compute node connection params.
pub struct NodeInfo {
    pub db_info: DatabaseInfo,
    pub scram_keys: Option<ScramKeys>,
}

impl NodeInfo {
    async fn connect_raw(&self) -> io::Result<(SocketAddr, TcpStream)> {
        let host_port = (self.db_info.host.as_str(), self.db_info.port);
        let socket = TcpStream::connect(host_port).await?;
        let socket_addr = socket.peer_addr()?;
        socket2::SockRef::from(&socket).set_keepalive(true)?;

        Ok((socket_addr, socket))
    }

    /// Connect to a corresponding compute node.
    pub async fn connect(self) -> Result<(TcpStream, Version, CancelClosure), ConnectionError> {
        let (socket_addr, mut socket) = self
            .connect_raw()
            .await
            .map_err(|_| ConnectionError::FailedToConnectToCompute)?;

        let mut config = tokio_postgres::Config::from(self.db_info);
        if let Some(scram_keys) = self.scram_keys {
            config.auth_keys(tokio_postgres::config::AuthKeys::ScramSha256(scram_keys));
        }

        // TODO: establish a secure connection to the DB
        let (client, conn) = config.connect_raw(&mut socket, NoTls).await?;
        let version = conn
            .parameter("server_version")
            .ok_or(ConnectionError::FailedToFetchPgVersion)?
            .into();

        let cancel_closure = CancelClosure::new(socket_addr, client.cancel_token());

        Ok((socket, version, cancel_closure))
    }
}
