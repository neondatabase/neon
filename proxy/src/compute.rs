use crate::cancellation::CancelClosure;
use crate::error::UserFacingError;
use serde::{Deserialize, Serialize};
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

/// Compute node connection params.
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct DatabaseInfo {
    pub host: String,
    pub port: u16,
    pub dbname: String,
    pub user: String,
    pub password: Option<String>,
}

/// PostgreSQL version as [`String`].
pub type Version = String;

impl DatabaseInfo {
    async fn connect_raw(&self) -> io::Result<(SocketAddr, TcpStream)> {
        let host_port = format!("{}:{}", self.host, self.port);
        let socket = TcpStream::connect(host_port).await?;
        let socket_addr = socket.peer_addr()?;

        Ok((socket_addr, socket))
    }

    /// Connect to a corresponding compute node.
    pub async fn connect(self) -> Result<(TcpStream, Version, CancelClosure), ConnectionError> {
        let (socket_addr, mut socket) = self
            .connect_raw()
            .await
            .map_err(|_| ConnectionError::FailedToConnectToCompute)?;

        // TODO: establish a secure connection to the DB
        let (client, conn) = tokio_postgres::Config::from(self)
            .connect_raw(&mut socket, NoTls)
            .await?;

        let version = conn
            .parameter("server_version")
            .ok_or(ConnectionError::FailedToFetchPgVersion)?
            .into();

        let cancel_closure = CancelClosure::new(socket_addr, client.cancel_token());

        Ok((socket, version, cancel_closure))
    }
}

impl From<DatabaseInfo> for tokio_postgres::Config {
    fn from(db_info: DatabaseInfo) -> Self {
        let mut config = tokio_postgres::Config::new();

        config
            .host(&db_info.host)
            .port(db_info.port)
            .dbname(&db_info.dbname)
            .user(&db_info.user);

        if let Some(password) = db_info.password {
            config.password(password);
        }

        config
    }
}
