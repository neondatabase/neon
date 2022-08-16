use crate::{cancellation::CancelClosure, error::UserFacingError};
use futures::TryFutureExt;
use std::{io, net::SocketAddr};
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

impl UserFacingError for ConnectionError {
    fn to_string_client(&self) -> String {
        use ConnectionError::*;
        match self {
            // This helps us drop irrelevant library-specific prefixes.
            // TODO: propagate severity level and other parameters.
            Postgres(err) => match err.as_db_error() {
                Some(err) => err.message().to_string(),
                None => err.to_string(),
            },
            other => other.to_string(),
        }
    }
}

/// A pair of `ClientKey` & `ServerKey` for `SCRAM-SHA-256`.
pub type ScramKeys = tokio_postgres::config::ScramKeys<32>;

pub type ComputeConnCfg = tokio_postgres::Config;

/// Various compute node info for establishing connection etc.
pub struct NodeInfo {
    /// Did we send [`utils::pq_proto::BeMessage::AuthenticationOk`]?
    pub reported_auth_ok: bool,
    /// Compute node connection params.
    pub config: tokio_postgres::Config,
}

impl NodeInfo {
    async fn connect_raw(&self) -> io::Result<(SocketAddr, TcpStream)> {
        use tokio_postgres::config::Host;

        let connect_once = |host, port| {
            TcpStream::connect((host, port)).and_then(|socket| async {
                let socket_addr = socket.peer_addr()?;
                // This prevents load balancer from severing the connection.
                socket2::SockRef::from(&socket).set_keepalive(true)?;
                Ok((socket_addr, socket))
            })
        };

        // We can't reuse connection establishing logic from `tokio_postgres` here,
        // because it has no means for extracting the underlying socket which we
        // require for our business.
        let mut connection_error = None;
        let ports = self.config.get_ports();
        for (i, host) in self.config.get_hosts().iter().enumerate() {
            let port = ports.get(i).or_else(|| ports.first()).unwrap_or(&5432);
            let host = match host {
                Host::Tcp(host) => host.as_str(),
                Host::Unix(_) => continue, // unix sockets are not welcome here
            };

            // TODO: maybe we should add a timeout.
            match connect_once(host, *port).await {
                Ok(socket) => return Ok(socket),
                Err(err) => {
                    // We can't throw an error here, as there might be more hosts to try.
                    println!("failed to connect to compute `{host}:{port}`: {err}");
                    connection_error = Some(err);
                }
            }
        }

        Err(connection_error.unwrap_or_else(|| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("couldn't connect: bad compute config: {:?}", self.config),
            )
        }))
    }
}

pub struct PostgresConnection {
    /// Socket connected to a compute node.
    pub stream: TcpStream,
    /// PostgreSQL version of this instance.
    pub version: String,
}

impl NodeInfo {
    /// Connect to a corresponding compute node.
    pub async fn connect(&self) -> Result<(PostgresConnection, CancelClosure), ConnectionError> {
        let (socket_addr, mut stream) = self
            .connect_raw()
            .await
            .map_err(|_| ConnectionError::FailedToConnectToCompute)?;

        // TODO: establish a secure connection to the DB
        let (client, conn) = self.config.connect_raw(&mut stream, NoTls).await?;
        let version = conn
            .parameter("server_version")
            .ok_or(ConnectionError::FailedToFetchPgVersion)?
            .into();

        let cancel_closure = CancelClosure::new(socket_addr, client.cancel_token());

        let db = PostgresConnection { stream, version };

        Ok((db, cancel_closure))
    }
}
