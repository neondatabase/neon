use crate::{cancellation::CancelClosure, error::UserFacingError};
use futures::TryFutureExt;
use itertools::Itertools;
use pq_proto::StartupMessageParams;
use std::{io, net::SocketAddr};
use thiserror::Error;
use tokio::net::TcpStream;
use tokio_postgres::NoTls;
use tracing::{error, info};

const COULD_NOT_CONNECT: &str = "Could not connect to compute node";

#[derive(Debug, Error)]
pub enum ConnectionError {
    /// This error doesn't seem to reveal any secrets; for instance,
    /// [`tokio_postgres::error::Kind`] doesn't contain ip addresses and such.
    #[error("{COULD_NOT_CONNECT}: {0}")]
    Postgres(#[from] tokio_postgres::Error),

    #[error("{COULD_NOT_CONNECT}: {0}")]
    CouldNotConnect(#[from] io::Error),
}

impl UserFacingError for ConnectionError {
    fn to_string_client(&self) -> String {
        use ConnectionError::*;
        match self {
            // This helps us drop irrelevant library-specific prefixes.
            // TODO: propagate severity level and other parameters.
            Postgres(err) => match err.as_db_error() {
                Some(err) => err.message().to_owned(),
                None => err.to_string(),
            },
            _ => COULD_NOT_CONNECT.to_owned(),
        }
    }
}

/// A pair of `ClientKey` & `ServerKey` for `SCRAM-SHA-256`.
pub type ScramKeys = tokio_postgres::config::ScramKeys<32>;

/// A config for establishing a connection to compute node.
/// Eventually, `tokio_postgres` will be replaced with something better.
/// Newtype allows us to implement methods on top of it.
#[derive(Clone)]
#[repr(transparent)]
pub struct ConnCfg(Box<tokio_postgres::Config>);

impl ConnCfg {
    /// Construct a new connection config.
    pub fn new() -> Self {
        Self(Default::default())
    }
}

impl std::ops::Deref for ConnCfg {
    type Target = tokio_postgres::Config;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// For now, let's make it easier to setup the config.
impl std::ops::DerefMut for ConnCfg {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl ConnCfg {
    /// Establish a raw TCP connection to the compute node.
    async fn connect_raw(&self) -> io::Result<(SocketAddr, TcpStream)> {
        use tokio_postgres::config::Host;

        let connect_once = |host, port| {
            info!("trying to connect to a compute node at {host}:{port}");
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
        let ports = self.0.get_ports();
        let hosts = self.0.get_hosts();
        // the ports array is supposed to have 0 entries, 1 entry, or as many entries as in the hosts array
        if ports.len() > 1 && ports.len() != hosts.len() {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!(
                    "couldn't connect: bad compute config, \
                     ports and hosts entries' count does not match: {:?}",
                    self.0
                ),
            ));
        }

        for (i, host) in hosts.iter().enumerate() {
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
                    error!("failed to connect to a compute node at {host}:{port}: {err}");
                    connection_error = Some(err);
                }
            }
        }

        Err(connection_error.unwrap_or_else(|| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("couldn't connect: bad compute config: {:?}", self.0),
            )
        }))
    }
}

pub struct PostgresConnection {
    /// Socket connected to a compute node.
    pub stream: TcpStream,
    /// PostgreSQL connection parameters.
    pub params: std::collections::HashMap<String, String>,
}

impl ConnCfg {
    /// Connect to a corresponding compute node.
    pub async fn connect(
        &mut self,
        params: &StartupMessageParams,
    ) -> Result<(PostgresConnection, CancelClosure), ConnectionError> {
        if let Some(options) = params.options_raw() {
            // We must drop all proxy-specific parameters.
            #[allow(unstable_name_collisions)]
            let options: String = options
                .filter(|opt| !opt.starts_with("project="))
                .intersperse(" ") // TODO: use impl from std once it's stabilized
                .collect();

            self.0.options(&options);
        }

        if let Some(app_name) = params.get("application_name") {
            self.0.application_name(app_name);
        }

        // TODO: This is especially ugly...
        if let Some(replication) = params.get("replication") {
            use tokio_postgres::config::ReplicationMode;
            match replication {
                "true" | "on" | "yes" | "1" => {
                    self.0.replication_mode(ReplicationMode::Physical);
                }
                "database" => {
                    self.0.replication_mode(ReplicationMode::Logical);
                }
                _other => {}
            }
        }

        // TODO: extend the list of the forwarded startup parameters.
        // Currently, tokio-postgres doesn't allow us to pass
        // arbitrary parameters, but the ones above are a good start.
        //
        // This and the reverse params problem can be better addressed
        // in a bespoke connection machinery (a new library for that sake).

        // TODO: establish a secure connection to the DB.
        let (socket_addr, mut stream) = self.connect_raw().await?;
        let (client, connection) = self.0.connect_raw(&mut stream, NoTls).await?;
        info!("connected to user's compute node at {socket_addr}");

        // This is very ugly but as of now there's no better way to
        // extract the connection parameters from tokio-postgres' connection.
        // TODO: solve this problem in a more elegant manner (e.g. the new library).
        let params = connection.parameters;

        // NB: CancelToken is supposed to hold socket_addr, but we use connect_raw.
        // Yet another reason to rework the connection establishing code.
        let cancel_closure = CancelClosure::new(socket_addr, client.cancel_token());
        let db = PostgresConnection { stream, params };

        Ok((db, cancel_closure))
    }
}
