use crate::{
    auth::parse_endpoint_param,
    cancellation::CancelClosure,
    console::messages::{DatabaseInfo, MetricsAuxInfo},
    console::CachedNodeInfo,
    error::UserFacingError,
};
use futures::{FutureExt, TryFutureExt};
use itertools::Itertools;
use pq_proto::StartupMessageParams;
use std::{
    io,
    net::SocketAddr,
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};
use thiserror::Error;
use tokio::net::TcpStream;
use tokio_postgres::tls::MakeTlsConnect;
use tracing::{error, info, warn};

/// Should we allow self-signed certificates in TLS connections?
/// Most definitely, this shouldn't be allowed in production.
pub static ALLOW_SELF_SIGNED_COMPUTE: AtomicBool = AtomicBool::new(false);

const COULD_NOT_CONNECT: &str = "Couldn't connect to compute node";

#[derive(Debug, Error)]
pub enum ConnectionError {
    /// This error doesn't seem to reveal any secrets; for instance,
    /// [`tokio_postgres::error::Kind`] doesn't contain ip addresses and such.
    #[error("{COULD_NOT_CONNECT}: {0}")]
    Postgres(#[from] tokio_postgres::Error),

    #[error("{COULD_NOT_CONNECT}: {0}")]
    CouldNotConnect(#[from] io::Error),

    #[error("{COULD_NOT_CONNECT}: {0}")]
    TlsError(#[from] native_tls::Error),
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

#[derive(Clone)]
pub enum Password {
    /// A regular cleartext password.
    ClearText(Vec<u8>),
    /// A pair of `ClientKey` & `ServerKey` for `SCRAM-SHA-256`.
    ScramKeys(ScramKeys),
}

pub enum ComputeNode {
    /// Route via link auth.
    Link(DatabaseInfo),
    /// Regular compute node.
    Static {
        password: Password,
        info: CachedNodeInfo,
    },
}

impl ComputeNode {
    /// Get metrics auxiliary info.
    pub fn metrics_aux_info(&self) -> &MetricsAuxInfo {
        match self {
            Self::Link(info) => &info.aux,
            Self::Static { info, .. } => &info.aux,
        }
    }

    /// Invalidate compute node info if it's cached.
    pub fn invalidate(&self) -> bool {
        if let Self::Static { info, .. } = self {
            warn!("invalidating compute node info cache entry");
            info.invalidate();
            return true;
        }

        false
    }

    /// Turn compute node info into a postgres connection config.
    pub fn to_conn_config(&self) -> ConnCfg {
        let mut config = ConnCfg::new();

        let (host, port) = match self {
            Self::Link(info) => {
                // NB: use pre-supplied dbname, user and password for link auth.
                // See `ConnCfg::set_startup_params` below.
                config.0.dbname(&info.dbname).user(&info.user);
                if let Some(password) = &info.password {
                    config.0.password(password.as_bytes());
                }

                (&info.host, info.port)
            }
            Self::Static { info, password } => {
                // NB: setup auth keys (for SCRAM) or plaintext password.
                match password {
                    Password::ClearText(text) => config.0.password(text),
                    Password::ScramKeys(keys) => {
                        use tokio_postgres::config::AuthKeys;
                        config.0.auth_keys(AuthKeys::ScramSha256(keys.to_owned()))
                    }
                };

                (&info.address.host, info.address.port)
            }
        };

        // Backwards compatibility. pg_sni_proxy uses "--" in domain names
        // while direct connections do not. Once we migrate to pg_sni_proxy
        // everywhere, we can remove this.
        config.0.ssl_mode(if host.contains("--") {
            // We need TLS connection with SNI info to properly route it.
            tokio_postgres::config::SslMode::Require
        } else {
            tokio_postgres::config::SslMode::Disable
        });

        config.0.host(host).port(port);
        config
    }
}

/// A config for establishing a connection to compute node.
/// Eventually, `tokio_postgres` will be replaced with something better.
/// Newtype allows us to implement methods on top of it.
#[derive(Clone)]
#[repr(transparent)]
pub struct ConnCfg(Box<tokio_postgres::Config>);

/// Creation and initialization routines.
impl ConnCfg {
    fn new() -> Self {
        Self(Default::default())
    }

    /// Apply startup message params to the connection config.
    pub fn set_startup_params(&mut self, params: &StartupMessageParams) {
        // Only set `user` if it's not present in the config.
        // Link auth flow takes username from the console's response.
        if let (None, Some(user)) = (self.0.get_user(), params.get("user")) {
            self.0.user(user);
        }

        // Only set `dbname` if it's not present in the config.
        // Link auth flow takes dbname from the console's response.
        if let (None, Some(dbname)) = (self.0.get_dbname(), params.get("database")) {
            self.0.dbname(dbname);
        }

        // Don't add `options` if they were only used for specifying a project.
        // Connection pools don't support `options`, because they affect backend startup.
        if let Some(options) = filtered_options(params) {
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
    }
}

impl ConnCfg {
    /// Establish a raw TCP connection to the compute node.
    async fn connect_raw(&self) -> io::Result<(SocketAddr, TcpStream, &str)> {
        use tokio_postgres::config::Host;

        // wrap TcpStream::connect with timeout
        let connect_with_timeout = |host, port| {
            let connection_timeout = Duration::from_millis(10000);
            tokio::time::timeout(connection_timeout, TcpStream::connect((host, port))).map(
                move |res| match res {
                    Ok(tcpstream_connect_res) => tcpstream_connect_res,
                    Err(_) => Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        format!("exceeded connection timeout {connection_timeout:?}"),
                    )),
                },
            )
        };

        let connect_once = |host, port| {
            info!("trying to connect to compute node at {host}:{port}");
            connect_with_timeout(host, port).and_then(|socket| async {
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
                    "bad compute config, \
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

            match connect_once(host, *port).await {
                Ok((sockaddr, stream)) => return Ok((sockaddr, stream, host)),
                Err(err) => {
                    // We can't throw an error here, as there might be more hosts to try.
                    warn!("couldn't connect to compute node at {host}:{port}: {err}");
                    connection_error = Some(err);
                }
            }
        }

        Err(connection_error.unwrap_or_else(|| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("bad compute config: {:?}", self.0),
            )
        }))
    }
}

pub struct PostgresConnection {
    /// Socket connected to a compute node.
    pub stream: tokio_postgres::maybe_tls_stream::MaybeTlsStream<
        tokio::net::TcpStream,
        postgres_native_tls::TlsStream<tokio::net::TcpStream>,
    >,
    /// PostgreSQL connection parameters.
    pub params: std::collections::HashMap<String, String>,
    /// Query cancellation token.
    pub cancel_closure: CancelClosure,
}

impl ConnCfg {
    async fn do_connect(&self) -> Result<PostgresConnection, ConnectionError> {
        let (socket_addr, stream, host) = self.connect_raw().await?;

        let tls_connector = native_tls::TlsConnector::builder()
            .danger_accept_invalid_certs(ALLOW_SELF_SIGNED_COMPUTE.load(Ordering::Relaxed))
            .build()?;

        let mut mk_tls = postgres_native_tls::MakeTlsConnector::new(tls_connector);
        let tls = MakeTlsConnect::<tokio::net::TcpStream>::make_tls_connect(&mut mk_tls, host)?;

        // connect_raw() will not use TLS if sslmode is "disable"
        let (client, connection) = self.0.connect_raw(stream, tls).await?;
        let stream = connection.stream.into_inner();

        info!(
            "connected to compute node at {host} ({socket_addr}) sslmode={:?}",
            self.0.get_ssl_mode()
        );

        // This is very ugly but as of now there's no better way to
        // extract the connection parameters from tokio-postgres' connection.
        // TODO: solve this problem in a more elegant manner (e.g. the new library).
        let params = connection.parameters;

        // NB: CancelToken is supposed to hold socket_addr, but we use connect_raw.
        // Yet another reason to rework the connection establishing code.
        let cancel_closure = CancelClosure::new(socket_addr, client.cancel_token());

        let connection = PostgresConnection {
            stream,
            params,
            cancel_closure,
        };

        Ok(connection)
    }

    /// Connect to a corresponding compute node.
    pub async fn connect(&self) -> Result<PostgresConnection, ConnectionError> {
        self.do_connect()
            .inspect_err(|err| {
                // Immediately log the error we have at our disposal.
                error!("couldn't connect to compute node: {err}");
            })
            .await
    }
}

/// Retrieve `options` from a startup message, dropping all proxy-secific flags.
fn filtered_options(params: &StartupMessageParams) -> Option<String> {
    #[allow(unstable_name_collisions)]
    let options: String = params
        .options_raw()?
        .filter(|opt| parse_endpoint_param(opt).is_none())
        .intersperse(" ") // TODO: use impl from std once it's stabilized
        .collect();

    // Don't even bother with empty options.
    if options.is_empty() {
        return None;
    }

    Some(options)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_filtered_options() {
        // Empty options is unlikely to be useful anyway.
        let params = StartupMessageParams::new([("options", "")]);
        assert_eq!(filtered_options(&params), None);

        // It's likely that clients will only use options to specify endpoint/project.
        let params = StartupMessageParams::new([("options", "project=foo")]);
        assert_eq!(filtered_options(&params), None);

        // Same, because unescaped whitespaces are no-op.
        let params = StartupMessageParams::new([("options", " project=foo ")]);
        assert_eq!(filtered_options(&params).as_deref(), None);

        let params = StartupMessageParams::new([("options", r"\  project=foo \ ")]);
        assert_eq!(filtered_options(&params).as_deref(), Some(r"\  \ "));

        let params = StartupMessageParams::new([("options", "project = foo")]);
        assert_eq!(filtered_options(&params).as_deref(), Some("project = foo"));
    }
}
