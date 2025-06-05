use std::fmt::Debug;
use std::io;
use std::net::IpAddr;
use std::net::SocketAddr;

use futures::{FutureExt, TryFutureExt};
use itertools::Itertools;
use postgres_client::config::AuthKeys;
use postgres_client::config::SslMode;
use rustls::pki_types::InvalidDnsNameError;
use thiserror::Error;
use tokio::net::{TcpStream, lookup_host};
use tracing::{debug, error, info, warn};

use crate::auth::backend::ComputeCredentialKeys;
use crate::auth::parse_endpoint_param;
use crate::config::ComputeConfig;
use crate::context::RequestContext;
use crate::control_plane::client::ApiLockError;
use crate::control_plane::errors::WakeComputeError;
use crate::control_plane::messages::MetricsAuxInfo;
use crate::error::{ReportableError, UserFacingError};
use crate::metrics::{Metrics, NumDbConnectionsGuard};
use crate::pqproto::StartupMessageParams;
use crate::proxy::neon_option;
use crate::stream::PostgresError;
use crate::stream::PqStream;
use crate::types::Host;

mod connect_raw;
pub use connect_raw::Stream;

pub const COULD_NOT_CONNECT: &str = "Couldn't connect to compute node";

#[derive(Debug, Error)]
pub(crate) enum ConnectionError {
    #[error("{COULD_NOT_CONNECT}: {0}")]
    Postgres(#[from] PostgresError),

    #[error("{COULD_NOT_CONNECT}: {0}")]
    TlsError(#[from] TlsError),

    #[error("{COULD_NOT_CONNECT}: {0}")]
    WakeComputeError(#[from] WakeComputeError),

    #[error("error acquiring resource permit: {0}")]
    TooManyConnectionAttempts(#[from] ApiLockError),
}

#[derive(Debug, Error)]
pub enum TlsError {
    #[error("{0}")]
    Dns(#[from] InvalidDnsNameError),
    #[error("{0}")]
    Connection(#[from] std::io::Error),
    #[error("TLS required but not provided")]
    Required,
}

impl UserFacingError for ConnectionError {
    fn to_string_client(&self) -> String {
        match self {
            // This helps us drop irrelevant library-specific prefixes.
            // TODO: propagate severity level and other parameters.
            ConnectionError::Postgres(PostgresError::Error(err)) => {
                let (_code, msg) = err.parse();
                let msg = String::from_utf8_lossy(msg);

                if msg.starts_with("unsupported startup parameter: ")
                    || msg.starts_with("unsupported startup parameter in options: ")
                {
                    format!(
                        "{msg}. Please use unpooled connection or remove this parameter from the startup package. More details: https://neon.tech/docs/connect/connection-errors#unsupported-startup-parameter"
                    )
                } else {
                    msg.into_owned()
                }
            }
            ConnectionError::Postgres(err) => err.to_string(),
            ConnectionError::WakeComputeError(err) => err.to_string_client(),
            ConnectionError::TooManyConnectionAttempts(_) => {
                "Failed to acquire permit to connect to the database. Too many database connection attempts are currently ongoing.".to_owned()
            }
            ConnectionError::TlsError(_) => {
                "Failed to establish secure connection to the database.".to_string()
            }
        }
    }
}

impl ReportableError for ConnectionError {
    fn get_error_kind(&self) -> crate::error::ErrorKind {
        match self {
            ConnectionError::Postgres(PostgresError::Io(_)) => crate::error::ErrorKind::Compute,
            ConnectionError::Postgres(
                PostgresError::Error(_)
                | PostgresError::InvalidAuthMessage
                | PostgresError::Unexpected(_),
            ) => crate::error::ErrorKind::Postgres,
            // ConnectionError::CouldNotConnect(_) => crate::error::ErrorKind::Compute,
            ConnectionError::TlsError(_) => crate::error::ErrorKind::Compute,
            ConnectionError::WakeComputeError(e) => e.get_error_kind(),
            ConnectionError::TooManyConnectionAttempts(e) => e.get_error_kind(),
        }
    }
}

/// Info for establishing a connection to a compute node.
/// This is what we get after auth succeeded, but not before!
#[derive(Clone)]
pub(crate) struct NodeInfo {
    /// Compute node connection params.
    /// It's sad that we have to clone this, but this will improve
    /// once we migrate to a bespoke connection logic.
    pub(crate) config: ConnectInfo,

    /// Labels for proxy's metrics.
    pub(crate) aux: MetricsAuxInfo,
}

/// A pair of `ClientKey` & `ServerKey` for `SCRAM-SHA-256`.
pub(crate) type ScramKeys = postgres_client::config::ScramKeys<32>;

#[derive(Clone)]
pub enum Auth {
    Password(Vec<u8>),
    Scram(Box<ScramKeys>),
}

/// A config for authenticating to the compute node.
#[derive(Clone)]
pub(crate) struct AuthInfo {
    pub(crate) auth: Option<Auth>,
    pub server_params: StartupMessageParams,
    skip_db_user: bool,
}
impl AuthInfo {
    pub(crate) fn with_keys(keys: ComputeCredentialKeys) -> Self {
        let auth = match keys {
            ComputeCredentialKeys::AuthKeys(AuthKeys::ScramSha256(auth_keys)) => {
                Some(Auth::Scram(Box::new(auth_keys)))
            }
            ComputeCredentialKeys::JwtPayload(_) | ComputeCredentialKeys::None => None,
        };
        Self {
            auth,
            server_params: StartupMessageParams::default(),
            skip_db_user: false,
        }
    }

    pub(crate) fn for_console_redirect(db: &str, user: &str, pw: Option<&str>) -> Self {
        let mut params = StartupMessageParams::default();
        params.insert("database", db);
        params.insert("user", user);

        Self {
            auth: pw.map(|pw| Auth::Password(pw.as_bytes().to_owned())),
            server_params: params,
            skip_db_user: true,
        }
    }

    /// Apply startup message params to the connection config.
    pub(crate) fn set_startup_params(
        mut self,
        params: &StartupMessageParams,
        arbitrary_params: bool,
    ) -> Self {
        if !arbitrary_params {
            self.server_params.insert("client_encoding", "UTF8");
        }
        for (k, v) in params.iter() {
            match k {
                // Only set `user` if it's not present in the config.
                // Console redirect auth flow takes username from the console's response.
                "user" | "database" if self.skip_db_user => {}
                // filter out neon specific options, as postgres won't
                // know what to do with them.
                "options" => {
                    if let Some(options) = filtered_options(v) {
                        self.server_params.insert(k, &options);
                    }
                }
                "user" | "database" | "application_name" | "replication" => {
                    self.server_params.insert(k, v);
                }

                // if we allow arbitrary params, then we forward them through.
                // this is a flag for a period of backwards compatibility
                k if arbitrary_params => {
                    self.server_params.insert(k, v);
                }
                _ => {}
            }
        }
        self
    }

    async fn authenticate(
        &self,
        stream: Stream<TcpStream>,
    ) -> Result<PqStream<Stream<TcpStream>>, PostgresError> {
        connect_raw::authenticate(stream, self.auth.as_ref(), &self.server_params).await
    }
}

/// Contains only the data needed to establish a secure connection to compute.
#[derive(Clone)]
pub struct ConnectInfo {
    pub host_addr: Option<IpAddr>,
    pub host: Host,
    pub port: u16,
    pub ssl_mode: SslMode,
}

impl ConnectInfo {
    pub(crate) fn new(host: Host, port: u16, ssl_mode: SslMode) -> Self {
        Self {
            host_addr: None,
            host,
            port,
            ssl_mode,
        }
    }

    pub fn to_postgres_client_config(&self) -> postgres_client::Config {
        let mut config = postgres_client::Config::new(self.host.to_string(), self.port);
        config.ssl_mode(self.ssl_mode);
        if let Some(host_addr) = self.host_addr {
            config.set_host_addr(host_addr);
        }
        config
    }

    /// Establish a raw TCP connection to the compute node.
    async fn connect(
        &self,
        config: &ComputeConfig,
    ) -> Result<(SocketAddr, Stream<TcpStream>), TlsError> {
        // wrap TcpStream::connect with timeout
        let connect_with_timeout = |addrs| {
            tokio::time::timeout(config.timeout, TcpStream::connect(addrs)).map(
                move |res| match res {
                    Ok(tcpstream_connect_res) => tcpstream_connect_res,
                    Err(_) => Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        format!("exceeded connection timeout {:?}", config.timeout),
                    )),
                },
            )
        };

        let connect_once = |addrs| {
            debug!("trying to connect to compute node at {addrs:?}");
            connect_with_timeout(addrs).and_then(|stream| async {
                let socket_addr = stream.peer_addr()?;
                let socket = socket2::SockRef::from(&stream);
                // Disable Nagle's algorithm to not introduce latency between
                // client and compute.
                socket.set_nodelay(true)?;
                // This prevents load balancer from severing the connection.
                socket.set_keepalive(true)?;
                Ok((socket_addr, stream))
            })
        };

        let host = &*self.host;
        let port = self.port;
        let addrs = match self.host_addr {
            Some(addr) => vec![SocketAddr::new(addr, port)],
            None => lookup_host((host, port)).await?.collect(),
        };

        let (sockaddr, stream) = connect_once(&*addrs).await.inspect_err(|err| {
            warn!("couldn't connect to compute node at {host}:{port}: {err}");
        })?;

        let stream =
            connect_raw::connect_tls(stream, self.ssl_mode, &config.tls, &self.host).await?;

        Ok((sockaddr, stream))
    }
}

pub(crate) struct PostgresConnection {
    /// Socket connected to a compute node.
    pub(crate) stream: PqStream<connect_raw::Stream<TcpStream>>,

    pub(crate) socket_addr: SocketAddr,
    pub(crate) ssl_mode: SslMode,
    pub(crate) hostname: String,
    pub(crate) aux: MetricsAuxInfo,

    pub(crate) guage: NumDbConnectionsGuard<'static>,
}

impl NodeInfo {
    /// Connect to a corresponding compute node.
    pub(crate) async fn connect(
        &self,
        ctx: &RequestContext,
        auth: &AuthInfo,
        config: &ComputeConfig,
    ) -> Result<PostgresConnection, ConnectionError> {
        let pause = ctx.latency_timer_pause(crate::metrics::Waiting::Compute);

        // what pglb will do
        let (socket_addr, stream) = self.config.connect(config).await?;

        // what neonkeeper will do
        let stream = auth.authenticate(stream).await?;

        drop(pause);

        ctx.span()
            .record("compute_id", tracing::field::display(&self.aux.compute_id));

        // TODO: lots of useful info but maybe we can move it elsewhere (eg traces?)
        info!(
            cold_start_info = ctx.cold_start_info().as_str(),
            "connected to compute node at {} ({socket_addr}) sslmode={:?}, latency={}, query_id={}",
            self.config.host,
            self.config.ssl_mode,
            ctx.get_proxy_latency(),
            ctx.get_testodrome_id().unwrap_or_default(),
        );

        Ok(PostgresConnection {
            stream,
            ssl_mode: self.config.ssl_mode,
            hostname: self.config.host.to_string(),
            socket_addr,
            aux: self.aux.clone(),
            guage: Metrics::get().proxy.db_connections.guard(ctx.protocol()),
        })
    }
}

/// Retrieve `options` from a startup message, dropping all proxy-secific flags.
fn filtered_options(options: &str) -> Option<String> {
    #[allow(unstable_name_collisions)]
    let options: String = StartupMessageParams::parse_options_raw(options)
        .filter(|opt| parse_endpoint_param(opt).is_none() && neon_option(opt).is_none())
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
        let params = "";
        assert_eq!(filtered_options(params), None);

        // It's likely that clients will only use options to specify endpoint/project.
        let params = "project=foo";
        assert_eq!(filtered_options(params), None);

        // Same, because unescaped whitespaces are no-op.
        let params = " project=foo ";
        assert_eq!(filtered_options(params).as_deref(), None);

        let params = r"\  project=foo \ ";
        assert_eq!(filtered_options(params).as_deref(), Some(r"\  \ "));

        let params = "project = foo";
        assert_eq!(filtered_options(params).as_deref(), Some("project = foo"));

        let params = "project = foo neon_endpoint_type:read_write   neon_lsn:0/2 neon_proxy_params_compat:true";
        assert_eq!(filtered_options(params).as_deref(), Some("project = foo"));
    }
}
