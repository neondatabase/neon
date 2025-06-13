mod tls;

use std::fmt::Debug;
use std::io;
use std::net::{IpAddr, SocketAddr};

use futures::{FutureExt, TryFutureExt};
use itertools::Itertools;
use postgres_client::config::{AuthKeys, ChannelBinding, SslMode};
use postgres_client::maybe_tls_stream::MaybeTlsStream;
use postgres_client::tls::MakeTlsConnect;
use postgres_client::{NoTls, RawCancelToken, RawConnection};
use postgres_protocol::message::backend::NoticeResponseBody;
use thiserror::Error;
use tokio::net::{TcpStream, lookup_host};
use tracing::{debug, error, info, warn};

use crate::auth::backend::{ComputeCredentialKeys, ComputeUserInfo};
use crate::auth::parse_endpoint_param;
use crate::cancellation::CancelClosure;
use crate::compute::tls::TlsError;
use crate::config::ComputeConfig;
use crate::context::RequestContext;
use crate::control_plane::client::ApiLockError;
use crate::control_plane::errors::WakeComputeError;
use crate::control_plane::messages::MetricsAuxInfo;
use crate::error::{ReportableError, UserFacingError};
use crate::metrics::{Metrics, NumDbConnectionsGuard};
use crate::pqproto::StartupMessageParams;
use crate::proxy::neon_option;
use crate::types::Host;

pub const COULD_NOT_CONNECT: &str = "Couldn't connect to compute node";

#[derive(Debug, Error)]
pub(crate) enum PostgresError {
    /// This error doesn't seem to reveal any secrets; for instance,
    /// `postgres_client::error::Kind` doesn't contain ip addresses and such.
    #[error("{COULD_NOT_CONNECT}: {0}")]
    Postgres(#[from] postgres_client::Error),
}

impl UserFacingError for PostgresError {
    fn to_string_client(&self) -> String {
        match self {
            // This helps us drop irrelevant library-specific prefixes.
            // TODO: propagate severity level and other parameters.
            PostgresError::Postgres(err) => match err.as_db_error() {
                Some(err) => {
                    let msg = err.message();

                    if msg.starts_with("unsupported startup parameter: ")
                        || msg.starts_with("unsupported startup parameter in options: ")
                    {
                        format!(
                            "{msg}. Please use unpooled connection or remove this parameter from the startup package. More details: https://neon.tech/docs/connect/connection-errors#unsupported-startup-parameter"
                        )
                    } else {
                        msg.to_owned()
                    }
                }
                None => err.to_string(),
            },
        }
    }
}

impl ReportableError for PostgresError {
    fn get_error_kind(&self) -> crate::error::ErrorKind {
        match self {
            PostgresError::Postgres(e) if e.as_db_error().is_some() => {
                crate::error::ErrorKind::Postgres
            }
            PostgresError::Postgres(_) => crate::error::ErrorKind::Compute,
        }
    }
}

#[derive(Debug, Error)]
pub(crate) enum ConnectionError {
    #[error("{COULD_NOT_CONNECT}: {0}")]
    TlsError(#[from] TlsError),

    #[error("{COULD_NOT_CONNECT}: {0}")]
    WakeComputeError(#[from] WakeComputeError),

    #[error("error acquiring resource permit: {0}")]
    TooManyConnectionAttempts(#[from] ApiLockError),
}

impl UserFacingError for ConnectionError {
    fn to_string_client(&self) -> String {
        match self {
            ConnectionError::WakeComputeError(err) => err.to_string_client(),
            ConnectionError::TooManyConnectionAttempts(_) => {
                "Failed to acquire permit to connect to the database. Too many database connection attempts are currently ongoing.".to_owned()
            }
            ConnectionError::TlsError(_) => COULD_NOT_CONNECT.to_owned(),
        }
    }
}

impl ReportableError for ConnectionError {
    fn get_error_kind(&self) -> crate::error::ErrorKind {
        match self {
            ConnectionError::TlsError(_) => crate::error::ErrorKind::Compute,
            ConnectionError::WakeComputeError(e) => e.get_error_kind(),
            ConnectionError::TooManyConnectionAttempts(e) => e.get_error_kind(),
        }
    }
}

/// A pair of `ClientKey` & `ServerKey` for `SCRAM-SHA-256`.
pub(crate) type ScramKeys = postgres_client::config::ScramKeys<32>;

#[derive(Clone)]
pub enum Auth {
    /// Only used during console-redirect.
    Password(Vec<u8>),
    /// Used by sql-over-http, ws, tcp.
    Scram(Box<ScramKeys>),
}

/// A config for authenticating to the compute node.
pub(crate) struct AuthInfo {
    /// None for local-proxy, as we use trust-based localhost auth.
    /// Some for sql-over-http, ws, tcp, and in most cases for console-redirect.
    /// Might be None for console-redirect, but that's only a consequence of testing environments ATM.
    auth: Option<Auth>,
    server_params: StartupMessageParams,

    channel_binding: ChannelBinding,

    /// Console redirect sets user and database, we shouldn't re-use those from the params.
    skip_db_user: bool,
}

/// Contains only the data needed to establish a secure connection to compute.
#[derive(Clone)]
pub struct ConnectInfo {
    pub host_addr: Option<IpAddr>,
    pub host: Host,
    pub port: u16,
    pub ssl_mode: SslMode,
}

/// Creation and initialization routines.
impl AuthInfo {
    pub(crate) fn for_console_redirect(db: &str, user: &str, pw: Option<&str>) -> Self {
        let mut server_params = StartupMessageParams::default();
        server_params.insert("database", db);
        server_params.insert("user", user);
        Self {
            auth: pw.map(|pw| Auth::Password(pw.as_bytes().to_owned())),
            server_params,
            skip_db_user: true,
            // pg-sni-router is a mitm so this would fail.
            channel_binding: ChannelBinding::Disable,
        }
    }

    pub(crate) fn with_auth_keys(keys: ComputeCredentialKeys) -> Self {
        Self {
            auth: match keys {
                ComputeCredentialKeys::AuthKeys(AuthKeys::ScramSha256(auth_keys)) => {
                    Some(Auth::Scram(Box::new(auth_keys)))
                }
                ComputeCredentialKeys::JwtPayload(_) | ComputeCredentialKeys::None => None,
            },
            server_params: StartupMessageParams::default(),
            skip_db_user: false,
            channel_binding: ChannelBinding::Prefer,
        }
    }
}

impl ConnectInfo {
    pub fn to_postgres_client_config(&self) -> postgres_client::Config {
        let mut config = postgres_client::Config::new(self.host.to_string(), self.port);
        config.ssl_mode(self.ssl_mode);
        if let Some(host_addr) = self.host_addr {
            config.set_host_addr(host_addr);
        }
        config
    }
}

impl AuthInfo {
    fn enrich(&self, mut config: postgres_client::Config) -> postgres_client::Config {
        match &self.auth {
            Some(Auth::Scram(keys)) => config.auth_keys(AuthKeys::ScramSha256(**keys)),
            Some(Auth::Password(pw)) => config.password(pw),
            None => &mut config,
        };
        config.channel_binding(self.channel_binding);
        for (k, v) in self.server_params.iter() {
            config.set_param(k, v);
        }
        config
    }

    /// Apply startup message params to the connection config.
    pub(crate) fn set_startup_params(
        &mut self,
        params: &StartupMessageParams,
        arbitrary_params: bool,
    ) {
        if !arbitrary_params {
            self.server_params.insert("client_encoding", "UTF8");
        }
        for (k, v) in params.iter() {
            match k {
                // Only set `user` if it's not present in the config.
                // Console redirect auth flow takes username from the console's response.
                "user" | "database" if self.skip_db_user => {}
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
    }

    pub async fn authenticate(
        &self,
        ctx: &RequestContext,
        compute: &mut ComputeConnection,
        user_info: ComputeUserInfo,
    ) -> Result<PostgresSettings, PostgresError> {
        // client config with stubbed connect info.
        // TODO(conrad): should we rewrite this to bypass tokio-postgres2 entirely,
        // utilising pqproto.rs.
        let mut tmp_config = postgres_client::Config::new(String::new(), 0);
        // We have already established SSL if necessary.
        tmp_config.ssl_mode(SslMode::Disable);
        let tmp_config = self.enrich(tmp_config);

        let pause = ctx.latency_timer_pause(crate::metrics::Waiting::Compute);
        let connection = tmp_config
            .tls_and_authenticate(&mut compute.stream, NoTls)
            .await?;
        drop(pause);

        let RawConnection {
            stream: _,
            parameters,
            delayed_notice,
            process_id,
            secret_key,
        } = connection;

        tracing::Span::current().record("pid", tracing::field::display(process_id));

        // NB: CancelToken is supposed to hold socket_addr, but we use connect_raw.
        // Yet another reason to rework the connection establishing code.
        let cancel_closure = CancelClosure::new(
            compute.socket_addr,
            RawCancelToken {
                ssl_mode: compute.ssl_mode,
                process_id,
                secret_key,
            },
            compute.hostname.to_string(),
            user_info,
        );

        Ok(PostgresSettings {
            params: parameters,
            cancel_closure,
            delayed_notice,
        })
    }
}

impl ConnectInfo {
    /// Establish a raw TCP+TLS connection to the compute node.
    async fn connect_raw(
        &self,
        config: &ComputeConfig,
    ) -> Result<(SocketAddr, MaybeTlsStream<TcpStream, RustlsStream>), TlsError> {
        let timeout = config.timeout;

        // wrap TcpStream::connect with timeout
        let connect_with_timeout = |addrs| {
            tokio::time::timeout(timeout, TcpStream::connect(addrs)).map(move |res| match res {
                Ok(tcpstream_connect_res) => tcpstream_connect_res,
                Err(_) => Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!("exceeded connection timeout {timeout:?}"),
                )),
            })
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

        // We can't reuse connection establishing logic from `postgres_client` here,
        // because it has no means for extracting the underlying socket which we
        // require for our business.
        let port = self.port;
        let host = &*self.host;

        let addrs = match self.host_addr {
            Some(addr) => vec![SocketAddr::new(addr, port)],
            None => lookup_host((host, port)).await?.collect(),
        };

        match connect_once(&*addrs).await {
            Ok((sockaddr, stream)) => Ok((
                sockaddr,
                tls::connect_tls(stream, self.ssl_mode, config, host).await?,
            )),
            Err(err) => {
                warn!("couldn't connect to compute node at {host}:{port}: {err}");
                Err(TlsError::Connection(err))
            }
        }
    }
}

pub type RustlsStream = <ComputeConfig as MakeTlsConnect<tokio::net::TcpStream>>::Stream;
pub type MaybeRustlsStream = MaybeTlsStream<tokio::net::TcpStream, RustlsStream>;

// TODO(conrad): we don't need to parse these.
// These are just immediately forwarded back to the client.
// We could instead stream them out instead of reading them into memory.
pub struct PostgresSettings {
    /// PostgreSQL connection parameters.
    pub params: std::collections::HashMap<String, String>,
    /// Query cancellation token.
    pub cancel_closure: CancelClosure,
    /// Notices received from compute after authenticating
    pub delayed_notice: Vec<NoticeResponseBody>,
}

pub struct ComputeConnection {
    /// Socket connected to a compute node.
    pub stream: MaybeTlsStream<tokio::net::TcpStream, RustlsStream>,
    /// Labels for proxy's metrics.
    pub aux: MetricsAuxInfo,
    pub hostname: Host,
    pub ssl_mode: SslMode,
    pub socket_addr: SocketAddr,
    pub guage: NumDbConnectionsGuard<'static>,
}

impl ConnectInfo {
    /// Connect to a corresponding compute node.
    pub async fn connect(
        &self,
        ctx: &RequestContext,
        aux: &MetricsAuxInfo,
        config: &ComputeConfig,
    ) -> Result<ComputeConnection, ConnectionError> {
        let pause = ctx.latency_timer_pause(crate::metrics::Waiting::Compute);
        let (socket_addr, stream) = self.connect_raw(config).await?;
        drop(pause);

        tracing::Span::current().record("compute_id", tracing::field::display(&aux.compute_id));

        // TODO: lots of useful info but maybe we can move it elsewhere (eg traces?)
        info!(
            cold_start_info = ctx.cold_start_info().as_str(),
            "connected to compute node at {} ({socket_addr}) sslmode={:?}, latency={}, query_id={}",
            self.host,
            self.ssl_mode,
            ctx.get_proxy_latency(),
            ctx.get_testodrome_id().unwrap_or_default(),
        );

        let connection = ComputeConnection {
            stream,
            socket_addr,
            hostname: self.host.clone(),
            ssl_mode: self.ssl_mode,
            aux: aux.clone(),
            guage: Metrics::get().proxy.db_connections.guard(ctx.protocol()),
        };

        Ok(connection)
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
