//! Connection configuration.

use crate::connect::connect;
use crate::connect_raw::connect_raw;
use crate::connect_raw::RawConnection;
use crate::tls::MakeTlsConnect;
use crate::tls::TlsConnect;
use crate::{Client, Connection, Error};
use std::fmt;
use std::str;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};

pub use postgres_protocol2::authentication::sasl::ScramKeys;
use tokio::net::TcpStream;

/// Properties required of a session.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum TargetSessionAttrs {
    /// No special properties are required.
    Any,
    /// The session must allow writes.
    ReadWrite,
}

/// TLS configuration.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum SslMode {
    /// Do not use TLS.
    Disable,
    /// Attempt to connect with TLS but allow sessions without.
    Prefer,
    /// Require the use of TLS.
    Require,
}

/// Channel binding configuration.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ChannelBinding {
    /// Do not use channel binding.
    Disable,
    /// Attempt to use channel binding but allow sessions without.
    Prefer,
    /// Require the use of channel binding.
    Require,
}

/// Replication mode configuration.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ReplicationMode {
    /// Physical replication.
    Physical,
    /// Logical replication.
    Logical,
}

/// A host specification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Host {
    /// A TCP hostname.
    Tcp(String),
}

/// Precomputed keys which may override password during auth.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthKeys {
    /// A `ClientKey` & `ServerKey` pair for `SCRAM-SHA-256`.
    ScramSha256(ScramKeys<32>),
}

/// Connection configuration.
///
/// Configuration can be parsed from libpq-style connection strings. These strings come in two formats:
///
/// # Key-Value
///
/// This format consists of space-separated key-value pairs. Values which are either the empty string or contain
/// whitespace should be wrapped in `'`. `'` and `\` characters should be backslash-escaped.
///
/// ## Keys
///
/// * `user` - The username to authenticate with. Required.
/// * `password` - The password to authenticate with.
/// * `dbname` - The name of the database to connect to. Defaults to the username.
/// * `options` - Command line options used to configure the server.
/// * `application_name` - Sets the `application_name` parameter on the server.
/// * `sslmode` - Controls usage of TLS. If set to `disable`, TLS will not be used. If set to `prefer`, TLS will be used
///     if available, but not used otherwise. If set to `require`, TLS will be forced to be used. Defaults to `prefer`.
/// * `host` - The host to connect to. On Unix platforms, if the host starts with a `/` character it is treated as the
///     path to the directory containing Unix domain sockets. Otherwise, it is treated as a hostname. Multiple hosts
///     can be specified, separated by commas. Each host will be tried in turn when connecting. Required if connecting
///     with the `connect` method.
/// * `port` - The port to connect to. Multiple ports can be specified, separated by commas. The number of ports must be
///     either 1, in which case it will be used for all hosts, or the same as the number of hosts. Defaults to 5432 if
///     omitted or the empty string.
/// * `connect_timeout` - The time limit in seconds applied to each socket-level connection attempt. Note that hostnames
///     can resolve to multiple IP addresses, and this limit is applied to each address. Defaults to no timeout.
/// * `target_session_attrs` - Specifies requirements of the session. If set to `read-write`, the client will check that
///     the `transaction_read_write` session parameter is set to `on`. This can be used to connect to the primary server
///     in a database cluster as opposed to the secondary read-only mirrors. Defaults to `all`.
/// * `channel_binding` - Controls usage of channel binding in the authentication process. If set to `disable`, channel
///     binding will not be used. If set to `prefer`, channel binding will be used if available, but not used otherwise.
///     If set to `require`, the authentication process will fail if channel binding is not used. Defaults to `prefer`.
///
/// ## Examples
///
/// ```not_rust
/// host=localhost user=postgres connect_timeout=10 keepalives=0
/// ```
///
/// ```not_rust
/// host=/var/lib/postgresql,localhost port=1234 user=postgres password='password with spaces'
/// ```
///
/// ```not_rust
/// host=host1,host2,host3 port=1234,,5678 user=postgres target_session_attrs=read-write
/// ```
///
/// # Url
///
/// This format resembles a URL with a scheme of either `postgres://` or `postgresql://`. All components are optional,
/// and the format accepts query parameters for all of the key-value pairs described in the section above. Multiple
/// host/port pairs can be comma-separated. Unix socket paths in the host section of the URL should be percent-encoded,
/// as the path component of the URL specifies the database name.
///
/// ## Examples
///
/// ```not_rust
/// postgresql://user@localhost
/// ```
///
/// ```not_rust
/// postgresql://user:password@%2Fvar%2Flib%2Fpostgresql/mydb?connect_timeout=10
/// ```
///
/// ```not_rust
/// postgresql://user@host1:1234,host2,host3:5678?target_session_attrs=read-write
/// ```
///
/// ```not_rust
/// postgresql:///mydb?user=user&host=/var/lib/postgresql
/// ```
#[derive(Clone, PartialEq, Eq)]
pub struct Config {
    pub(crate) host: Host,
    pub(crate) port: u16,

    pub(crate) user: Option<String>,
    pub(crate) password: Option<Vec<u8>>,
    pub(crate) auth_keys: Option<Box<AuthKeys>>,
    pub(crate) dbname: Option<String>,
    pub(crate) options: Option<String>,
    pub(crate) application_name: Option<String>,
    pub(crate) ssl_mode: SslMode,
    pub(crate) connect_timeout: Option<Duration>,
    pub(crate) target_session_attrs: TargetSessionAttrs,
    pub(crate) channel_binding: ChannelBinding,
    pub(crate) replication_mode: Option<ReplicationMode>,
    pub(crate) max_backend_message_size: Option<usize>,
}

impl Config {
    /// Creates a new configuration.
    pub fn new(host: String, port: u16) -> Config {
        Config {
            host: Host::Tcp(host),
            port,
            user: None,
            password: None,
            auth_keys: None,
            dbname: None,
            options: None,
            application_name: None,
            ssl_mode: SslMode::Prefer,
            connect_timeout: None,
            target_session_attrs: TargetSessionAttrs::Any,
            channel_binding: ChannelBinding::Prefer,
            replication_mode: None,
            max_backend_message_size: None,
        }
    }

    /// Sets the user to authenticate with.
    ///
    /// Required.
    pub fn user(&mut self, user: &str) -> &mut Config {
        self.user = Some(user.to_string());
        self
    }

    /// Gets the user to authenticate with, if one has been configured with
    /// the `user` method.
    pub fn get_user(&self) -> Option<&str> {
        self.user.as_deref()
    }

    /// Sets the password to authenticate with.
    pub fn password<T>(&mut self, password: T) -> &mut Config
    where
        T: AsRef<[u8]>,
    {
        self.password = Some(password.as_ref().to_vec());
        self
    }

    /// Gets the password to authenticate with, if one has been configured with
    /// the `password` method.
    pub fn get_password(&self) -> Option<&[u8]> {
        self.password.as_deref()
    }

    /// Sets precomputed protocol-specific keys to authenticate with.
    /// When set, this option will override `password`.
    /// See [`AuthKeys`] for more information.
    pub fn auth_keys(&mut self, keys: AuthKeys) -> &mut Config {
        self.auth_keys = Some(Box::new(keys));
        self
    }

    /// Gets precomputed protocol-specific keys to authenticate with.
    /// if one has been configured with the `auth_keys` method.
    pub fn get_auth_keys(&self) -> Option<AuthKeys> {
        self.auth_keys.as_deref().copied()
    }

    /// Sets the name of the database to connect to.
    ///
    /// Defaults to the user.
    pub fn dbname(&mut self, dbname: &str) -> &mut Config {
        self.dbname = Some(dbname.to_string());
        self
    }

    /// Gets the name of the database to connect to, if one has been configured
    /// with the `dbname` method.
    pub fn get_dbname(&self) -> Option<&str> {
        self.dbname.as_deref()
    }

    /// Sets command line options used to configure the server.
    pub fn options(&mut self, options: &str) -> &mut Config {
        self.options = Some(options.to_string());
        self
    }

    /// Gets the command line options used to configure the server, if the
    /// options have been set with the `options` method.
    pub fn get_options(&self) -> Option<&str> {
        self.options.as_deref()
    }

    /// Sets the value of the `application_name` runtime parameter.
    pub fn application_name(&mut self, application_name: &str) -> &mut Config {
        self.application_name = Some(application_name.to_string());
        self
    }

    /// Gets the value of the `application_name` runtime parameter, if it has
    /// been set with the `application_name` method.
    pub fn get_application_name(&self) -> Option<&str> {
        self.application_name.as_deref()
    }

    /// Sets the SSL configuration.
    ///
    /// Defaults to `prefer`.
    pub fn ssl_mode(&mut self, ssl_mode: SslMode) -> &mut Config {
        self.ssl_mode = ssl_mode;
        self
    }

    /// Gets the SSL configuration.
    pub fn get_ssl_mode(&self) -> SslMode {
        self.ssl_mode
    }

    /// Gets the hosts that have been added to the configuration with `host`.
    pub fn get_host(&self) -> &Host {
        &self.host
    }

    /// Gets the ports that have been added to the configuration with `port`.
    pub fn get_port(&self) -> u16 {
        self.port
    }

    /// Sets the timeout applied to socket-level connection attempts.
    ///
    /// Note that hostnames can resolve to multiple IP addresses, and this timeout will apply to each address of each
    /// host separately. Defaults to no limit.
    pub fn connect_timeout(&mut self, connect_timeout: Duration) -> &mut Config {
        self.connect_timeout = Some(connect_timeout);
        self
    }

    /// Gets the connection timeout, if one has been set with the
    /// `connect_timeout` method.
    pub fn get_connect_timeout(&self) -> Option<&Duration> {
        self.connect_timeout.as_ref()
    }

    /// Sets the requirements of the session.
    ///
    /// This can be used to connect to the primary server in a clustered database rather than one of the read-only
    /// secondary servers. Defaults to `Any`.
    pub fn target_session_attrs(
        &mut self,
        target_session_attrs: TargetSessionAttrs,
    ) -> &mut Config {
        self.target_session_attrs = target_session_attrs;
        self
    }

    /// Gets the requirements of the session.
    pub fn get_target_session_attrs(&self) -> TargetSessionAttrs {
        self.target_session_attrs
    }

    /// Sets the channel binding behavior.
    ///
    /// Defaults to `prefer`.
    pub fn channel_binding(&mut self, channel_binding: ChannelBinding) -> &mut Config {
        self.channel_binding = channel_binding;
        self
    }

    /// Gets the channel binding behavior.
    pub fn get_channel_binding(&self) -> ChannelBinding {
        self.channel_binding
    }

    /// Set replication mode.
    pub fn replication_mode(&mut self, replication_mode: ReplicationMode) -> &mut Config {
        self.replication_mode = Some(replication_mode);
        self
    }

    /// Get replication mode.
    pub fn get_replication_mode(&self) -> Option<ReplicationMode> {
        self.replication_mode
    }

    /// Set limit for backend messages size.
    pub fn max_backend_message_size(&mut self, max_backend_message_size: usize) -> &mut Config {
        self.max_backend_message_size = Some(max_backend_message_size);
        self
    }

    /// Get limit for backend messages size.
    pub fn get_max_backend_message_size(&self) -> Option<usize> {
        self.max_backend_message_size
    }

    /// Opens a connection to a PostgreSQL database.
    ///
    /// Requires the `runtime` Cargo feature (enabled by default).
    pub async fn connect<T>(
        &self,
        tls: T,
    ) -> Result<(Client, Connection<TcpStream, T::Stream>), Error>
    where
        T: MakeTlsConnect<TcpStream>,
    {
        connect(tls, self).await
    }

    pub async fn connect_raw<S, T>(
        &self,
        stream: S,
        tls: T,
    ) -> Result<RawConnection<S, T::Stream>, Error>
    where
        S: AsyncRead + AsyncWrite + Unpin,
        T: TlsConnect<S>,
    {
        connect_raw(stream, tls, self).await
    }
}

// Omit password from debug output
impl fmt::Debug for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        struct Redaction {}
        impl fmt::Debug for Redaction {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "_")
            }
        }

        f.debug_struct("Config")
            .field("user", &self.user)
            .field("password", &self.password.as_ref().map(|_| Redaction {}))
            .field("dbname", &self.dbname)
            .field("options", &self.options)
            .field("application_name", &self.application_name)
            .field("ssl_mode", &self.ssl_mode)
            .field("host", &self.host)
            .field("port", &self.port)
            .field("connect_timeout", &self.connect_timeout)
            .field("target_session_attrs", &self.target_session_attrs)
            .field("channel_binding", &self.channel_binding)
            .field("replication", &self.replication_mode)
            .finish()
    }
}
