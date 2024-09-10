#![deny(unsafe_code)]
#![deny(clippy::undocumented_unsafe_blocks)]
use anyhow::{bail, Context};
use itertools::Itertools;
use std::borrow::Cow;
use std::fmt;
use url::Host;

/// Parses a string of format either `host:port` or `host` into a corresponding pair.
///
/// The `host` part should be a correct `url::Host`, while `port` (if present) should be
/// a valid decimal u16 of digits only.
pub fn parse_host_port<S: AsRef<str>>(host_port: S) -> Result<(Host, Option<u16>), anyhow::Error> {
    let (host, port) = match host_port.as_ref().rsplit_once(':') {
        Some((host, port)) => (
            host,
            // +80 is a valid u16, but not a valid port
            if port.chars().all(|c| c.is_ascii_digit()) {
                Some(port.parse::<u16>().context("Unable to parse port")?)
            } else {
                bail!("Port contains a non-ascii-digit")
            },
        ),
        None => (host_port.as_ref(), None), // No colons, no port specified
    };
    let host = Host::parse(host).context("Unable to parse host")?;
    Ok((host, port))
}

#[cfg(test)]
mod tests_parse_host_port {
    use crate::parse_host_port;
    use url::Host;

    #[test]
    fn test_normal() {
        let (host, port) = parse_host_port("hello:123").unwrap();
        assert_eq!(host, Host::Domain("hello".to_owned()));
        assert_eq!(port, Some(123));
    }

    #[test]
    fn test_no_port() {
        let (host, port) = parse_host_port("hello").unwrap();
        assert_eq!(host, Host::Domain("hello".to_owned()));
        assert_eq!(port, None);
    }

    #[test]
    fn test_ipv6() {
        let (host, port) = parse_host_port("[::1]:123").unwrap();
        assert_eq!(host, Host::<String>::Ipv6(std::net::Ipv6Addr::LOCALHOST));
        assert_eq!(port, Some(123));
    }

    #[test]
    fn test_invalid_host() {
        assert!(parse_host_port("hello world").is_err());
    }

    #[test]
    fn test_invalid_port() {
        assert!(parse_host_port("hello:+80").is_err());
    }
}

#[derive(Clone)]
pub struct PgConnectionConfig {
    host: Host,
    port: u16,
    password: Option<String>,
    options: Vec<String>,
}

/// A simplified PostgreSQL connection configuration. Supports only a subset of possible
/// settings for simplicity. A password getter or `to_connection_string` methods are not
/// added by design to avoid accidentally leaking password through logging, command line
/// arguments to a child process, or likewise.
impl PgConnectionConfig {
    pub fn new_host_port(host: Host, port: u16) -> Self {
        PgConnectionConfig {
            host,
            port,
            password: None,
            options: vec![],
        }
    }

    pub fn host(&self) -> &Host {
        &self.host
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn set_host(mut self, h: Host) -> Self {
        self.host = h;
        self
    }

    pub fn set_port(mut self, p: u16) -> Self {
        self.port = p;
        self
    }

    pub fn set_password(mut self, s: Option<String>) -> Self {
        self.password = s;
        self
    }

    pub fn extend_options<I: IntoIterator<Item = S>, S: Into<String>>(mut self, i: I) -> Self {
        self.options.extend(i.into_iter().map(|s| s.into()));
        self
    }

    /// Return a `<host>:<port>` string.
    pub fn raw_address(&self) -> String {
        format!("{}:{}", self.host(), self.port())
    }

    /// Build a client library-specific connection configuration.
    /// Used for testing and when we need to add some obscure configuration
    /// elements at the last moment.
    pub fn to_tokio_postgres_config(&self) -> tokio_postgres::Config {
        // Use `tokio_postgres::Config` instead of `postgres::Config` because
        // the former supports more options to fiddle with later.
        let mut config = tokio_postgres::Config::new();
        config.host(&self.host().to_string()).port(self.port);
        if let Some(password) = &self.password {
            config.password(password);
        }
        if !self.options.is_empty() {
            // These options are command-line options and should be escaped before being passed
            // as an 'options' connection string parameter, see
            // https://www.postgresql.org/docs/15/libpq-connect.html#LIBPQ-CONNECT-OPTIONS
            //
            // They will be space-separated, so each space inside an option should be escaped,
            // and all backslashes should be escaped before that. Although we don't expect options
            // with spaces at the moment, they're supported by PostgreSQL. Hence we support them
            // in this typesafe interface.
            //
            // We use `Cow` to avoid allocations in the best case (no escaping). A fully imperative
            // solution would require 1-2 allocations in the worst case as well, but it's harder to
            // implement and this function is hardly a bottleneck. The function is only called around
            // establishing a new connection.
            #[allow(unstable_name_collisions)]
            config.options(
                &self
                    .options
                    .iter()
                    .map(|s| {
                        if s.contains(['\\', ' ']) {
                            Cow::Owned(s.replace('\\', "\\\\").replace(' ', "\\ "))
                        } else {
                            Cow::Borrowed(s.as_str())
                        }
                    })
                    .intersperse(Cow::Borrowed(" ")) // TODO: use impl from std once it's stabilized
                    .collect::<String>(),
            );
        }
        config
    }

    /// Connect using postgres protocol with TLS disabled.
    pub async fn connect_no_tls(
        &self,
    ) -> Result<
        (
            tokio_postgres::Client,
            tokio_postgres::Connection<tokio_postgres::Socket, tokio_postgres::tls::NoTlsStream>,
        ),
        postgres::Error,
    > {
        self.to_tokio_postgres_config()
            .connect(postgres::NoTls)
            .await
    }
}

impl fmt::Display for PgConnectionConfig {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // The password is intentionally hidden and not part of this display string.
        write!(f, "postgresql://{}:{}", self.host, self.port)
    }
}

impl fmt::Debug for PgConnectionConfig {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // We want `password: Some(REDACTED-STRING)`, not `password: Some("REDACTED-STRING")`
        // so even if the password is `REDACTED-STRING` (quite unlikely) there is no confusion.
        // Hence `format_args!()`, it returns a "safe" string which is not escaped by `Debug`.
        f.debug_struct("PgConnectionConfig")
            .field("host", &self.host)
            .field("port", &self.port)
            .field(
                "password",
                &self
                    .password
                    .as_ref()
                    .map(|_| format_args!("REDACTED-STRING")),
            )
            .finish()
    }
}

#[cfg(test)]
mod tests_pg_connection_config {
    use crate::PgConnectionConfig;
    use once_cell::sync::Lazy;
    use url::Host;

    static STUB_HOST: Lazy<Host> = Lazy::new(|| Host::Domain("stub.host.example".to_owned()));

    #[test]
    fn test_no_password() {
        let cfg = PgConnectionConfig::new_host_port(STUB_HOST.clone(), 123);
        assert_eq!(cfg.host(), &*STUB_HOST);
        assert_eq!(cfg.port(), 123);
        assert_eq!(cfg.raw_address(), "stub.host.example:123");
        assert_eq!(
            format!("{:?}", cfg),
            "PgConnectionConfig { host: Domain(\"stub.host.example\"), port: 123, password: None }"
        );
    }

    #[test]
    fn test_ipv6() {
        // May be a special case because hostname contains a colon.
        let cfg = PgConnectionConfig::new_host_port(Host::parse("[::1]").unwrap(), 123);
        assert_eq!(
            cfg.host(),
            &Host::<String>::Ipv6(std::net::Ipv6Addr::LOCALHOST)
        );
        assert_eq!(cfg.port(), 123);
        assert_eq!(cfg.raw_address(), "[::1]:123");
        assert_eq!(
            format!("{:?}", cfg),
            "PgConnectionConfig { host: Ipv6(::1), port: 123, password: None }"
        );
    }

    #[test]
    fn test_with_password() {
        let cfg = PgConnectionConfig::new_host_port(STUB_HOST.clone(), 123)
            .set_password(Some("password".to_owned()));
        assert_eq!(cfg.host(), &*STUB_HOST);
        assert_eq!(cfg.port(), 123);
        assert_eq!(cfg.raw_address(), "stub.host.example:123");
        assert_eq!(
            format!("{:?}", cfg),
            "PgConnectionConfig { host: Domain(\"stub.host.example\"), port: 123, password: Some(REDACTED-STRING) }"
        );
    }

    #[test]
    fn test_with_options() {
        let cfg = PgConnectionConfig::new_host_port(STUB_HOST.clone(), 123).extend_options([
            "hello",
            "world",
            "with space",
            "and \\ backslashes",
        ]);
        assert_eq!(cfg.host(), &*STUB_HOST);
        assert_eq!(cfg.port(), 123);
        assert_eq!(cfg.raw_address(), "stub.host.example:123");
        assert_eq!(
            cfg.to_tokio_postgres_config().get_options(),
            Some("hello world with\\ space and\\ \\\\\\ backslashes")
        );
    }
}
