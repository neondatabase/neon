use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use futures::{FutureExt, TryFutureExt};
use itertools::Itertools;
use once_cell::sync::OnceCell;
use postgres_client::tls::MakeTlsConnect;
use postgres_client::{CancelToken, RawConnection};
use postgres_protocol::message::backend::NoticeResponseBody;
use pq_proto::StartupMessageParams;
use rustls::client::danger::ServerCertVerifier;
use rustls::crypto::ring;
use rustls::pki_types::InvalidDnsNameError;
use thiserror::Error;
use tokio::net::TcpStream;
use tracing::{debug, error, info, warn};

use crate::auth::parse_endpoint_param;
use crate::cancellation::CancelClosure;
use crate::context::RequestContext;
use crate::control_plane::client::ApiLockError;
use crate::control_plane::errors::WakeComputeError;
use crate::control_plane::messages::MetricsAuxInfo;
use crate::error::{ReportableError, UserFacingError};
use crate::metrics::{Metrics, NumDbConnectionsGuard};
use crate::postgres_rustls::MakeRustlsConnect;
use crate::proxy::neon_option;
use crate::types::Host;

pub const COULD_NOT_CONNECT: &str = "Couldn't connect to compute node";

#[derive(Debug, Error)]
pub(crate) enum ConnectionError {
    /// This error doesn't seem to reveal any secrets; for instance,
    /// `postgres_client::error::Kind` doesn't contain ip addresses and such.
    #[error("{COULD_NOT_CONNECT}: {0}")]
    Postgres(#[from] postgres_client::Error),

    #[error("{COULD_NOT_CONNECT}: {0}")]
    CouldNotConnect(#[from] io::Error),

    #[error("Couldn't load native TLS certificates: {0:?}")]
    TlsCertificateError(Vec<rustls_native_certs::Error>),

    #[error("{COULD_NOT_CONNECT}: {0}")]
    TlsError(#[from] InvalidDnsNameError),

    #[error("{COULD_NOT_CONNECT}: {0}")]
    WakeComputeError(#[from] WakeComputeError),

    #[error("error acquiring resource permit: {0}")]
    TooManyConnectionAttempts(#[from] ApiLockError),
}

impl UserFacingError for ConnectionError {
    fn to_string_client(&self) -> String {
        match self {
            // This helps us drop irrelevant library-specific prefixes.
            // TODO: propagate severity level and other parameters.
            ConnectionError::Postgres(err) => match err.as_db_error() {
                Some(err) => {
                    let msg = err.message();

                    if msg.starts_with("unsupported startup parameter: ")
                        || msg.starts_with("unsupported startup parameter in options: ")
                    {
                        format!("{msg}. Please use unpooled connection or remove this parameter from the startup package. More details: https://neon.tech/docs/connect/connection-errors#unsupported-startup-parameter")
                    } else {
                        msg.to_owned()
                    }
                }
                None => err.to_string(),
            },
            ConnectionError::WakeComputeError(err) => err.to_string_client(),
            ConnectionError::TooManyConnectionAttempts(_) => {
                "Failed to acquire permit to connect to the database. Too many database connection attempts are currently ongoing.".to_owned()
            }
            _ => COULD_NOT_CONNECT.to_owned(),
        }
    }
}

impl ReportableError for ConnectionError {
    fn get_error_kind(&self) -> crate::error::ErrorKind {
        match self {
            ConnectionError::Postgres(e) if e.as_db_error().is_some() => {
                crate::error::ErrorKind::Postgres
            }
            ConnectionError::Postgres(_) => crate::error::ErrorKind::Compute,
            ConnectionError::CouldNotConnect(_) => crate::error::ErrorKind::Compute,
            ConnectionError::TlsCertificateError(_) => crate::error::ErrorKind::Service,
            ConnectionError::TlsError(_) => crate::error::ErrorKind::Compute,
            ConnectionError::WakeComputeError(e) => e.get_error_kind(),
            ConnectionError::TooManyConnectionAttempts(e) => e.get_error_kind(),
        }
    }
}

/// A pair of `ClientKey` & `ServerKey` for `SCRAM-SHA-256`.
pub(crate) type ScramKeys = postgres_client::config::ScramKeys<32>;

/// A config for establishing a connection to compute node.
/// Eventually, `postgres_client` will be replaced with something better.
/// Newtype allows us to implement methods on top of it.
#[derive(Clone)]
pub(crate) struct ConnCfg(Box<postgres_client::Config>);

/// Creation and initialization routines.
impl ConnCfg {
    pub(crate) fn new(host: String, port: u16) -> Self {
        Self(Box::new(postgres_client::Config::new(host, port)))
    }

    /// Reuse password or auth keys from the other config.
    pub(crate) fn reuse_password(&mut self, other: Self) {
        if let Some(password) = other.get_password() {
            self.password(password);
        }

        if let Some(keys) = other.get_auth_keys() {
            self.auth_keys(keys);
        }
    }

    pub(crate) fn get_host(&self) -> Host {
        match self.0.get_host() {
            postgres_client::config::Host::Tcp(s) => s.into(),
        }
    }

    /// Apply startup message params to the connection config.
    pub(crate) fn set_startup_params(
        &mut self,
        params: &StartupMessageParams,
        arbitrary_params: bool,
    ) {
        if !arbitrary_params {
            self.set_param("client_encoding", "UTF8");
        }
        for (k, v) in params.iter() {
            match k {
                // Only set `user` if it's not present in the config.
                // Console redirect auth flow takes username from the console's response.
                "user" if self.user_is_set() => continue,
                "database" if self.db_is_set() => continue,
                "options" => {
                    if let Some(options) = filtered_options(v) {
                        self.set_param(k, &options);
                    }
                }
                "user" | "database" | "application_name" | "replication" => {
                    self.set_param(k, v);
                }

                // if we allow arbitrary params, then we forward them through.
                // this is a flag for a period of backwards compatibility
                k if arbitrary_params => {
                    self.set_param(k, v);
                }
                _ => {}
            }
        }
    }
}

impl std::ops::Deref for ConnCfg {
    type Target = postgres_client::Config;

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
    async fn connect_raw(&self, timeout: Duration) -> io::Result<(SocketAddr, TcpStream, &str)> {
        use postgres_client::config::Host;

        // wrap TcpStream::connect with timeout
        let connect_with_timeout = |host, port| {
            tokio::time::timeout(timeout, TcpStream::connect((host, port))).map(
                move |res| match res {
                    Ok(tcpstream_connect_res) => tcpstream_connect_res,
                    Err(_) => Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        format!("exceeded connection timeout {timeout:?}"),
                    )),
                },
            )
        };

        let connect_once = |host, port| {
            debug!("trying to connect to compute node at {host}:{port}");
            connect_with_timeout(host, port).and_then(|socket| async {
                let socket_addr = socket.peer_addr()?;
                // This prevents load balancer from severing the connection.
                socket2::SockRef::from(&socket).set_keepalive(true)?;
                Ok((socket_addr, socket))
            })
        };

        // We can't reuse connection establishing logic from `postgres_client` here,
        // because it has no means for extracting the underlying socket which we
        // require for our business.
        let port = self.0.get_port();
        let host = self.0.get_host();

        let host = match host {
            Host::Tcp(host) => host.as_str(),
        };

        match connect_once(host, port).await {
            Ok((sockaddr, stream)) => Ok((sockaddr, stream, host)),
            Err(err) => {
                warn!("couldn't connect to compute node at {host}:{port}: {err}");
                Err(err)
            }
        }
    }
}

type RustlsStream = <MakeRustlsConnect as MakeTlsConnect<tokio::net::TcpStream>>::Stream;

pub(crate) struct PostgresConnection {
    /// Socket connected to a compute node.
    pub(crate) stream:
        postgres_client::maybe_tls_stream::MaybeTlsStream<tokio::net::TcpStream, RustlsStream>,
    /// PostgreSQL connection parameters.
    pub(crate) params: std::collections::HashMap<String, String>,
    /// Query cancellation token.
    pub(crate) cancel_closure: CancelClosure,
    /// Labels for proxy's metrics.
    pub(crate) aux: MetricsAuxInfo,
    /// Notices received from compute after authenticating
    pub(crate) delayed_notice: Vec<NoticeResponseBody>,

    _guage: NumDbConnectionsGuard<'static>,
}

impl ConnCfg {
    /// Connect to a corresponding compute node.
    pub(crate) async fn connect(
        &self,
        ctx: &RequestContext,
        allow_self_signed_compute: bool,
        aux: MetricsAuxInfo,
        timeout: Duration,
    ) -> Result<PostgresConnection, ConnectionError> {
        let pause = ctx.latency_timer_pause(crate::metrics::Waiting::Compute);
        let (socket_addr, stream, host) = self.connect_raw(timeout).await?;
        drop(pause);

        let client_config = if allow_self_signed_compute {
            // Allow all certificates for creating the connection
            let verifier = Arc::new(AcceptEverythingVerifier);
            rustls::ClientConfig::builder_with_provider(Arc::new(ring::default_provider()))
                .with_safe_default_protocol_versions()
                .expect("ring should support the default protocol versions")
                .dangerous()
                .with_custom_certificate_verifier(verifier)
        } else {
            let root_store = TLS_ROOTS
                .get_or_try_init(load_certs)
                .map_err(ConnectionError::TlsCertificateError)?
                .clone();
            rustls::ClientConfig::builder_with_provider(Arc::new(ring::default_provider()))
                .with_safe_default_protocol_versions()
                .expect("ring should support the default protocol versions")
                .with_root_certificates(root_store)
        };
        let client_config = client_config.with_no_client_auth();

        let mut mk_tls = crate::postgres_rustls::MakeRustlsConnect::new(client_config);
        let tls = <MakeRustlsConnect as MakeTlsConnect<tokio::net::TcpStream>>::make_tls_connect(
            &mut mk_tls,
            host,
        )?;

        // connect_raw() will not use TLS if sslmode is "disable"
        let pause = ctx.latency_timer_pause(crate::metrics::Waiting::Compute);
        let connection = self.0.connect_raw(stream, tls).await?;
        drop(pause);

        let RawConnection {
            stream,
            parameters,
            delayed_notice,
            process_id,
            secret_key,
        } = connection;

        tracing::Span::current().record("pid", tracing::field::display(process_id));
        let stream = stream.into_inner();

        // TODO: lots of useful info but maybe we can move it elsewhere (eg traces?)
        info!(
            cold_start_info = ctx.cold_start_info().as_str(),
            "connected to compute node at {host} ({socket_addr}) sslmode={:?}",
            self.0.get_ssl_mode()
        );

        // NB: CancelToken is supposed to hold socket_addr, but we use connect_raw.
        // Yet another reason to rework the connection establishing code.
        let cancel_closure = CancelClosure::new(
            socket_addr,
            CancelToken {
                socket_config: None,
                ssl_mode: self.0.get_ssl_mode(),
                process_id,
                secret_key,
            },
            vec![],
        );

        let connection = PostgresConnection {
            stream,
            params: parameters,
            delayed_notice,
            cancel_closure,
            aux,
            _guage: Metrics::get().proxy.db_connections.guard(ctx.protocol()),
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

fn load_certs() -> Result<Arc<rustls::RootCertStore>, Vec<rustls_native_certs::Error>> {
    let der_certs = rustls_native_certs::load_native_certs();

    if !der_certs.errors.is_empty() {
        return Err(der_certs.errors);
    }

    let mut store = rustls::RootCertStore::empty();
    store.add_parsable_certificates(der_certs.certs);
    Ok(Arc::new(store))
}
static TLS_ROOTS: OnceCell<Arc<rustls::RootCertStore>> = OnceCell::new();

#[derive(Debug)]
struct AcceptEverythingVerifier;
impl ServerCertVerifier for AcceptEverythingVerifier {
    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        use rustls::SignatureScheme;
        // The schemes for which `SignatureScheme::supported_in_tls13` returns true.
        vec![
            SignatureScheme::ECDSA_NISTP521_SHA512,
            SignatureScheme::ECDSA_NISTP384_SHA384,
            SignatureScheme::ECDSA_NISTP256_SHA256,
            SignatureScheme::RSA_PSS_SHA512,
            SignatureScheme::RSA_PSS_SHA384,
            SignatureScheme::RSA_PSS_SHA256,
            SignatureScheme::ED25519,
        ]
    }
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }
    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }
    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }
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
