use crate::auth;
use crate::cancellation::{self, CancelMap};
use crate::config::{ProxyConfig, TlsConfig};
use crate::stream::{MetricsStream, PqStream, Stream};
use anyhow::{bail, Context};
use futures::TryFutureExt;
use lazy_static::lazy_static;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
use zenith_metrics::{new_common_metric_name, register_int_counter, IntCounter};
use zenith_utils::pq_proto::{BeMessage as Be, *};

const ERR_INSECURE_CONNECTION: &str = "connection is insecure (try using `sslmode=require`)";
const ERR_PROTO_VIOLATION: &str = "protocol violation";

lazy_static! {
    static ref NUM_CONNECTIONS_ACCEPTED_COUNTER: IntCounter = register_int_counter!(
        new_common_metric_name("num_connections_accepted"),
        "Number of TCP client connections accepted."
    )
    .unwrap();
    static ref NUM_CONNECTIONS_CLOSED_COUNTER: IntCounter = register_int_counter!(
        new_common_metric_name("num_connections_closed"),
        "Number of TCP client connections closed."
    )
    .unwrap();
    static ref NUM_BYTES_PROXIED_COUNTER: IntCounter = register_int_counter!(
        new_common_metric_name("num_bytes_proxied"),
        "Number of bytes sent/received between any client and backend."
    )
    .unwrap();
}

/// A small combinator for pluggable error logging.
async fn log_error<R, F>(future: F) -> F::Output
where
    F: std::future::Future<Output = anyhow::Result<R>>,
{
    future.await.map_err(|err| {
        println!("error: {}", err);
        err
    })
}

pub async fn thread_main(
    config: &'static ProxyConfig,
    listener: tokio::net::TcpListener,
) -> anyhow::Result<()> {
    scopeguard::defer! {
        println!("proxy has shut down");
    }

    // When set for the server socket, the keepalive setting
    // will be inherited by all accepted client sockets.
    socket2::SockRef::from(&listener).set_keepalive(true)?;

    let cancel_map = Arc::new(CancelMap::default());
    loop {
        let (socket, peer_addr) = listener.accept().await?;
        println!("accepted connection from {}", peer_addr);

        let cancel_map = Arc::clone(&cancel_map);
        tokio::spawn(log_error(async move {
            socket
                .set_nodelay(true)
                .context("failed to set socket option")?;

            handle_client(config, &cancel_map, socket).await
        }));
    }
}

async fn handle_client(
    config: &ProxyConfig,
    cancel_map: &CancelMap,
    stream: impl AsyncRead + AsyncWrite + Unpin,
) -> anyhow::Result<()> {
    // The `closed` counter will increase when this future is destroyed.
    NUM_CONNECTIONS_ACCEPTED_COUNTER.inc();
    scopeguard::defer! {
        NUM_CONNECTIONS_CLOSED_COUNTER.inc();
    }

    let tls = config.tls_config.clone();
    let (stream, creds) = match handshake(stream, tls, cancel_map).await? {
        Some(x) => x,
        None => return Ok(()), // it's a cancellation request
    };

    let client = Client::new(stream, creds);
    cancel_map
        .with_session(|session| client.connect_to_db(config, session))
        .await
}

/// Establish a (most probably, secure) connection with the client.
/// For better testing experience, `stream` can be any object satisfying the traits.
/// It's easier to work with owned `stream` here as we need to updgrade it to TLS;
/// we also take an extra care of propagating only the select handshake errors to client.
async fn handshake<S: AsyncRead + AsyncWrite + Unpin>(
    stream: S,
    mut tls: Option<TlsConfig>,
    cancel_map: &CancelMap,
) -> anyhow::Result<Option<(PqStream<Stream<S>>, auth::ClientCredentials)>> {
    // Client may try upgrading to each protocol only once
    let (mut tried_ssl, mut tried_gss) = (false, false);

    let mut stream = PqStream::new(Stream::from_raw(stream));
    loop {
        let msg = stream.read_startup_packet().await?;
        println!("got message: {:?}", msg);

        use FeStartupPacket::*;
        match msg {
            SslRequest => match stream.get_ref() {
                Stream::Raw { .. } if !tried_ssl => {
                    tried_ssl = true;

                    // We can't perform TLS handshake without a config
                    let enc = tls.is_some();
                    stream.write_message(&Be::EncryptionResponse(enc)).await?;
                    if let Some(tls) = tls.take() {
                        // Upgrade raw stream into a secure TLS-backed stream.
                        // NOTE: We've consumed `tls`; this fact will be used later.
                        stream = PqStream::new(stream.into_inner().upgrade(tls).await?);
                    }
                }
                _ => bail!(ERR_PROTO_VIOLATION),
            },
            GssEncRequest => match stream.get_ref() {
                Stream::Raw { .. } if !tried_gss => {
                    tried_gss = true;

                    // Currently, we don't support GSSAPI
                    stream.write_message(&Be::EncryptionResponse(false)).await?;
                }
                _ => bail!(ERR_PROTO_VIOLATION),
            },
            StartupMessage { params, .. } => {
                // Check that the config has been consumed during upgrade
                // OR we didn't provide it at all (for dev purposes).
                if tls.is_some() {
                    stream.throw_error_str(ERR_INSECURE_CONNECTION).await?;
                }

                // Here and forth: `or_else` demands that we use a future here
                let creds = async { params.try_into() }
                    .or_else(|e| stream.throw_error(e))
                    .await?;

                break Ok(Some((stream, creds)));
            }
            CancelRequest(cancel_key_data) => {
                cancel_map.cancel_session(cancel_key_data).await?;

                break Ok(None);
            }
        }
    }
}

/// Thin connection context.
struct Client<S> {
    /// The underlying libpq protocol stream.
    stream: PqStream<S>,
    /// Client credentials that we care about.
    creds: auth::ClientCredentials,
}

impl<S> Client<S> {
    /// Construct a new connection context.
    fn new(stream: PqStream<S>, creds: auth::ClientCredentials) -> Self {
        Self { stream, creds }
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> Client<S> {
    /// Let the client authenticate and connect to the designated compute node.
    async fn connect_to_db(
        self,
        config: &ProxyConfig,
        session: cancellation::Session<'_>,
    ) -> anyhow::Result<()> {
        let Self { mut stream, creds } = self;

        // Authenticate and connect to a compute node.
        let auth = creds.authenticate(config, &mut stream).await;
        let db_info = async { auth }.or_else(|e| stream.throw_error(e)).await?;

        let (db, version, cancel_closure) =
            db_info.connect().or_else(|e| stream.throw_error(e)).await?;
        let cancel_key_data = session.enable_cancellation(cancel_closure);

        stream
            .write_message_noflush(&BeMessage::ParameterStatus(
                BeParameterStatusMessage::ServerVersion(&version),
            ))?
            .write_message_noflush(&Be::BackendKeyData(cancel_key_data))?
            .write_message(&BeMessage::ReadyForQuery)
            .await?;

        /// This function will be called for writes to either direction.
        fn inc_proxied(cnt: usize) {
            // Consider inventing something more sophisticated
            // if this ever becomes a bottleneck (cacheline bouncing).
            NUM_BYTES_PROXIED_COUNTER.inc_by(cnt as u64);
        }

        // Starting from here we only proxy the client's traffic.
        let mut db = MetricsStream::new(db, inc_proxied);
        let mut client = MetricsStream::new(stream.into_inner(), inc_proxied);
        let _ = tokio::io::copy_bidirectional(&mut client, &mut db).await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{auth, scram};
    use async_trait::async_trait;
    use rstest::rstest;
    use tokio_postgres::config::SslMode;
    use tokio_postgres::tls::{MakeTlsConnect, NoTls};
    use tokio_postgres_rustls::MakeRustlsConnect;

    /// Generate a set of TLS certificates: CA + server.
    fn generate_certs(
        hostname: &str,
    ) -> anyhow::Result<(rustls::Certificate, rustls::Certificate, rustls::PrivateKey)> {
        let ca = rcgen::Certificate::from_params({
            let mut params = rcgen::CertificateParams::default();
            params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
            params
        })?;

        let cert = rcgen::generate_simple_self_signed(vec![hostname.into()])?;
        Ok((
            rustls::Certificate(ca.serialize_der()?),
            rustls::Certificate(cert.serialize_der_with_signer(&ca)?),
            rustls::PrivateKey(cert.serialize_private_key_der()),
        ))
    }

    struct ClientConfig<'a> {
        config: rustls::ClientConfig,
        hostname: &'a str,
    }

    impl ClientConfig<'_> {
        fn make_tls_connect<S: AsyncRead + AsyncWrite + Unpin + Send + 'static>(
            self,
        ) -> anyhow::Result<impl tokio_postgres::tls::TlsConnect<S>> {
            let mut mk = MakeRustlsConnect::new(self.config);
            let tls = MakeTlsConnect::<S>::make_tls_connect(&mut mk, self.hostname)?;
            Ok(tls)
        }
    }

    /// Generate TLS certificates and build rustls configs for client and server.
    fn generate_tls_config(
        hostname: &str,
    ) -> anyhow::Result<(ClientConfig<'_>, Arc<rustls::ServerConfig>)> {
        let (ca, cert, key) = generate_certs(hostname)?;

        let server_config = {
            let config = rustls::ServerConfig::builder()
                .with_safe_defaults()
                .with_no_client_auth()
                .with_single_cert(vec![cert], key)?;

            config.into()
        };

        let client_config = {
            let config = rustls::ClientConfig::builder()
                .with_safe_defaults()
                .with_root_certificates({
                    let mut store = rustls::RootCertStore::empty();
                    store.add(&ca)?;
                    store
                })
                .with_no_client_auth();

            ClientConfig { config, hostname }
        };

        Ok((client_config, server_config))
    }

    #[async_trait]
    trait TestAuth: Sized {
        async fn authenticate<S: AsyncRead + AsyncWrite + Unpin + Send>(
            self,
            _stream: &mut PqStream<Stream<S>>,
        ) -> anyhow::Result<()> {
            Ok(())
        }
    }

    struct NoAuth;
    impl TestAuth for NoAuth {}

    struct Scram(scram::ServerSecret);

    impl Scram {
        fn new(password: &str) -> anyhow::Result<Self> {
            let salt = rand::random::<[u8; 16]>();
            let secret = scram::ServerSecret::build(password, &salt, 256)
                .context("failed to generate scram secret")?;
            Ok(Scram(secret))
        }

        fn mock(user: &str) -> Self {
            let salt = rand::random::<[u8; 32]>();
            Scram(scram::ServerSecret::mock(user, &salt))
        }
    }

    #[async_trait]
    impl TestAuth for Scram {
        async fn authenticate<S: AsyncRead + AsyncWrite + Unpin + Send>(
            self,
            stream: &mut PqStream<Stream<S>>,
        ) -> anyhow::Result<()> {
            auth::AuthFlow::new(stream)
                .begin(auth::Scram(&self.0))
                .await?
                .authenticate()
                .await?;

            Ok(())
        }
    }

    /// A dummy proxy impl which performs a handshake and reports auth success.
    async fn dummy_proxy(
        client: impl AsyncRead + AsyncWrite + Unpin + Send,
        tls: Option<TlsConfig>,
        auth: impl TestAuth + Send,
    ) -> anyhow::Result<()> {
        let cancel_map = CancelMap::default();
        let (mut stream, _creds) = handshake(client, tls, &cancel_map)
            .await?
            .context("handshake failed")?;

        auth.authenticate(&mut stream).await?;

        stream
            .write_message_noflush(&Be::AuthenticationOk)?
            .write_message_noflush(&BeParameterStatusMessage::encoding())?
            .write_message(&BeMessage::ReadyForQuery)
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn handshake_tls_is_enforced_by_proxy() -> anyhow::Result<()> {
        let (client, server) = tokio::io::duplex(1024);

        let (_, server_config) = generate_tls_config("localhost")?;
        let proxy = tokio::spawn(dummy_proxy(client, Some(server_config), NoAuth));

        let client_err = tokio_postgres::Config::new()
            .user("john_doe")
            .dbname("earth")
            .ssl_mode(SslMode::Disable)
            .connect_raw(server, NoTls)
            .await
            .err() // -> Option<E>
            .context("client shouldn't be able to connect")?;

        assert!(client_err.to_string().contains(ERR_INSECURE_CONNECTION));

        let server_err = proxy
            .await?
            .err() // -> Option<E>
            .context("server shouldn't accept client")?;

        assert!(client_err.to_string().contains(&server_err.to_string()));

        Ok(())
    }

    #[tokio::test]
    async fn handshake_tls() -> anyhow::Result<()> {
        let (client, server) = tokio::io::duplex(1024);

        let (client_config, server_config) = generate_tls_config("localhost")?;
        let proxy = tokio::spawn(dummy_proxy(client, Some(server_config), NoAuth));

        let (_client, _conn) = tokio_postgres::Config::new()
            .user("john_doe")
            .dbname("earth")
            .ssl_mode(SslMode::Require)
            .connect_raw(server, client_config.make_tls_connect()?)
            .await?;

        proxy.await?
    }

    #[tokio::test]
    async fn handshake_raw() -> anyhow::Result<()> {
        let (client, server) = tokio::io::duplex(1024);

        let proxy = tokio::spawn(dummy_proxy(client, None, NoAuth));

        let (_client, _conn) = tokio_postgres::Config::new()
            .user("john_doe")
            .dbname("earth")
            .ssl_mode(SslMode::Prefer)
            .connect_raw(server, NoTls)
            .await?;

        proxy.await?
    }

    #[tokio::test]
    async fn give_user_an_error_for_bad_creds() -> anyhow::Result<()> {
        let (client, server) = tokio::io::duplex(1024);

        let proxy = tokio::spawn(dummy_proxy(client, None, NoAuth));

        let client_err = tokio_postgres::Config::new()
            .ssl_mode(SslMode::Disable)
            .connect_raw(server, NoTls)
            .await
            .err() // -> Option<E>
            .context("client shouldn't be able to connect")?;

        // TODO: this is ugly, but `format!` won't allow us to extract fmt string
        assert!(client_err.to_string().contains("missing in startup packet"));

        let server_err = proxy
            .await?
            .err() // -> Option<E>
            .context("server shouldn't accept client")?;

        assert!(client_err.to_string().contains(&server_err.to_string()));

        Ok(())
    }

    #[tokio::test]
    async fn keepalive_is_inherited() -> anyhow::Result<()> {
        use tokio::net::{TcpListener, TcpStream};

        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let port = listener.local_addr()?.port();
        socket2::SockRef::from(&listener).set_keepalive(true)?;

        let t = tokio::spawn(async move {
            let (client, _) = listener.accept().await?;
            let keepalive = socket2::SockRef::from(&client).keepalive()?;
            anyhow::Ok(keepalive)
        });

        let _ = TcpStream::connect(("127.0.0.1", port)).await?;
        assert!(t.await??, "keepalive should be inherited");

        Ok(())
    }

    #[rstest]
    #[case("password_foo")]
    #[case("pwd-bar")]
    #[case("")]
    #[tokio::test]
    async fn scram_auth_good(#[case] password: &str) -> anyhow::Result<()> {
        let (client, server) = tokio::io::duplex(1024);

        let (client_config, server_config) = generate_tls_config("localhost")?;
        let proxy = tokio::spawn(dummy_proxy(
            client,
            Some(server_config),
            Scram::new(password)?,
        ));

        let (_client, _conn) = tokio_postgres::Config::new()
            .user("user")
            .dbname("db")
            .password(password)
            .ssl_mode(SslMode::Require)
            .connect_raw(server, client_config.make_tls_connect()?)
            .await?;

        proxy.await?
    }

    #[tokio::test]
    async fn scram_auth_mock() -> anyhow::Result<()> {
        let (client, server) = tokio::io::duplex(1024);

        let (client_config, server_config) = generate_tls_config("localhost")?;
        let proxy = tokio::spawn(dummy_proxy(
            client,
            Some(server_config),
            Scram::mock("user"),
        ));

        use rand::{distributions::Alphanumeric, Rng};
        let password: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(rand::random::<u8>() as usize)
            .map(char::from)
            .collect();

        let _client_err = tokio_postgres::Config::new()
            .user("user")
            .dbname("db")
            .password(&password) // no password will match the mocked secret
            .ssl_mode(SslMode::Require)
            .connect_raw(server, client_config.make_tls_connect()?)
            .await
            .err() // -> Option<E>
            .context("client shouldn't be able to connect")?;

        let _server_err = proxy
            .await?
            .err() // -> Option<E>
            .context("server shouldn't accept client")?;

        Ok(())
    }
}
