///! A group of high-level tests for connection establishing logic and auth.
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
fn generate_tls_config<'a>(
    hostname: &'a str,
    common_name: &'a str,
) -> anyhow::Result<(ClientConfig<'a>, TlsConfig)> {
    let (ca, cert, key) = generate_certs(hostname)?;

    let tls_config = {
        let config = rustls::ServerConfig::builder()
            .with_safe_defaults()
            .with_no_client_auth()
            .with_single_cert(vec![cert], key)?
            .into();

        TlsConfig {
            config,
            common_name: Some(common_name.to_string()),
        }
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

    Ok((client_config, tls_config))
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
    let (mut stream, _params) = handshake(client, tls.as_ref(), &cancel_map)
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

    let (_, server_config) = generate_tls_config("generic-project-name.localhost", "localhost")?;
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

    let (client_config, server_config) =
        generate_tls_config("generic-project-name.localhost", "localhost")?;
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
        .options("project=generic-project-name")
        .ssl_mode(SslMode::Prefer)
        .connect_raw(server, NoTls)
        .await?;

    proxy.await?
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

    let (client_config, server_config) =
        generate_tls_config("generic-project-name.localhost", "localhost")?;
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

    let (client_config, server_config) =
        generate_tls_config("generic-project-name.localhost", "localhost")?;
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
