//! A group of high-level tests for connection establishing logic and auth.
//!
use super::*;
use crate::auth::backend::TestBackend;
use crate::auth::ClientCredentials;
use crate::console::{CachedNodeInfo, NodeInfo};
use crate::{auth, http, sasl, scram};
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

        let common_names = Some([common_name.to_owned()].iter().cloned().collect());

        TlsConfig {
            config,
            common_names,
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
        Scram(scram::ServerSecret::mock(user, rand::random()))
    }
}

#[async_trait]
impl TestAuth for Scram {
    async fn authenticate<S: AsyncRead + AsyncWrite + Unpin + Send>(
        self,
        stream: &mut PqStream<Stream<S>>,
    ) -> anyhow::Result<()> {
        let outcome = auth::AuthFlow::new(stream)
            .begin(auth::Scram(&self.0))
            .await?
            .authenticate()
            .await?;

        use sasl::Outcome::*;
        match outcome {
            Success(_) => Ok(()),
            Failure(reason) => bail!("autentication failed with an error: {reason}"),
        }
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
        .write_message_noflush(&Be::CLIENT_ENCODING)?
        .write_message(&Be::ReadyForQuery)
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

#[test]
fn connect_compute_total_wait() {
    let mut total_wait = tokio::time::Duration::ZERO;
    for num_retries in 1..10 {
        total_wait += retry_after(num_retries);
    }
    assert!(total_wait < tokio::time::Duration::from_secs(12));
    assert!(total_wait > tokio::time::Duration::from_secs(10));
}

#[derive(Clone, Copy, Debug)]
enum ConnectAction {
    Wake,
    WakeFail,
    WakeRetry,
    Connect,
    Retry,
    Fail,
}

struct TestConnectMechanism {
    counter: Arc<std::sync::Mutex<usize>>,
    sequence: Vec<ConnectAction>,
}

impl TestConnectMechanism {
    fn verify(&self) {
        let counter = self.counter.lock().unwrap();
        assert_eq!(
            *counter,
            self.sequence.len(),
            "sequence does not proceed to the end"
        );
    }
}

impl TestConnectMechanism {
    fn new(sequence: Vec<ConnectAction>) -> Self {
        Self {
            counter: Arc::new(std::sync::Mutex::new(0)),
            sequence,
        }
    }
}

#[derive(Debug)]
struct TestConnection;

#[derive(Debug)]
struct TestConnectError {
    retryable: bool,
}

impl std::fmt::Display for TestConnectError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for TestConnectError {}

impl ShouldRetry for TestConnectError {
    fn could_retry(&self) -> bool {
        self.retryable
    }
}

#[async_trait]
impl ConnectMechanism for TestConnectMechanism {
    type Connection = TestConnection;
    type ConnectError = TestConnectError;
    type Error = anyhow::Error;

    async fn connect_once(
        &self,
        _node_info: &console::CachedNodeInfo,
        _timeout: time::Duration,
    ) -> Result<Self::Connection, Self::ConnectError> {
        let mut counter = self.counter.lock().unwrap();
        let action = self.sequence[*counter];
        *counter += 1;
        match action {
            ConnectAction::Connect => Ok(TestConnection),
            ConnectAction::Retry => Err(TestConnectError { retryable: true }),
            ConnectAction::Fail => Err(TestConnectError { retryable: false }),
            x => panic!("expecting action {:?}, connect is called instead", x),
        }
    }

    fn update_connect_config(&self, _conf: &mut compute::ConnCfg) {}
}

impl TestBackend for TestConnectMechanism {
    fn wake_compute(&self) -> Result<CachedNodeInfo, console::errors::WakeComputeError> {
        let mut counter = self.counter.lock().unwrap();
        let action = self.sequence[*counter];
        *counter += 1;
        match action {
            ConnectAction::Wake => Ok(helper_create_cached_node_info()),
            ConnectAction::WakeFail => {
                let err = console::errors::ApiError::Console {
                    status: http::StatusCode::FORBIDDEN,
                    text: "TEST".into(),
                };
                assert!(!err.could_retry());
                Err(console::errors::WakeComputeError::ApiError(err))
            }
            ConnectAction::WakeRetry => {
                let err = console::errors::ApiError::Console {
                    status: http::StatusCode::INTERNAL_SERVER_ERROR,
                    text: "TEST".into(),
                };
                assert!(err.could_retry());
                Err(console::errors::WakeComputeError::ApiError(err))
            }
            x => panic!("expecting action {:?}, wake_compute is called instead", x),
        }
    }
}

fn helper_create_cached_node_info() -> CachedNodeInfo {
    let node = NodeInfo {
        config: compute::ConnCfg::new(),
        aux: Default::default(),
        allow_self_signed_compute: false,
    };
    CachedNodeInfo::new_uncached(node)
}

fn helper_create_connect_info(
    mechanism: &TestConnectMechanism,
) -> (
    CachedNodeInfo,
    console::ConsoleReqExtra<'static>,
    auth::BackendType<'_, ClientCredentials<'static>>,
) {
    let cache = helper_create_cached_node_info();
    let extra = console::ConsoleReqExtra {
        session_id: uuid::Uuid::new_v4(),
        application_name: Some("TEST"),
    };
    let creds = auth::BackendType::Test(mechanism);
    (cache, extra, creds)
}

#[tokio::test]
async fn connect_to_compute_success() {
    use ConnectAction::*;
    let mechanism = TestConnectMechanism::new(vec![Connect]);
    let (cache, extra, creds) = helper_create_connect_info(&mechanism);
    connect_to_compute(&mechanism, cache, &extra, &creds)
        .await
        .unwrap();
    mechanism.verify();
}

#[tokio::test]
async fn connect_to_compute_retry() {
    use ConnectAction::*;
    let mechanism = TestConnectMechanism::new(vec![Retry, Wake, Retry, Connect]);
    let (cache, extra, creds) = helper_create_connect_info(&mechanism);
    connect_to_compute(&mechanism, cache, &extra, &creds)
        .await
        .unwrap();
    mechanism.verify();
}

/// Test that we don't retry if the error is not retryable.
#[tokio::test]
async fn connect_to_compute_non_retry_1() {
    use ConnectAction::*;
    let mechanism = TestConnectMechanism::new(vec![Retry, Wake, Retry, Fail]);
    let (cache, extra, creds) = helper_create_connect_info(&mechanism);
    connect_to_compute(&mechanism, cache, &extra, &creds)
        .await
        .unwrap_err();
    mechanism.verify();
}

/// Even for non-retryable errors, we should retry at least once.
#[tokio::test]
async fn connect_to_compute_non_retry_2() {
    use ConnectAction::*;
    let mechanism = TestConnectMechanism::new(vec![Fail, Wake, Retry, Connect]);
    let (cache, extra, creds) = helper_create_connect_info(&mechanism);
    connect_to_compute(&mechanism, cache, &extra, &creds)
        .await
        .unwrap();
    mechanism.verify();
}

/// Retry for at most `NUM_RETRIES_CONNECT` times.
#[tokio::test]
async fn connect_to_compute_non_retry_3() {
    assert_eq!(NUM_RETRIES_CONNECT, 10);
    use ConnectAction::*;
    let mechanism = TestConnectMechanism::new(vec![
        Retry, Wake, Retry, Retry, Retry, Retry, Retry, Retry, Retry, Retry, Retry,
        /* the 11th time */ Retry,
    ]);
    let (cache, extra, creds) = helper_create_connect_info(&mechanism);
    connect_to_compute(&mechanism, cache, &extra, &creds)
        .await
        .unwrap_err();
    mechanism.verify();
}

/// Should retry wake compute.
#[tokio::test]
async fn wake_retry() {
    use ConnectAction::*;
    let mechanism = TestConnectMechanism::new(vec![Retry, WakeRetry, Wake, Connect]);
    let (cache, extra, creds) = helper_create_connect_info(&mechanism);
    connect_to_compute(&mechanism, cache, &extra, &creds)
        .await
        .unwrap();
    mechanism.verify();
}

/// Wake failed with a non-retryable error.
#[tokio::test]
async fn wake_non_retry() {
    use ConnectAction::*;
    let mechanism = TestConnectMechanism::new(vec![Retry, WakeFail]);
    let (cache, extra, creds) = helper_create_connect_info(&mechanism);
    connect_to_compute(&mechanism, cache, &extra, &creds)
        .await
        .unwrap_err();
    mechanism.verify();
}
