//! A group of high-level tests for connection establishing logic and auth.
#![allow(clippy::unimplemented)]

mod mitm;

use std::time::Duration;

use anyhow::{Context, bail};
use async_trait::async_trait;
use http::StatusCode;
use postgres_client::config::SslMode;
use postgres_client::tls::{MakeTlsConnect, NoTls};
use retry::{ShouldRetryWakeCompute, retry_after};
use rstest::rstest;
use rustls::crypto::ring;
use rustls::pki_types;
use tokio::io::DuplexStream;
use tracing_test::traced_test;

use super::retry::CouldRetry;
use super::*;
use crate::auth::backend::{ComputeUserInfo, MaybeOwned};
use crate::config::{ComputeConfig, RetryConfig};
use crate::control_plane::client::{ControlPlaneClient, TestControlPlaneClient};
use crate::control_plane::messages::{ControlPlaneErrorMessage, Details, MetricsAuxInfo, Status};
use crate::control_plane::{self, CachedNodeInfo, NodeInfo, NodeInfoCache};
use crate::error::ErrorKind;
use crate::proxy::connect_compute::ConnectMechanism;
use crate::tls::client_config::compute_client_config_with_certs;
use crate::tls::server_config::CertResolver;
use crate::types::{BranchId, EndpointId, ProjectId};
use crate::{sasl, scram};

/// Generate a set of TLS certificates: CA + server.
fn generate_certs(
    hostname: &str,
    common_name: &str,
) -> anyhow::Result<(
    pki_types::CertificateDer<'static>,
    pki_types::CertificateDer<'static>,
    pki_types::PrivateKeyDer<'static>,
)> {
    let ca_key = rcgen::KeyPair::generate()?;
    let ca = {
        let mut params = rcgen::CertificateParams::default();
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        params.self_signed(&ca_key)?
    };

    let cert_key = rcgen::KeyPair::generate()?;
    let cert = {
        let mut params = rcgen::CertificateParams::new(vec![hostname.into()])?;
        params.distinguished_name = rcgen::DistinguishedName::new();
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, common_name);
        params.signed_by(&cert_key, &ca, &ca_key)?
    };

    Ok((
        ca.der().clone(),
        cert.der().clone(),
        pki_types::PrivateKeyDer::Pkcs8(cert_key.serialize_der().into()),
    ))
}

struct ClientConfig<'a> {
    config: Arc<rustls::ClientConfig>,
    hostname: &'a str,
}

type TlsConnect<S> = <ComputeConfig as MakeTlsConnect<S>>::TlsConnect;

impl ClientConfig<'_> {
    fn make_tls_connect(self) -> anyhow::Result<TlsConnect<DuplexStream>> {
        Ok(crate::tls::postgres_rustls::make_tls_connect(
            &self.config,
            self.hostname,
        )?)
    }
}

/// Generate TLS certificates and build rustls configs for client and server.
fn generate_tls_config<'a>(
    hostname: &'a str,
    common_name: &'a str,
) -> anyhow::Result<(ClientConfig<'a>, TlsConfig)> {
    let (ca, cert, key) = generate_certs(hostname, common_name)?;

    let tls_config = {
        let config =
            rustls::ServerConfig::builder_with_provider(Arc::new(ring::default_provider()))
                .with_safe_default_protocol_versions()
                .context("ring should support the default protocol versions")?
                .with_no_client_auth()
                .with_single_cert(vec![cert.clone()], key.clone_key())?;

        let cert_resolver = CertResolver::new(key, vec![cert])?;

        let common_names = cert_resolver.get_common_names();

        let config = Arc::new(config);

        TlsConfig {
            http_config: config.clone(),
            pg_config: config,
            common_names,
            cert_resolver: Arc::new(cert_resolver),
        }
    };

    let client_config = {
        let config = Arc::new(compute_client_config_with_certs([ca]));

        ClientConfig { config, hostname }
    };

    Ok((client_config, tls_config))
}

#[async_trait]
trait TestAuth: Sized {
    async fn authenticate<S: AsyncRead + AsyncWrite + Unpin + Send>(
        self,
        stream: &mut PqStream<Stream<S>>,
    ) -> anyhow::Result<()> {
        stream.write_message(BeMessage::AuthenticationOk);
        Ok(())
    }
}

struct NoAuth;
impl TestAuth for NoAuth {}

struct Scram(scram::ServerSecret);

impl Scram {
    async fn new(password: &str) -> anyhow::Result<Self> {
        let secret = scram::ServerSecret::build(password)
            .await
            .context("failed to generate scram secret")?;
        Ok(Scram(secret))
    }

    fn mock() -> Self {
        Scram(scram::ServerSecret::mock(rand::random()))
    }
}

#[async_trait]
impl TestAuth for Scram {
    async fn authenticate<S: AsyncRead + AsyncWrite + Unpin + Send>(
        self,
        stream: &mut PqStream<Stream<S>>,
    ) -> anyhow::Result<()> {
        let outcome = auth::AuthFlow::new(stream, auth::Scram(&self.0, &RequestContext::test()))
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
    let mut stream = match handshake(&RequestContext::test(), client, tls.as_ref(), false).await? {
        HandshakeData::Startup(stream, _) => stream,
        HandshakeData::Cancel(_) => bail!("cancellation not supported"),
    };

    auth.authenticate(&mut stream).await?;

    stream.write_message(BeMessage::ParameterStatus {
        name: b"client_encoding",
        value: b"UTF8",
    });
    stream.write_message(BeMessage::ReadyForQuery);
    stream.flush().await?;

    Ok(())
}

#[tokio::test]
async fn handshake_tls_is_enforced_by_proxy() -> anyhow::Result<()> {
    let (client, server) = tokio::io::duplex(1024);

    let (_, server_config) = generate_tls_config("generic-project-name.localhost", "localhost")?;
    let proxy = tokio::spawn(dummy_proxy(client, Some(server_config), NoAuth));

    let client_err = postgres_client::Config::new("test".to_owned(), 5432)
        .user("john_doe")
        .dbname("earth")
        .ssl_mode(SslMode::Disable)
        .tls_and_authenticate(server, NoTls)
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

    let _conn = postgres_client::Config::new("test".to_owned(), 5432)
        .user("john_doe")
        .dbname("earth")
        .ssl_mode(SslMode::Require)
        .tls_and_authenticate(server, client_config.make_tls_connect()?)
        .await?;

    proxy.await?
}

#[tokio::test]
async fn handshake_raw() -> anyhow::Result<()> {
    let (client, server) = tokio::io::duplex(1024);

    let proxy = tokio::spawn(dummy_proxy(client, None, NoAuth));

    let _conn = postgres_client::Config::new("test".to_owned(), 5432)
        .user("john_doe")
        .dbname("earth")
        .set_param("options", "project=generic-project-name")
        .ssl_mode(SslMode::Prefer)
        .tls_and_authenticate(server, NoTls)
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

    TcpStream::connect(("127.0.0.1", port)).await?;
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
        Scram::new(password).await?,
    ));

    let _conn = postgres_client::Config::new("test".to_owned(), 5432)
        .channel_binding(postgres_client::config::ChannelBinding::Require)
        .user("user")
        .dbname("db")
        .password(password)
        .ssl_mode(SslMode::Require)
        .tls_and_authenticate(server, client_config.make_tls_connect()?)
        .await?;

    proxy.await?
}

#[tokio::test]
async fn scram_auth_disable_channel_binding() -> anyhow::Result<()> {
    let (client, server) = tokio::io::duplex(1024);

    let (client_config, server_config) =
        generate_tls_config("generic-project-name.localhost", "localhost")?;
    let proxy = tokio::spawn(dummy_proxy(
        client,
        Some(server_config),
        Scram::new("password").await?,
    ));

    let _conn = postgres_client::Config::new("test".to_owned(), 5432)
        .channel_binding(postgres_client::config::ChannelBinding::Disable)
        .user("user")
        .dbname("db")
        .password("password")
        .ssl_mode(SslMode::Require)
        .tls_and_authenticate(server, client_config.make_tls_connect()?)
        .await?;

    proxy.await?
}

#[tokio::test]
async fn scram_auth_mock() -> anyhow::Result<()> {
    let (client, server) = tokio::io::duplex(1024);

    let (client_config, server_config) =
        generate_tls_config("generic-project-name.localhost", "localhost")?;
    let proxy = tokio::spawn(dummy_proxy(client, Some(server_config), Scram::mock()));

    use rand::Rng;
    use rand::distributions::Alphanumeric;
    let password: String = rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(rand::random::<u8>() as usize)
        .map(char::from)
        .collect();

    let _client_err = postgres_client::Config::new("test".to_owned(), 5432)
        .user("user")
        .dbname("db")
        .password(&password) // no password will match the mocked secret
        .ssl_mode(SslMode::Require)
        .tls_and_authenticate(server, client_config.make_tls_connect()?)
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
    let config = RetryConfig {
        base_delay: Duration::from_secs(1),
        max_retries: 5,
        backoff_factor: 2.0,
    };
    for num_retries in 1..config.max_retries {
        total_wait += retry_after(num_retries, config);
    }
    assert!(f64::abs(total_wait.as_secs_f64() - 15.0) < 0.1);
}

#[derive(Clone, Copy, Debug)]
enum ConnectAction {
    Wake,
    WakeFail,
    WakeRetry,
    Connect,
    // connect_once -> Err, could_retry = true, should_retry_wake_compute = true
    Retry,
    // connect_once -> Err, could_retry = true, should_retry_wake_compute = false
    RetryNoWake,
    // connect_once -> Err, could_retry = false, should_retry_wake_compute = true
    Fail,
    // connect_once -> Err, could_retry = false, should_retry_wake_compute = false
    FailNoWake,
}

#[derive(Clone)]
struct TestConnectMechanism {
    counter: Arc<std::sync::Mutex<usize>>,
    sequence: Vec<ConnectAction>,
    cache: &'static NodeInfoCache,
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
            cache: Box::leak(Box::new(NodeInfoCache::new(
                "test",
                1,
                Duration::from_secs(100),
                false,
            ))),
        }
    }
}

#[derive(Debug)]
struct TestConnection;

#[derive(Debug)]
struct TestConnectError {
    retryable: bool,
    wakeable: bool,
    kind: crate::error::ErrorKind,
}

impl ReportableError for TestConnectError {
    fn get_error_kind(&self) -> crate::error::ErrorKind {
        self.kind
    }
}

impl std::fmt::Display for TestConnectError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for TestConnectError {}

impl CouldRetry for TestConnectError {
    fn could_retry(&self) -> bool {
        self.retryable
    }
}
impl ShouldRetryWakeCompute for TestConnectError {
    fn should_retry_wake_compute(&self) -> bool {
        self.wakeable
    }
}

#[async_trait]
impl ConnectMechanism for TestConnectMechanism {
    type Connection = TestConnection;
    type ConnectError = TestConnectError;
    type Error = anyhow::Error;

    async fn connect_once(
        &self,
        _ctx: &RequestContext,
        _node_info: &control_plane::CachedNodeInfo,
        _config: &ComputeConfig,
    ) -> Result<Self::Connection, Self::ConnectError> {
        let mut counter = self.counter.lock().unwrap();
        let action = self.sequence[*counter];
        *counter += 1;
        match action {
            ConnectAction::Connect => Ok(TestConnection),
            ConnectAction::Retry => Err(TestConnectError {
                retryable: true,
                wakeable: true,
                kind: ErrorKind::Compute,
            }),
            ConnectAction::RetryNoWake => Err(TestConnectError {
                retryable: true,
                wakeable: false,
                kind: ErrorKind::Compute,
            }),
            ConnectAction::Fail => Err(TestConnectError {
                retryable: false,
                wakeable: true,
                kind: ErrorKind::Compute,
            }),
            ConnectAction::FailNoWake => Err(TestConnectError {
                retryable: false,
                wakeable: false,
                kind: ErrorKind::Compute,
            }),
            x => panic!("expecting action {x:?}, connect is called instead"),
        }
    }
}

impl TestControlPlaneClient for TestConnectMechanism {
    fn wake_compute(&self) -> Result<CachedNodeInfo, control_plane::errors::WakeComputeError> {
        let mut counter = self.counter.lock().unwrap();
        let action = self.sequence[*counter];
        *counter += 1;
        match action {
            ConnectAction::Wake => Ok(helper_create_cached_node_info(self.cache)),
            ConnectAction::WakeFail => {
                let err = control_plane::errors::ControlPlaneError::Message(Box::new(
                    ControlPlaneErrorMessage {
                        http_status_code: StatusCode::BAD_REQUEST,
                        error: "TEST".into(),
                        status: None,
                    },
                ));
                assert!(!err.could_retry());
                Err(control_plane::errors::WakeComputeError::ControlPlane(err))
            }
            ConnectAction::WakeRetry => {
                let err = control_plane::errors::ControlPlaneError::Message(Box::new(
                    ControlPlaneErrorMessage {
                        http_status_code: StatusCode::BAD_REQUEST,
                        error: "TEST".into(),
                        status: Some(Status {
                            code: "error".into(),
                            message: "error".into(),
                            details: Details {
                                error_info: None,
                                retry_info: Some(control_plane::messages::RetryInfo {
                                    retry_delay_ms: 1,
                                }),
                                user_facing_message: None,
                            },
                        }),
                    },
                ));
                assert!(err.could_retry());
                Err(control_plane::errors::WakeComputeError::ControlPlane(err))
            }
            x => panic!("expecting action {x:?}, wake_compute is called instead"),
        }
    }

    fn get_access_control(
        &self,
    ) -> Result<control_plane::EndpointAccessControl, control_plane::errors::GetAuthInfoError> {
        unimplemented!("not used in tests")
    }

    fn dyn_clone(&self) -> Box<dyn TestControlPlaneClient> {
        Box::new(self.clone())
    }
}

fn helper_create_cached_node_info(cache: &'static NodeInfoCache) -> CachedNodeInfo {
    let node = NodeInfo {
        conn_info: compute::ConnectInfo {
            host: "test".into(),
            port: 5432,
            ssl_mode: SslMode::Disable,
            host_addr: None,
        },
        aux: MetricsAuxInfo {
            endpoint_id: (&EndpointId::from("endpoint")).into(),
            project_id: (&ProjectId::from("project")).into(),
            branch_id: (&BranchId::from("branch")).into(),
            compute_id: "compute".into(),
            cold_start_info: crate::control_plane::messages::ColdStartInfo::Warm,
        },
    };
    let (_, node2) = cache.insert_unit("key".into(), Ok(node.clone()));
    node2.map(|()| node)
}

fn helper_create_connect_info(
    mechanism: &TestConnectMechanism,
) -> auth::Backend<'static, ComputeUserInfo> {
    auth::Backend::ControlPlane(
        MaybeOwned::Owned(ControlPlaneClient::Test(Box::new(mechanism.clone()))),
        ComputeUserInfo {
            endpoint: "endpoint".into(),
            user: "user".into(),
            options: NeonOptions::parse_options_raw(""),
        },
    )
}

fn config() -> ComputeConfig {
    let retry = RetryConfig {
        base_delay: Duration::from_secs(1),
        max_retries: 5,
        backoff_factor: 2.0,
    };

    ComputeConfig {
        retry,
        tls: Arc::new(compute_client_config_with_certs(std::iter::empty())),
        timeout: Duration::from_secs(2),
    }
}

#[tokio::test]
async fn connect_to_compute_success() {
    let _ = env_logger::try_init();
    use ConnectAction::*;
    let ctx = RequestContext::test();
    let mechanism = TestConnectMechanism::new(vec![Wake, Connect]);
    let user_info = helper_create_connect_info(&mechanism);
    let config = config();
    connect_to_compute(&ctx, &mechanism, &user_info, config.retry, &config)
        .await
        .unwrap();
    mechanism.verify();
}

#[tokio::test]
async fn connect_to_compute_retry() {
    let _ = env_logger::try_init();
    use ConnectAction::*;
    let ctx = RequestContext::test();
    let mechanism = TestConnectMechanism::new(vec![Wake, Retry, Wake, Connect]);
    let user_info = helper_create_connect_info(&mechanism);
    let config = config();
    connect_to_compute(&ctx, &mechanism, &user_info, config.retry, &config)
        .await
        .unwrap();
    mechanism.verify();
}

/// Test that we don't retry if the error is not retryable.
#[tokio::test]
async fn connect_to_compute_non_retry_1() {
    let _ = env_logger::try_init();
    use ConnectAction::*;
    let ctx = RequestContext::test();
    let mechanism = TestConnectMechanism::new(vec![Wake, Retry, Wake, Fail]);
    let user_info = helper_create_connect_info(&mechanism);
    let config = config();
    connect_to_compute(&ctx, &mechanism, &user_info, config.retry, &config)
        .await
        .unwrap_err();
    mechanism.verify();
}

/// Even for non-retryable errors, we should retry at least once.
#[tokio::test]
async fn connect_to_compute_non_retry_2() {
    let _ = env_logger::try_init();
    use ConnectAction::*;
    let ctx = RequestContext::test();
    let mechanism = TestConnectMechanism::new(vec![Wake, Fail, Wake, Connect]);
    let user_info = helper_create_connect_info(&mechanism);
    let config = config();
    connect_to_compute(&ctx, &mechanism, &user_info, config.retry, &config)
        .await
        .unwrap();
    mechanism.verify();
}

/// Retry for at most `NUM_RETRIES_CONNECT` times.
#[tokio::test]
async fn connect_to_compute_non_retry_3() {
    let _ = env_logger::try_init();
    tokio::time::pause();
    use ConnectAction::*;
    let ctx = RequestContext::test();
    let mechanism =
        TestConnectMechanism::new(vec![Wake, Retry, Wake, Retry, Retry, Retry, Retry, Retry]);
    let user_info = helper_create_connect_info(&mechanism);
    let wake_compute_retry_config = RetryConfig {
        base_delay: Duration::from_secs(1),
        max_retries: 1,
        backoff_factor: 2.0,
    };
    let config = config();
    connect_to_compute(
        &ctx,
        &mechanism,
        &user_info,
        wake_compute_retry_config,
        &config,
    )
    .await
    .unwrap_err();
    mechanism.verify();
}

/// Should retry wake compute.
#[tokio::test]
async fn wake_retry() {
    let _ = env_logger::try_init();
    use ConnectAction::*;
    let ctx = RequestContext::test();
    let mechanism = TestConnectMechanism::new(vec![WakeRetry, Wake, Connect]);
    let user_info = helper_create_connect_info(&mechanism);
    let config = config();
    connect_to_compute(&ctx, &mechanism, &user_info, config.retry, &config)
        .await
        .unwrap();
    mechanism.verify();
}

/// Wake failed with a non-retryable error.
#[tokio::test]
async fn wake_non_retry() {
    let _ = env_logger::try_init();
    use ConnectAction::*;
    let ctx = RequestContext::test();
    let mechanism = TestConnectMechanism::new(vec![WakeRetry, WakeFail]);
    let user_info = helper_create_connect_info(&mechanism);
    let config = config();
    connect_to_compute(&ctx, &mechanism, &user_info, config.retry, &config)
        .await
        .unwrap_err();
    mechanism.verify();
}

#[tokio::test]
#[traced_test]
async fn fail_but_wake_invalidates_cache() {
    let ctx = RequestContext::test();
    let mech = TestConnectMechanism::new(vec![
        ConnectAction::Wake,
        ConnectAction::Fail,
        ConnectAction::Wake,
        ConnectAction::Connect,
    ]);
    let user = helper_create_connect_info(&mech);
    let cfg = config();

    connect_to_compute(&ctx, &mech, &user, cfg.retry, &cfg)
        .await
        .unwrap();

    assert!(logs_contain(
        "invalidating stalled compute node info cache entry"
    ));
}

#[tokio::test]
#[traced_test]
async fn fail_no_wake_skips_cache_invalidation() {
    let ctx = RequestContext::test();
    let mech = TestConnectMechanism::new(vec![
        ConnectAction::Wake,
        ConnectAction::FailNoWake,
        ConnectAction::Connect,
    ]);
    let user = helper_create_connect_info(&mech);
    let cfg = config();

    connect_to_compute(&ctx, &mech, &user, cfg.retry, &cfg)
        .await
        .unwrap();

    assert!(!logs_contain(
        "invalidating stalled compute node info cache entry"
    ));
}

#[tokio::test]
#[traced_test]
async fn retry_but_wake_invalidates_cache() {
    let _ = env_logger::try_init();
    use ConnectAction::*;

    let ctx = RequestContext::test();
    // Wake → Retry (retryable + wakeable) → Wake → Connect
    let mechanism = TestConnectMechanism::new(vec![Wake, Retry, Wake, Connect]);
    let user_info = helper_create_connect_info(&mechanism);
    let cfg = config();

    connect_to_compute(&ctx, &mechanism, &user_info, cfg.retry, &cfg)
        .await
        .unwrap();
    mechanism.verify();

    // Because Retry has wakeable=true, we should see invalidate_cache
    assert!(logs_contain(
        "invalidating stalled compute node info cache entry"
    ));
}

#[tokio::test]
#[traced_test]
async fn retry_no_wake_skips_invalidation() {
    let _ = env_logger::try_init();
    use ConnectAction::*;

    let ctx = RequestContext::test();
    // Wake → RetryNoWake (retryable + NOT wakeable)
    let mechanism = TestConnectMechanism::new(vec![Wake, RetryNoWake]);
    let user_info = helper_create_connect_info(&mechanism);
    let cfg = config();

    connect_to_compute(&ctx, &mechanism, &user_info, cfg.retry, &cfg)
        .await
        .unwrap_err();
    mechanism.verify();

    // Because RetryNoWake has wakeable=false, we must NOT see invalidate_cache
    assert!(!logs_contain(
        "invalidating stalled compute node info cache entry"
    ));
}
