#[cfg(test)]
mod tests;

pub mod connect_compute;
pub mod retry;

use crate::{
    auth,
    cancellation::{self, CancelMap},
    compute,
    config::{AuthenticationConfig, ProxyConfig, TlsConfig},
    console::{self, messages::MetricsAuxInfo},
    context::RequestContext,
    metrics::{
        LatencyTimer, NUM_BYTES_PROXIED_COUNTER, NUM_BYTES_PROXIED_PER_CLIENT_COUNTER,
        NUM_CLIENT_CONNECTION_GAUGE, NUM_CONNECTION_REQUESTS_GAUGE,
    },
    protocol2::WithClientIp,
    rate_limiter::EndpointRateLimiter,
    stream::{PqStream, Stream},
    usage_metrics::{Ids, USAGE_METRICS},
};
use anyhow::{bail, Context};
use futures::TryFutureExt;
use itertools::Itertools;
use once_cell::sync::OnceCell;
use pq_proto::{BeMessage as Be, FeStartupPacket, StartupMessageParams};
use regex::Regex;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, info_span, Instrument};
use utils::measured_stream::MeasuredStream;

use self::connect_compute::{connect_to_compute, TcpMechanism};

const ERR_INSECURE_CONNECTION: &str = "connection is insecure (try using `sslmode=require`)";
const ERR_PROTO_VIOLATION: &str = "protocol violation";

pub async fn run_until_cancelled<F: std::future::Future>(
    f: F,
    cancellation_token: &CancellationToken,
) -> Option<F::Output> {
    match futures::future::select(
        std::pin::pin!(f),
        std::pin::pin!(cancellation_token.cancelled()),
    )
    .await
    {
        futures::future::Either::Left((f, _)) => Some(f),
        futures::future::Either::Right(((), _)) => None,
    }
}

pub async fn task_main(
    config: &'static ProxyConfig,
    listener: tokio::net::TcpListener,
    cancellation_token: CancellationToken,
    endpoint_rate_limiter: Arc<EndpointRateLimiter>,
) -> anyhow::Result<()> {
    scopeguard::defer! {
        info!("proxy has shut down");
    }

    // When set for the server socket, the keepalive setting
    // will be inherited by all accepted client sockets.
    socket2::SockRef::from(&listener).set_keepalive(true)?;

    let connections = tokio_util::task::task_tracker::TaskTracker::new();
    let cancel_map = Arc::new(CancelMap::default());

    while let Some(accept_result) =
        run_until_cancelled(listener.accept(), &cancellation_token).await
    {
        let (socket, peer_addr) = accept_result?;

        let session_id = uuid::Uuid::new_v4();
        let cancel_map = Arc::clone(&cancel_map);
        let endpoint_rate_limiter = endpoint_rate_limiter.clone();

        connections.spawn(
            async move {
                info!("accepted postgres client connection");

                let mut socket = WithClientIp::new(socket);
                let mut peer_addr = peer_addr.ip();
                if let Some(addr) = socket.wait_for_addr().await? {
                    peer_addr = addr.ip();
                    tracing::Span::current().record("peer_addr", &tracing::field::display(addr));
                } else if config.require_client_ip {
                    bail!("missing required client IP");
                }

                let mut ctx = RequestContext {
                    peer_addr,
                    session_id,
                    first_packet: tokio::time::Instant::now(),
                    protocol: "tcp",
                    project: None,
                    branch: None,
                    endpoint_id: None,
                    user: None,
                    application: None,
                    cluster: &config.cluster,
                    error_kind: None,
                    latency_timer: LatencyTimer::new("tcp"),
                };
                // ctx.latency_timer.

                socket
                    .inner
                    .set_nodelay(true)
                    .context("failed to set socket option")?;

                handle_client(
                    config,
                    &mut ctx,
                    &cancel_map,
                    socket,
                    ClientMode::Tcp,
                    endpoint_rate_limiter,
                )
                .await
            }
            .instrument(info_span!(
                "handle_client",
                ?session_id,
                peer_addr = tracing::field::Empty
            ))
            .unwrap_or_else(move |e| {
                // Acknowledge that the task has finished with an error.
                error!(?session_id, "per-client task finished with an error: {e:#}");
            }),
        );
    }

    connections.close();
    drop(listener);

    // Drain connections
    connections.wait().await;

    Ok(())
}

pub enum ClientMode {
    Tcp,
    Websockets { hostname: Option<String> },
}

/// Abstracts the logic of handling TCP vs WS clients
impl ClientMode {
    fn protocol_label(&self) -> &'static str {
        match self {
            ClientMode::Tcp => "tcp",
            ClientMode::Websockets { .. } => "ws",
        }
    }

    fn allow_cleartext(&self) -> bool {
        match self {
            ClientMode::Tcp => false,
            ClientMode::Websockets { .. } => true,
        }
    }

    fn allow_self_signed_compute(&self, config: &ProxyConfig) -> bool {
        match self {
            ClientMode::Tcp => config.allow_self_signed_compute,
            ClientMode::Websockets { .. } => false,
        }
    }

    fn hostname<'a, S>(&'a self, s: &'a Stream<S>) -> Option<&'a str> {
        match self {
            ClientMode::Tcp => s.sni_hostname(),
            ClientMode::Websockets { hostname } => hostname.as_deref(),
        }
    }

    fn handshake_tls<'a>(&self, tls: Option<&'a TlsConfig>) -> Option<&'a TlsConfig> {
        match self {
            ClientMode::Tcp => tls,
            // TLS is None here if using websockets, because the connection is already encrypted.
            ClientMode::Websockets { .. } => None,
        }
    }
}

pub async fn handle_client<S: AsyncRead + AsyncWrite + Unpin>(
    config: &'static ProxyConfig,
    ctx: &mut RequestContext,
    cancel_map: &CancelMap,
    stream: S,
    mode: ClientMode,
    endpoint_rate_limiter: Arc<EndpointRateLimiter>,
) -> anyhow::Result<()> {
    info!(
        protocol = mode.protocol_label(),
        "handling interactive connection from client"
    );

    let proto = mode.protocol_label();
    let _client_gauge = NUM_CLIENT_CONNECTION_GAUGE
        .with_label_values(&[proto])
        .guard();
    let _request_gauge = NUM_CONNECTION_REQUESTS_GAUGE
        .with_label_values(&[proto])
        .guard();

    let tls = config.tls_config.as_ref();

    let do_handshake = handshake(stream, mode.handshake_tls(tls), cancel_map);
    let (mut stream, params) = match do_handshake.await? {
        Some(x) => x,
        None => return Ok(()), // it's a cancellation request
    };

    // Extract credentials which we're going to use for auth.
    let creds = {
        let hostname = mode.hostname(stream.get_ref());
        let common_names = tls.and_then(|tls| tls.common_names.clone());
        let result = config
            .auth_backend
            .as_ref()
            .map(|_| auth::ClientCredentials::parse(&params, hostname, common_names, ctx.peer_addr))
            .transpose();

        match result {
            Ok(creds) => creds,
            Err(e) => stream.throw_error(e).await?,
        }
    };

    let client = Client::new(
        stream,
        creds,
        &params,
        ctx.session_id,
        mode.allow_self_signed_compute(config),
        endpoint_rate_limiter,
    );
    cancel_map
        .with_session(|session| client.connect_to_db(session, mode, &config.authentication_config))
        .await
}

/// Establish a (most probably, secure) connection with the client.
/// For better testing experience, `stream` can be any object satisfying the traits.
/// It's easier to work with owned `stream` here as we need to upgrade it to TLS;
/// we also take an extra care of propagating only the select handshake errors to client.
#[tracing::instrument(skip_all)]
async fn handshake<S: AsyncRead + AsyncWrite + Unpin>(
    stream: S,
    mut tls: Option<&TlsConfig>,
    cancel_map: &CancelMap,
) -> anyhow::Result<Option<(PqStream<Stream<S>>, StartupMessageParams)>> {
    // Client may try upgrading to each protocol only once
    let (mut tried_ssl, mut tried_gss) = (false, false);

    let mut stream = PqStream::new(Stream::from_raw(stream));
    loop {
        let msg = stream.read_startup_packet().await?;
        info!("received {msg:?}");

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

                        let (raw, read_buf) = stream.into_inner();
                        // TODO: Normally, client doesn't send any data before
                        // server says TLS handshake is ok and read_buf is empy.
                        // However, you could imagine pipelining of postgres
                        // SSLRequest + TLS ClientHello in one hunk similar to
                        // pipelining in our node js driver. We should probably
                        // support that by chaining read_buf with the stream.
                        if !read_buf.is_empty() {
                            bail!("data is sent before server replied with EncryptionResponse");
                        }
                        let tls_stream = raw.upgrade(tls.to_server_config()).await?;

                        let (_, tls_server_end_point) = tls
                            .cert_resolver
                            .resolve(tls_stream.get_ref().1.server_name())
                            .context("missing certificate")?;

                        stream = PqStream::new(Stream::Tls {
                            tls: Box::new(tls_stream),
                            tls_server_end_point,
                        });
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

                info!(session_type = "normal", "successful handshake");
                break Ok(Some((stream, params)));
            }
            CancelRequest(cancel_key_data) => {
                cancel_map.cancel_session(cancel_key_data).await?;

                info!(session_type = "cancellation", "successful handshake");
                break Ok(None);
            }
        }
    }
}

/// Finish client connection initialization: confirm auth success, send params, etc.
#[tracing::instrument(skip_all)]
async fn prepare_client_connection(
    node: &compute::PostgresConnection,
    session: cancellation::Session<'_>,
    stream: &mut PqStream<impl AsyncRead + AsyncWrite + Unpin>,
) -> anyhow::Result<()> {
    // Register compute's query cancellation token and produce a new, unique one.
    // The new token (cancel_key_data) will be sent to the client.
    let cancel_key_data = session.enable_query_cancellation(node.cancel_closure.clone());

    // Forward all postgres connection params to the client.
    // Right now the implementation is very hacky and inefficent (ideally,
    // we don't need an intermediate hashmap), but at least it should be correct.
    for (name, value) in &node.params {
        // TODO: Theoretically, this could result in a big pile of params...
        stream.write_message_noflush(&Be::ParameterStatus {
            name: name.as_bytes(),
            value: value.as_bytes(),
        })?;
    }

    stream
        .write_message_noflush(&Be::BackendKeyData(cancel_key_data))?
        .write_message(&Be::ReadyForQuery)
        .await?;

    Ok(())
}

/// Forward bytes in both directions (client <-> compute).
#[tracing::instrument(skip_all)]
pub async fn proxy_pass(
    client: impl AsyncRead + AsyncWrite + Unpin,
    compute: impl AsyncRead + AsyncWrite + Unpin,
    aux: MetricsAuxInfo,
) -> anyhow::Result<()> {
    let usage = USAGE_METRICS.register(Ids {
        endpoint_id: aux.endpoint_id.clone(),
        branch_id: aux.branch_id.clone(),
    });

    let m_sent = NUM_BYTES_PROXIED_COUNTER.with_label_values(&["tx"]);
    let m_sent2 = NUM_BYTES_PROXIED_PER_CLIENT_COUNTER.with_label_values(&aux.traffic_labels("tx"));
    let mut client = MeasuredStream::new(
        client,
        |_| {},
        |cnt| {
            // Number of bytes we sent to the client (outbound).
            m_sent.inc_by(cnt as u64);
            m_sent2.inc_by(cnt as u64);
            usage.record_egress(cnt as u64);
        },
    );

    let m_recv = NUM_BYTES_PROXIED_COUNTER.with_label_values(&["rx"]);
    let m_recv2 = NUM_BYTES_PROXIED_PER_CLIENT_COUNTER.with_label_values(&aux.traffic_labels("rx"));
    let mut compute = MeasuredStream::new(
        compute,
        |_| {},
        |cnt| {
            // Number of bytes the client sent to the compute node (inbound).
            m_recv.inc_by(cnt as u64);
            m_recv2.inc_by(cnt as u64);
        },
    );

    // Starting from here we only proxy the client's traffic.
    info!("performing the proxy pass...");
    let _ = tokio::io::copy_bidirectional(&mut client, &mut compute).await?;

    Ok(())
}

/// Thin connection context.
struct Client<'a, S> {
    /// The underlying libpq protocol stream.
    stream: PqStream<Stream<S>>,
    /// Client credentials that we care about.
    creds: auth::BackendType<'a, auth::ClientCredentials>,
    /// KV-dictionary with PostgreSQL connection params.
    params: &'a StartupMessageParams,
    /// Unique connection ID.
    session_id: uuid::Uuid,
    /// Allow self-signed certificates (for testing).
    allow_self_signed_compute: bool,
    /// Rate limiter for endpoints
    endpoint_rate_limiter: Arc<EndpointRateLimiter>,
}

impl<'a, S> Client<'a, S> {
    /// Construct a new connection context.
    fn new(
        stream: PqStream<Stream<S>>,
        creds: auth::BackendType<'a, auth::ClientCredentials>,
        params: &'a StartupMessageParams,
        session_id: uuid::Uuid,
        allow_self_signed_compute: bool,
        endpoint_rate_limiter: Arc<EndpointRateLimiter>,
    ) -> Self {
        Self {
            stream,
            creds,
            params,
            session_id,
            allow_self_signed_compute,
            endpoint_rate_limiter,
        }
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> Client<'_, S> {
    /// Let the client authenticate and connect to the designated compute node.
    // Instrumentation logs endpoint name everywhere. Doesn't work for link
    // auth; strictly speaking we don't know endpoint name in its case.
    #[tracing::instrument(name = "", fields(ep = %self.creds.get_endpoint().unwrap_or_default()), skip_all)]
    async fn connect_to_db(
        self,
        session: cancellation::Session<'_>,
        mode: ClientMode,
        config: &'static AuthenticationConfig,
    ) -> anyhow::Result<()> {
        let Self {
            mut stream,
            creds,
            params,
            session_id,
            allow_self_signed_compute,
            endpoint_rate_limiter,
        } = self;

        // check rate limit
        if let Some(ep) = creds.get_endpoint() {
            if !endpoint_rate_limiter.check(ep) {
                return stream
                    .throw_error(auth::AuthError::too_many_connections())
                    .await;
            }
        }

        let proto = mode.protocol_label();
        let extra = console::ConsoleReqExtra {
            session_id, // aka this connection's id
            application_name: format!(
                "{}/{}",
                params.get("application_name").unwrap_or_default(),
                proto
            ),
            options: neon_options(params),
        };
        let mut latency_timer = LatencyTimer::new(proto);

        let user = creds.get_user().to_owned();
        let auth_result = match creds
            .authenticate(
                &extra,
                &mut stream,
                mode.allow_cleartext(),
                config,
                &mut latency_timer,
            )
            .await
        {
            Ok(auth_result) => auth_result,
            Err(e) => {
                let db = params.get("database");
                let app = params.get("application_name");
                let params_span = tracing::info_span!("", ?user, ?db, ?app);

                return stream.throw_error(e).instrument(params_span).await;
            }
        };

        let (mut node_info, creds) = auth_result;

        node_info.allow_self_signed_compute = allow_self_signed_compute;

        let aux = node_info.aux.clone();
        let mut node = connect_to_compute(
            &TcpMechanism { params, proto },
            node_info,
            &extra,
            &creds,
            latency_timer,
        )
        .or_else(|e| stream.throw_error(e))
        .await?;

        prepare_client_connection(&node, session, &mut stream).await?;
        // Before proxy passing, forward to compute whatever data is left in the
        // PqStream input buffer. Normally there is none, but our serverless npm
        // driver in pipeline mode sends startup, password and first query
        // immediately after opening the connection.
        let (stream, read_buf) = stream.into_inner();
        node.stream.write_all(&read_buf).await?;
        proxy_pass(stream, node.stream, aux).await
    }
}

pub fn neon_options(params: &StartupMessageParams) -> Vec<(String, String)> {
    #[allow(unstable_name_collisions)]
    match params.options_raw() {
        Some(options) => options.filter_map(neon_option).collect(),
        None => vec![],
    }
}

pub fn neon_options_str(params: &StartupMessageParams) -> String {
    #[allow(unstable_name_collisions)]
    neon_options(params)
        .iter()
        .map(|(k, v)| format!("{}:{}", k, v))
        .sorted() // we sort it to use as cache key
        .intersperse(" ".to_owned())
        .collect()
}

pub fn neon_option(bytes: &str) -> Option<(String, String)> {
    static RE: OnceCell<Regex> = OnceCell::new();
    let re = RE.get_or_init(|| Regex::new(r"^neon_(\w+):(.+)").unwrap());

    let cap = re.captures(bytes)?;
    let (_, [k, v]) = cap.extract();
    Some((k.to_owned(), v.to_owned()))
}
