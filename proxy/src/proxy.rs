#[cfg(test)]
mod tests;

pub mod connect_compute;
pub mod retry;

use crate::{
    auth,
    cancellation::{self, CancelMap},
    compute,
    config::{AuthenticationConfig, ProxyConfig, TlsConfig},
    console::messages::MetricsAuxInfo,
    context::RequestMonitoring,
    metrics::{
        NUM_BYTES_PROXIED_COUNTER, NUM_BYTES_PROXIED_PER_CLIENT_COUNTER,
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
use smol_str::SmolStr;
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

                let mut ctx = RequestMonitoring::new(session_id, peer_addr, "tcp", &config.region);

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
    ctx: &mut RequestMonitoring,
    cancel_map: &CancelMap,
    stream: S,
    mode: ClientMode,
    endpoint_rate_limiter: Arc<EndpointRateLimiter>,
) -> anyhow::Result<()> {
    info!(
        protocol = ctx.protocol,
        "handling interactive connection from client"
    );

    let proto = ctx.protocol;
    let _client_gauge = NUM_CLIENT_CONNECTION_GAUGE
        .with_label_values(&[proto])
        .guard();
    let _request_gauge = NUM_CONNECTION_REQUESTS_GAUGE
        .with_label_values(&[proto])
        .guard();

    let tls = config.tls_config.as_ref();

    let pause = ctx.latency_timer.pause();
    let do_handshake = handshake(stream, mode.handshake_tls(tls), cancel_map);
    let (mut stream, params) = match do_handshake.await? {
        Some(x) => x,
        None => return Ok(()), // it's a cancellation request
    };
    drop(pause);

    // Extract credentials which we're going to use for auth.
    let user_info = {
        let hostname = mode.hostname(stream.get_ref());

        let common_names = tls.map(|tls| &tls.common_names);
        let result = config
            .auth_backend
            .as_ref()
            .map(|_| {
                auth::ComputeUserInfoMaybeEndpoint::parse(ctx, &params, hostname, common_names)
            })
            .transpose();

        match result {
            Ok(user_info) => user_info,
            Err(e) => stream.throw_error(e).await?,
        }
    };

    ctx.set_endpoint_id(user_info.get_endpoint());

    let client = Client::new(
        stream,
        user_info,
        &params,
        mode.allow_self_signed_compute(config),
        endpoint_rate_limiter,
    );
    cancel_map
        .with_session(|session| {
            client.connect_to_db(ctx, session, mode, &config.authentication_config)
        })
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
    ctx: &mut RequestMonitoring,
    client: impl AsyncRead + AsyncWrite + Unpin,
    compute: impl AsyncRead + AsyncWrite + Unpin,
    aux: MetricsAuxInfo,
) -> anyhow::Result<()> {
    ctx.log();

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
    user_info: auth::BackendType<'a, auth::ComputeUserInfoMaybeEndpoint>,
    /// KV-dictionary with PostgreSQL connection params.
    params: &'a StartupMessageParams,
    /// Allow self-signed certificates (for testing).
    allow_self_signed_compute: bool,
    /// Rate limiter for endpoints
    endpoint_rate_limiter: Arc<EndpointRateLimiter>,
}

impl<'a, S> Client<'a, S> {
    /// Construct a new connection context.
    fn new(
        stream: PqStream<Stream<S>>,
        user_info: auth::BackendType<'a, auth::ComputeUserInfoMaybeEndpoint>,
        params: &'a StartupMessageParams,
        allow_self_signed_compute: bool,
        endpoint_rate_limiter: Arc<EndpointRateLimiter>,
    ) -> Self {
        Self {
            stream,
            user_info,
            params,
            allow_self_signed_compute,
            endpoint_rate_limiter,
        }
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> Client<'_, S> {
    /// Let the client authenticate and connect to the designated compute node.
    // Instrumentation logs endpoint name everywhere. Doesn't work for link
    // auth; strictly speaking we don't know endpoint name in its case.
    #[tracing::instrument(name = "", fields(ep = %self.user_info.get_endpoint().unwrap_or_default()), skip_all)]
    async fn connect_to_db(
        self,
        ctx: &mut RequestMonitoring,
        session: cancellation::Session<'_>,
        mode: ClientMode,
        config: &'static AuthenticationConfig,
    ) -> anyhow::Result<()> {
        let Self {
            mut stream,
            user_info,
            params,
            allow_self_signed_compute,
            endpoint_rate_limiter,
        } = self;

        // check rate limit
        if let Some(ep) = user_info.get_endpoint() {
            if !endpoint_rate_limiter.check(ep) {
                return stream
                    .throw_error(auth::AuthError::too_many_connections())
                    .await;
            }
        }

        let user = user_info.get_user().to_owned();
        let auth_result = match user_info
            .authenticate(ctx, &mut stream, mode.allow_cleartext(), config)
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

        let (mut node_info, user_info) = auth_result;

        node_info.allow_self_signed_compute = allow_self_signed_compute;

        let aux = node_info.aux.clone();
        let mut node = connect_to_compute(ctx, &TcpMechanism { params }, node_info, &user_info)
            .or_else(|e| stream.throw_error(e))
            .await?;

        prepare_client_connection(&node, session, &mut stream).await?;
        // Before proxy passing, forward to compute whatever data is left in the
        // PqStream input buffer. Normally there is none, but our serverless npm
        // driver in pipeline mode sends startup, password and first query
        // immediately after opening the connection.
        let (stream, read_buf) = stream.into_inner();
        node.stream.write_all(&read_buf).await?;
        proxy_pass(ctx, stream, node.stream, aux).await
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct NeonOptions(Vec<(SmolStr, SmolStr)>);

impl NeonOptions {
    pub fn parse_params(params: &StartupMessageParams) -> Self {
        params
            .options_raw()
            .map(Self::parse_from_iter)
            .unwrap_or_default()
    }
    pub fn parse_options_raw(options: &str) -> Self {
        Self::parse_from_iter(StartupMessageParams::parse_options_raw(options))
    }

    fn parse_from_iter<'a>(options: impl Iterator<Item = &'a str>) -> Self {
        let mut options = options
            .filter_map(neon_option)
            .map(|(k, v)| (k.into(), v.into()))
            .collect_vec();
        options.sort();
        Self(options)
    }

    pub fn get_cache_key(&self, prefix: &str) -> SmolStr {
        // prefix + format!(" {k}:{v}")
        // kinda jank because SmolStr is immutable
        std::iter::once(prefix)
            .chain(self.0.iter().flat_map(|(k, v)| [" ", &**k, ":", &**v]))
            .collect()
    }

    /// <https://swagger.io/docs/specification/serialization/> DeepObject format
    /// `paramName[prop1]=value1&paramName[prop2]=value2&...`
    pub fn to_deep_object(&self) -> Vec<(String, SmolStr)> {
        self.0
            .iter()
            .map(|(k, v)| (format!("options[{}]", k), v.clone()))
            .collect()
    }
}

pub fn neon_option(bytes: &str) -> Option<(&str, &str)> {
    static RE: OnceCell<Regex> = OnceCell::new();
    let re = RE.get_or_init(|| Regex::new(r"^neon_(\w+):(.+)").unwrap());

    let cap = re.captures(bytes)?;
    let (_, [k, v]) = cap.extract();
    Some((k, v))
}
