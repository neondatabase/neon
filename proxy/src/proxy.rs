#[cfg(test)]
mod tests;

use crate::{
    auth::{self, backend::AuthSuccess},
    cancellation::{self, CancelMap},
    compute::{self, PostgresConnection},
    config::{ProxyConfig, TlsConfig},
    console::{
        self,
        errors::{ApiError, WakeComputeError},
        messages::MetricsAuxInfo,
    },
    error::io_error,
    stream::{PqStream, Stream},
};
use anyhow::{bail, Context};
use futures::TryFutureExt;
use hyper::StatusCode;
use metrics::{register_int_counter, register_int_counter_vec, IntCounter, IntCounterVec};
use once_cell::sync::Lazy;
use pq_proto::{BeMessage as Be, FeStartupPacket, StartupMessageParams};
use std::{ops::ControlFlow, sync::Arc};
use tokio::{
    io::{AsyncRead, AsyncWrite, AsyncWriteExt},
    time,
};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use utils::measured_stream::MeasuredStream;

/// Number of times we should retry the `/proxy_wake_compute` http request.
/// Retry duration is BASE_RETRY_WAIT_DURATION * 1.5^n
pub const NUM_RETRIES_WAKE_COMPUTE: u32 = 10;
const BASE_RETRY_WAIT_DURATION: time::Duration = time::Duration::from_millis(100);

const ERR_INSECURE_CONNECTION: &str = "connection is insecure (try using `sslmode=require`)";
const ERR_PROTO_VIOLATION: &str = "protocol violation";

static NUM_CONNECTIONS_ACCEPTED_COUNTER: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "proxy_accepted_connections_total",
        "Number of TCP client connections accepted."
    )
    .unwrap()
});

static NUM_CONNECTIONS_CLOSED_COUNTER: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "proxy_closed_connections_total",
        "Number of TCP client connections closed."
    )
    .unwrap()
});

static NUM_CONNECTION_FAILURES: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "proxy_connection_failures_total",
        "Number of connection failures (per kind).",
        &["kind"],
    )
    .unwrap()
});

static NUM_BYTES_PROXIED_COUNTER: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "proxy_io_bytes_per_client",
        "Number of bytes sent/received between client and backend.",
        crate::console::messages::MetricsAuxInfo::TRAFFIC_LABELS,
    )
    .unwrap()
});

pub async fn task_main(
    config: &'static ProxyConfig,
    listener: tokio::net::TcpListener,
    cancellation_token: CancellationToken,
) -> anyhow::Result<()> {
    scopeguard::defer! {
        info!("proxy has shut down");
    }

    // When set for the server socket, the keepalive setting
    // will be inherited by all accepted client sockets.
    socket2::SockRef::from(&listener).set_keepalive(true)?;

    let mut connections = tokio::task::JoinSet::new();
    let cancel_map = Arc::new(CancelMap::default());

    loop {
        tokio::select! {
            accept_result = listener.accept() => {
                let (socket, peer_addr) = accept_result?;
                info!("accepted postgres client connection from {peer_addr}");

                let session_id = uuid::Uuid::new_v4();
                let cancel_map = Arc::clone(&cancel_map);
                connections.spawn(
                    async move {
                        info!("spawned a task for {peer_addr}");

                        socket
                            .set_nodelay(true)
                            .context("failed to set socket option")?;

                        handle_client(config, &cancel_map, session_id, socket).await
                    }
                    .unwrap_or_else(move |e| {
                        // Acknowledge that the task has finished with an error.
                        error!(?session_id, "per-client task finished with an error: {e:#}");
                    }),
                );
            }
            _ = cancellation_token.cancelled() => {
                drop(listener);
                break;
            }
        }
    }
    // Drain connections
    while let Some(res) = connections.join_next().await {
        if let Err(e) = res {
            if !e.is_panic() && !e.is_cancelled() {
                warn!("unexpected error from joined connection task: {e:?}");
            }
        }
    }
    Ok(())
}

// TODO(tech debt): unite this with its twin below.
#[tracing::instrument(fields(session_id = ?session_id), skip_all)]
pub async fn handle_ws_client(
    config: &'static ProxyConfig,
    cancel_map: &CancelMap,
    session_id: uuid::Uuid,
    stream: impl AsyncRead + AsyncWrite + Unpin,
    hostname: Option<String>,
) -> anyhow::Result<()> {
    // The `closed` counter will increase when this future is destroyed.
    NUM_CONNECTIONS_ACCEPTED_COUNTER.inc();
    scopeguard::defer! {
        NUM_CONNECTIONS_CLOSED_COUNTER.inc();
    }

    let tls = config.tls_config.as_ref();
    let hostname = hostname.as_deref();

    // TLS is None here, because the connection is already encrypted.
    let do_handshake = handshake(stream, None, cancel_map);
    let (mut stream, params) = match do_handshake.await? {
        Some(x) => x,
        None => return Ok(()), // it's a cancellation request
    };

    // Extract credentials which we're going to use for auth.
    let creds = {
        let common_names = tls.and_then(|tls| tls.common_names.clone());
        let result = config
            .auth_backend
            .as_ref()
            .map(|_| auth::ClientCredentials::parse(&params, hostname, common_names))
            .transpose();

        async { result }.or_else(|e| stream.throw_error(e)).await?
    };

    let client = Client::new(stream, creds, &params, session_id, false);
    cancel_map
        .with_session(|session| client.connect_to_db(session, true))
        .await
}

#[tracing::instrument(fields(session_id = ?session_id), skip_all)]
async fn handle_client(
    config: &'static ProxyConfig,
    cancel_map: &CancelMap,
    session_id: uuid::Uuid,
    stream: impl AsyncRead + AsyncWrite + Unpin,
) -> anyhow::Result<()> {
    // The `closed` counter will increase when this future is destroyed.
    NUM_CONNECTIONS_ACCEPTED_COUNTER.inc();
    scopeguard::defer! {
        NUM_CONNECTIONS_CLOSED_COUNTER.inc();
    }

    let tls = config.tls_config.as_ref();
    let do_handshake = handshake(stream, tls, cancel_map);
    let (mut stream, params) = match do_handshake.await? {
        Some(x) => x,
        None => return Ok(()), // it's a cancellation request
    };

    // Extract credentials which we're going to use for auth.
    let creds = {
        let sni = stream.get_ref().sni_hostname();
        let common_names = tls.and_then(|tls| tls.common_names.clone());
        let result = config
            .auth_backend
            .as_ref()
            .map(|_| auth::ClientCredentials::parse(&params, sni, common_names))
            .transpose();

        async { result }.or_else(|e| stream.throw_error(e)).await?
    };

    let allow_self_signed_compute = config.allow_self_signed_compute;

    let client = Client::new(
        stream,
        creds,
        &params,
        session_id,
        allow_self_signed_compute,
    );
    cancel_map
        .with_session(|session| client.connect_to_db(session, false))
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
                        stream = PqStream::new(raw.upgrade(tls.to_server_config()).await?);
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

/// If we couldn't connect, a cached connection info might be to blame
/// (e.g. the compute node's address might've changed at the wrong time).
/// Invalidate the cache entry (if any) to prevent subsequent errors.
#[tracing::instrument(name = "invalidate_cache", skip_all)]
pub fn invalidate_cache(node_info: &console::CachedNodeInfo) {
    let is_cached = node_info.cached();
    if is_cached {
        warn!("invalidating stalled compute node info cache entry");
        node_info.invalidate();
    }

    let label = match is_cached {
        true => "compute_cached",
        false => "compute_uncached",
    };
    NUM_CONNECTION_FAILURES.with_label_values(&[label]).inc();
}

/// Try to connect to the compute node once.
#[tracing::instrument(name = "connect_once", skip_all)]
async fn connect_to_compute_once(
    node_info: &console::CachedNodeInfo,
    timeout: time::Duration,
) -> Result<PostgresConnection, compute::ConnectionError> {
    let allow_self_signed_compute = node_info.allow_self_signed_compute;

    node_info
        .config
        .connect(allow_self_signed_compute, timeout)
        .await
}

/// Try to connect to the compute node, retrying if necessary.
/// This function might update `node_info`, so we take it by `&mut`.
#[tracing::instrument(skip_all)]
async fn connect_to_compute(
    node_info: &mut console::CachedNodeInfo,
    params: &StartupMessageParams,
    extra: &console::ConsoleReqExtra<'_>,
    creds: &auth::BackendType<'_, auth::ClientCredentials<'_>>,
) -> Result<PostgresConnection, compute::ConnectionError> {
    let mut num_retries = 0;
    let mut wait_duration = time::Duration::ZERO;
    let mut should_wake_with_error = None;
    loop {
        // Apply startup params to the (possibly, cached) compute node info.
        node_info.config.set_startup_params(params);

        if !wait_duration.is_zero() {
            time::sleep(wait_duration).await;
        }

        // try wake the compute node if we have determined it's sensible to do so
        if let Some(err) = should_wake_with_error.take() {
            match try_wake(node_info, extra, creds).await {
                // we can't wake up the compute node
                Ok(None) => return Err(err),
                // there was an error communicating with the control plane
                Err(e) => return Err(io_error(e).into()),
                // failed to wake up but we can continue to retry
                Ok(Some(ControlFlow::Continue(()))) => {
                    wait_duration = retry_after(num_retries);
                    should_wake_with_error = Some(err);

                    num_retries += 1;
                    info!(num_retries, "retrying wake compute");
                    continue;
                }
                // successfully woke up a compute node and can break the wakeup loop
                Ok(Some(ControlFlow::Break(()))) => {}
            }
        }

        // Set a shorter timeout for the initial connection attempt.
        //
        // In case we try to connect to an outdated address that is no longer valid, the
        // default behavior of Kubernetes is to drop the packets, causing us to wait for
        // the entire timeout period. We want to fail fast in such cases.
        //
        // A specific case to consider is when we have cached compute node information
        // with a 4-minute TTL (Time To Live), but the user has executed a `/suspend` API
        // call, resulting in the nonexistence of the compute node.
        //
        // We only use caching in case of scram proxy backed by the console, so reduce
        // the timeout only in that case.
        let is_scram_proxy = matches!(creds, auth::BackendType::Console(_, _));
        let timeout = if is_scram_proxy && num_retries == 0 {
            time::Duration::from_secs(2)
        } else {
            time::Duration::from_secs(10)
        };

        // do this again to ensure we have username?
        node_info.config.set_startup_params(params);

        match connect_to_compute_once(node_info, timeout).await {
            Ok(res) => return Ok(res),
            Err(e) => {
                error!(error = ?e, "could not connect to compute node");
                if !can_retry_error(&e, num_retries) {
                    return Err(e);
                }
                wait_duration = retry_after(num_retries);

                // after the first connect failure,
                // we should invalidate the cache and wake up a new compute node
                if num_retries == 0 {
                    invalidate_cache(node_info);
                    should_wake_with_error = Some(e);
                }
            }
        }

        num_retries += 1;
        info!(num_retries, "retrying connect");
    }
}

/// Attempts to wake up the compute node.
/// * Returns Ok(Some(true)) if there was an error waking but retries are acceptable
/// * Returns Ok(Some(false)) if the wakeup succeeded
/// * Returns Ok(None) or Err(e) if there was an error
pub async fn try_wake(
    node_info: &mut console::CachedNodeInfo,
    extra: &console::ConsoleReqExtra<'_>,
    creds: &auth::BackendType<'_, auth::ClientCredentials<'_>>,
) -> Result<Option<ControlFlow<()>>, WakeComputeError> {
    info!("compute node's state has likely changed; requesting a wake-up");
    match creds.wake_compute(extra).await {
        // retry wake if the compute was in an invalid state
        Err(WakeComputeError::ApiError(ApiError::Console {
            status: StatusCode::BAD_REQUEST,
            ..
        })) => Ok(Some(ControlFlow::Continue(()))),
        // Update `node_info` and try again.
        Ok(Some(mut new)) => {
            new.config.reuse_password(&node_info.config);
            *node_info = new;
            Ok(Some(ControlFlow::Break(())))
        }
        Err(e) => Err(e),
        Ok(None) => Ok(None),
    }
}

fn can_retry_error(err: &compute::ConnectionError, num_retries: u32) -> bool {
    use std::io::ErrorKind;
    match err {
        // retry all errors at least once
        _ if num_retries == 0 => true,
        // keep retrying connection errors
        compute::ConnectionError::CouldNotConnect(io_err)
            if num_retries < NUM_RETRIES_WAKE_COMPUTE =>
        {
            matches!(
                io_err.kind(),
                ErrorKind::ConnectionRefused | ErrorKind::AddrNotAvailable
            )
        }
        // otherwise, don't retry
        _ => false,
    }
}

pub fn retry_after(num_retries: u32) -> time::Duration {
    match num_retries {
        0 => time::Duration::ZERO,
        _ => {
            // 3/2 = 1.5 which seems to be an ok growth factor heuristic
            BASE_RETRY_WAIT_DURATION * 3_u32.pow(num_retries) / 2_u32.pow(num_retries)
        }
    }
}

/// Finish client connection initialization: confirm auth success, send params, etc.
#[tracing::instrument(skip_all)]
async fn prepare_client_connection(
    node: &compute::PostgresConnection,
    reported_auth_ok: bool,
    session: cancellation::Session<'_>,
    stream: &mut PqStream<impl AsyncRead + AsyncWrite + Unpin>,
) -> anyhow::Result<()> {
    // Register compute's query cancellation token and produce a new, unique one.
    // The new token (cancel_key_data) will be sent to the client.
    let cancel_key_data = session.enable_query_cancellation(node.cancel_closure.clone());

    // Report authentication success if we haven't done this already.
    // Note that we do this only (for the most part) after we've connected
    // to a compute (see above) which performs its own authentication.
    if !reported_auth_ok {
        stream.write_message_noflush(&Be::AuthenticationOk)?;
    }

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
    aux: &MetricsAuxInfo,
) -> anyhow::Result<()> {
    let m_sent = NUM_BYTES_PROXIED_COUNTER.with_label_values(&aux.traffic_labels("tx"));
    let mut client = MeasuredStream::new(
        client,
        |_| {},
        |cnt| {
            // Number of bytes we sent to the client (outbound).
            m_sent.inc_by(cnt as u64);
        },
    );

    let m_recv = NUM_BYTES_PROXIED_COUNTER.with_label_values(&aux.traffic_labels("rx"));
    let mut compute = MeasuredStream::new(
        compute,
        |_| {},
        |cnt| {
            // Number of bytes the client sent to the compute node (inbound).
            m_recv.inc_by(cnt as u64);
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
    stream: PqStream<S>,
    /// Client credentials that we care about.
    creds: auth::BackendType<'a, auth::ClientCredentials<'a>>,
    /// KV-dictionary with PostgreSQL connection params.
    params: &'a StartupMessageParams,
    /// Unique connection ID.
    session_id: uuid::Uuid,
    /// Allow self-signed certificates (for testing).
    allow_self_signed_compute: bool,
}

impl<'a, S> Client<'a, S> {
    /// Construct a new connection context.
    fn new(
        stream: PqStream<S>,
        creds: auth::BackendType<'a, auth::ClientCredentials<'a>>,
        params: &'a StartupMessageParams,
        session_id: uuid::Uuid,
        allow_self_signed_compute: bool,
    ) -> Self {
        Self {
            stream,
            creds,
            params,
            session_id,
            allow_self_signed_compute,
        }
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> Client<'_, S> {
    /// Let the client authenticate and connect to the designated compute node.
    // Instrumentation logs endpoint name everywhere. Doesn't work for link
    // auth; strictly speaking we don't know endpoint name in its case.
    #[tracing::instrument(name = "", fields(ep = self.creds.get_endpoint().unwrap_or("".to_owned())), skip_all)]
    async fn connect_to_db(
        self,
        session: cancellation::Session<'_>,
        allow_cleartext: bool,
    ) -> anyhow::Result<()> {
        let Self {
            mut stream,
            mut creds,
            params,
            session_id,
            allow_self_signed_compute,
        } = self;

        let extra = console::ConsoleReqExtra {
            session_id, // aka this connection's id
            application_name: params.get("application_name"),
        };

        let auth_result = async {
            // `&mut stream` doesn't let us merge those 2 lines.
            let res = creds
                .authenticate(&extra, &mut stream, allow_cleartext)
                .await;

            async { res }.or_else(|e| stream.throw_error(e)).await
        }
        .await?;

        let AuthSuccess {
            reported_auth_ok,
            value: mut node_info,
        } = auth_result;

        node_info.allow_self_signed_compute = allow_self_signed_compute;

        let mut node = connect_to_compute(&mut node_info, params, &extra, &creds)
            .or_else(|e| stream.throw_error(e))
            .await?;

        prepare_client_connection(&node, reported_auth_ok, session, &mut stream).await?;
        // Before proxy passing, forward to compute whatever data is left in the
        // PqStream input buffer. Normally there is none, but our serverless npm
        // driver in pipeline mode sends startup, password and first query
        // immediately after opening the connection.
        let (stream, read_buf) = stream.into_inner();
        node.stream.write_all(&read_buf).await?;
        proxy_pass(stream, node.stream, &node_info.aux).await
    }
}
