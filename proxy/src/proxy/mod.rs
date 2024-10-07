#[cfg(test)]
mod tests;

pub(crate) mod connect_compute;
mod copy_bidirectional;
pub(crate) mod handshake;
pub(crate) mod passthrough;
pub(crate) mod retry;
pub(crate) mod wake_compute;
pub use copy_bidirectional::copy_bidirectional_client_compute;
pub use copy_bidirectional::ErrorSource;

use crate::control_plane::provider::ControlPlaneBackend;
use crate::metrics::Protocol;
use crate::{
    auth,
    cancellation::{self, CancellationHandlerMain},
    compute,
    config::{ProxyConfig, TlsConfig},
    context::RequestMonitoring,
    error::ReportableError,
    metrics::{Metrics, NumClientConnectionsGuard},
    proxy::handshake::{handshake, HandshakeData},
    rate_limiter::EndpointRateLimiter,
    stream::{PqStream, Stream},
    EndpointCacheKey,
};
use futures::TryFutureExt;
use itertools::Itertools;
use once_cell::sync::OnceCell;
use pq_proto::{BeMessage as Be, StartupMessageParams};
use regex::Regex;
use smol_str::{format_smolstr, SmolStr};
use std::net::IpAddr;
use std::sync::Arc;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, Instrument};

use self::{
    connect_compute::{connect_to_compute, TcpMechanism},
    passthrough::ProxyPassthrough,
};

const ERR_INSECURE_CONNECTION: &str = "connection is insecure (try using `sslmode=require`)";

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
    auth_backend: &'static ControlPlaneBackend,
    listener: tokio::net::TcpListener,
    cancellation_token: CancellationToken,
    cancellation_handler: Arc<CancellationHandlerMain>,
    endpoint_rate_limiter: Arc<EndpointRateLimiter>,
) -> anyhow::Result<()> {
    scopeguard::defer! {
        info!("proxy has shut down");
    }

    super::connection_loop(
        config,
        listener,
        cancellation_token,
        Protocol::Tcp,
        C {
            config,
            auth_backend,
            cancellation_handler,
            endpoint_rate_limiter,
        },
    )
    .await
}

#[derive(Clone)]
struct C {
    config: &'static ProxyConfig,
    auth_backend: &'static ControlPlaneBackend,
    cancellation_handler: Arc<CancellationHandlerMain>,
    endpoint_rate_limiter: Arc<EndpointRateLimiter>,
}

impl super::ConnHandler for C {
    async fn handle(
        self,
        session_id: uuid::Uuid,
        peer_addr: IpAddr,
        socket: crate::protocol2::ChainRW<tokio::net::TcpStream>,
        conn_gauge: crate::metrics::NumClientConnectionsGuard<'static>,
    ) {
        let ctx = RequestMonitoring::new(
            session_id,
            peer_addr,
            crate::metrics::Protocol::Tcp,
            &self.config.region,
        );
        let span = ctx.span();

        let startup = Box::pin(
            handle_client(
                self.config,
                self.auth_backend,
                &ctx,
                self.cancellation_handler,
                socket,
                ClientMode::Tcp,
                self.endpoint_rate_limiter,
                conn_gauge,
            )
            .instrument(span.clone()),
        );

        let res = startup.await;
        transition_connection(ctx, res).await;
    }
}

pub(crate) async fn transition_connection<S: AsyncRead + AsyncWrite + Unpin>(
    ctx: RequestMonitoring,
    res: Result<Option<ProxyPassthrough<S>>, ClientRequestError>,
) {
    let span = ctx.span();
    match res {
        Err(e) => {
            ctx.set_error_kind(e.get_error_kind());
            error!(parent: &span, "per-client task finished with an error: {e:#}");
        }
        Ok(None) => {
            ctx.set_success();
        }
        Ok(Some(p)) => {
            ctx.set_success();
            ctx.log_connect();
            match p.proxy_pass().instrument(span.clone()).await {
                Ok(()) => {}
                Err(ErrorSource::Client(e)) => {
                    error!(parent: &span, "per-client task finished with an IO error from the client: {e:#}");
                }
                Err(ErrorSource::Compute(e)) => {
                    error!(parent: &span, "per-client task finished with an IO error from the compute: {e:#}");
                }
            }
        }
    }
}

pub(crate) enum ClientMode {
    Tcp,
    Websockets { hostname: Option<String> },
}

/// Abstracts the logic of handling TCP vs WS clients
impl ClientMode {
    pub(crate) fn allow_cleartext(&self) -> bool {
        match self {
            ClientMode::Tcp => false,
            ClientMode::Websockets { .. } => true,
        }
    }

    pub(crate) fn allow_self_signed_compute(&self, config: &ProxyConfig) -> bool {
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

#[derive(Debug, Error)]
// almost all errors should be reported to the user, but there's a few cases where we cannot
// 1. Cancellation: we are not allowed to tell the client any cancellation statuses for security reasons
// 2. Handshake: handshake reports errors if it can, otherwise if the handshake fails due to protocol violation,
//    we cannot be sure the client even understands our error message
// 3. PrepareClient: The client disconnected, so we can't tell them anyway...
pub(crate) enum ClientRequestError {
    #[error("{0}")]
    Cancellation(#[from] cancellation::CancelError),
    #[error("{0}")]
    Handshake(#[from] handshake::HandshakeError),
    #[error("{0}")]
    HandshakeTimeout(#[from] tokio::time::error::Elapsed),
    #[error("{0}")]
    PrepareClient(#[from] std::io::Error),
    #[error("{0}")]
    ReportedError(#[from] crate::stream::ReportedError),
}

impl ReportableError for ClientRequestError {
    fn get_error_kind(&self) -> crate::error::ErrorKind {
        match self {
            ClientRequestError::Cancellation(e) => e.get_error_kind(),
            ClientRequestError::Handshake(e) => e.get_error_kind(),
            ClientRequestError::HandshakeTimeout(_) => crate::error::ErrorKind::RateLimit,
            ClientRequestError::ReportedError(e) => e.get_error_kind(),
            ClientRequestError::PrepareClient(_) => crate::error::ErrorKind::ClientDisconnect,
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn handle_client<S: AsyncRead + AsyncWrite + Unpin>(
    config: &'static ProxyConfig,
    auth_backend: &'static ControlPlaneBackend,
    ctx: &RequestMonitoring,
    cancellation_handler: Arc<CancellationHandlerMain>,
    stream: S,
    mode: ClientMode,
    endpoint_rate_limiter: Arc<EndpointRateLimiter>,
    conn_gauge: NumClientConnectionsGuard<'static>,
) -> Result<Option<ProxyPassthrough<S>>, ClientRequestError> {
    info!(
        protocol = %ctx.protocol(),
        "handling interactive connection from client"
    );

    let metrics = &Metrics::get().proxy;
    let proto = ctx.protocol();
    let request_gauge = metrics.connection_requests.guard(proto);

    let tls = config.tls_config.as_ref();

    let record_handshake_error = !ctx.has_private_peer_addr();
    let pause = ctx.latency_timer_pause(crate::metrics::Waiting::Client);
    let do_handshake = handshake(ctx, stream, mode.handshake_tls(tls), record_handshake_error);
    let (mut stream, params) =
        match tokio::time::timeout(config.handshake_timeout, do_handshake).await?? {
            HandshakeData::Startup(stream, params) => (stream, params),
            HandshakeData::Cancel(cancel_key_data) => {
                return Ok(cancellation_handler
                    .cancel_session(cancel_key_data, ctx.session_id())
                    .await
                    .map(|()| None)?)
            }
        };
    drop(pause);

    ctx.set_db_options(params.clone());

    let hostname = mode.hostname(stream.get_ref());

    let common_names = tls.map(|tls| &tls.common_names);

    // Extract credentials which we're going to use for auth.
    let result = auth::ComputeUserInfoMaybeEndpoint::parse(ctx, &params, hostname, common_names);
    let user_info = match result {
        Ok(user_info) => user_info,
        Err(e) => stream.throw_error(e).await?,
    };

    let user = user_info.user.clone();
    let user_info = match auth_backend
        .authenticate(
            ctx,
            user_info,
            &mut stream,
            mode.allow_cleartext(),
            &config.authentication_config,
            endpoint_rate_limiter,
        )
        .await
    {
        Ok(auth_result) => auth_result,
        Err(e) => {
            let db = params.get("database");
            let app = params.get("application_name");
            let params_span = tracing::info_span!("", ?user, ?db, ?app);

            return stream.throw_error(e).instrument(params_span).await?;
        }
    };

    let mut node = connect_to_compute(
        ctx,
        &TcpMechanism {
            params: &params,
            locks: &config.connect_compute_locks,
        },
        &user_info,
        mode.allow_self_signed_compute(config),
        config.wake_compute_retry_config,
        config.connect_to_compute_retry_config,
    )
    .or_else(|e| stream.throw_error(e))
    .await?;

    let session = cancellation_handler.get_session();
    prepare_client_connection(&node, &session, &mut stream).await?;

    // Before proxy passing, forward to compute whatever data is left in the
    // PqStream input buffer. Normally there is none, but our serverless npm
    // driver in pipeline mode sends startup, password and first query
    // immediately after opening the connection.
    let (stream, read_buf) = stream.into_inner();
    node.stream.write_all(&read_buf).await?;

    Ok(Some(ProxyPassthrough {
        client: stream,
        aux: node.aux.clone(),
        compute: node,
        _req: request_gauge,
        _conn: conn_gauge,
        _cancel: session,
    }))
}

/// Finish client connection initialization: confirm auth success, send params, etc.
#[tracing::instrument(skip_all)]
pub(crate) async fn prepare_client_connection<P>(
    node: &compute::PostgresConnection,
    session: &cancellation::Session<P>,
    stream: &mut PqStream<impl AsyncRead + AsyncWrite + Unpin>,
) -> Result<(), std::io::Error> {
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

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct NeonOptions(Vec<(SmolStr, SmolStr)>);

impl NeonOptions {
    pub(crate) fn parse_params(params: &StartupMessageParams) -> Self {
        params
            .options_raw()
            .map(Self::parse_from_iter)
            .unwrap_or_default()
    }
    pub(crate) fn parse_options_raw(options: &str) -> Self {
        Self::parse_from_iter(StartupMessageParams::parse_options_raw(options))
    }

    pub(crate) fn is_ephemeral(&self) -> bool {
        // Currently, neon endpoint options are all reserved for ephemeral endpoints.
        !self.0.is_empty()
    }

    fn parse_from_iter<'a>(options: impl Iterator<Item = &'a str>) -> Self {
        let mut options = options
            .filter_map(neon_option)
            .map(|(k, v)| (k.into(), v.into()))
            .collect_vec();
        options.sort();
        Self(options)
    }

    pub(crate) fn get_cache_key(&self, prefix: &str) -> EndpointCacheKey {
        // prefix + format!(" {k}:{v}")
        // kinda jank because SmolStr is immutable
        std::iter::once(prefix)
            .chain(self.0.iter().flat_map(|(k, v)| [" ", &**k, ":", &**v]))
            .collect::<SmolStr>()
            .into()
    }

    /// <https://swagger.io/docs/specification/serialization/> DeepObject format
    /// `paramName[prop1]=value1&paramName[prop2]=value2&...`
    pub(crate) fn to_deep_object(&self) -> Vec<(SmolStr, SmolStr)> {
        self.0
            .iter()
            .map(|(k, v)| (format_smolstr!("options[{}]", k), v.clone()))
            .collect()
    }
}

pub(crate) fn neon_option(bytes: &str) -> Option<(&str, &str)> {
    static RE: OnceCell<Regex> = OnceCell::new();
    let re = RE.get_or_init(|| Regex::new(r"^neon_(\w+):(.+)").unwrap());

    let cap = re.captures(bytes)?;
    let (_, [k, v]) = cap.extract();
    Some((k, v))
}
