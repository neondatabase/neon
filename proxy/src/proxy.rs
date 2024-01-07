#[cfg(test)]
mod tests;

pub mod connect_compute;
pub mod handshake;
pub mod pass;
pub mod retry;
pub mod wake_compute;

use crate::{
    cancellation::CancelMap,
    config::{ProxyConfig, TlsConfig},
    context::RequestMonitoring,
    metrics::{NUM_CLIENT_CONNECTION_GAUGE, NUM_CONNECTION_REQUESTS_GAUGE},
    protocol2::WithClientIp,
    proxy::handshake::NeedsHandshake,
    rate_limiter::EndpointRateLimiter,
    state_machine::{DynStage, StageResult},
    stream::Stream,
};
use anyhow::Context;
use itertools::Itertools;
use once_cell::sync::OnceCell;
use pq_proto::StartupMessageParams;
use regex::Regex;
use smol_str::SmolStr;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, info_span, Instrument};

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

        let root_span = info_span!(
            "handle_client",
            ?session_id,
            peer_addr = tracing::field::Empty,
            ep = tracing::field::Empty,
        );
        let root_span2 = root_span.clone();

        connections.spawn(
            async move {
                info!("accepted postgres client connection");

                let mut socket = WithClientIp::new(socket);
                let mut peer_addr = peer_addr.ip();
                match socket.wait_for_addr().await {
                    Err(e) => {
                        error!("IO error: {e:#}");
                        return;
                    }
                    Ok(Some(addr)) => {
                        peer_addr = addr.ip();
                        root_span2.record("peer_addr", &tracing::field::display(addr));
                    }
                    Ok(None) if config.require_client_ip => {
                        error!("missing required client IP");
                        return;
                    }
                    Ok(None) => {}
                };

                let ctx = RequestMonitoring::new(
                    session_id,
                    peer_addr,
                    "tcp",
                    &config.region,
                    root_span2,
                );

                if let Err(e) = socket
                    .inner
                    .set_nodelay(true)
                    .context("failed to set socket option")
                {
                    error!("could not set nodelay: {e:#}");
                    return;
                }

                handle_client(
                    config,
                    ctx,
                    cancel_map,
                    socket,
                    ClientMode::Tcp,
                    endpoint_rate_limiter,
                )
                .await;
            }
            .instrument(root_span),
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
    pub fn allow_cleartext(&self) -> bool {
        match self {
            ClientMode::Tcp => false,
            ClientMode::Websockets { .. } => true,
        }
    }

    pub fn allow_self_signed_compute(&self, config: &ProxyConfig) -> bool {
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

pub async fn handle_client<S: AsyncRead + AsyncWrite + Unpin + 'static + Send>(
    config: &'static ProxyConfig,
    ctx: RequestMonitoring,
    cancel_map: Arc<CancelMap>,
    stream: S,
    mode: ClientMode,
    endpoint_rate_limiter: Arc<EndpointRateLimiter>,
) {
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

    let mut stage = Box::new(NeedsHandshake {
        stream,
        config,
        cancel_map,
        mode,
        endpoint_rate_limiter,
        ctx,
    }) as DynStage;

    while let StageResult::Run(handle) = stage.run() {
        stage = match handle.await.expect("tasks should not panic") {
            Ok(s) => s,
            Err(e) => {
                e.finish().await;
                break;
            }
        }
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
