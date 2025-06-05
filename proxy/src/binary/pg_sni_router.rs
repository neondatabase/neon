//! A stand-alone program that routes connections, e.g. from
//! `aaa--bbb--1234.external.domain` to `aaa.bbb.internal.domain:1234`.
//!
//! This allows connecting to pods/services running in the same Kubernetes cluster from
//! the outside. Similar to an ingress controller for HTTPS.

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, anyhow, bail, ensure};
use clap::Arg;
use futures::future::Either;
use futures::{FutureExt, TryFutureExt};
use itertools::Itertools;
use rustls::crypto::ring;
use rustls::pki_types::{DnsName, PrivateKeyDer};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio_rustls::TlsConnector;
use tokio_rustls::server::TlsStream;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, error, info};
use utils::project_git_version;
use utils::sentry_init::init_sentry;

use crate::context::RequestContext;
use crate::metrics::{Metrics, ThreadPoolMetrics};
use crate::pqproto::FeStartupPacket;
use crate::protocol2::ConnectionInfo;
use crate::proxy::{ErrorSource, TlsRequired, copy_bidirectional_client_compute};
use crate::stream::{PqStream, Stream};
use crate::util::run_until_cancelled;

project_git_version!(GIT_VERSION);

fn cli() -> clap::Command {
    clap::Command::new("Neon proxy/router")
        .version(GIT_VERSION)
        .arg(
            Arg::new("listen")
                .short('l')
                .long("listen")
                .help("listen for incoming client connections on ip:port")
                .default_value("127.0.0.1:4432"),
        )
        .arg(
            Arg::new("listen-tls")
                .long("listen-tls")
                .help("listen for incoming client connections on ip:port, requiring TLS to compute")
                .default_value("127.0.0.1:4433"),
        )
        .arg(
            Arg::new("tls-key")
                .short('k')
                .long("tls-key")
                .help("path to TLS key for client postgres connections")
                .required(true),
        )
        .arg(
            Arg::new("tls-cert")
                .short('c')
                .long("tls-cert")
                .help("path to TLS cert for client postgres connections")
                .required(true),
        )
        .arg(
            Arg::new("dest")
                .short('d')
                .long("destination")
                .help("append this domain zone to the SNI hostname to get the destination address")
                .required(true),
        )
}

pub async fn run() -> anyhow::Result<()> {
    let _logging_guard = crate::logging::init().await?;
    let _panic_hook_guard = utils::logging::replace_panic_hook_with_tracing_panic_hook();
    let _sentry_guard = init_sentry(Some(GIT_VERSION.into()), &[]);

    Metrics::install(Arc::new(ThreadPoolMetrics::new(0)));

    let args = cli().get_matches();
    let destination: String = args
        .get_one::<String>("dest")
        .expect("string argument defined")
        .parse()?;

    // Configure TLS
    let tls_config = match (
        args.get_one::<String>("tls-key"),
        args.get_one::<String>("tls-cert"),
    ) {
        (Some(key_path), Some(cert_path)) => parse_tls(key_path.as_ref(), cert_path.as_ref())?,
        _ => bail!("tls-key and tls-cert must be specified"),
    };

    let compute_tls_config =
        Arc::new(crate::tls::client_config::compute_client_config_with_root_certs()?);

    // Start listening for incoming client connections
    let proxy_address: SocketAddr = args
        .get_one::<String>("listen")
        .expect("listen argument defined")
        .parse()?;
    let proxy_address_compute_tls: SocketAddr = args
        .get_one::<String>("listen-tls")
        .expect("listen-tls argument defined")
        .parse()?;

    info!("Starting sni router on {proxy_address}");
    info!("Starting sni router on {proxy_address_compute_tls}");
    let proxy_listener = TcpListener::bind(proxy_address).await?;
    let proxy_listener_compute_tls = TcpListener::bind(proxy_address_compute_tls).await?;

    let cancellation_token = CancellationToken::new();
    let dest = Arc::new(destination);

    let main = tokio::spawn(task_main(
        dest.clone(),
        tls_config.clone(),
        None,
        proxy_listener,
        cancellation_token.clone(),
    ))
    .map(crate::error::flatten_err);

    let main_tls = tokio::spawn(task_main(
        dest,
        tls_config,
        Some(compute_tls_config),
        proxy_listener_compute_tls,
        cancellation_token.clone(),
    ))
    .map(crate::error::flatten_err);
    let signals_task = tokio::spawn(crate::signals::handle(cancellation_token, || {}));

    // the signal task cant ever succeed.
    // the main task can error, or can succeed on cancellation.
    // we want to immediately exit on either of these cases
    let main = futures::future::try_join(main, main_tls);
    let signal = match futures::future::select(signals_task, main).await {
        Either::Left((res, _)) => crate::error::flatten_err(res)?,
        Either::Right((res, _)) => {
            res?;
            return Ok(());
        }
    };

    // maintenance tasks return `Infallible` success values, this is an impossible value
    // so this match statically ensures that there are no possibilities for that value
    match signal {}
}

pub(super) fn parse_tls(
    key_path: &Path,
    cert_path: &Path,
) -> anyhow::Result<Arc<rustls::ServerConfig>> {
    let key = {
        let key_bytes = std::fs::read(key_path).context("TLS key file")?;

        let mut keys = rustls_pemfile::pkcs8_private_keys(&mut &key_bytes[..]).collect_vec();

        ensure!(keys.len() == 1, "keys.len() = {} (should be 1)", keys.len());
        PrivateKeyDer::Pkcs8(
            keys.pop()
                .expect("keys should not be empty")
                .context(format!(
                    "Failed to read TLS keys at '{}'",
                    key_path.display()
                ))?,
        )
    };

    let cert_chain_bytes = std::fs::read(cert_path).context(format!(
        "Failed to read TLS cert file at '{}.'",
        cert_path.display()
    ))?;

    let cert_chain: Vec<_> = {
        rustls_pemfile::certs(&mut &cert_chain_bytes[..])
            .try_collect()
            .with_context(|| {
                format!(
                    "Failed to read TLS certificate chain from bytes from file at '{}'.",
                    cert_path.display()
                )
            })?
    };

    let tls_config =
        rustls::ServerConfig::builder_with_provider(Arc::new(ring::default_provider()))
            .with_protocol_versions(&[&rustls::version::TLS13, &rustls::version::TLS12])
            .context("ring should support TLS1.2 and TLS1.3")?
            .with_no_client_auth()
            .with_single_cert(cert_chain, key)?
            .into();

    Ok(tls_config)
}

pub(super) async fn task_main(
    dest_suffix: Arc<String>,
    tls_config: Arc<rustls::ServerConfig>,
    compute_tls_config: Option<Arc<rustls::ClientConfig>>,
    listener: tokio::net::TcpListener,
    cancellation_token: CancellationToken,
) -> anyhow::Result<()> {
    // When set for the server socket, the keepalive setting
    // will be inherited by all accepted client sockets.
    socket2::SockRef::from(&listener).set_keepalive(true)?;

    let connections = tokio_util::task::task_tracker::TaskTracker::new();

    while let Some(accept_result) =
        run_until_cancelled(listener.accept(), &cancellation_token).await
    {
        let (socket, peer_addr) = accept_result?;

        let session_id = uuid::Uuid::new_v4();
        let tls_config = Arc::clone(&tls_config);
        let dest_suffix = Arc::clone(&dest_suffix);
        let compute_tls_config = compute_tls_config.clone();

        connections.spawn(
            async move {
                socket
                    .set_nodelay(true)
                    .context("failed to set socket option")?;

                info!(%peer_addr, "serving");
                let ctx = RequestContext::new(
                    session_id,
                    ConnectionInfo {
                        addr: peer_addr,
                        extra: None,
                    },
                    crate::metrics::Protocol::SniRouter,
                    "sni",
                );
                handle_client(ctx, dest_suffix, tls_config, compute_tls_config, socket).await
            }
            .unwrap_or_else(|e| {
                // Acknowledge that the task has finished with an error.
                error!("per-client task finished with an error: {e:#}");
            })
            .instrument(tracing::info_span!("handle_client", ?session_id)),
        );
    }

    connections.close();
    drop(listener);

    connections.wait().await;

    info!("all client connections have finished");
    Ok(())
}

async fn ssl_handshake<S: AsyncRead + AsyncWrite + Unpin>(
    ctx: &RequestContext,
    raw_stream: S,
    tls_config: Arc<rustls::ServerConfig>,
) -> anyhow::Result<TlsStream<S>> {
    let (mut stream, msg) = PqStream::parse_startup(Stream::from_raw(raw_stream)).await?;
    match msg {
        FeStartupPacket::SslRequest { direct: None } => {
            let raw = stream.accept_tls().await?;

            Ok(raw
                .upgrade(tls_config, !ctx.has_private_peer_addr())
                .await?)
        }
        unexpected => {
            info!(
                ?unexpected,
                "unexpected startup packet, rejecting connection"
            );
            Err(stream.throw_error(TlsRequired, None).await)?
        }
    }
}

async fn handle_client(
    ctx: RequestContext,
    dest_suffix: Arc<String>,
    tls_config: Arc<rustls::ServerConfig>,
    compute_tls_config: Option<Arc<rustls::ClientConfig>>,
    stream: impl AsyncRead + AsyncWrite + Unpin,
) -> anyhow::Result<()> {
    let mut tls_stream = ssl_handshake(&ctx, stream, tls_config).await?;

    // Cut off first part of the SNI domain
    // We receive required destination details in the format of
    //   `{k8s_service_name}--{k8s_namespace}--{port}.non-sni-domain`
    let sni = tls_stream
        .get_ref()
        .1
        .server_name()
        .ok_or(anyhow!("SNI missing"))?;
    let dest: Vec<&str> = sni
        .split_once('.')
        .context("invalid SNI")?
        .0
        .splitn(3, "--")
        .collect();
    let port = dest[2].parse::<u16>().context("invalid port")?;
    let destination = format!("{}.{}.{}:{}", dest[0], dest[1], dest_suffix, port);

    info!("destination: {}", destination);

    let mut client = tokio::net::TcpStream::connect(&destination).await?;

    let client = if let Some(compute_tls_config) = compute_tls_config {
        info!("upgrading TLS");

        // send SslRequest
        client
            .write_all(b"\x00\x00\x00\x08\x04\xd2\x16\x2f")
            .await?;

        // wait for S/N respons
        let mut resp = b'N';
        client.read_exact(std::slice::from_mut(&mut resp)).await?;

        // error if not S
        ensure!(resp == b'S', "compute refused TLS");

        // upgrade to TLS.
        let domain = DnsName::try_from(destination)?;
        let domain = rustls::pki_types::ServerName::DnsName(domain);
        let client = TlsConnector::from(compute_tls_config)
            .connect(domain, client)
            .await?;
        Connection::Tls(client)
    } else {
        Connection::Raw(client)
    };

    // doesn't yet matter as pg-sni-router doesn't report analytics logs
    ctx.set_success();
    ctx.log_connect();

    // Starting from here we only proxy the client's traffic.
    info!("performing the proxy pass...");

    let res = match client {
        Connection::Raw(mut c) => copy_bidirectional_client_compute(&mut tls_stream, &mut c).await,
        Connection::Tls(mut c) => copy_bidirectional_client_compute(&mut tls_stream, &mut c).await,
    };

    match res {
        Ok(_) => Ok(()),
        Err(ErrorSource::Client(err)) => Err(err).context("client"),
        Err(ErrorSource::Compute(err)) => Err(err).context("compute"),
    }
}

#[allow(clippy::large_enum_variant)]
enum Connection {
    Raw(tokio::net::TcpStream),
    Tls(tokio_rustls::client::TlsStream<tokio::net::TcpStream>),
}
