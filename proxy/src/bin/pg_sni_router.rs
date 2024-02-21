/// A stand-alone program that routes connections, e.g. from
/// `aaa--bbb--1234.external.domain` to `aaa.bbb.internal.domain:1234`.
///
/// This allows connecting to pods/services running in the same Kubernetes cluster from
/// the outside. Similar to an ingress controller for HTTPS.
use std::{net::SocketAddr, sync::Arc};

use futures::future::Either;
use itertools::Itertools;
use proxy::config::TlsServerEndPoint;
use proxy::context::RequestMonitoring;
use proxy::proxy::run_until_cancelled;
use tokio::net::TcpListener;

use anyhow::{anyhow, bail, ensure, Context};
use clap::{self, Arg};
use futures::TryFutureExt;
use proxy::console::messages::MetricsAuxInfo;
use proxy::stream::{PqStream, Stream};

use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::sync::CancellationToken;
use utils::{project_git_version, sentry_init::init_sentry};

use tracing::{error, info, Instrument};

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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _logging_guard = proxy::logging::init().await?;
    let _panic_hook_guard = utils::logging::replace_panic_hook_with_tracing_panic_hook();
    let _sentry_guard = init_sentry(Some(GIT_VERSION.into()), &[]);

    let args = cli().get_matches();
    let destination: String = args.get_one::<String>("dest").unwrap().parse()?;

    // Configure TLS
    let (tls_config, tls_server_end_point): (Arc<rustls::ServerConfig>, TlsServerEndPoint) = match (
        args.get_one::<String>("tls-key"),
        args.get_one::<String>("tls-cert"),
    ) {
        (Some(key_path), Some(cert_path)) => {
            let key = {
                let key_bytes = std::fs::read(key_path).context("TLS key file")?;
                let mut keys = rustls_pemfile::pkcs8_private_keys(&mut &key_bytes[..])
                    .context(format!("Failed to read TLS keys at '{key_path}'"))?;

                ensure!(keys.len() == 1, "keys.len() = {} (should be 1)", keys.len());
                keys.pop().map(rustls::PrivateKey).unwrap()
            };

            let cert_chain_bytes = std::fs::read(cert_path)
                .context(format!("Failed to read TLS cert file at '{cert_path}.'"))?;

            let cert_chain = {
                rustls_pemfile::certs(&mut &cert_chain_bytes[..])
                    .context(format!(
                        "Failed to read TLS certificate chain from bytes from file at '{cert_path}'."
                    ))?
                    .into_iter()
                    .map(rustls::Certificate)
                    .collect_vec()
            };

            // needed for channel bindings
            let first_cert = cert_chain.first().context("missing certificate")?;
            let tls_server_end_point = TlsServerEndPoint::new(first_cert)?;

            let tls_config = rustls::ServerConfig::builder()
                .with_safe_default_cipher_suites()
                .with_safe_default_kx_groups()
                .with_protocol_versions(&[&rustls::version::TLS13, &rustls::version::TLS12])?
                .with_no_client_auth()
                .with_single_cert(cert_chain, key)?
                .into();

            (tls_config, tls_server_end_point)
        }
        _ => bail!("tls-key and tls-cert must be specified"),
    };

    // Start listening for incoming client connections
    let proxy_address: SocketAddr = args.get_one::<String>("listen").unwrap().parse()?;
    info!("Starting sni router on {proxy_address}");
    let proxy_listener = TcpListener::bind(proxy_address).await?;

    let cancellation_token = CancellationToken::new();

    let main = tokio::spawn(task_main(
        Arc::new(destination),
        tls_config,
        tls_server_end_point,
        proxy_listener,
        cancellation_token.clone(),
    ));
    let signals_task = tokio::spawn(proxy::handle_signals(cancellation_token));

    // the signal task cant ever succeed.
    // the main task can error, or can succeed on cancellation.
    // we want to immediately exit on either of these cases
    let signal = match futures::future::select(signals_task, main).await {
        Either::Left((res, _)) => proxy::flatten_err(res)?,
        Either::Right((res, _)) => return proxy::flatten_err(res),
    };

    // maintenance tasks return `Infallible` success values, this is an impossible value
    // so this match statically ensures that there are no possibilities for that value
    match signal {}
}

async fn task_main(
    dest_suffix: Arc<String>,
    tls_config: Arc<rustls::ServerConfig>,
    tls_server_end_point: TlsServerEndPoint,
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

        connections.spawn(
            async move {
                socket
                    .set_nodelay(true)
                    .context("failed to set socket option")?;

                info!(%peer_addr, "serving");
                let mut ctx =
                    RequestMonitoring::new(session_id, peer_addr.ip(), "sni_router", "sni");
                handle_client(
                    &mut ctx,
                    dest_suffix,
                    tls_config,
                    tls_server_end_point,
                    socket,
                )
                .await
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

const ERR_INSECURE_CONNECTION: &str = "connection is insecure (try using `sslmode=require`)";

async fn ssl_handshake<S: AsyncRead + AsyncWrite + Unpin>(
    raw_stream: S,
    tls_config: Arc<rustls::ServerConfig>,
    tls_server_end_point: TlsServerEndPoint,
) -> anyhow::Result<Stream<S>> {
    let mut stream = PqStream::new(Stream::from_raw(raw_stream));

    let msg = stream.read_startup_packet().await?;
    use pq_proto::FeStartupPacket::*;

    match msg {
        SslRequest => {
            stream
                .write_message(&pq_proto::BeMessage::EncryptionResponse(true))
                .await?;
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

            Ok(Stream::Tls {
                tls: Box::new(raw.upgrade(tls_config).await?),
                tls_server_end_point,
            })
        }
        unexpected => {
            info!(
                ?unexpected,
                "unexpected startup packet, rejecting connection"
            );
            stream
                .throw_error_str(ERR_INSECURE_CONNECTION, proxy::error::ErrorKind::User)
                .await?
        }
    }
}

async fn handle_client(
    ctx: &mut RequestMonitoring,
    dest_suffix: Arc<String>,
    tls_config: Arc<rustls::ServerConfig>,
    tls_server_end_point: TlsServerEndPoint,
    stream: impl AsyncRead + AsyncWrite + Unpin,
) -> anyhow::Result<()> {
    let tls_stream = ssl_handshake(stream, tls_config, tls_server_end_point).await?;

    // Cut off first part of the SNI domain
    // We receive required destination details in the format of
    //   `{k8s_service_name}--{k8s_namespace}--{port}.non-sni-domain`
    let sni = tls_stream.sni_hostname().ok_or(anyhow!("SNI missing"))?;
    let dest: Vec<&str> = sni
        .split_once('.')
        .context("invalid SNI")?
        .0
        .splitn(3, "--")
        .collect();
    let port = dest[2].parse::<u16>().context("invalid port")?;
    let destination = format!("{}.{}.{}:{}", dest[0], dest[1], dest_suffix, port);

    info!("destination: {}", destination);

    let client = tokio::net::TcpStream::connect(destination).await?;

    let metrics_aux: MetricsAuxInfo = Default::default();

    // doesn't yet matter as pg-sni-router doesn't report analytics logs
    ctx.set_success();
    ctx.log();

    proxy::proxy::passthrough::proxy_pass(tls_stream, client, metrics_aux).await
}
