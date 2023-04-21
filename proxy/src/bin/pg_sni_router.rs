use std::{net::SocketAddr, sync::Arc};
use tokio::{net::TcpListener, io::AsyncWriteExt};

use anyhow::{bail, ensure, Context};
use clap::{self, Arg};
use futures::TryFutureExt;
use proxy::{cancellation::CancelMap, auth::{AuthFlow, self}, compute::ConnCfg, console::messages::MetricsAuxInfo};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::sync::CancellationToken;
use utils::{project_git_version, sentry_init::init_sentry};

use tracing::{error, info, warn};

project_git_version!(GIT_VERSION);

fn cli() -> clap::Command {
    clap::Command::new("Neon proxy/router")
        .disable_help_flag(true)
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
                .help("path to TLS key for client postgres connections"),
        )
        .arg(
            Arg::new("tls-cert")
                .short('c')
                .long("tls-cert")
                .help("path to TLS cert for client postgres connections"),
        )
        .arg(
            Arg::new("dest")
                .short('d')
                .long("destination")
                .help("append this domain zone to the SNI hostname to get the destination address"),
        )
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _logging_guard = proxy::logging::init().await?;
    let _panic_hook_guard = utils::logging::replace_panic_hook_with_tracing_panic_hook();
    let _sentry_guard = init_sentry(Some(GIT_VERSION.into()), &[]);

    let args = cli().get_matches();

    // Configure TLS
    let tls_config: Arc<rustls::ServerConfig> = match (
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
                    .collect()
            };

            rustls::ServerConfig::builder()
                .with_safe_default_cipher_suites()
                .with_safe_default_kx_groups()
                .with_protocol_versions(&[&rustls::version::TLS13, &rustls::version::TLS12])?
                .with_no_client_auth()
                .with_single_cert(cert_chain, key)?
                .into()
        }
        _ => bail!("tls-key and tls-cert must be specified"),
    };

    let destination: String = args.get_one::<String>("dest").unwrap().parse()?;

    // Start listening for incoming client connections
    let proxy_address: SocketAddr = args.get_one::<String>("listen").unwrap().parse()?;
    info!("Starting proxy on {proxy_address}");
    let proxy_listener = TcpListener::bind(proxy_address).await?;

    let cancellation_token = CancellationToken::new();
    let tasks = vec![
        tokio::spawn(proxy::handle_signals(cancellation_token.clone())),
        tokio::spawn(task_main(
            Arc::new(destination),
            tls_config,
            proxy_listener,
            cancellation_token.clone(),
        )),
    ];

    let _tasks = futures::future::try_join_all(tasks.into_iter().map(proxy::flatten_err)).await?;

    Ok(())
}

async fn task_main(
    dest_suffix: Arc<String>,
    tls_config: Arc<rustls::ServerConfig>,
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
                let tls_config = Arc::clone(&tls_config);
                let dest_suffix = Arc::clone(&dest_suffix);

                connections.spawn(
                    async move {
                        info!("spawned a task for {peer_addr}");

                        socket
                            .set_nodelay(true)
                            .context("failed to set socket option")?;

                        handle_client(dest_suffix, tls_config, &cancel_map, session_id, socket).await
                    }
                    .unwrap_or_else(|e| {
                        // Acknowledge that the task has finished with an error.
                        error!("per-client task finished with an error: {e:#}");
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

#[tracing::instrument(fields(session_id = ?session_id), skip_all)]
async fn handle_client(
    dest_suffix: Arc<String>,
    tls: Arc<rustls::ServerConfig>,
    cancel_map: &CancelMap,
    session_id: uuid::Uuid,
    stream: impl AsyncRead + AsyncWrite + Unpin,
) -> anyhow::Result<()> {
    let do_handshake = proxy::proxy::handshake(stream, Some(tls), cancel_map);
    let (mut stream, params) = match do_handshake.await? {
        Some(x) => x,
        None => return Ok(()), // it's a cancellation request
    };

    let password = AuthFlow::new(&mut stream)
        .begin(auth::CleartextPassword)
        .await?
        .authenticate()
        .await?;

    let mut conn_cfg = ConnCfg::new();
    conn_cfg.set_startup_params(&params);
    conn_cfg.password(password);

    // cut off first part of the sni domain
    let sni = stream.get_ref().sni_hostname().unwrap();
    let dest = sni
        .split_once('.').context("invalid sni")?.0
        .replace("--", ".");

    let destination = format!("{}.{}", dest, dest_suffix);

    info!("destination: {:?}", destination);

    conn_cfg.host(destination.as_str());

    let mut conn = conn_cfg.connect()
        .or_else(|e| stream.throw_error(e))
        .await?;

    cancel_map.with_session(|session| async {
        proxy::proxy::prepare_client_connection(&conn, false, session, &mut stream).await?;
        let (stream, read_buf) = stream.into_inner();
        conn.stream.write_all(&read_buf).await?;
        let metrics_aux: MetricsAuxInfo = Default::default();
        proxy::proxy::proxy_pass(stream, conn.stream, &metrics_aux).await
    })
    .await
}
