use std::{net::SocketAddr, sync::Arc};
use tokio::{io::AsyncWriteExt, net::TcpListener};

use anyhow::Context;
use clap::{self, Arg};
use futures::TryFutureExt;
use proxy::{
    auth::{self, AuthFlow},
    cancellation::CancelMap,
    compute::ConnCfg,
    console::messages::MetricsAuxInfo,
};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_postgres::config::SslMode;
use tokio_util::sync::CancellationToken;
use utils::project_git_version;

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
            Arg::new("dest-host")
                .long("dest-host")
                .help("destination hosts")
                .required(true),
        )
        .arg(
            Arg::new("dest-port")
                .long("dest-port")
                .help("destination port")
                .default_value("5432"),
        )
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _logging_guard = proxy::logging::init().await?;
    let _panic_hook_guard = utils::logging::replace_panic_hook_with_tracing_panic_hook();

    let args = cli().get_matches();

    let dest_host: String = args.get_one::<String>("dest-host").unwrap().parse()?;
    let dest_port: u16 = args.get_one::<String>("dest-port").unwrap().parse()?;
    let listen_address: SocketAddr = args.get_one::<String>("listen").unwrap().parse()?;

    // Start listening for incoming client connections
    info!("Starting proxy on {listen_address}");
    let proxy_listener = TcpListener::bind(listen_address).await?;

    let cancellation_token = CancellationToken::new();

    let main = proxy::flatten_err(tokio::spawn(task_main(
        Arc::new(dest_host),
        dest_port,
        proxy_listener,
        cancellation_token.clone(),
    )));
    let signals_task = proxy::flatten_err(tokio::spawn(proxy::handle_signals(cancellation_token)));

    tokio::select! {
        res = main => { res?; },
        res = signals_task => { res?; },
    }

    Ok(())
}

async fn task_main(
    dest_host: Arc<String>,
    dest_port: u16,
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

                let cancel_map = Arc::clone(&cancel_map);
                let dest_host = Arc::clone(&dest_host);

                connections.spawn(
                    async move {
                        info!("spawned a task for {peer_addr}");

                        socket
                            .set_nodelay(true)
                            .context("failed to set socket option")?;

                        handle_client(dest_host, dest_port, &cancel_map, socket).await
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

async fn handle_client(
    dest_host: Arc<String>,
    dest_port: u16,
    cancel_map: &CancelMap,
    stream: impl AsyncRead + AsyncWrite + Unpin,
) -> anyhow::Result<()> {
    let do_handshake = proxy::proxy::handshake(stream, None, cancel_map);
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
    conn_cfg.host(dest_host.as_str());
    conn_cfg.port(dest_port);
    conn_cfg.ssl_mode(SslMode::Require);

    info!("destination: {:?}:{}", dest_host, dest_port);

    let mut conn = conn_cfg
        .connect(false)
        .or_else(|e| stream.throw_error(e))
        .await?;

    cancel_map
        .with_session(|session| async {
            proxy::proxy::prepare_client_connection(&conn, false, session, &mut stream).await?;
            let (stream, read_buf) = stream.into_inner();
            conn.stream.write_all(&read_buf).await?;
            let metrics_aux: MetricsAuxInfo = Default::default();
            proxy::proxy::proxy_pass(stream, conn.stream, &metrics_aux).await
        })
        .await
}
