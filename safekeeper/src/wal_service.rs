//!
//!   WAL service listens for client connections and
//!   receive WAL from wal_proposer and send it to WAL receivers
//!
use anyhow::{Context, Result};
use postgres_backend::QueryError;
use std::{future, thread, time::Duration};
use tokio::net::TcpStream;
use tokio_io_timeout::TimeoutReader;
use tracing::*;
use utils::measured_stream::MeasuredStream;

use crate::handler::SafekeeperPostgresHandler;
use crate::metrics::TrafficMetrics;
use crate::SafeKeeperConf;
use postgres_backend::{AuthType, PostgresBackend};

/// Accept incoming TCP connections and spawn them into a background thread.
pub fn thread_main(conf: SafeKeeperConf, pg_listener: std::net::TcpListener) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("create runtime")
        // todo catch error in main thread
        .expect("failed to create runtime");

    runtime
        .block_on(async move {
            // Tokio's from_std won't do this for us, per its comment.
            pg_listener.set_nonblocking(true)?;
            let listener = tokio::net::TcpListener::from_std(pg_listener)?;
            let mut connection_count: ConnectionCount = 0;

            loop {
                match listener.accept().await {
                    Ok((socket, peer_addr)) => {
                        debug!("accepted connection from {}", peer_addr);
                        let conf = conf.clone();
                        let conn_id = issue_connection_id(&mut connection_count);

                        let _ = thread::Builder::new()
                            .name("WAL service thread".into())
                            .spawn(move || {
                                if let Err(err) = handle_socket(socket, conf, conn_id) {
                                    error!("connection handler exited: {}", err);
                                }
                            })
                            .unwrap();
                    }
                    Err(e) => error!("Failed to accept connection: {}", e),
                }
            }
            #[allow(unreachable_code)] // hint compiler the closure return type
            Ok::<(), anyhow::Error>(())
        })
        .expect("listener failed")
}

/// This is run by `thread_main` above, inside a background thread.
///
fn handle_socket(
    socket: TcpStream,
    conf: SafeKeeperConf,
    conn_id: ConnectionId,
) -> Result<(), QueryError> {
    let _enter = info_span!("", cid = %conn_id).entered();

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    socket.set_nodelay(true)?;
    let peer_addr = socket.peer_addr()?;

    // TimeoutReader wants async runtime during creation.
    runtime.block_on(async move {
        // Set timeout on reading from the socket. It prevents hanged up connection
        // if client suddenly disappears. Note that TCP_KEEPALIVE is not enabled by
        // default, and tokio doesn't provide ability to set it out of the box.
        let mut socket = TimeoutReader::new(socket);
        let wal_service_timeout = Duration::from_secs(60 * 10);
        socket.set_timeout(Some(wal_service_timeout));
        // pin! is here because TimeoutReader (due to storing sleep future inside)
        // is not Unpin, and all pgbackend/framed/tokio dependencies require stream
        // to be Unpin. Which is reasonable, as indeed something like TimeoutReader
        // shouldn't be moved.
        tokio::pin!(socket);

        let traffic_metrics = TrafficMetrics::new();
        if let Some(current_az) = conf.availability_zone.as_deref() {
            traffic_metrics.set_sk_az(current_az);
        }

        let socket = MeasuredStream::new(
            socket,
            |cnt| {
                traffic_metrics.observe_read(cnt);
            },
            |cnt| {
                traffic_metrics.observe_write(cnt);
            },
        );

        let auth_type = match conf.auth {
            None => AuthType::Trust,
            Some(_) => AuthType::NeonJWT,
        };
        let mut conn_handler =
            SafekeeperPostgresHandler::new(conf, conn_id, Some(traffic_metrics.clone()));
        let pgbackend = PostgresBackend::new_from_io(socket, peer_addr, auth_type, None)?;
        // libpq protocol between safekeeper and walproposer / pageserver
        // We don't use shutdown.
        pgbackend
            .run(&mut conn_handler, future::pending::<()>)
            .await
    })
}

/// Unique WAL service connection ids are logged in spans for observability.
pub type ConnectionId = u32;
pub type ConnectionCount = u32;

pub fn issue_connection_id(count: &mut ConnectionCount) -> ConnectionId {
    *count = count.wrapping_add(1);
    *count
}
