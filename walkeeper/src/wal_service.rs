//!
//!   WAL service listens for client connections and
//!   receive WAL from wal_proposer and send it to WAL receivers
//!
use anyhow::Result;
use log::*;
use std::net::{TcpListener, TcpStream};
use std::thread;

use crate::send_wal::SendWalHandler;
use crate::WalAcceptorConf;
use zenith_utils::postgres_backend::{AuthType, PostgresBackend};

/// Accept incoming TCP connections and spawn them into a background thread.
pub fn thread_main(conf: WalAcceptorConf) -> Result<()> {
    info!("Starting wal acceptor on {}", conf.listen_pg_addr);
    let listener = TcpListener::bind(conf.listen_pg_addr.clone()).map_err(|e| {
        error!("failed to bind to address {}: {}", conf.listen_pg_addr, e);
        e
    })?;

    loop {
        match listener.accept() {
            Ok((socket, peer_addr)) => {
                debug!("accepted connection from {}", peer_addr);
                let conf = conf.clone();
                thread::spawn(move || {
                    if let Err(err) = handle_socket(socket, conf) {
                        error!("connection handler exited: {}", err);
                    }
                });
            }
            Err(e) => error!("Failed to accept connection: {}", e),
        }
    }
}

/// This is run by `thread_main` above, inside a background thread.
///
fn handle_socket(socket: TcpStream, conf: WalAcceptorConf) -> Result<()> {
    socket.set_nodelay(true)?;

    let mut conn_handler = SendWalHandler::new(conf);
    let pgbackend = PostgresBackend::new(socket, AuthType::Trust, None)?;
    // libpq replication protocol between wal_acceptor and replicas/pagers
    pgbackend.run(&mut conn_handler)?;

    Ok(())
}
