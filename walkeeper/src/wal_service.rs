//!
//!   WAL service listens for client connections and
//!   receive WAL from wal_proposer and send it to WAL receivers
//!
use anyhow::Result;
use regex::Regex;
use std::net::{TcpListener, TcpStream};
use std::thread;
use tracing::*;

use crate::callmemaybe::CallmeEvent;
use crate::send_wal::SendWalHandler;
use crate::SafeKeeperConf;
use tokio::sync::mpsc::UnboundedSender;
use zenith_utils::postgres_backend::{AuthType, PostgresBackend};

/// Accept incoming TCP connections and spawn them into a background thread.
pub fn thread_main(
    conf: SafeKeeperConf,
    listener: TcpListener,
    tx: UnboundedSender<CallmeEvent>,
) -> Result<()> {
    loop {
        match listener.accept() {
            Ok((socket, peer_addr)) => {
                debug!("accepted connection from {}", peer_addr);
                let conf = conf.clone();

                let tx_clone = tx.clone();
                let _ = thread::Builder::new()
                    .name("WAL service thread".into())
                    .spawn(move || {
                        if let Err(err) = handle_socket(socket, conf, tx_clone) {
                            error!("connection handler exited: {}", err);
                        }
                    })
                    .unwrap();
            }
            Err(e) => error!("Failed to accept connection: {}", e),
        }
    }
}

// Get unique thread id (Rust internal), with ThreadId removed for shorter printing
fn get_tid() -> u64 {
    let tids = format!("{:?}", thread::current().id());
    let r = Regex::new(r"ThreadId\((\d+)\)").unwrap();
    let caps = r.captures(&tids).unwrap();
    caps.get(1).unwrap().as_str().parse().unwrap()
}

/// This is run by `thread_main` above, inside a background thread.
///
fn handle_socket(
    socket: TcpStream,
    conf: SafeKeeperConf,
    tx: UnboundedSender<CallmeEvent>,
) -> Result<()> {
    let _enter = info_span!("", tid = ?get_tid()).entered();

    socket.set_nodelay(true)?;

    let mut conn_handler = SendWalHandler::new(conf, tx);
    let pgbackend = PostgresBackend::new(socket, AuthType::Trust, None, false)?;
    // libpq replication protocol between safekeeper and replicas/pagers
    pgbackend.run(&mut conn_handler)?;

    Ok(())
}
