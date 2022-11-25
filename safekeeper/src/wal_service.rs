//!
//!   WAL service listens for client connections and
//!   receive WAL from wal_proposer and send it to WAL receivers
//!
use anyhow::Result;
use regex::Regex;
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;
use tracing::*;
use utils::auth::JwtAuth;

use crate::handler::SafekeeperPostgresHandler;
use crate::SafeKeeperConf;
use utils::postgres_backend::{AuthType, PostgresBackend};

/// Accept incoming TCP connections and spawn them into a background thread.
pub fn thread_main(
    conf: SafeKeeperConf,
    listener: TcpListener,
    auth: Option<Arc<JwtAuth>>,
) -> Result<()> {
    loop {
        match listener.accept() {
            Ok((socket, peer_addr)) => {
                debug!("accepted connection from {}", peer_addr);
                let conf = conf.clone();

                let auth = auth.clone();
                let _ = thread::Builder::new()
                    .name("WAL service thread".into())
                    .spawn(move || {
                        if let Err(err) = handle_socket(socket, conf, auth) {
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
    auth: Option<Arc<JwtAuth>>,
) -> Result<()> {
    let _enter = info_span!("", tid = ?get_tid()).entered();

    socket.set_nodelay(true)?;

    let mut conn_handler = SafekeeperPostgresHandler::new(conf, auth.clone());
    let pgbackend = PostgresBackend::new(
        socket,
        match auth {
            None => AuthType::Trust,
            Some(_) => AuthType::NeonJWT,
        },
        None,
        false,
    )?;
    // libpq replication protocol between safekeeper and replicas/pagers
    pgbackend.run(&mut conn_handler)?;

    Ok(())
}
