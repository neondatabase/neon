//!
//!   WAL service listens for client connections and
//!   receive WAL from wal_proposer and send it to WAL receivers
//!
use anyhow::Result;
use log::*;
use std::io::Read;
use std::net::{TcpListener, TcpStream};
use std::thread;

use crate::receive_wal::ReceiveWalConn;
use crate::send_wal::SendWalHandler;
use crate::WalAcceptorConf;
use zenith_utils::postgres_backend::PostgresBackend;

/// Accept incoming TCP connections and spawn them into a background thread.
pub fn thread_main(conf: WalAcceptorConf) -> Result<()> {
    info!("Starting wal acceptor on {}", conf.listen_addr);
    let listener = TcpListener::bind(conf.listen_addr).map_err(|e| {
        error!("failed to bind to address {}: {}", conf.listen_addr, e);
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

/// This is run by main_loop, inside a background thread.
///
fn handle_socket(mut socket: TcpStream, conf: WalAcceptorConf) -> Result<()> {
    socket.set_nodelay(true)?;

    // Peek at the incoming data to see what protocol is being sent.
    let peeked = peek_u32(&mut socket)?;
    if peeked == 0 {
        // Consume the 4 bytes we peeked at. This protocol begins after them.
        socket.read_exact(&mut [0u8; 4])?;
        ReceiveWalConn::new(socket, conf)?.run()?; // internal protocol between wal_proposer and wal_acceptor
    } else {
        let mut conn_handler = SendWalHandler::new(conf);
        let mut pgbackend = PostgresBackend::new(socket)?;
        // libpq replication protocol between wal_acceptor and replicas/pagers
        pgbackend.run(&mut conn_handler)?;
    }
    Ok(())
}

/// Fetch the first 4 bytes from the network (big endian), without consuming them.
///
/// This is used to help determine what protocol the peer is using.
fn peek_u32(stream: &mut TcpStream) -> Result<u32> {
    let mut buf = [0u8; 4];
    loop {
        let num_bytes = stream.peek(&mut buf)?;
        if num_bytes == 4 {
            return Ok(u32::from_be_bytes(buf));
        }
    }
}
