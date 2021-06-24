use std::{
    net::{TcpListener, TcpStream},
    thread,
};

use bytes::Bytes;
use zenith_utils::{
    postgres_backend::{self, PostgresBackend},
    pq_proto::{BeMessage, SINGLE_COL_ROWDESC},
};

use crate::ProxyConf;

///
/// Main proxy listener loop.
///
/// Listens for connections, and launches a new handler thread for each.
///
pub fn thread_main(conf: &'static ProxyConf, listener: TcpListener) -> anyhow::Result<()> {
    loop {
        let (socket, peer_addr) = listener.accept()?;
        println!("accepted connection from {}", peer_addr);
        socket.set_nodelay(true).unwrap();

        thread::spawn(move || {
            if let Err(err) = mgmt_conn_main(conf, socket) {
                println!("error: {}", err);
            }
        });
    }
}

pub fn mgmt_conn_main(conf: &'static ProxyConf, socket: TcpStream) -> anyhow::Result<()> {
    let mut conn_handler = MgmtHandler { conf };
    let mut pgbackend = PostgresBackend::new(socket, postgres_backend::AuthType::Trust)?;
    pgbackend.run(&mut conn_handler)
}

struct MgmtHandler {
    conf: &'static ProxyConf,
}

impl postgres_backend::Handler for MgmtHandler {
    fn process_query(
        &mut self,
        pgb: &mut PostgresBackend,
        query_string: Bytes,
    ) -> anyhow::Result<()> {
        println!("Got mgmt query: {:?}", query_string);

        pgb.write_message_noflush(&SINGLE_COL_ROWDESC)?
            .write_message_noflush(&BeMessage::DataRow(&[Some(b"mgmt_ok")]))?
            .write_message_noflush(&BeMessage::CommandComplete(b"SELECT 1"))?;

        pgb.flush()?;
        Ok(())
    }
}
