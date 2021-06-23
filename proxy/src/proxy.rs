use crate::ProxyConf;
use anyhow::bail;
use bytes::Bytes;
use std::{
    net::{TcpListener, TcpStream},
    thread,
};
use zenith_utils::postgres_backend::PostgresBackend;
use zenith_utils::{
    postgres_backend,
    pq_proto::{BeMessage, HELLO_WORLD_ROW, SINGLE_COL_ROWDESC},
};

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
            if let Err(err) = proxy_conn_main(conf, socket) {
                println!("error: {}", err);
            }
        });
    }
}

pub fn proxy_conn_main(conf: &'static ProxyConf, socket: TcpStream) -> anyhow::Result<()> {
    let mut conn_handler = ProxyHandler { conf };
    let mut pgbackend = PostgresBackend::new(socket, postgres_backend::AuthType::MD5)?;
    pgbackend.run(&mut conn_handler)
}

struct ProxyHandler {
    conf: &'static ProxyConf,
}

// impl ProxyHandler {
// }

impl postgres_backend::Handler for ProxyHandler {
    fn process_query(
        &mut self,
        pgb: &mut PostgresBackend,
        query_string: Bytes,
    ) -> anyhow::Result<()> {
        println!("Got query: {:?}", query_string);
        pgb.write_message_noflush(&SINGLE_COL_ROWDESC)?
            .write_message_noflush(&HELLO_WORLD_ROW)?
            .write_message_noflush(&BeMessage::CommandComplete(b"SELECT 1"))?;
        pgb.flush()?;
        Ok(())
    }

    fn startup(
        &mut self,
        _pgb: &mut PostgresBackend,
        sm: &zenith_utils::pq_proto::FeStartupMessage,
    ) -> anyhow::Result<()> {
        println!("Got startup: {:?}", sm);
        Ok(())
    }

    fn check_auth_md5(
        &mut self,
        pgb: &mut PostgresBackend,
        md5_response: &[u8],
    ) -> anyhow::Result<()> {
        let user = "stask";
        let pass = "mypassword";
        let stored_hash = format!(
            "{:x}",
            md5::compute([pass.as_bytes(), user.as_bytes()].concat())
        );
        let salted_stored_hash = format!(
            "md5{:x}",
            md5::compute([stored_hash.as_bytes(), &pgb.md5_salt].concat())
        );

        let received_hash = std::str::from_utf8(&md5_response)?;

        println!(
            "check_auth_md5: {:?} vs {}, salt {:?}",
            received_hash, salted_stored_hash, &pgb.md5_salt
        );

        if received_hash == salted_stored_hash {
            Ok(())
        } else {
            bail!("Auth failed")
        }
    }
}
