use crate::{cplane_api::CPlaneApi, ProxyConf};

use bytes::Bytes;
use std::{
    net::{TcpListener, TcpStream},
    thread,
};
use zenith_utils::postgres_backend::{AuthType, PostgresBackend};
use zenith_utils::{
    postgres_backend,
    pq_proto::{BeMessage, SINGLE_COL_ROWDESC},
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
    let mut conn_handler = ProxyHandler {
        conf,
        existing_user: false,
        cplane: CPlaneApi::new(&conf.cplane_address),
        user: "".into(),
        database: "".into(),
    };
    let mut pgbackend = PostgresBackend::new(socket, postgres_backend::AuthType::Trust)?;
    pgbackend.run(&mut conn_handler)
}

struct ProxyHandler {
    conf: &'static ProxyConf,
    existing_user: bool,
    cplane: CPlaneApi,

    user: String,
    database: String,
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

        if !self.existing_user {
            pgb.write_message_noflush(&SINGLE_COL_ROWDESC)?
                .write_message_noflush(&BeMessage::DataRow(&[Some(b"new user scenario")]))?
                .write_message_noflush(&BeMessage::CommandComplete(b"SELECT 1"))?;
        } else {
            pgb.write_message_noflush(&SINGLE_COL_ROWDESC)?
                .write_message_noflush(&BeMessage::DataRow(&[Some(b"existing user")]))?
                .write_message_noflush(&BeMessage::CommandComplete(b"SELECT 1"))?;
        }

        pgb.flush()?;
        Ok(())
    }

    fn startup(
        &mut self,
        pgb: &mut PostgresBackend,
        sm: &zenith_utils::pq_proto::FeStartupMessage,
    ) -> anyhow::Result<()> {
        println!("Got startup: {:?}", sm);

        self.user = sm
            .params
            .get("user")
            .ok_or_else(|| anyhow::Error::msg("user is required in startup packet"))?
            .into();
        self.database = sm
            .params
            .get("database")
            .ok_or_else(|| anyhow::Error::msg("database is required in startup packet"))?
            .into();

        // We use '@zenith' in username as an indicator that user already created
        // this database and not logging in with his system username.
        //
        // With that approach we can create new databases on demand with something like
        // psql -h zenith.tech -U stas@zenith my_new_db (assuming .pgpass is set). That is
        // especially helpful if one is setting configuration files for some app that requires
        // database -- he can just fill config and run initial migration without any other actions.
        if self.user.ends_with("@zenith") {
            pgb.auth_type = AuthType::MD5;
            self.existing_user = true;
        }

        Ok(())
    }

    fn check_auth_md5(
        &mut self,
        pgb: &mut PostgresBackend,
        md5_response: &[u8],
    ) -> anyhow::Result<()> {
        assert!(self.existing_user);
        self.cplane
            .check_auth(self.user.as_str(), md5_response, &pgb.md5_salt)
    }
}
