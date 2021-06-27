use crate::cplane_api::CPlaneApi;
use crate::cplane_api::DatabaseInfo;
use crate::ProxyState;

use anyhow::bail;
use tokio_postgres::NoTls;

use rand::Rng;
use std::sync::mpsc::channel;
use std::thread;
use tokio::io::AsyncWriteExt;
use zenith_utils::postgres_backend::{PostgresBackend, ProtoState};
use zenith_utils::pq_proto::*;
use zenith_utils::{postgres_backend, pq_proto::BeMessage};

///
/// Main proxy listener loop.
///
/// Listens for connections, and launches a new handler thread for each.
///
pub fn thread_main(
    state: &'static ProxyState,
    listener: std::net::TcpListener,
) -> anyhow::Result<()> {
    loop {
        let (socket, peer_addr) = listener.accept()?;
        println!("accepted connection from {}", peer_addr);
        socket.set_nodelay(true).unwrap();

        thread::spawn(move || {
            if let Err(err) = proxy_conn_main(state, socket) {
                println!("error: {}", err);
            }
        });
    }
}

// XXX: clean up fields
struct ProxyConnection {
    state: &'static ProxyState,

    existing_user: bool,
    cplane: CPlaneApi,

    user: String,
    database: String,

    pgb: PostgresBackend,
    md5_salt: [u8; 4],

    psql_session_id: String,
}

pub fn proxy_conn_main(
    state: &'static ProxyState,
    socket: std::net::TcpStream,
) -> anyhow::Result<()> {
    let mut conn = ProxyConnection {
        state,
        existing_user: false,
        cplane: CPlaneApi::new(&state.conf.cplane_address),
        user: "".into(),
        database: "".into(),
        pgb: PostgresBackend::new(socket, postgres_backend::AuthType::MD5)?,
        md5_salt: [0u8; 4],
        psql_session_id: "".into(),
    };

    // Check StartupMessage
    // This will set conn.existing_user and we can decide on next actions
    conn.handle_startup()?;

    // both scenarious here should end up producing database connection string
    let db_info = if conn.existing_user {
        conn.handle_existing_user()?
    } else {
        conn.handle_new_user()?
    };

    // ok, proxy pass user connection to database_uri
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let _ = runtime.block_on(proxy_pass(conn.pgb, db_info))?;

    println!("proxy_conn_main done;");

    Ok(())
}

impl ProxyConnection {
    fn handle_startup(&mut self) -> anyhow::Result<()> {
        loop {
            let msg = self.pgb.read_message()?;
            println!("got message {:?}", msg);
            match msg {
                Some(FeMessage::StartupMessage(m)) => {
                    println!("got startup message {:?}", m);

                    match m.kind {
                        StartupRequestCode::NegotiateGss | StartupRequestCode::NegotiateSsl => {
                            println!("SSL requested");
                            self.pgb.write_message(&BeMessage::Negotiate)?;
                        }
                        StartupRequestCode::Normal => {
                            self.user = m
                                .params
                                .get("user")
                                .ok_or_else(|| {
                                    anyhow::Error::msg("user is required in startup packet")
                                })?
                                .into();
                            self.database = m
                                .params
                                .get("database")
                                .ok_or_else(|| {
                                    anyhow::Error::msg("database is required in startup packet")
                                })?
                                .into();

                            self.existing_user = self.user.ends_with("@zenith");

                            break;
                        }
                        StartupRequestCode::Cancel => break,
                    }
                }
                None => {
                    bail!("connection closed")
                }
                unexpected => {
                    bail!("unexpected message type : {:?}", unexpected)
                }
            }
        }
        Ok(())
    }

    fn handle_existing_user(&mut self) -> anyhow::Result<DatabaseInfo> {
        // ask password
        rand::thread_rng().fill(&mut self.md5_salt);
        self.pgb
            .write_message(&BeMessage::AuthenticationMD5Password(&self.md5_salt))?;
        self.pgb.state = ProtoState::Authentication; // XXX

        // check password
        println!("handle_existing_user");
        let msg = self.pgb.read_message()?;
        println!("got message {:?}", msg);
        if let Some(FeMessage::PasswordMessage(m)) = msg {
            println!("got password message '{:?}'", m);

            assert!(self.existing_user);

            let (_trailing_null, md5_response) = m
                .split_last()
                .ok_or_else(|| anyhow::Error::msg("unexpected password message"))?;

            if let Err(e) = self.check_auth_md5(md5_response) {
                self.pgb
                    .write_message(&BeMessage::ErrorResponse(format!("{}", e)))?;
                bail!("auth failed: {}", e);
            } else {
                self.pgb
                    .write_message_noflush(&BeMessage::AuthenticationOk)?;
                self.pgb
                    .write_message_noflush(&BeMessage::ParameterStatus)?;
                self.pgb.write_message(&BeMessage::ReadyForQuery)?;
            }
        }

        // ok, we are authorized
        self.cplane.get_database_uri(&self.user, &self.database)
    }

    fn handle_new_user(&mut self) -> anyhow::Result<DatabaseInfo> {
        let mut psql_session_id_buf = [0u8; 8];
        rand::thread_rng().fill(&mut psql_session_id_buf);
        self.psql_session_id = hex::encode(psql_session_id_buf);

        let hello_message = format!("☀️  Welcome to Zenith!

To proceed with database creation open following link:

    https://console.zenith.tech/psql_session/{}

It needed to be done once and we will send you '.pgpass' file which will allow you to access or create
databases without opening the browser.

", self.psql_session_id);

        self.pgb
            .write_message_noflush(&BeMessage::AuthenticationOk)?;
        self.pgb
            .write_message_noflush(&BeMessage::ParameterStatus)?;
        self.pgb
            .write_message(&BeMessage::NoticeResponse(hello_message.to_string()))?;

        // await for database creation
        let (tx, rx) = channel::<anyhow::Result<DatabaseInfo>>();
        let _ = self
            .state
            .waiters
            .lock()
            .unwrap()
            .insert(self.psql_session_id.clone(), tx);

        // Wait for web console response
        // XXX: respond with error to client
        let dbinfo = rx.recv()??;

        self.pgb.write_message_noflush(&BeMessage::NoticeResponse(
            "Connecting to database.".to_string(),
        ))?;
        self.pgb.write_message(&BeMessage::ReadyForQuery)?;

        Ok(dbinfo)
    }

    fn check_auth_md5(&self, md5_response: &[u8]) -> anyhow::Result<()> {
        assert!(self.existing_user);
        self.cplane
            .check_auth(self.user.as_str(), md5_response, &self.md5_salt)
    }
}

async fn proxy_pass(pgb: PostgresBackend, db_info: DatabaseInfo) -> anyhow::Result<()> {
    let mut socket = tokio::net::TcpStream::connect(db_info.socket_addr()).await?;
    let config = db_info.conn_string().parse::<tokio_postgres::Config>()?;
    let _ = config.connect_raw(&mut socket, NoTls).await?;

    println!("Connected to pg, proxying");

    let incoming_std = pgb.into_stream();
    incoming_std.set_nonblocking(true)?;
    let mut incoming_conn = tokio::net::TcpStream::from_std(incoming_std)?;

    let (mut ri, mut wi) = incoming_conn.split();
    let (mut ro, mut wo) = socket.split();

    let client_to_server = async {
        tokio::io::copy(&mut ri, &mut wo).await?;
        wo.shutdown().await
    };

    let server_to_client = async {
        tokio::io::copy(&mut ro, &mut wi).await?;
        wi.shutdown().await
    };

    tokio::try_join!(client_to_server, server_to_client)?;

    Ok(())
}
