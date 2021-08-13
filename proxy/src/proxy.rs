use crate::cplane_api::CPlaneApi;
use crate::cplane_api::DatabaseInfo;
use crate::ProxyState;

use anyhow::bail;
use tokio_postgres::NoTls;

use rand::Rng;
use std::io::Write;
use std::{io, sync::mpsc::channel, thread};
use zenith_utils::postgres_backend::Stream;
use zenith_utils::postgres_backend::{PostgresBackend, ProtoState};
use zenith_utils::pq_proto::*;
use zenith_utils::sock_split::{OwnedReadHalf, OwnedWriteHalf};
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
        cplane: CPlaneApi::new(&state.conf.cplane_address),
        user: "".into(),
        database: "".into(),
        pgb: PostgresBackend::new(
            socket,
            postgres_backend::AuthType::MD5,
            state.conf.ssl_config.clone(),
        )?,
        md5_salt: [0u8; 4],
        psql_session_id: "".into(),
    };

    // Check StartupMessage
    // This will set conn.existing_user and we can decide on next actions
    conn.handle_startup()?;

    // both scenarious here should end up producing database connection string
    let db_info = if conn.is_existing_user() {
        conn.handle_existing_user()?
    } else {
        conn.handle_new_user()?
    };

    proxy_pass(conn.pgb, db_info)
}

impl ProxyConnection {
    fn is_existing_user(&self) -> bool {
        self.user.ends_with("@zenith")
    }

    fn handle_startup(&mut self) -> anyhow::Result<()> {
        loop {
            let msg = self.pgb.read_message()?;
            println!("got message {:?}", msg);
            match msg {
                Some(FeMessage::StartupMessage(m)) => {
                    println!("got startup message {:?}", m);

                    match m.kind {
                        StartupRequestCode::NegotiateGss => {
                            self.pgb
                                .write_message(&BeMessage::EncryptionResponse(false))?;
                        }
                        StartupRequestCode::NegotiateSsl => {
                            println!("SSL requested");
                            if self.pgb.tls_config.is_some() {
                                self.pgb
                                    .write_message(&BeMessage::EncryptionResponse(true))?;
                                self.pgb.start_tls()?;
                            } else {
                                self.pgb
                                    .write_message(&BeMessage::EncryptionResponse(false))?;
                            }
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

            assert!(self.is_existing_user());

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

To proceed with database creation, open the following link:

    {redirect_uri}{sess_id}

It needs to be done once and we will send you '.pgpass' file, which will allow you to access or create
databases without opening the browser.

", redirect_uri = self.state.conf.redirect_uri, sess_id = self.psql_session_id);

        self.pgb
            .write_message_noflush(&BeMessage::AuthenticationOk)?;
        self.pgb
            .write_message_noflush(&BeMessage::ParameterStatus)?;
        self.pgb
            .write_message(&BeMessage::NoticeResponse(hello_message))?;

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
        assert!(self.is_existing_user());
        self.cplane
            .check_auth(self.user.as_str(), md5_response, &self.md5_salt)
    }
}

/// Create a TCP connection to a postgres database, authenticate with it, and receive the ReadyForQuery message
async fn connect_to_db(db_info: DatabaseInfo) -> anyhow::Result<tokio::net::TcpStream> {
    let mut socket = tokio::net::TcpStream::connect(db_info.socket_addr()).await?;
    let config = db_info.conn_string().parse::<tokio_postgres::Config>()?;
    let _ = config.connect_raw(&mut socket, NoTls).await?;
    Ok(socket)
}

/// Concurrently proxy both directions of the client and server connections
fn proxy(
    client_read: OwnedReadHalf,
    client_write: OwnedWriteHalf,
    server_read: OwnedReadHalf,
    server_write: OwnedWriteHalf,
) -> anyhow::Result<()> {
    fn do_proxy(mut reader: OwnedReadHalf, mut writer: OwnedWriteHalf) -> io::Result<()> {
        std::io::copy(&mut reader, &mut writer)?;
        writer.flush()?;
        writer.shutdown(std::net::Shutdown::Both)
    }

    let client_to_server_jh = thread::spawn(move || do_proxy(client_read, server_write));

    let res1 = do_proxy(server_read, client_write);
    let res2 = client_to_server_jh.join().unwrap();
    res1?;
    res2?;

    Ok(())
}

/// Proxy a client connection to a postgres database
fn proxy_pass(pgb: PostgresBackend, db_info: DatabaseInfo) -> anyhow::Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread().build()?;
    let db_stream = runtime.block_on(connect_to_db(db_info))?;
    let db_stream = db_stream.into_std()?;
    db_stream.set_nonblocking(false)?;

    let db_stream = zenith_utils::sock_split::BiDiStream::RawTcp(db_stream);
    let (db_read, db_write) = db_stream.split();

    let stream = match pgb.into_stream() {
        Stream::BiDirectional(bidi_stream) => bidi_stream,
        _ => bail!("invalid stream"),
    };

    let (client_read, client_write) = stream.split();
    proxy(client_read, client_write, db_read, db_write)
}
