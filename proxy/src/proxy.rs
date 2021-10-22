use crate::cplane_api::CPlaneApi;
use crate::cplane_api::DatabaseInfo;
use crate::ProxyState;

use anyhow::bail;
use tokio_postgres::NoTls;

use rand::Rng;
use std::{io, sync::mpsc::channel, thread};
use zenith_utils::postgres_backend::Stream;
use zenith_utils::postgres_backend::{PostgresBackend, ProtoState};
use zenith_utils::pq_proto::*;
use zenith_utils::sock_split::{ReadStream, WriteStream};
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

        thread::Builder::new()
            .name("Proxy thread".into())
            .spawn(move || {
                if let Err(err) = proxy_conn_main(state, socket) {
                    println!("error: {}", err);
                }
            })?;
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
        cplane: CPlaneApi::new(&state.conf.auth_endpoint),
        user: "".into(),
        database: "".into(),
        pgb: PostgresBackend::new(
            socket,
            postgres_backend::AuthType::MD5,
            state.conf.ssl_config.clone(),
            false,
        )?,
        md5_salt: [0u8; 4],
        psql_session_id: "".into(),
    };

    // Check StartupMessage
    // This will set conn.existing_user and we can decide on next actions
    conn.handle_startup()?;

    let mut psql_session_id_buf = [0u8; 8];
    rand::thread_rng().fill(&mut psql_session_id_buf);
    conn.psql_session_id = hex::encode(psql_session_id_buf);

    // both scenarious here should end up producing database connection string
    let conn_info = if conn.is_existing_user() {
        conn.handle_existing_user()?
    } else {
        conn.handle_new_user()?
    };

    // XXX: move that inside handle_new_user/handle_existing_user to be able to
    // report wrong connection error.
    proxy_pass(conn.pgb, conn_info)
}

impl ProxyConnection {
    fn is_existing_user(&self) -> bool {
        self.user.ends_with("@zenith")
    }

    fn handle_startup(&mut self) -> anyhow::Result<()> {
        let mut encrypted = false;
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
                                encrypted = true;
                            } else {
                                self.pgb
                                    .write_message(&BeMessage::EncryptionResponse(false))?;
                            }
                        }
                        StartupRequestCode::Normal => {
                            if self.state.conf.ssl_config.is_some() && !encrypted {
                                self.pgb.write_message(&BeMessage::ErrorResponse(
                                    "must connect with TLS".to_string(),
                                ))?;
                                bail!("client did not connect with TLS");
                            }
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

    // Wait for proxy kick form the console with conninfo
    fn wait_for_conninfo(&mut self) -> anyhow::Result<DatabaseInfo> {
        let (tx, rx) = channel::<anyhow::Result<DatabaseInfo>>();
        let _ = self
            .state
            .waiters
            .lock()
            .unwrap()
            .insert(self.psql_session_id.clone(), tx);

        // Wait for web console response
        // TODO: respond with error to client
        rx.recv()?
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

            match self.cplane.authenticate_proxy_request(
                self.user.as_str(),
                self.database.as_str(),
                md5_response,
                &self.md5_salt,
                &self.psql_session_id,
            ) {
                Err(e) => {
                    self.pgb.write_message(&BeMessage::ErrorResponse(format!(
                        "cannot authenticate proxy: {}",
                        e
                    )))?;

                    bail!("auth failed: {}", e);
                }

                Ok(auth_info) => {
                    let conn_info = if auth_info.ready {
                        // Cluster is ready, so just take `conn_info` and respond to the client.
                        auth_info
                            .conn_info
                            .expect("conn_info should be provided with ready cluster")
                    } else {
                        match auth_info.error {
                            Some(e) => {
                                self.pgb.write_message(&BeMessage::ErrorResponse(format!(
                                    "cannot authenticate proxy: {}",
                                    e
                                )))?;

                                bail!("auth failed: {}", e);
                            }
                            None => {
                                // Cluster exists, but isn't active, await its start and proxy kick
                                // with `conn_info`.
                                self.wait_for_conninfo()?
                            }
                        }
                    };

                    self.pgb
                        .write_message_noflush(&BeMessage::AuthenticationOk)?;
                    self.pgb
                        .write_message_noflush(&BeMessage::ParameterStatus)?;
                    self.pgb.write_message(&BeMessage::ReadyForQuery)?;

                    Ok(conn_info)
                }
            }
        } else {
            bail!("protocol violation");
        }
    }

    fn handle_new_user(&mut self) -> anyhow::Result<DatabaseInfo> {
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

        // We requested the DB creation from the console. Now wait for conninfo
        let conn_info = self.wait_for_conninfo()?;

        self.pgb.write_message_noflush(&BeMessage::NoticeResponse(
            "Connecting to database.".to_string(),
        ))?;
        self.pgb.write_message(&BeMessage::ReadyForQuery)?;

        Ok(conn_info)
    }
}

/// Create a TCP connection to a postgres database, authenticate with it, and receive the ReadyForQuery message
async fn connect_to_db(db_info: DatabaseInfo) -> anyhow::Result<tokio::net::TcpStream> {
    let mut socket = tokio::net::TcpStream::connect(db_info.socket_addr()?).await?;
    let config = tokio_postgres::Config::from(db_info);
    let _ = config.connect_raw(&mut socket, NoTls).await?;
    Ok(socket)
}

/// Concurrently proxy both directions of the client and server connections
fn proxy(
    (client_read, client_write): (ReadStream, WriteStream),
    (server_read, server_write): (ReadStream, WriteStream),
) -> anyhow::Result<()> {
    fn do_proxy(mut reader: impl io::Read, mut writer: WriteStream) -> io::Result<u64> {
        /// FlushWriter will make sure that every message is sent as soon as possible
        struct FlushWriter<W>(W);

        impl<W: io::Write> io::Write for FlushWriter<W> {
            fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
                // `std::io::copy` is guaranteed to exit if we return an error,
                // so we can afford to lose `res` in case `flush` fails
                let res = self.0.write(buf);
                if res.is_ok() {
                    self.0.flush()?;
                }
                res
            }

            fn flush(&mut self) -> io::Result<()> {
                self.0.flush()
            }
        }

        let res = std::io::copy(&mut reader, &mut FlushWriter(&mut writer));
        writer.shutdown(std::net::Shutdown::Both)?;
        res
    }

    let client_to_server_jh = thread::spawn(move || do_proxy(client_read, server_write));

    do_proxy(server_read, client_write)?;
    client_to_server_jh.join().unwrap()?;

    Ok(())
}

/// Proxy a client connection to a postgres database
fn proxy_pass(pgb: PostgresBackend, db_info: DatabaseInfo) -> anyhow::Result<()> {
    let db_stream = {
        // We'll get rid of this once migration to async is complete
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;

        let stream = runtime.block_on(connect_to_db(db_info))?.into_std()?;
        stream.set_nonblocking(false)?;
        stream
    };

    let db = zenith_utils::sock_split::BidiStream::from_tcp(db_stream);

    let client = match pgb.into_stream() {
        Stream::Bidirectional(bidi_stream) => bidi_stream,
        _ => bail!("invalid stream"),
    };

    proxy(client.split(), db.split())
}
