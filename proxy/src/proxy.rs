use crate::cplane_api::{CPlaneApi, DatabaseInfo};
use crate::ProxyState;
use anyhow::{anyhow, bail};
use std::net::TcpStream;
use std::{io, thread};
use tokio_postgres::NoTls;
use zenith_utils::postgres_backend::{self, PostgresBackend, ProtoState, Stream};
use zenith_utils::pq_proto::{BeMessage as Be, FeMessage as Fe, *};
use zenith_utils::sock_split::{ReadStream, WriteStream};

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

// TODO: clean up fields
struct ProxyConnection {
    state: &'static ProxyState,
    psql_session_id: String,
    pgb: PostgresBackend,
}

pub fn proxy_conn_main(state: &'static ProxyState, socket: TcpStream) -> anyhow::Result<()> {
    let conn = ProxyConnection {
        state,
        psql_session_id: hex::encode(rand::random::<[u8; 8]>()),
        pgb: PostgresBackend::new(
            socket,
            postgres_backend::AuthType::MD5,
            state.conf.ssl_config.clone(),
            false,
        )?,
    };

    let (client, server) = conn.handle_client()?;

    let server = zenith_utils::sock_split::BidiStream::from_tcp(server);

    let client = match client {
        Stream::Bidirectional(bidi_stream) => bidi_stream,
        _ => panic!("invalid stream type"),
    };

    proxy(client.split(), server.split())
}

impl ProxyConnection {
    fn handle_client(mut self) -> anyhow::Result<(Stream, TcpStream)> {
        let mut authenticate = || {
            let (username, dbname) = self.handle_startup()?;

            // Both scenarios here should end up producing database credentials
            if username.ends_with("@zenith") {
                self.handle_existing_user(&username, &dbname)
            } else {
                self.handle_new_user()
            }
        };

        let conn = match authenticate() {
            Ok(db_info) => connect_to_db(db_info),
            Err(e) => {
                // Report the error to the client
                self.pgb.write_message(&Be::ErrorResponse(e.to_string()))?;
                bail!("failed to handle client: {:?}", e);
            }
        };

        // We'll get rid of this once migration to async is complete
        let db_stream = {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;

            let stream = runtime.block_on(conn)?.into_std()?;
            stream.set_nonblocking(false)?;
            stream
        };

        // Let the client send new requests
        self.pgb.write_message(&Be::ReadyForQuery)?;

        Ok((self.pgb.into_stream(), db_stream))
    }

    fn handle_startup(&mut self) -> anyhow::Result<(String, String)> {
        let have_tls = self.pgb.tls_config.is_some();
        let mut encrypted = false;

        loop {
            let mut msg = match self.pgb.read_message()? {
                Some(Fe::StartupMessage(msg)) => msg,
                None => bail!("connection is lost"),
                bad => bail!("unexpected message type: {:?}", bad),
            };
            println!("got message: {:?}", msg);

            match msg.kind {
                StartupRequestCode::NegotiateGss => {
                    self.pgb.write_message(&Be::EncryptionResponse(false))?;
                }
                StartupRequestCode::NegotiateSsl => {
                    self.pgb.write_message(&Be::EncryptionResponse(have_tls))?;
                    if have_tls {
                        self.pgb.start_tls()?;
                        encrypted = true;
                    }
                }
                StartupRequestCode::Normal => {
                    if have_tls && !encrypted {
                        bail!("must connect with TLS");
                    }

                    let mut get_param = |key| {
                        msg.params
                            .remove(key)
                            .ok_or_else(|| anyhow!("{} is missing in startup packet", key))
                    };

                    return Ok((get_param("user")?, get_param("database")?));
                }
                // TODO: implement proper stmt cancellation
                StartupRequestCode::Cancel => bail!("query cancellation is not supported"),
            }
        }
    }

    fn handle_existing_user(&mut self, user: &str, db: &str) -> anyhow::Result<DatabaseInfo> {
        let md5_salt = rand::random::<[u8; 4]>();

        // Ask password
        self.pgb
            .write_message(&Be::AuthenticationMD5Password(&md5_salt))?;
        self.pgb.state = ProtoState::Authentication; // XXX

        // Check password
        let msg = match self.pgb.read_message()? {
            Some(Fe::PasswordMessage(msg)) => msg,
            None => bail!("connection is lost"),
            bad => bail!("unexpected message type: {:?}", bad),
        };
        println!("got message: {:?}", msg);

        let (_trailing_null, md5_response) = msg
            .split_last()
            .ok_or_else(|| anyhow!("unexpected password message"))?;

        let cplane = CPlaneApi::new(&self.state.conf.auth_endpoint, &self.state.waiters);
        let db_info = cplane.authenticate_proxy_request(
            user,
            db,
            md5_response,
            &md5_salt,
            &self.psql_session_id,
        )?;

        self.pgb
            .write_message_noflush(&Be::AuthenticationOk)?
            .write_message_noflush(&Be::ParameterStatus)?;

        Ok(db_info)
    }

    fn handle_new_user(&mut self) -> anyhow::Result<DatabaseInfo> {
        let greeting = hello_message(&self.state.conf.redirect_uri, &self.psql_session_id);

        // First, register this session
        let waiter = self.state.waiters.register(self.psql_session_id.clone());

        // Give user a URL to spawn a new database
        self.pgb
            .write_message_noflush(&Be::AuthenticationOk)?
            .write_message_noflush(&Be::ParameterStatus)?
            .write_message(&Be::NoticeResponse(greeting))?;

        // Wait for web console response
        let db_info = waiter.wait()?.map_err(|e| anyhow!(e))?;

        self.pgb
            .write_message_noflush(&Be::NoticeResponse("Connecting to database.".into()))?;

        Ok(db_info)
    }
}

fn hello_message(redirect_uri: &str, session_id: &str) -> String {
    format!(
        concat![
            "☀️  Welcome to Zenith!\n",
            "To proceed with database creation, open the following link:\n\n",
            "    {redirect_uri}{session_id}\n\n",
            "It needs to be done once and we will send you '.pgpass' file,\n",
            "which will allow you to access or create ",
            "databases without opening your web browser."
        ],
        redirect_uri = redirect_uri,
        session_id = session_id,
    )
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
                    self.flush()?;
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
