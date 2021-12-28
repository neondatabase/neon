use crate::cplane_api::{CPlaneApi, DatabaseInfo};
use crate::ProxyState;
use anyhow::{anyhow, bail};
use lazy_static::lazy_static;
use parking_lot::Mutex;
use rand::prelude::StdRng;
use rand::{CryptoRng, Rng, SeedableRng};
use tokio::try_join;
use std::collections::HashMap;
use std::net::TcpStream;
use std::{io, thread};
use tokio_postgres::{CancelToken, NoTls};
use zenith_utils::postgres_backend::{self, PostgresBackend, ProtoState, Stream};
use zenith_utils::pq_proto::{BeMessage as Be, FeMessage as Fe, *};
use zenith_utils::sock_split::{ReadStream, WriteStream};

lazy_static! {
    // NOTE including DatabaseInfo in the value would be unnecessary if I can figure out
    //      how to use cancel_token.cancel_query. For now I rely on cancel_query_raw,
    //      which needs the DatabaseInfo.
    static ref CANCEL_MAP: Mutex<HashMap<(i32, i32), (CancelToken, DatabaseInfo)>> =
        Mutex::new(HashMap::new());
}

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

    let (client, server) = match conn.handle_client()? {
        Some(x) => x,
        None => return Ok(())
    };

    let server = zenith_utils::sock_split::BidiStream::from_tcp(server);

    let client = match client {
        Stream::Bidirectional(bidi_stream) => bidi_stream,
        _ => panic!("invalid stream type"),
    };

    proxy(client.split(), server.split())
}

impl ProxyConnection {
    // TODO all this Result<Option<_>> nesting is getting hairy.
    //      Consider simplifying before asking for review.
    //      Maybe compose these functions instead of nesting them?
    //      If not at least document what None means everywhere
    //      (successfully closed connection).
    fn handle_client(mut self) -> anyhow::Result<Option<(Stream, TcpStream)>> {
        let mut authenticate = || {
            let (username, dbname) = match self.handle_startup()? {
                Some(x) => x,
                None => return Ok(None)
            };

            // Both scenarios here should end up producing database credentials
            // HACK, will remove this before PR, just ignore it
            if true || username.ends_with("@zenith") {
                self.handle_existing_user(&username, &dbname).map(|x| Some(x))
            } else {
                self.handle_new_user().map(|x| Some(x))
            }
        };

        let conn = match authenticate() {
            Ok(Some(db_info)) => connect_to_db(db_info),
            Ok(None) => return Ok(None),
            Err(e) => {
                // Report the error to the client
                self.pgb.write_message(&Be::ErrorResponse(e.to_string()))?;
                bail!("failed to handle client: {:?}", e);
            }
        };

        // We'll get rid of this once migration to async is complete
        let (pg_version, db_stream) = {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;

            let (pg_version, stream, cancel_key_data) = runtime.block_on(conn)?;
            self.pgb.write_message(&BeMessage::BackendKeyData(cancel_key_data.0, cancel_key_data.1))?;
            let stream = stream.into_std()?;
            stream.set_nonblocking(false)?;

            (pg_version, stream)
        };

        // Let the client send new requests
        self.pgb
            .write_message_noflush(&BeMessage::ParameterStatus(
                BeParameterStatusMessage::ServerVersion(&pg_version),
            ))?
            .write_message(&Be::ReadyForQuery)?;

        Ok(Some((self.pgb.into_stream(), db_stream)))
    }

    fn handle_startup(&mut self) -> anyhow::Result<Option<(String, String)>> {
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

                    return Ok(Some((get_param("user")?, get_param("database")?)));
                }
                StartupRequestCode::Cancel => {
                    // NOTE using unwrap instead of ? because I don't want to send an
                    //      unauthenticated user surprisingly transparent errors.
                    let backend_pid: i32 = msg.params["backend_pid"].parse().unwrap();
                    let cancel_key: i32 = msg.params["cancel_key"].parse().unwrap();
                    if let Some((cancel_token, db_info)) = CANCEL_MAP.lock().get(&(backend_pid, cancel_key)) {
                        let runtime = tokio::runtime::Builder::new_current_thread()
                            .enable_all()
                            .build().unwrap();

                        // TODO why NoTls? Currently i'm cargo-culting from how
                        //      we initialize connect_raw.
                        // if let Err(e) = runtime.block_on(cancel_token.cancel_query(NoTls)) {
                            // Error {
                            //     kind: Connect,
                            //     cause: Some(
                            //         Custom {
                            //             kind: InvalidInput,
                            //             error: "unknown host",
                            //         },
                            //     ),
                            // }
                            // dbg!(e);
                        // }

                        // TODO figure out cancel_token.cancel_query (see commented code above)
                        let socket = runtime.block_on(tokio::net::TcpStream::connect(db_info.socket_addr().unwrap())).unwrap();
                        runtime.block_on(cancel_token.cancel_query_raw(socket, NoTls)).unwrap();
                    }
                    return Ok(None);
                }
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

        // HACK, will remove this before PR, just ignore it
        // let cplane = CPlaneApi::new(&self.state.conf.auth_endpoint, &self.state.waiters);
        // let db_info = cplane.authenticate_proxy_request(
        //     user,
        //     db,
        //     md5_response,
        //     &md5_salt,
        //     &self.psql_session_id,
        // )?;
        let db_info = DatabaseInfo {
            host: "localhost".into(),
            port: 5432,
            dbname: "postgres".into(),
            user: "postgres".into(),
            password: Some("postgres".into()),
        };

        self.pgb
            .write_message_noflush(&Be::AuthenticationOk)?
            .write_message_noflush(&BeParameterStatusMessage::encoding())?;

        Ok(db_info)
    }

    fn handle_new_user(&mut self) -> anyhow::Result<DatabaseInfo> {
        let greeting = hello_message(&self.state.conf.redirect_uri, &self.psql_session_id);

        // First, register this session
        let waiter = self.state.waiters.register(self.psql_session_id.clone());

        // Give user a URL to spawn a new database
        self.pgb
            .write_message_noflush(&Be::AuthenticationOk)?
            .write_message_noflush(&BeParameterStatusMessage::encoding())?
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
async fn connect_to_db(db_info: DatabaseInfo) ->
    anyhow::Result<(String, tokio::net::TcpStream, (i32, i32))> {
    let mut socket = tokio::net::TcpStream::connect(db_info.socket_addr()?).await?;
    let config = tokio_postgres::Config::from(db_info.clone());
    let (client, conn) = config.connect_raw(&mut socket, NoTls).await?;
    let cancel_token = client.cancel_token().clone();

    // NOTE We effectively ignore any ParameterStatus and NoticeResponse
    //      messages here. Not sure if that could break something.

    let get_version_and_backend = || async {
        let pipeline = try_join!(
            client.query_one("select current_setting('server_version')", &[]),
            client.query_one("select pg_backend_pid()", &[]))?;
        let version: String = pipeline.0.try_get(0)?;
        let backend_pid: i32 = pipeline.1.try_get(0)?;
        Result::<(String, i32), tokio_postgres::Error>::Ok((version, backend_pid))
    };

    // Allow conn to talk with pg until we get our results back
    let (version, backend_pid) = tokio::select! {
        x = get_version_and_backend() => x?,
        _ = conn => bail!("connection closed too early"),
    };

    let mut rng = rand::rngs::StdRng::from_entropy();
    let cancel_key: i32 = rng.gen();
    let cancel_key_data = (backend_pid, cancel_key);

    CANCEL_MAP.lock().insert(cancel_key_data, (cancel_token, db_info.clone()));
    Ok((version, socket, cancel_key_data))
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
