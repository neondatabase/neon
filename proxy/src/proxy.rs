use crate::cplane_api::{CPlaneApi, DatabaseInfo};
use crate::ProxyState;
use anyhow::{anyhow, bail, Context};
use lazy_static::lazy_static;
use rand::prelude::StdRng;
use rand::{Rng, SeedableRng};
use std::cell::Cell;
use std::collections::HashMap;
use std::net::{SocketAddr, TcpStream};
#[cfg(unix)]
use std::os::unix::io::AsRawFd;
use std::sync::Mutex;
use std::{io, thread};
use tokio_postgres::NoTls;
use zenith_metrics::{new_common_metric_name, register_int_counter, IntCounter};
use zenith_utils::postgres_backend::{self, PostgresBackend, ProtoState, Stream};
use zenith_utils::pq_proto::{BeMessage as Be, FeMessage as Fe, *};
use zenith_utils::sock_split::{ReadStream, WriteStream};

struct CancelClosure {
    socket_addr: SocketAddr,
    cancel_token: tokio_postgres::CancelToken,
}

impl CancelClosure {
    async fn try_cancel_query(&self) {
        if let Ok(socket) = tokio::net::TcpStream::connect(self.socket_addr).await {
            // NOTE ignoring the result because:
            // 1. This is a best effort attempt, the database doesn't have to listen
            // 2. Being opaque about errors here helps avoid leaking info to unauthenticated user
            let _ = self.cancel_token.cancel_query_raw(socket, NoTls).await;
        }
    }
}

lazy_static! {
    // Enables serving CancelRequests
    static ref CANCEL_MAP: Mutex<HashMap<CancelKeyData, CancelClosure>> = Mutex::new(HashMap::new());

    // Metrics
    static ref NUM_CONNECTIONS_ACCEPTED_COUNTER: IntCounter = register_int_counter!(
        new_common_metric_name("num_connections_accepted"),
        "Number of TCP client connections accepted."
    ).unwrap();
    static ref NUM_CONNECTIONS_CLOSED_COUNTER: IntCounter = register_int_counter!(
        new_common_metric_name("num_connections_closed"),
        "Number of TCP client connections closed."
    ).unwrap();
    static ref NUM_CONNECTIONS_FAILED_COUNTER: IntCounter = register_int_counter!(
        new_common_metric_name("num_connections_failed"),
        "Number of TCP client connections that closed due to error."
    ).unwrap();
    static ref NUM_BYTES_PROXIED_COUNTER: IntCounter = register_int_counter!(
        new_common_metric_name("num_bytes_proxied"),
        "Number of bytes sent/received between any client and backend."
    ).unwrap();
}

thread_local! {
    // Used to clean up the CANCEL_MAP. Might not be necessary if we use tokio thread pool in main loop.
    static THREAD_CANCEL_KEY_DATA: Cell<Option<CancelKeyData>> = Cell::new(None);
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
        NUM_CONNECTIONS_ACCEPTED_COUNTER.inc();
        socket.set_nodelay(true).unwrap();

        // TODO Use a threadpool instead. Maybe use tokio's threadpool by
        //      spawning a future into its runtime. Tokio's JoinError should
        //      allow us to handle cleanup properly even if the future panics.
        thread::Builder::new()
            .name("Proxy thread".into())
            .spawn(move || {
                if let Err(err) = proxy_conn_main(state, socket) {
                    NUM_CONNECTIONS_FAILED_COUNTER.inc();
                    println!("error: {}", err);
                }

                // Clean up CANCEL_MAP.
                NUM_CONNECTIONS_CLOSED_COUNTER.inc();
                THREAD_CANCEL_KEY_DATA.with(|cell| {
                    if let Some(cancel_key_data) = cell.get() {
                        CANCEL_MAP.lock().unwrap().remove(&cancel_key_data);
                    };
                });
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
    let fd = socket.as_raw_fd();
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
        None => return Ok(()),
    };

    if state.conf.tcp_keepalive {
        nix::sys::socket::setsockopt(
            fd,
            nix::sys::socket::sockopt::KeepAlive,
            &state.conf.tcp_keepalive,
        )?;
        nix::sys::socket::setsockopt(
            server.as_raw_fd(),
            nix::sys::socket::sockopt::KeepAlive,
            &state.conf.tcp_keepalive,
        )?;
    }

    let server = zenith_utils::sock_split::BidiStream::from_tcp(server);

    let client = match client {
        Stream::Bidirectional(bidi_stream) => bidi_stream,
        _ => panic!("invalid stream type"),
    };
    proxy(client.split(), server.split())
}

impl ProxyConnection {
    /// Returns Ok(None) when connection was successfully closed.
    fn handle_client(mut self) -> anyhow::Result<Option<(Stream, TcpStream)>> {
        let mut authenticate = || {
            let (username, dbname) = match self.handle_startup()? {
                Some(x) => x,
                None => return Ok(None),
            };

            // Both scenarios here should end up producing database credentials
            if username.ends_with("@zenith") {
                self.handle_existing_user(&username, &dbname).map(Some)
            } else {
                self.handle_new_user().map(Some)
            }
        };

        let conn = match authenticate() {
            Ok(Some(db_info)) => connect_to_db(db_info),
            Ok(None) => return Ok(None),
            Err(e) => {
                // Report the error to the client
                self.pgb.write_message(&Be::ErrorResponse(&e.to_string()))?;
                bail!("failed to handle client: {:?}", e);
            }
        };

        // We'll get rid of this once migration to async is complete
        let (pg_version, db_stream) = {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;

            let (pg_version, stream, cancel_key_data) = runtime.block_on(conn)?;
            self.pgb
                .write_message(&BeMessage::BackendKeyData(cancel_key_data))?;
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

    /// Returns Ok(None) when connection was successfully closed.
    fn handle_startup(&mut self) -> anyhow::Result<Option<(String, String)>> {
        let have_tls = self.pgb.tls_config.is_some();
        let mut encrypted = false;

        loop {
            let msg = match self.pgb.read_message()? {
                Some(Fe::StartupPacket(msg)) => msg,
                None => bail!("connection is lost"),
                bad => bail!("unexpected message type: {:?}", bad),
            };
            println!("got message: {:?}", msg);

            match msg {
                FeStartupPacket::GssEncRequest => {
                    self.pgb.write_message(&Be::EncryptionResponse(false))?;
                }
                FeStartupPacket::SslRequest => {
                    self.pgb.write_message(&Be::EncryptionResponse(have_tls))?;
                    if have_tls {
                        self.pgb.start_tls()?;
                        encrypted = true;
                    }
                }
                FeStartupPacket::StartupMessage { mut params, .. } => {
                    if have_tls && !encrypted {
                        bail!("must connect with TLS");
                    }

                    let mut get_param = |key| {
                        params
                            .remove(key)
                            .with_context(|| format!("{} is missing in startup packet", key))
                    };

                    return Ok(Some((get_param("user")?, get_param("database")?)));
                }
                FeStartupPacket::CancelRequest(cancel_key_data) => {
                    if let Some(cancel_closure) = CANCEL_MAP.lock().unwrap().get(&cancel_key_data) {
                        let runtime = tokio::runtime::Builder::new_current_thread()
                            .enable_all()
                            .build()
                            .unwrap();
                        runtime.block_on(cancel_closure.try_cancel_query());
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
async fn connect_to_db(
    db_info: DatabaseInfo,
) -> anyhow::Result<(String, tokio::net::TcpStream, CancelKeyData)> {
    // Make raw connection. When connect_raw finishes we've received ReadyForQuery.
    let socket_addr = db_info.socket_addr()?;
    let mut socket = tokio::net::TcpStream::connect(socket_addr).await?;
    let config = tokio_postgres::Config::from(db_info);
    // NOTE We effectively ignore some ParameterStatus and NoticeResponse
    //      messages here. Not sure if that could break something.
    let (client, conn) = config.connect_raw(&mut socket, NoTls).await?;

    // Save info for potentially cancelling the query later
    let mut rng = StdRng::from_entropy();
    let cancel_key_data = CancelKeyData {
        // HACK We'd rather get the real backend_pid but tokio_postgres doesn't
        //      expose it and we don't want to do another roundtrip to query
        //      for it. The client will be able to notice that this is not the
        //      actual backend_pid, but backend_pid is not used for anything
        //      so it doesn't matter.
        backend_pid: rng.gen(),
        cancel_key: rng.gen(),
    };
    let cancel_closure = CancelClosure {
        socket_addr,
        cancel_token: client.cancel_token(),
    };
    CANCEL_MAP
        .lock()
        .unwrap()
        .insert(cancel_key_data, cancel_closure);
    THREAD_CANCEL_KEY_DATA.with(|cell| {
        let prev_value = cell.replace(Some(cancel_key_data));
        assert!(
            prev_value.is_none(),
            "THREAD_CANCEL_KEY_DATA was already set"
        );
    });

    let version = conn.parameter("server_version").unwrap();
    Ok((version.into(), socket, cancel_key_data))
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
                if let Ok(count) = res {
                    NUM_BYTES_PROXIED_COUNTER.inc_by(count as u64);
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
