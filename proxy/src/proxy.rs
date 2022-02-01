use crate::auth::{self, AuthStream};
use crate::cplane_api::{CPlaneApi, DatabaseInfo};
use crate::ProxyState;
use crate::db::{AuthSecret, DatabaseAuthInfo, ScramAuthSecret};
use crate::scram::key::{SCRAM_KEY_LEN, ScramKey};
use anyhow::{anyhow, bail};
use hmac::digest::{FixedOutput};
use hmac::{Hmac, Mac, NewMac};
use lazy_static::lazy_static;
use parking_lot::Mutex;
use rand::prelude::StdRng;
use rand::{Fill, Rng, SeedableRng};
use sha2::{Digest, Sha256};
use std::cell::Cell;
use std::collections::HashMap;
use std::convert::TryInto;
use std::net::{SocketAddr, TcpStream};
use std::{io, thread};
use tokio_postgres::NoTls;
use zenith_utils::postgres_backend::{self, PostgresBackend, Stream};
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
        socket.set_nodelay(true).unwrap();

        // TODO Use a threadpool instead. Maybe use tokio's threadpool by
        //      spawning a future into its runtime. Tokio's JoinError should
        //      allow us to handle cleanup properly even if the future panics.
        thread::Builder::new()
            .name("Proxy thread".into())
            .spawn(move || {
                if let Err(err) = proxy_conn_main(state, socket) {
                    println!("error: {}", err);
                }

                // Clean up CANCEL_MAP.
                THREAD_CANCEL_KEY_DATA.with(|cell| {
                    if let Some(cancel_key_data) = cell.get() {
                        CANCEL_MAP.lock().remove(&cancel_key_data);
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

    let server = zenith_utils::sock_split::BidiStream::from_tcp(server);

    let client = match client {
        Stream::Bidirectional(bidi_stream) => bidi_stream,
        _ => panic!("invalid stream type"),
    };

    proxy(client.split(), server.split())
}

// HACK copied from tokio-postgres
// since postgres passwords are not required to exclude saslprep-prohibited
// characters or even be valid UTF8, we run saslprep if possible and otherwise
// return the raw password.
fn normalize(pass: &str) -> Vec<u8> {
    match stringprep::saslprep(pass) {
        Ok(pass) => pass.into_owned().into_bytes(),
        Err(_) => pass.as_bytes().to_vec(),
    }
}

// HACK copied from tokio-postgres
fn hi(str: &[u8], salt: &[u8], i: u32) -> [u8; 32] {
    let mut hmac = Hmac::<Sha256>::new_varkey(str).expect("HMAC is able to accept all key sizes");
    hmac.update(salt);
    hmac.update(&[0, 0, 0, 1]);
    let mut prev = hmac.finalize().into_bytes();

    let mut hi = prev;

    for _ in 1..i {
        let mut hmac = Hmac::<Sha256>::new_varkey(str).expect("already checked above");
        hmac.update(&prev);
        prev = hmac.finalize().into_bytes();

        for (hi, prev) in hi.iter_mut().zip(prev) {
            *hi ^= prev;
        }
    }

    hi.into()
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
            if true || username.ends_with("@zenith") {
                self.handle_existing_user(&username, &dbname).map(Some)
            } else {
                self.handle_new_user().map(Some)
            }
        };

        let conn = match authenticate() {
            Ok(Some(db_auth_info)) => connect_to_db(db_auth_info),
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
                            .ok_or_else(|| anyhow!("{} is missing in startup packet", key))
                    };

                    return Ok(Some((get_param("user")?, get_param("database")?)));
                }
                FeStartupPacket::CancelRequest(cancel_key_data) => {
                    if let Some(cancel_closure) = CANCEL_MAP.lock().get(&cancel_key_data) {
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

    fn handle_existing_user(&mut self, user: &str, db: &str) -> anyhow::Result<DatabaseAuthInfo> {
        let _cplane = CPlaneApi::new(&self.state.conf.auth_endpoint, &self.state.waiters);

        // TODO read from postres instance
        // I got these values from the proxy error log on key cache miss
        let salt = [180, 76, 114, 155, 212, 214, 236, 192, 101, 236, 235, 4, 212, 87, 25, 85];
        let iterations = 4096;

        // TODO read from CLI
        let password = "postgres";

        let normalized = normalize(password);
        let salted = hi(&normalized, &salt, iterations);

        let mut mac = Hmac::<Sha256>::new_varkey(&salted).unwrap();
        mac.update(b"Client Key");
        let client_key = mac.finalize().into_bytes();

        let mut mac = Hmac::<Sha256>::new_varkey(&salted).unwrap();
        mac.update(b"Server Key");
        let server_key = mac.finalize().into_bytes();

        let mut hash = Sha256::default();
        hash.update(client_key);
        let stored_key = hash.finalize_fixed();

        let secret = crate::scram::ScramSecret {
            iterations,
            salt_base64: base64::encode(salt),
            stored_key: ScramKey { bytes: stored_key.try_into()? },
            server_key: ScramKey { bytes: server_key.try_into()? },
        };

        // TODO: fetch secret from console
        // user='user' password='password'
        // let secret = crate::scram::ScramSecret::parse(
        //     &[
        //         "SCRAM-SHA-256",
        //         "4096:XiWzgkfGNyY3ipsz08PY+A==",
        //         &[
        //             "YMmirZHYtTB6erVDCxL4Zjn66Kn7RCfS+aV3qROV4o8=",
        //             "aCSKHnugk1l9Ut6VhO5VeeWsB8xhVdPk/NyEgjOJ3nk=",
        //         ]
        //         .join(":"),
        //     ]
        //     .join("$"),
        // )
        // .unwrap();

        AuthStream::new(&mut self.pgb)
            .begin(auth::Scram(&secret))?
            .authenticate()?;

        self.pgb
            .write_message_noflush(&Be::AuthenticationOk)?
            .write_message_noflush(&BeParameterStatusMessage::encoding())?;

        // TODO get this info from console and tell it to start the db
        let host = "127.0.0.1";
        let port = 5432;

        // TODO fish out the real client_key from the authenticate() call above
        // let client_key: ScramKey = [0; SCRAM_KEY_LEN].into();
        let client_key = ScramKey { bytes: client_key.as_slice().try_into().unwrap() };
        let scram_auth_secret = ScramAuthSecret {
            iterations: secret.iterations,
            salt_base64: secret.salt_base64,
            client_key,
            server_key: secret.server_key,
        };
        let auth_info = DatabaseAuthInfo {
            host: host.into(),
            port,
            dbname: db.into(),
            user: user.into(),
            auth_secret: AuthSecret::Scram(scram_auth_secret)
        };

        Ok(auth_info)
    }

    fn handle_new_user(&mut self) -> anyhow::Result<DatabaseAuthInfo> {
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

        let db_auth_info = DatabaseAuthInfo {
            host: db_info.host,
            port: db_info.port,
            dbname: db_info.dbname,
            user: db_info.user,
            auth_secret: AuthSecret::Password(db_info.password.unwrap())
        };

        Ok(db_auth_info)
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
    db_info: DatabaseAuthInfo,
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
    CANCEL_MAP.lock().insert(cancel_key_data, cancel_closure);
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
