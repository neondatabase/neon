use std::{
    convert::Infallible,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::Context;
use indexmap::IndexMap;
use quinn::{Connection, Endpoint};
use tokio::{
    net::{TcpListener, TcpStream},
    time::timeout,
};
use tracing::error;

type AuthConnId = usize;
struct AuthConnState {
    conns: Mutex<IndexMap<AuthConnId, AuthConn>>,
}

struct AuthConn {
    conn: Connection,
    // latency info...
}

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _logging_guard = proxy::logging::init().await?;

    let auth_endpoint: Endpoint = endpoint_config("0.0.0.0:5634".parse().unwrap())
        .await
        .unwrap();

    let auth_connections = Arc::new(AuthConnState {
        conns: Mutex::new(IndexMap::new()),
    });

    let quinn_handle = tokio::spawn(quinn_server(auth_endpoint, auth_connections.clone()));

    let _frontend_handle = tokio::spawn(start_frontend("127.0.0.1:0"));

    quinn_handle.await.unwrap();

    Ok(())
}

async fn endpoint_config(addr: SocketAddr) -> anyhow::Result<Endpoint> {
    use quinn::rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};

    let mut params = rcgen::CertificateParams::new(vec!["pglb".to_string()]);
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "pglb");
    let key = rcgen::KeyPair::generate(&rcgen::PKCS_ECDSA_P256_SHA256).context("keygen")?;
    params.key_pair = Some(key);

    let cert = rcgen::Certificate::from_params(params).context("cert")?;
    let cert_der = cert.serialize_der().context("serialize")?;
    let key_der = cert.serialize_private_key_der();
    let cert = CertificateDer::from(cert_der);
    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key_der));

    let config = quinn::ServerConfig::with_single_cert(vec![cert], key).context("server config")?;
    Endpoint::server(config, addr).context("endpoint")
}

async fn quinn_server(ep: Endpoint, state: Arc<AuthConnState>) {
    loop {
        let incoming = ep.accept().await.expect("quinn server should not crash");
        let state = state.clone();
        tokio::spawn(async move {
            let conn = incoming.await.unwrap();

            let conn_id = conn.stable_id();
            println!("[{conn_id:?}] new conn");

            state
                .conns
                .lock()
                .unwrap()
                .insert(conn_id, AuthConn { conn: conn.clone() });

            // heartbeat loop
            loop {
                match timeout(Duration::from_secs(10), conn.accept_uni()).await {
                    Ok(Ok(mut heartbeat_stream)) => {
                        let data = heartbeat_stream.read_to_end(128).await.unwrap();
                        if data.starts_with(b"shutdown") {
                            println!("[{conn_id:?}] conn shutdown");
                            break;
                        }
                        // else update latency info
                    }
                    Ok(Err(conn_err)) => {
                        println!("[{conn_id:?}] conn err {conn_err:?}");
                        break;
                    }
                    Err(_) => {
                        println!("[{conn_id:?}] conn timeout err");
                        break;
                    }
                }
            }

            state.conns.lock().unwrap().remove(&conn_id);
            let conn_closed = conn.closed().await;
            println!("[{conn_id:?}] conn closed {conn_closed:?}");
        });
    }
}

async fn start_frontend(addr: &str) -> anyhow::Result<Infallible> {
    let addr: SocketAddr = addr.parse()?;
    let listener = TcpListener::bind(addr).await?;
    socket2::SockRef::from(&listener).set_keepalive(true)?;

    let connections = tokio_util::task::task_tracker::TaskTracker::new();

    loop {
        match listener.accept().await {
            Ok((socket, peer_addr)) => {
                connections.spawn(handle_frontend_connection(socket, peer_addr));
            }
            Err(e) => {}
        }
    }
}

async fn handle_frontend_connection(socket: TcpStream, _peer_addr: SocketAddr) {
    match socket.set_nodelay(true) {
        Ok(()) => {}
        Err(e) => {
            error!("per-client task finished with an error: failed to set socket option: {e:#}");
            return;
        }
    };

    // TODO: HAProxy protocol?
}

// TODO: client state machine
