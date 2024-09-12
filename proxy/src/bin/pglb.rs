use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::Context;
use quinn::{Connection, Endpoint};
use tokio::time::timeout;

struct ConnState {
    conns: Mutex<HashMap<usize, Conn>>,
}

struct Conn {
    conn: Connection,
    // latency info...
}

#[tokio::main]
async fn main() {
    let endpoint: Endpoint = endpoint_config("0.0.0.0:5634".parse().unwrap())
        .await
        .unwrap();

    let connections = Arc::new(ConnState {
        conns: Mutex::new(HashMap::new()),
    });

    let quinn_handle = tokio::spawn(quinn_server(endpoint.clone(), connections.clone()));

    // tcp listener goes here

    quinn_handle.await.unwrap();
}

async fn endpoint_config(addr: SocketAddr) -> anyhow::Result<Endpoint> {
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};

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

async fn quinn_server(ep: Endpoint, state: Arc<ConnState>) {
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
                .insert(conn_id, Conn { conn: conn.clone() });

            loop {
                match timeout(Duration::from_secs(1), conn.accept_uni()).await {
                    Ok(Ok(_)) => {}
                    Ok(Err(conn_err)) => {
                        println!("[{conn_id:?}] conn err {conn_err:?}");
                        state.conns.lock().unwrap().remove(&conn_id);
                        break;
                    }
                    Err(_) => {
                        println!("[{conn_id:?}] conn timeout err");
                        state.conns.lock().unwrap().remove(&conn_id);
                        break;
                    }
                }
            }
        });
    }
}
