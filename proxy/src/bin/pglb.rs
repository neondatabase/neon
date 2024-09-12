use std::{
    convert::Infallible,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{anyhow, bail, Context, Result};
use bytes::BytesMut;
use futures::sink::SinkExt;
use indexmap::IndexMap;
use proxy::{
    config::{CertResolver, TlsServerEndPoint, PG_ALPN_PROTOCOL},
    ConnectionInitiatedPayload, PglbCodec,
};
use quinn::{Connection, Endpoint, RecvStream, SendStream};
use rand::Rng;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use tokio::{
    io::{join, AsyncReadExt, Join},
    net::{TcpListener, TcpStream},
    time::timeout,
};
use tokio_rustls::server::TlsStream;
use tokio_util::codec::Framed;
use tracing::{error, warn};

type AuthConnId = usize;

#[derive(Debug)]
struct AuthConnState {
    conns: Mutex<IndexMap<AuthConnId, AuthConn>>,
}

#[derive(Clone, Debug)]
struct AuthConn {
    conn: Connection,
    // latency info...
}

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[tokio::main]
async fn main() -> Result<()> {
    let _logging_guard = proxy::logging::init().await?;

    let auth_endpoint: Endpoint = endpoint_config("0.0.0.0:5634".parse()?).await?;

    let auth_connections = Arc::new(AuthConnState {
        conns: Mutex::new(IndexMap::new()),
    });

    let quinn_handle = tokio::spawn(quinn_server(auth_endpoint, auth_connections.clone()));

    let frontend_config = frontent_tls_config("pglb-fe", "pglb-fe")?;

    let _frontend_handle = tokio::spawn(start_frontend(
        "0.0.0.0:5432".parse()?,
        frontend_config,
        auth_connections.clone(),
    ));

    quinn_handle.await.unwrap();

    Ok(())
}

async fn endpoint_config(addr: SocketAddr) -> Result<Endpoint> {
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

fn frontent_tls_config(hostname: &str, common_name: &str) -> Result<TlsConfig> {
    let ca = rcgen::Certificate::from_params({
        let mut params = rcgen::CertificateParams::default();
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        params
    })?;

    let cert = rcgen::Certificate::from_params({
        let mut params = rcgen::CertificateParams::new(vec![hostname.into()]);
        params.distinguished_name = rcgen::DistinguishedName::new();
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, common_name);
        params
    })?;

    let (cert, key) = (
        CertificateDer::from(cert.serialize_der_with_signer(&ca)?),
        PrivateKeyDer::Pkcs8(cert.serialize_private_key_der().into()),
    );

    let config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert.clone()], key.clone_key())?
        .into();

    let mut cert_resolver = CertResolver::new();
    cert_resolver.add_cert(key, vec![cert], true)?;

    Ok(TlsConfig {
        config,
        cert_resolver: Arc::new(cert_resolver),
    })
}

async fn start_frontend(
    addr: SocketAddr,
    tls: TlsConfig,
    state: Arc<AuthConnState>,
) -> Result<Infallible> {
    let listener = TcpListener::bind(addr).await?;
    socket2::SockRef::from(&listener).set_keepalive(true)?;

    let connections = tokio_util::task::task_tracker::TaskTracker::new();

    loop {
        match listener.accept().await {
            Ok((socket, client_addr)) => {
                let conn = PglbConn::new(&state, &tls)?;
                connections.spawn(conn.handle(socket, client_addr));
            }
            Err(e) => {
                error!("connection accept error: {e}");
            }
        }
    }
}

#[derive(Clone, Debug)]
struct TlsConfig {
    config: Arc<rustls::ServerConfig>,
    cert_resolver: Arc<CertResolver>,
}

#[derive(Debug)]
struct PglbConn<S: PglbConnState> {
    inner: PglbConnInner,
    state: S,
}

#[derive(Debug)]
struct PglbConnInner {
    tls_config: TlsConfig,
    auth_conns: Arc<AuthConnState>,
}

trait PglbConnState: std::fmt::Debug {}
impl PglbConnState for Start {}
impl PglbConnState for ClientConnect {}
impl PglbConnState for AuthPassthrough {}
impl PglbConnState for ComputeConnect {}
impl PglbConnState for ComputePassthrough {}
impl PglbConnState for End {}

#[derive(Debug)]
struct Start;

impl PglbConn<Start> {
    fn new(auth_conns: &Arc<AuthConnState>, tls_config: &TlsConfig) -> Result<Self> {
        Ok(PglbConn {
            inner: PglbConnInner {
                auth_conns: Arc::clone(auth_conns),
                tls_config: tls_config.clone(),
            },
            state: Start,
        })
    }

    async fn handle(
        self,
        client_stream: TcpStream,
        client_addr: SocketAddr,
    ) -> Result<PglbConn<End>> {
        self.handle_start(client_stream, client_addr)
            .await?
            .handle_client_connect()
            .await?
            .handle_auth_passthrough()
            .await?
            .handle_connect_connect()
            .await?
            .handle_compute_passthrough()
            .await
    }
}

impl PglbConn<Start> {
    async fn handle_start(
        self,
        mut client_stream: TcpStream,
        client_addr: SocketAddr,
    ) -> Result<PglbConn<ClientConnect>> {
        match client_stream.set_nodelay(true) {
            Ok(()) => {}
            Err(e) => {
                bail!("socket option error: {e}");
            }
        };

        // TODO: HAProxy protocol

        let tls_requested = match Self::handle_ssl_request_message(&mut client_stream).await {
            Ok(tls_requested) => tls_requested,
            Err(e) => {
                bail!("check_for_ssl_request: {e}");
            }
        };

        let (client_stream, payload) = if tls_requested {
            let (stream, tls_server_end_point, server_name) =
                match Self::tls_upgrade(client_stream, self.inner.tls_config.clone()).await {
                    Ok((stream, ep, sn)) => (stream, ep, sn),
                    Err(e) => {
                        bail!("tls_upgrade: {e}");
                    }
                };

            (
                stream,
                ConnectionInitiatedPayload {
                    tls_server_end_point,
                    server_name,
                    ip_addr: client_addr.ip(),
                },
            )
        } else {
            // TODO: support unsecured connections?
            bail!("closing non-TLS connection");
        };

        Ok(PglbConn {
            inner: self.inner,
            state: ClientConnect {
                client_stream,
                payload,
            },
        })
    }

    async fn handle_ssl_request_message(stream: &mut TcpStream) -> Result<bool> {
        let mut buf = BytesMut::with_capacity(8);

        let n_peek = stream.peek(&mut buf).await?;
        if n_peek == 0 {
            bail!("EOF");
        }

        assert_eq!(buf.len(), 8); // TODO: loop, read more

        if buf.len() != 8
            || buf[0..4] != 8u32.to_be_bytes()
            || buf[4..8] != 80877103u32.to_be_bytes()
        {
            return Ok(false);
        }

        buf.clear();
        let n_read = stream.read(&mut buf).await?;

        assert_eq!(n_peek, n_read); // TODO: loop, read more

        Ok(true)
    }

    async fn tls_upgrade(
        stream: TcpStream,
        tls: TlsConfig,
    ) -> Result<(TlsStream<TcpStream>, TlsServerEndPoint, Option<String>)> {
        let tls_stream = tokio_rustls::TlsAcceptor::from(tls.config)
            .accept(stream)
            .await?;

        let conn_info = tls_stream.get_ref().1;
        let server_name = conn_info.server_name().map(|s| s.to_string());

        match conn_info.alpn_protocol() {
            None | Some(PG_ALPN_PROTOCOL) => {}
            Some(other) => {
                let alpn = String::from_utf8_lossy(other);
                warn!(%alpn, "unexpected ALPN");
                bail!("protocol violation");
            }
        }

        let (_, tls_server_end_point) = tls
            .cert_resolver
            .resolve(server_name.as_deref())
            .ok_or(anyhow!("missing cert"))?;

        Ok((tls_stream, tls_server_end_point, server_name))
    }
}

#[derive(Debug)]
struct ClientConnect {
    client_stream: TlsStream<TcpStream>,
    payload: ConnectionInitiatedPayload,
}

impl PglbConn<ClientConnect> {
    async fn handle_client_connect(self) -> Result<PglbConn<AuthPassthrough>> {
        let auth_conn = {
            let conns = self.inner.auth_conns.conns.lock().unwrap();
            if conns.is_empty() {
                bail!("no auth proxies avaiable");
            }

            let mut rng = rand::thread_rng();
            conns
                .get_index(rng.gen_range(0..conns.len()))
                .unwrap()
                .1
                .clone()

            // TODO: check closed?
        };

        let (send, recv) = auth_conn.conn.open_bi().await?;
        let mut auth_stream = Framed::new(join(recv, send), PglbCodec);

        auth_stream
            .send(proxy::PglbMessage::Control(
                proxy::PglbControlMessage::ConnectionInitiated(self.state.payload),
            ))
            .await?;

        Ok(PglbConn {
            inner: self.inner,
            state: AuthPassthrough {
                client_stream: self.state.client_stream,
                auth_stream,
            },
        })
    }
}

#[derive(Debug)]
struct AuthPassthrough {
    client_stream: TlsStream<TcpStream>,
    auth_stream: Framed<Join<RecvStream, SendStream>, PglbCodec>,
}

impl PglbConn<AuthPassthrough> {
    async fn handle_auth_passthrough(self) -> Result<PglbConn<ComputeConnect>> {
        todo!()
    }
}

#[derive(Debug)]
struct ComputeConnect {
    client_stream: TlsStream<TcpStream>,
    auth_stream: Framed<Join<RecvStream, SendStream>, PglbCodec>,
}

impl PglbConn<ComputeConnect> {
    async fn handle_connect_connect(self) -> Result<PglbConn<ComputePassthrough>> {
        todo!()
    }
}

#[derive(Debug)]
struct ComputePassthrough {
    client_stream: TlsStream<TcpStream>,
    compute_conn: (),
}

impl PglbConn<ComputePassthrough> {
    async fn handle_compute_passthrough(self) -> Result<PglbConn<End>> {
        todo!()
    }
}

#[derive(Debug)]
struct End;
