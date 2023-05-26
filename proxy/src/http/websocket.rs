use crate::http::pg_to_json::postgres_row_to_json_value;
use crate::{
    auth, cancellation::CancelMap, config::ProxyConfig, console, error::io_error,
    proxy::handle_ws_client,
};
use bytes::{Buf, Bytes};
use futures::{Sink, Stream, StreamExt, TryStreamExt};
use tokio_postgres::Row;
use std::collections::HashMap;
use hyper::{
    server::{accept, conn::AddrIncoming},
    upgrade::Upgraded,
    Body, Method, Request, Response, StatusCode,
};
use hyper_tungstenite::{tungstenite::Message, HyperWebsocket, WebSocketStream};
use percent_encoding::percent_decode;
use pin_project_lite::pin_project;
use pq_proto::StartupMessageParams;
use serde_json::{Value, json};
use tokio::sync::Mutex;

use tokio_postgres::types::{ToSql};
use std::{
    convert::Infallible,
    future::ready,
    pin::Pin,
    sync::Arc,
    task::{ready, Context, Poll},
};
use tls_listener::TlsListener;
use tokio::{
    io::{self, AsyncBufRead, AsyncRead, AsyncWrite, ReadBuf},
    net::TcpListener,
};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, info_span, warn, Instrument};
use url::{Url};
use utils::http::{error::ApiError, json::json_response};

// TODO: use `std::sync::Exclusive` once it's stabilized.
// Tracking issue: https://github.com/rust-lang/rust/issues/98407.
use sync_wrapper::SyncWrapper;

pin_project! {
    /// This is a wrapper around a [`WebSocketStream`] that
    /// implements [`AsyncRead`] and [`AsyncWrite`].
    pub struct WebSocketRw {
        #[pin]
        stream: SyncWrapper<WebSocketStream<Upgraded>>,
        bytes: Bytes,
    }
}

impl WebSocketRw {
    pub fn new(stream: WebSocketStream<Upgraded>, startup_data: Bytes) -> Self {
        Self {
            stream: stream.into(),
            bytes: startup_data,
        }
    }
}

impl AsyncWrite for WebSocketRw {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let mut stream = self.project().stream.get_pin_mut();

        ready!(stream.as_mut().poll_ready(cx).map_err(io_error))?;
        match stream.as_mut().start_send(Message::Binary(buf.into())) {
            Ok(()) => Poll::Ready(Ok(buf.len())),
            Err(e) => Poll::Ready(Err(io_error(e))),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let stream = self.project().stream.get_pin_mut();
        stream.poll_flush(cx).map_err(io_error)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let stream = self.project().stream.get_pin_mut();
        stream.poll_close(cx).map_err(io_error)
    }
}

impl AsyncRead for WebSocketRw {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        
        if buf.remaining() > 0 {
            let bytes = ready!(self.as_mut().poll_fill_buf(cx))?;
            let len = std::cmp::min(bytes.len(), buf.remaining());
            buf.put_slice(&bytes[..len]);
            self.consume(len);
        }

        Poll::Ready(Ok(()))
    }
}

impl AsyncBufRead for WebSocketRw {
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<&[u8]>> {
        // Please refer to poll_fill_buf's documentation.
        const EOF: Poll<io::Result<&[u8]>> = Poll::Ready(Ok(&[]));

        let mut this = self.project();
        loop {
            if !this.bytes.chunk().is_empty() {
                let chunk = (*this.bytes).chunk();
                return Poll::Ready(Ok(chunk));
            }

            let res = ready!(this.stream.as_mut().get_pin_mut().poll_next(cx));
            match res.transpose().map_err(io_error)? {
                Some(message) => match message {
                    Message::Ping(_) => {}
                    Message::Pong(_) => {}
                    Message::Text(text) => {
                        // We expect to see only binary messages.
                        let error = "unexpected text message in the websocket";
                        warn!(length = text.len(), error);
                        return Poll::Ready(Err(io_error(error)));
                    }
                    Message::Frame(_) => {
                        // This case is impossible according to Frame's doc.
                        panic!("unexpected raw frame in the websocket");
                    }
                    Message::Binary(chunk) => {
                        assert!(this.bytes.is_empty());
                        *this.bytes = Bytes::from(chunk);
                    }
                    Message::Close(_) => return EOF,
                },
                None => return EOF,
            }
        }
    }

    fn consume(self: Pin<&mut Self>, amount: usize) {
        self.project().bytes.advance(amount);
    }
}

async fn serve_websocket(
    websocket: HyperWebsocket,
    config: &'static ProxyConfig,
    cancel_map: &CancelMap,
    session_id: uuid::Uuid,
    hostname: Option<String>,
    startup_data: Vec<u8>,
) -> anyhow::Result<()> {
    
    let websocket = websocket.await?;

    handle_ws_client(
        config,
        cancel_map,
        session_id,
        WebSocketRw::new(websocket, startup_data.into()),
        hostname,
    )
    .await?;
    Ok(())
}

struct MyObject {
    data: Vec<u8>,
    recv_data: Vec<u8>,
}

impl MyObject {
    fn new(data: Vec<u8>) -> Self {
        MyObject { 
            data, 
            recv_data: Vec::with_capacity(512),
        }
    }
}
impl AsyncRead for MyObject {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<Result<(), io::Error>> {

        let data = &self.get_mut().data;

        // let len: usize = recv_data.len();
        // info!("len: {}", len);
        // let mut i: usize = 0;
        // let mut zcount = 0;
        // while len >= i + 5 {
        //     let cmd = recv_data[i];
        //     if cmd == 0x5a { zcount += 1; }
        //     let size = u32::from_be_bytes(recv_data[(i + 1)..(i + 5)].try_into().unwrap());
        //     info!("cmd: {}  size: {}  buf: {:?}", cmd, size, recv_data);
        //     i += usize::try_from(size).unwrap() + 1;
        // }
        // if zcount < 2 {
        //     Poll::Pending

        // } else {
        //     let mut reader = &recv_data[..];
        //     Pin::new(&mut reader).poll_read(cx, buf)
        // }

        let mut reader = &data[..];
        Pin::new(&mut reader).poll_read(cx, buf)
    }
}

impl AsyncWrite for MyObject {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        
        // eprintln!("{:?}", buf);
        let recv_data = &mut self.get_mut().recv_data;
        recv_data.extend(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        // ...
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        // ...
        Poll::Ready(Ok(()))
    }
}


async fn ws_handler(
    mut request: Request<Body>,
    config: &'static ProxyConfig,
    cancel_map: Arc<CancelMap>,
    session_id: uuid::Uuid,
    cache: Arc<Mutex<ConnectionCache>>,
) -> Result<Response<Body>, ApiError> {

    let host = request
        .headers()
        .get("host")
        .and_then(|h| h.to_str().ok())
        .and_then(|h| h.split(':').next())
        .map(|s| s.to_string());

    // Check if the request is a websocket upgrade request.
    if hyper_tungstenite::is_upgrade_request(&request) {
        let startup_data = match request.uri().query() {
            Some(b64_str) => match base64::decode_config(b64_str, base64::URL_SAFE) {
                Ok(x) => x,
                Err(_) => {
                    eprintln!("invalid WebSocket base64 startup data");
                    vec![]
                }
            },
            None => vec![],
        };

        info!("{} bytes of startup data received via WebSocket URL query", startup_data.len());

        let (response, websocket) = hyper_tungstenite::upgrade(&mut request, None)
            .map_err(|e| ApiError::BadRequest(e.into()))?;

        tokio::spawn(async move {
            if let Err(e) = serve_websocket(websocket, config, &cancel_map, session_id, host, startup_data).await {
                error!("error in websocket connection: {e:?}");
            }
        });

        // Return the response so the spawned future can continue.
        Ok(response)

    } else if request.uri().path() == "/pg-protocol" && request.method() == Method::POST { 
        let mut body = request.into_body();
        let mut data = Vec::with_capacity(512);
        while let Some(chunk) = body.next().await {
            data.extend(&chunk.map_err(|e| ApiError::InternalServerError(e.into()))?);
        }

        let mut my_object = MyObject::new(data);
        let my_object = tokio::spawn(async move {
            let result = handle_ws_client(
                config,
                &cancel_map,
                session_id,
                &mut my_object,
                host,
            ).await;
            my_object
        })
        .await
        .map_err(|e| ApiError::InternalServerError(e.into()))?;

        let response = Response::builder()
          .header("Content-Type", "application/octet-stream")
          .header("Access-Control-Allow-Origin", "*")
          .status(StatusCode::OK)
          .body(Body::from(my_object.recv_data))
          .map_err(|e| ApiError::InternalServerError(e.into()))?;

        Ok(response)
    
    } else if request.uri().path() == "/sql" && request.method() == Method::POST {
        let result = handle_sql(config, request).await;
        let status_code = match result {
            Ok(_) => StatusCode::OK,
            Err(_) => StatusCode::BAD_REQUEST
        };
        let json = match result {
            Ok(r) => serde_json::to_value(r).unwrap(),
            Err(e) => {
                let message = format!("{:?}", e);
                let code = match e.downcast_ref::<tokio_postgres::Error>() {
                    Some(e) => match e.code() {
                        Some(e) => serde_json::to_value(e.code()).unwrap(),
                        None => Value::Null
                    },
                    None => Value::Null
                };
                json!({ "message": message, "code": code })
            }   
        };
        json_response(status_code, json).map(|mut r| {
            r.headers_mut().insert(
                "Access-Control-Allow-Origin",
                hyper::http::HeaderValue::from_static("*"),
            );
            r
        })

    } else if request.uri().path() == "/sleep" {
        // sleep 15ms
        tokio::time::sleep(std::time::Duration::from_millis(15)).await;
        json_response(StatusCode::OK, "done")

    } else {
        json_response(StatusCode::BAD_REQUEST, "query is not supported")
    }
}

fn boxed_from_json(json: Vec<Value>) -> Result<Vec<Box<dyn ToSql + Sync + Send>>, anyhow::Error> {
    json.iter().map(|value| {
        let boxed: Result<Box<dyn ToSql + Sync + Send>, anyhow::Error> = match value {
            Value::Bool(b) => Ok(Box::new(b.clone())),
            Value::Number(n) => Ok(Box::new(n.as_f64().unwrap())),
            Value::String(s) => Ok(Box::new(s.clone())),
            // TODO: support null (not like this: `Value::Null => Ok(Box::new(None::<bool>)),`)
            // TODO: support arrays            
            x => Err(anyhow::anyhow!("unsupported param {:?}", x))
        };
        boxed
    })
    .collect::<Result<Vec<_>, anyhow::Error>>()
}

// XXX: return different error codes
async fn handle_sql(
    config: &'static ProxyConfig,
    request: Request<Body>
) -> anyhow::Result<Vec<Value>> {

    let headers = request.headers();

    let connection_string = headers
        .get("X-Neon-ConnectionString")
        .ok_or(anyhow::anyhow!("missing connection string"))?
        .to_str()?;

    let connection_url = Url::parse(connection_string)?;

    let protocol = connection_url.scheme();
    if protocol != "postgres" && protocol != "postgresql" {
        return Err(anyhow::anyhow!("connection string must start with postgres: or postgresql:"))
    }

    let mut url_path = connection_url
        .path_segments()
        .ok_or(anyhow::anyhow!("missing database name"))?;

    let dbname = url_path
        .next()
        .ok_or(anyhow::anyhow!("invalid database name"))?;

    let username = connection_url.username();

    let password = connection_url
        .password()
        .ok_or(anyhow::anyhow!("no password"))?;

    let hostname = connection_url
        .host_str()
        .ok_or(anyhow::anyhow!("no host"))?;

    let host_header = request
        .headers()
        .get("host")
        .and_then(|h| h.to_str().ok())
        .and_then(|h| h.split(':').next());
    
    match host_header {
        Some(h) if h == hostname => h,
        Some(_) => return Err(anyhow::anyhow!("mismatched host header and hostname")),
        None => return Err(anyhow::anyhow!("no host header"))
    };

    let mut body = request.into_body();
    let mut data = Vec::with_capacity(512);
    while let Some(chunk) = body.next().await {
        data.extend(&chunk?);
    }
    
    #[derive(serde::Deserialize)]
    struct QueryData {
        query: String,
        params: Vec<serde_json::Value>
    }
    let query_data: QueryData = serde_json::from_slice(&data)?;

    let credential_params = StartupMessageParams::new([
        ("user", username),
        ("database", dbname),
        ("application_name", "proxy_http_sql"),
    ]);
    let tls = config.tls_config.as_ref();
    let common_names = tls.and_then(|tls| tls.common_names.clone());
    let creds = config
        .auth_backend
        .as_ref()
        .map(|_| auth::ClientCredentials::parse(&credential_params, Some(hostname), common_names))
        .transpose()?;

    let extra = console::ConsoleReqExtra {
        session_id: uuid::Uuid::new_v4(),
        application_name: Some("proxy_http_sql"),
    };

    let node = creds.wake_compute(&extra).await?.expect("msg");
    let conf = node.value.config;

    let host = match conf.get_hosts().first().expect("no host") {
        tokio_postgres::config::Host::Tcp(host) => host,
        tokio_postgres::config::Host::Unix(_) => {
            return Err(anyhow::anyhow!("unix socket is not supported"));
        }
    };

    let conn_string = &format!(
        "host={} port={} user={} password={} dbname={}",
        host,
        conf.get_ports().first().expect("no port"),
        username,
        password,
        dbname
    );

    let (client, connection) = tokio_postgres::connect(conn_string, tokio_postgres::NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    let query = &query_data.query;
    let query_params = boxed_from_json(query_data.params)?;

    // TODO: find a way to catch the panic and return an error if the number of params is wrong
    let pg_rows: Vec<Row> = client
        .query_raw(query, query_params)
        .await?
        .try_collect::<Vec<Row>>()
        .await?;

    let rows: Result<Vec<serde_json::Value>, anyhow::Error> = pg_rows
        .iter()
        .map(postgres_row_to_json_value)
        .collect();

    let rows = rows?;

    Ok(rows)
}

pub struct ConnectionCache {
    connections: HashMap<String, tokio_postgres::Client>,
}


impl ConnectionCache {
    pub fn new() -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(Self {
            connections: HashMap::new(),
        }))
    }

    pub async fn execute(
        cache: &Arc<Mutex<ConnectionCache>>,
        conn_string: &str,
        hostname: &str,
        sql: &str,
    ) -> anyhow::Result<String> {
        // TODO: let go mutex when establishing connection
        let mut cache = cache.lock().await;
        let cache_key = format!("connstr={}, hostname={}", conn_string, hostname);
        let client = if let Some(client) = cache.connections.get(&cache_key) {
            info!("using cached connection {}", conn_string);
            client
        } else {
            info!("!!!! connecting to: {}", conn_string);

            let (client, connection) =
                tokio_postgres::connect(conn_string, tokio_postgres::NoTls).await?;

            tokio::spawn(async move {
                // TODO: remove connection from cache
                if let Err(e) = connection.await {
                    eprintln!("connection error: {}", e);
                }
            });

            cache.connections.insert(cache_key.clone(), client);
            cache.connections.get(&cache_key).unwrap()
        };

        let sql = percent_decode(sql.as_bytes()).decode_utf8()?.to_string();

        let rows: Vec<HashMap<_, _>> = client
            .simple_query(&sql)
            .await?
            .into_iter()
            .filter_map(|el| {
                if let tokio_postgres::SimpleQueryMessage::Row(row) = el {
                    let mut serialized_row: HashMap<String, String> = HashMap::new();
                    for i in 0..row.len() {
                        let col = row.columns().get(i).map_or("?", |c| c.name());
                        let val = row.get(i).unwrap_or("?");
                        serialized_row.insert(col.into(), val.into());
                    }
                    Some(serialized_row)
                } else {
                    None
                }
            })
            .collect();

        Ok(serde_json::to_string(&rows)?)
    }
}


pub async fn task_main(
    config: &'static ProxyConfig,
    cache: &'static Arc<Mutex<ConnectionCache>>,
    ws_listener: TcpListener,
    cancellation_token: CancellationToken,
) -> anyhow::Result<()> {
    scopeguard::defer! {
        info!("websocket server has shut down");
    }

    let tls_config = config.tls_config.as_ref().map(|cfg| cfg.to_server_config());
    let tls_acceptor: tokio_rustls::TlsAcceptor = match tls_config {
        Some(config) => config.into(),
        None => {
            warn!("TLS config is missing, WebSocket Secure server will not be started");
            return Ok(());
        }
    };

    let mut addr_incoming = AddrIncoming::from_listener(ws_listener)?;
    let _ = addr_incoming.set_nodelay(true);

    let tls_listener = TlsListener::new(tls_acceptor, addr_incoming).filter(|conn| {
        if let Err(err) = conn {
            error!("failed to accept TLS connection for websockets: {err:?}");
            ready(false)
        } else {
            ready(true)
        }
    });

    let make_svc = hyper::service::make_service_fn(|_stream| async move {
        Ok::<_, Infallible>(hyper::service::service_fn(
            move |req: Request<Body>| async move {
                let cancel_map = Arc::new(CancelMap::default());
                let session_id = uuid::Uuid::new_v4();
                ws_handler(req, config, cancel_map, session_id, cache.clone())
                    .instrument(info_span!(
                        "ws-client",
                        session = format_args!("{session_id}")
                    ))
                    .await
            },
        ))
    });

    hyper::Server::builder(accept::from_stream(tls_listener))
        .serve(make_svc)
        .with_graceful_shutdown(cancellation_token.cancelled())
        .await?;

    Ok(())
}
