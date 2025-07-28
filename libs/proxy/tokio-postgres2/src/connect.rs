use std::net::IpAddr;

use futures_util::TryStreamExt;
use postgres_protocol2::message::backend::Message;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::sync::mpsc;

use crate::client::SocketConfig;
use crate::config::{Host, SslMode};
use crate::connect_raw::StartupStream;
use crate::connect_socket::connect_socket;
use crate::tls::{MakeTlsConnect, TlsConnect};
use crate::{Client, Config, Connection, Error};

pub async fn connect<T>(
    tls: &T,
    config: &Config,
) -> Result<(Client, Connection<TcpStream, T::Stream>), Error>
where
    T: MakeTlsConnect<TcpStream>,
{
    let hostname = match &config.host {
        Host::Tcp(host) => host.as_str(),
    };

    let tls = tls
        .make_tls_connect(hostname)
        .map_err(|e| Error::tls(e.into()))?;

    match connect_once(config.host_addr, &config.host, config.port, tls, config).await {
        Ok((client, connection)) => Ok((client, connection)),
        Err(e) => Err(e),
    }
}

async fn connect_once<T>(
    host_addr: Option<IpAddr>,
    host: &Host,
    port: u16,
    tls: T,
    config: &Config,
) -> Result<(Client, Connection<TcpStream, T::Stream>), Error>
where
    T: TlsConnect<TcpStream>,
{
    let socket = connect_socket(host_addr, host, port, config.connect_timeout).await?;
    let stream = config.tls_and_authenticate(socket, tls).await?;
    managed(
        stream,
        host_addr,
        host.clone(),
        port,
        config.ssl_mode,
        config.connect_timeout,
    )
    .await
}

pub async fn managed<TlsStream>(
    mut stream: StartupStream<TcpStream, TlsStream>,
    host_addr: Option<IpAddr>,
    host: Host,
    port: u16,
    ssl_mode: SslMode,
    connect_timeout: Option<std::time::Duration>,
) -> Result<(Client, Connection<TcpStream, TlsStream>), Error>
where
    TlsStream: AsyncRead + AsyncWrite + Unpin,
{
    let (process_id, secret_key) = wait_until_ready(&mut stream).await?;

    let socket_config = SocketConfig {
        host_addr,
        host,
        port,
        connect_timeout,
    };

    let mut stream = stream.into_framed();
    let write_buf = std::mem::take(stream.write_buffer_mut());

    let (client_tx, conn_rx) = mpsc::unbounded_channel();
    let (conn_tx, client_rx) = mpsc::channel(4);
    let client = Client::new(
        client_tx,
        client_rx,
        socket_config,
        ssl_mode,
        process_id,
        secret_key,
        write_buf,
    );

    let connection = Connection::new(stream, conn_tx, conn_rx);

    Ok((client, connection))
}

async fn wait_until_ready<S, T>(stream: &mut StartupStream<S, T>) -> Result<(i32, i32), Error>
where
    S: AsyncRead + AsyncWrite + Unpin,
    T: AsyncRead + AsyncWrite + Unpin,
{
    let mut process_id = 0;
    let mut secret_key = 0;

    loop {
        match stream.try_next().await.map_err(Error::io)? {
            Some(Message::BackendKeyData(body)) => {
                process_id = body.process_id();
                secret_key = body.secret_key();
            }
            // These values are currently not used by `Client`/`Connection`. Ignore them.
            Some(Message::ParameterStatus(_)) | Some(Message::NoticeResponse(_)) => {}
            Some(Message::ReadyForQuery(_)) => return Ok((process_id, secret_key)),
            Some(Message::ErrorResponse(body)) => return Err(Error::db(body)),
            Some(_) => return Err(Error::unexpected_message()),
            None => return Err(Error::closed()),
        }
    }
}
