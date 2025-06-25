use std::net::IpAddr;

use postgres_protocol2::message::backend::Message;
use tokio::net::TcpStream;
use tokio::sync::mpsc;

use crate::client::SocketConfig;
use crate::codec::BackendMessage;
use crate::config::Host;
use crate::connect_raw::connect_raw;
use crate::connect_socket::connect_socket;
use crate::connect_tls::connect_tls;
use crate::tls::{MakeTlsConnect, TlsConnect};
use crate::{Client, Config, Connection, Error, RawConnection};

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
    let stream = connect_tls(socket, config.ssl_mode, tls).await?;
    let RawConnection {
        stream,
        parameters,
        delayed_notice,
        process_id,
        secret_key,
    } = connect_raw(stream, config).await?;

    let socket_config = SocketConfig {
        host_addr,
        host: host.clone(),
        port,
        connect_timeout: config.connect_timeout,
    };

    let (client_tx, conn_rx) = mpsc::unbounded_channel();
    let (conn_tx, client_rx) = mpsc::channel(4);
    let client = Client::new(
        client_tx,
        client_rx,
        socket_config,
        config.ssl_mode,
        process_id,
        secret_key,
    );

    // delayed notices are always sent as "Async" messages.
    let delayed = delayed_notice
        .into_iter()
        .map(|m| BackendMessage::Async(Message::NoticeResponse(m)))
        .collect();

    let connection = Connection::new(stream, delayed, parameters, conn_tx, conn_rx);

    Ok((client, connection))
}
