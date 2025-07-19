use std::net::IpAddr;

use tokio::net::TcpStream;
use tokio::sync::mpsc;

use crate::client::SocketConfig;
use crate::config::Host;
use crate::connect_raw::{connect_raw, read_info};
use crate::connect_socket::connect_socket;
use crate::connect_tls::connect_tls;
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
    let stream = connect_tls(socket, config.ssl_mode, tls).await?;
    let mut stream = connect_raw(stream, config).await?;

    let (process_id, secret_key, _, _) = read_info(&mut stream).await?;
    let stream = stream.into_framed();

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

    let connection = Connection::new(stream, conn_tx, conn_rx);

    Ok((client, connection))
}
