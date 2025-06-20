use tokio::net::TcpStream;

use crate::client::SocketConfig;
use crate::config::Host;
use crate::tls::MakeTlsConnect;
use crate::{Error, cancel_query_raw, connect_socket, connect_tls};

pub(crate) async fn cancel_query<T>(
    config: SocketConfig,
    tls: T,
    process_id: i32,
    secret_key: i32,
) -> Result<(), Error>
where
    T: MakeTlsConnect<TcpStream>,
{
    let hostname = match &config.host {
        Host::Tcp(host) => &**host,
    };
    let tls = tls
        .make_tls_connect(hostname)
        .map_err(|e| Error::tls(e.into()))?;

    let socket = connect_socket::connect_socket(
        config.host_addr,
        &config.host,
        config.port,
        config.connect_timeout,
    )
    .await?;

    let stream = connect_tls::connect_tls(socket, config.ssl_mode, tls).await?;
    cancel_query_raw::cancel_query_raw(stream, process_id, secret_key).await
}
