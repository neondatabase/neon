use crate::client::SocketConfig;
use crate::codec::BackendMessage;
use crate::config::{Host, TargetSessionAttrs};
use crate::connect_raw::connect_raw;
use crate::connect_socket::connect_socket;
use crate::tls::{MakeTlsConnect, TlsConnect};
use crate::{Client, Config, Connection, Error, RawConnection, SimpleQueryMessage};
use futures_util::{future, pin_mut, Future, FutureExt, Stream};
use postgres_protocol2::message::backend::Message;
use std::io;
use std::task::Poll;
use tokio::net::TcpStream;
use tokio::sync::mpsc;

pub async fn connect<T>(
    mut tls: T,
    config: &Config,
) -> Result<(Client, Connection<TcpStream, T::Stream>), Error>
where
    T: MakeTlsConnect<TcpStream>,
{
    if config.host.is_empty() {
        return Err(Error::config("host missing".into()));
    }

    if config.port.len() > 1 && config.port.len() != config.host.len() {
        return Err(Error::config("invalid number of ports".into()));
    }

    let mut error = None;
    for (i, host) in config.host.iter().enumerate() {
        let port = config
            .port
            .get(i)
            .or_else(|| config.port.first())
            .copied()
            .unwrap_or(5432);

        let hostname = match host {
            Host::Tcp(host) => host.as_str(),
        };

        let tls = tls
            .make_tls_connect(hostname)
            .map_err(|e| Error::tls(e.into()))?;

        match connect_once(host, port, tls, config).await {
            Ok((client, connection)) => return Ok((client, connection)),
            Err(e) => error = Some(e),
        }
    }

    Err(error.unwrap())
}

async fn connect_once<T>(
    host: &Host,
    port: u16,
    tls: T,
    config: &Config,
) -> Result<(Client, Connection<TcpStream, T::Stream>), Error>
where
    T: TlsConnect<TcpStream>,
{
    let socket = connect_socket(host, port, config.connect_timeout).await?;
    let RawConnection {
        stream,
        parameters,
        delayed_notice,
        process_id,
        secret_key,
    } = connect_raw(socket, tls, config).await?;

    let socket_config = SocketConfig {
        host: host.clone(),
        port,
        connect_timeout: config.connect_timeout,
    };

    let (sender, receiver) = mpsc::unbounded_channel();
    let client = Client::new(
        sender,
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

    let mut connection = Connection::new(stream, delayed, parameters, receiver);

    if let TargetSessionAttrs::ReadWrite = config.target_session_attrs {
        let rows = client.simple_query_raw("SHOW transaction_read_only");
        pin_mut!(rows);

        let rows = future::poll_fn(|cx| {
            if connection.poll_unpin(cx)?.is_ready() {
                return Poll::Ready(Err(Error::closed()));
            }

            rows.as_mut().poll(cx)
        })
        .await?;
        pin_mut!(rows);

        loop {
            let next = future::poll_fn(|cx| {
                if connection.poll_unpin(cx)?.is_ready() {
                    return Poll::Ready(Some(Err(Error::closed())));
                }

                rows.as_mut().poll_next(cx)
            });

            match next.await.transpose()? {
                Some(SimpleQueryMessage::Row(row)) => {
                    if row.try_get(0)? == Some("on") {
                        return Err(Error::connect(io::Error::new(
                            io::ErrorKind::PermissionDenied,
                            "database does not allow writes",
                        )));
                    } else {
                        break;
                    }
                }
                Some(_) => {}
                None => return Err(Error::unexpected_message()),
            }
        }
    }

    Ok((client, connection))
}
