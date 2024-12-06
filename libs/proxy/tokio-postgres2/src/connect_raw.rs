use crate::codec::{BackendMessage, BackendMessages, FrontendMessage, PostgresCodec};
use crate::config::{self, AuthKeys, Config};
use crate::connect_tls::connect_tls;
use crate::maybe_tls_stream::MaybeTlsStream;
use crate::tls::{TlsConnect, TlsStream};
use crate::Error;
use bytes::BytesMut;
use fallible_iterator::FallibleIterator;
use futures_util::{ready, Sink, SinkExt, Stream, TryStreamExt};
use postgres_protocol2::authentication::sasl;
use postgres_protocol2::authentication::sasl::ScramSha256;
use postgres_protocol2::message::backend::{AuthenticationSaslBody, Message, NoticeResponseBody};
use postgres_protocol2::message::frontend;
use std::collections::HashMap;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::Framed;

pub struct StartupStream<S, T> {
    inner: Framed<MaybeTlsStream<S, T>, PostgresCodec>,
    buf: BackendMessages,
    delayed_notice: Vec<NoticeResponseBody>,
}

impl<S, T> Sink<FrontendMessage> for StartupStream<S, T>
where
    S: AsyncRead + AsyncWrite + Unpin,
    T: AsyncRead + AsyncWrite + Unpin,
{
    type Error = io::Error;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_ready(cx)
    }

    fn start_send(mut self: Pin<&mut Self>, item: FrontendMessage) -> io::Result<()> {
        Pin::new(&mut self.inner).start_send(item)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_close(cx)
    }
}

impl<S, T> Stream for StartupStream<S, T>
where
    S: AsyncRead + AsyncWrite + Unpin,
    T: AsyncRead + AsyncWrite + Unpin,
{
    type Item = io::Result<Message>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<io::Result<Message>>> {
        loop {
            match self.buf.next() {
                Ok(Some(message)) => return Poll::Ready(Some(Ok(message))),
                Ok(None) => {}
                Err(e) => return Poll::Ready(Some(Err(e))),
            }

            match ready!(Pin::new(&mut self.inner).poll_next(cx)) {
                Some(Ok(BackendMessage::Normal { messages, .. })) => self.buf = messages,
                Some(Ok(BackendMessage::Async(message))) => return Poll::Ready(Some(Ok(message))),
                Some(Err(e)) => return Poll::Ready(Some(Err(e))),
                None => return Poll::Ready(None),
            }
        }
    }
}

pub struct RawConnection<S, T> {
    pub stream: Framed<MaybeTlsStream<S, T>, PostgresCodec>,
    pub parameters: HashMap<String, String>,
    pub delayed_notice: Vec<NoticeResponseBody>,
    pub process_id: i32,
    pub secret_key: i32,
}

pub async fn connect_raw<S, T>(
    stream: S,
    tls: T,
    config: &Config,
) -> Result<RawConnection<S, T::Stream>, Error>
where
    S: AsyncRead + AsyncWrite + Unpin,
    T: TlsConnect<S>,
{
    let stream = connect_tls(stream, config.ssl_mode, tls).await?;

    let mut stream = StartupStream {
        inner: Framed::new(stream, PostgresCodec),
        buf: BackendMessages::empty(),
        delayed_notice: Vec::new(),
    };

    startup(&mut stream, config).await?;
    authenticate(&mut stream, config).await?;
    let (process_id, secret_key, parameters) = read_info(&mut stream).await?;

    Ok(RawConnection {
        stream: stream.inner,
        parameters,
        delayed_notice: stream.delayed_notice,
        process_id,
        secret_key,
    })
}

async fn startup<S, T>(stream: &mut StartupStream<S, T>, config: &Config) -> Result<(), Error>
where
    S: AsyncRead + AsyncWrite + Unpin,
    T: AsyncRead + AsyncWrite + Unpin,
{
    let mut buf = BytesMut::new();
    frontend::startup_message(&config.server_params, &mut buf).map_err(Error::encode)?;

    stream
        .send(FrontendMessage::Raw(buf.freeze()))
        .await
        .map_err(Error::io)
}

async fn authenticate<S, T>(stream: &mut StartupStream<S, T>, config: &Config) -> Result<(), Error>
where
    S: AsyncRead + AsyncWrite + Unpin,
    T: TlsStream + Unpin,
{
    match stream.try_next().await.map_err(Error::io)? {
        Some(Message::AuthenticationOk) => {
            can_skip_channel_binding(config)?;
            return Ok(());
        }
        Some(Message::AuthenticationCleartextPassword) => {
            can_skip_channel_binding(config)?;

            let pass = config
                .password
                .as_ref()
                .ok_or_else(|| Error::config("password missing".into()))?;

            authenticate_password(stream, pass).await?;
        }
        Some(Message::AuthenticationSasl(body)) => {
            authenticate_sasl(stream, body, config).await?;
        }
        Some(Message::AuthenticationMd5Password)
        | Some(Message::AuthenticationKerberosV5)
        | Some(Message::AuthenticationScmCredential)
        | Some(Message::AuthenticationGss)
        | Some(Message::AuthenticationSspi) => {
            return Err(Error::authentication(
                "unsupported authentication method".into(),
            ))
        }
        Some(Message::ErrorResponse(body)) => return Err(Error::db(body)),
        Some(_) => return Err(Error::unexpected_message()),
        None => return Err(Error::closed()),
    }

    match stream.try_next().await.map_err(Error::io)? {
        Some(Message::AuthenticationOk) => Ok(()),
        Some(Message::ErrorResponse(body)) => Err(Error::db(body)),
        Some(_) => Err(Error::unexpected_message()),
        None => Err(Error::closed()),
    }
}

fn can_skip_channel_binding(config: &Config) -> Result<(), Error> {
    match config.channel_binding {
        config::ChannelBinding::Disable | config::ChannelBinding::Prefer => Ok(()),
        config::ChannelBinding::Require => Err(Error::authentication(
            "server did not use channel binding".into(),
        )),
    }
}

async fn authenticate_password<S, T>(
    stream: &mut StartupStream<S, T>,
    password: &[u8],
) -> Result<(), Error>
where
    S: AsyncRead + AsyncWrite + Unpin,
    T: AsyncRead + AsyncWrite + Unpin,
{
    let mut buf = BytesMut::new();
    frontend::password_message(password, &mut buf).map_err(Error::encode)?;

    stream
        .send(FrontendMessage::Raw(buf.freeze()))
        .await
        .map_err(Error::io)
}

async fn authenticate_sasl<S, T>(
    stream: &mut StartupStream<S, T>,
    body: AuthenticationSaslBody,
    config: &Config,
) -> Result<(), Error>
where
    S: AsyncRead + AsyncWrite + Unpin,
    T: TlsStream + Unpin,
{
    let mut has_scram = false;
    let mut has_scram_plus = false;
    let mut mechanisms = body.mechanisms();
    while let Some(mechanism) = mechanisms.next().map_err(Error::parse)? {
        match mechanism {
            sasl::SCRAM_SHA_256 => has_scram = true,
            sasl::SCRAM_SHA_256_PLUS => has_scram_plus = true,
            _ => {}
        }
    }

    let channel_binding = stream
        .inner
        .get_ref()
        .channel_binding()
        .tls_server_end_point
        .filter(|_| config.channel_binding != config::ChannelBinding::Disable)
        .map(sasl::ChannelBinding::tls_server_end_point);

    let (channel_binding, mechanism) = if has_scram_plus {
        match channel_binding {
            Some(channel_binding) => (channel_binding, sasl::SCRAM_SHA_256_PLUS),
            None => (sasl::ChannelBinding::unsupported(), sasl::SCRAM_SHA_256),
        }
    } else if has_scram {
        match channel_binding {
            Some(_) => (sasl::ChannelBinding::unrequested(), sasl::SCRAM_SHA_256),
            None => (sasl::ChannelBinding::unsupported(), sasl::SCRAM_SHA_256),
        }
    } else {
        return Err(Error::authentication("unsupported SASL mechanism".into()));
    };

    if mechanism != sasl::SCRAM_SHA_256_PLUS {
        can_skip_channel_binding(config)?;
    }

    let mut scram = if let Some(AuthKeys::ScramSha256(keys)) = config.get_auth_keys() {
        ScramSha256::new_with_keys(keys, channel_binding)
    } else if let Some(password) = config.get_password() {
        ScramSha256::new(password, channel_binding)
    } else {
        return Err(Error::config("password or auth keys missing".into()));
    };

    let mut buf = BytesMut::new();
    frontend::sasl_initial_response(mechanism, scram.message(), &mut buf).map_err(Error::encode)?;
    stream
        .send(FrontendMessage::Raw(buf.freeze()))
        .await
        .map_err(Error::io)?;

    let body = match stream.try_next().await.map_err(Error::io)? {
        Some(Message::AuthenticationSaslContinue(body)) => body,
        Some(Message::ErrorResponse(body)) => return Err(Error::db(body)),
        Some(_) => return Err(Error::unexpected_message()),
        None => return Err(Error::closed()),
    };

    scram
        .update(body.data())
        .await
        .map_err(|e| Error::authentication(e.into()))?;

    let mut buf = BytesMut::new();
    frontend::sasl_response(scram.message(), &mut buf).map_err(Error::encode)?;
    stream
        .send(FrontendMessage::Raw(buf.freeze()))
        .await
        .map_err(Error::io)?;

    let body = match stream.try_next().await.map_err(Error::io)? {
        Some(Message::AuthenticationSaslFinal(body)) => body,
        Some(Message::ErrorResponse(body)) => return Err(Error::db(body)),
        Some(_) => return Err(Error::unexpected_message()),
        None => return Err(Error::closed()),
    };

    scram
        .finish(body.data())
        .map_err(|e| Error::authentication(e.into()))?;

    Ok(())
}

async fn read_info<S, T>(
    stream: &mut StartupStream<S, T>,
) -> Result<(i32, i32, HashMap<String, String>), Error>
where
    S: AsyncRead + AsyncWrite + Unpin,
    T: AsyncRead + AsyncWrite + Unpin,
{
    let mut process_id = 0;
    let mut secret_key = 0;
    let mut parameters = HashMap::new();

    loop {
        match stream.try_next().await.map_err(Error::io)? {
            Some(Message::BackendKeyData(body)) => {
                process_id = body.process_id();
                secret_key = body.secret_key();
            }
            Some(Message::ParameterStatus(body)) => {
                parameters.insert(
                    body.name().map_err(Error::parse)?.to_string(),
                    body.value().map_err(Error::parse)?.to_string(),
                );
            }
            Some(Message::NoticeResponse(body)) => stream.delayed_notice.push(body),
            Some(Message::ReadyForQuery(_)) => return Ok((process_id, secret_key, parameters)),
            Some(Message::ErrorResponse(body)) => return Err(Error::db(body)),
            Some(_) => return Err(Error::unexpected_message()),
            None => return Err(Error::closed()),
        }
    }
}
