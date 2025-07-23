use std::io;
use std::pin::Pin;
use std::task::{Context, Poll, ready};

use bytes::{Bytes, BytesMut};
use fallible_iterator::FallibleIterator;
use futures_util::{Sink, SinkExt, Stream, TryStreamExt};
use postgres_protocol2::authentication::sasl;
use postgres_protocol2::authentication::sasl::ScramSha256;
use postgres_protocol2::message::backend::{AuthenticationSaslBody, Message};
use postgres_protocol2::message::frontend;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_util::codec::{Framed, FramedParts, FramedWrite};

use crate::Error;
use crate::codec::PostgresCodec;
use crate::config::{self, AuthKeys, Config};
use crate::maybe_tls_stream::MaybeTlsStream;
use crate::tls::TlsStream;

pub struct StartupStream<S, T> {
    inner: FramedWrite<MaybeTlsStream<S, T>, PostgresCodec>,
    read_buf: BytesMut,
}

impl<S, T> Sink<Bytes> for StartupStream<S, T>
where
    S: AsyncRead + AsyncWrite + Unpin,
    T: AsyncRead + AsyncWrite + Unpin,
{
    type Error = io::Error;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_ready(cx)
    }

    fn start_send(mut self: Pin<&mut Self>, item: Bytes) -> io::Result<()> {
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

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // read 1 byte tag, 4 bytes length.
        let header = ready!(self.as_mut().poll_fill_buf_exact(cx, 5)?);

        let len = u32::from_be_bytes(header[1..5].try_into().unwrap());
        if len < 4 {
            return Poll::Ready(Some(Err(std::io::Error::other(
                "postgres message too small",
            ))));
        }
        if len >= 65536 {
            return Poll::Ready(Some(Err(std::io::Error::other(
                "postgres message too large",
            ))));
        }

        // the tag is an additional byte.
        let _message = ready!(self.as_mut().poll_fill_buf_exact(cx, len as usize + 1)?);

        // Message::parse will remove the all the bytes from the buffer.
        Poll::Ready(Message::parse(&mut self.read_buf).transpose())
    }
}

impl<S, T> StartupStream<S, T>
where
    S: AsyncRead + AsyncWrite + Unpin,
    T: AsyncRead + AsyncWrite + Unpin,
{
    /// Fill the buffer until it's the exact length provided. No additional data will be read from the socket.
    ///
    /// If the current buffer length is greater, nothing happens.
    fn poll_fill_buf_exact(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        len: usize,
    ) -> Poll<Result<&[u8], std::io::Error>> {
        let this = self.get_mut();
        let mut stream = Pin::new(this.inner.get_mut());

        let mut n = this.read_buf.len();
        while n < len {
            this.read_buf.resize(len, 0);

            let mut buf = ReadBuf::new(&mut this.read_buf[..]);
            buf.set_filled(n);

            if stream.as_mut().poll_read(cx, &mut buf)?.is_pending() {
                this.read_buf.truncate(n);
                return Poll::Pending;
            }

            if buf.filled().len() == n {
                return Poll::Ready(Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "early eof",
                )));
            }
            n = buf.filled().len();

            this.read_buf.truncate(n);
        }

        Poll::Ready(Ok(&this.read_buf[..len]))
    }

    pub fn into_framed(mut self) -> Framed<MaybeTlsStream<S, T>, PostgresCodec> {
        let write_buf = std::mem::take(self.inner.write_buffer_mut());
        let io = self.inner.into_inner();
        let mut parts = FramedParts::new(io, PostgresCodec);
        parts.read_buf = self.read_buf;
        parts.write_buf = write_buf;
        Framed::from_parts(parts)
    }

    pub fn new(io: MaybeTlsStream<S, T>) -> Self {
        Self {
            inner: FramedWrite::new(io, PostgresCodec),
            read_buf: BytesMut::new(),
        }
    }
}

pub(crate) async fn startup<S, T>(
    stream: &mut StartupStream<S, T>,
    config: &Config,
) -> Result<(), Error>
where
    S: AsyncRead + AsyncWrite + Unpin,
    T: AsyncRead + AsyncWrite + Unpin,
{
    let mut buf = BytesMut::new();
    frontend::startup_message(&config.server_params, &mut buf).map_err(Error::encode)?;

    stream.send(buf.freeze()).await.map_err(Error::io)
}

pub(crate) async fn authenticate<S, T>(
    stream: &mut StartupStream<S, T>,
    config: &Config,
) -> Result<(), Error>
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
            ));
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

    stream.send(buf.freeze()).await.map_err(Error::io)
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
    stream.send(buf.freeze()).await.map_err(Error::io)?;

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
    stream.send(buf.freeze()).await.map_err(Error::io)?;

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
