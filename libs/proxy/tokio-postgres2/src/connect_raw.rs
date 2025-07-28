use std::io;
use std::pin::Pin;
use std::task::{Context, Poll, ready};

use bytes::BytesMut;
use fallible_iterator::FallibleIterator;
use futures_util::{SinkExt, Stream, TryStreamExt};
use postgres_protocol2::authentication::sasl;
use postgres_protocol2::authentication::sasl::ScramSha256;
use postgres_protocol2::message::backend::{AuthenticationSaslBody, Message};
use postgres_protocol2::message::frontend;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_util::codec::{Framed, FramedParts};

use crate::Error;
use crate::codec::PostgresCodec;
use crate::config::{self, AuthKeys, Config};
use crate::connection::{GC_THRESHOLD, INITIAL_CAPACITY};
use crate::maybe_tls_stream::MaybeTlsStream;
use crate::tls::TlsStream;

pub struct StartupStream<S, T> {
    inner: Framed<MaybeTlsStream<S, T>, PostgresCodec>,
    read_buf: BytesMut,
}

impl<S, T> Stream for StartupStream<S, T>
where
    S: AsyncRead + AsyncWrite + Unpin,
    T: AsyncRead + AsyncWrite + Unpin,
{
    type Item = io::Result<Message>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // We don't use `self.inner.poll_next()` as that might over-read into the read buffer.

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
        *self.inner.read_buffer_mut() = self.read_buf;
        self.inner
    }

    pub fn new(io: MaybeTlsStream<S, T>) -> Self {
        let mut parts = FramedParts::new(io, PostgresCodec);
        parts.write_buf = BytesMut::with_capacity(INITIAL_CAPACITY);

        let mut inner = Framed::from_parts(parts);

        // This is the default already, but nice to be explicit.
        // We divide by two because writes will overshoot the boundary.
        // We don't want constant overshoots to cause us to constantly re-shrink the buffer.
        inner.set_backpressure_boundary(GC_THRESHOLD / 2);

        Self {
            inner,
            read_buf: BytesMut::with_capacity(INITIAL_CAPACITY),
        }
    }
}

pub(crate) async fn authenticate<S, T>(
    stream: &mut StartupStream<S, T>,
    config: &Config,
) -> Result<(), Error>
where
    S: AsyncRead + AsyncWrite + Unpin,
    T: TlsStream + Unpin,
{
    frontend::startup_message(&config.server_params, stream.inner.write_buffer_mut())
        .map_err(Error::encode)?;

    stream.inner.flush().await.map_err(Error::io)?;
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

            frontend::password_message(pass, stream.inner.write_buffer_mut())
                .map_err(Error::encode)?;
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

    stream.inner.flush().await.map_err(Error::io)?;
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

    frontend::sasl_initial_response(mechanism, scram.message(), stream.inner.write_buffer_mut())
        .map_err(Error::encode)?;

    stream.inner.flush().await.map_err(Error::io)?;
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

    frontend::sasl_response(scram.message(), stream.inner.write_buffer_mut())
        .map_err(Error::encode)?;

    stream.inner.flush().await.map_err(Error::io)?;
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
