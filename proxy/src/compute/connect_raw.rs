use core::task;
use std::io;
use std::pin::Pin;
use std::sync::Arc;

use crate::pqproto::{
    AUTH_OK, AUTH_SASL, AUTH_SASL_CONT, AUTH_SASL_FINAL, FE_PASSWORD_MESSAGE, StartupMessageParams,
};
use crate::stream::{PostgresError, PqStream};
use crate::tls::TlsServerEndPoint;
use bytes::BufMut;
use futures::{FutureExt, TryFutureExt};
use postgres_client::config::SslMode;
use postgres_protocol::authentication::sasl;
use postgres_protocol::authentication::sasl::{SCRAM_SHA_256, SCRAM_SHA_256_PLUS};
use rustls::pki_types::{DnsName, ServerName};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_rustls::client::TlsStream;

use super::{Auth, TlsError};

pub async fn connect_tls<S>(
    mut stream: S,
    mode: SslMode,
    tls: &Arc<rustls::ClientConfig>,
    host: &str,
) -> Result<Stream<S>, TlsError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    match mode {
        SslMode::Disable => return Ok(Stream::Raw { raw: stream }),
        SslMode::Prefer | SslMode::Require => {}
    }

    if !PqStream::negotiate_tls(&mut stream).await? {
        if SslMode::Require == mode {
            return Err(TlsError::Required);
        }

        return Ok(Stream::Raw { raw: stream });
    }

    let tls = tokio_rustls::TlsConnector::from(tls.clone())
        .connect(
            ServerName::DnsName(DnsName::try_from_str(host)?.to_owned()),
            stream,
        )
        .map_ok(Box::new)
        .boxed()
        .await?;

    let tls_server_end_point = match tls.get_ref().1.peer_certificates() {
        Some([cert, ..]) => TlsServerEndPoint::new(cert)
            .inspect_err(|error| {
                tracing::error!(
                    ?error,
                    "could not parse TLS certificate for channel binding"
                );
            })
            .unwrap_or(TlsServerEndPoint::Undefined),
        _ => TlsServerEndPoint::Undefined,
    };

    Ok(Stream::Tls {
        tls,
        tls_server_end_point,
    })
}

pub async fn authenticate<S>(
    stream: Stream<S>,
    auth: Option<&Auth>,
    params: &StartupMessageParams,
) -> Result<PqStream<Stream<S>>, PostgresError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut stream = PqStream::new_startup(stream, params);
    stream.flush().await?;

    let channel_binding = match stream.get_ref().tls_server_end_point() {
        TlsServerEndPoint::Sha256(h) => Some(h),
        TlsServerEndPoint::Undefined => None,
    };

    // TODO: rather than checking for SASL, maybe we can just assume it.
    // With SCRAM_SHA_256 if we're not using TLS,
    // and SCRAM_SHA_256_PLUS if we are using TLS.
    // Problem: when testing locally, we don't use scram, so we cannot assume it.

    let (channel_binding, mechanism) = match stream.read_auth_message().await? {
        (AUTH_OK, _) => return Ok(stream),
        (AUTH_SASL, mechanisms) => {
            let mut has_scram = false;
            let mut has_scram_plus = false;
            for mechanism in mechanisms.split(|&b| b == b'0') {
                match mechanism {
                    b"SCRAM_SHA_256" => has_scram = true,
                    b"SCRAM_SHA_256_PLUS" => has_scram_plus = true,
                    _ => {}
                }
            }

            // we need at least one scram
            if !has_scram && !has_scram_plus {
                tracing::error!(
                    "compute responded with invalid auth mechanisms: {}",
                    String::from_utf8_lossy(mechanisms)
                );
                return Err(PostgresError::InvalidAuthMessage);
            }

            match channel_binding {
                Some(h) if has_scram_plus => (
                    sasl::ChannelBinding::tls_server_end_point(h.to_vec()),
                    SCRAM_SHA_256_PLUS,
                ),
                Some(_) => {
                    // I don't think this can happen in our setup, but I would like to monitor it.
                    tracing::warn!(
                        "TLS is enabled, but compute doesn't support SCRAM_SHA_256_PLUS."
                    );
                    (sasl::ChannelBinding::unrequested(), SCRAM_SHA_256)
                }
                None => (sasl::ChannelBinding::unsupported(), SCRAM_SHA_256),
            }
        }
        (tag, msg) => {
            tracing::error!(
                "compute responded with unexpected auth message with tag[{tag}]: {}",
                String::from_utf8_lossy(msg)
            );
            return Err(PostgresError::InvalidAuthMessage);
        }
    };

    let mut scram = match auth {
        // We only touch passwords when it comes to console-redirect.
        Some(Auth::Password(pw)) => sasl::ScramSha256::new(pw, channel_binding),
        Some(Auth::Scram(keys)) => sasl::ScramSha256::new_with_keys(**keys, channel_binding),
        None => {
            // local_proxy does not set credentials, since it relies on trust and expects an OK message above
            tracing::error!("compute requested SASL auth, but there are no credentials available",);
            return Err(PostgresError::InvalidAuthMessage);
        }
    };

    stream.write_raw(0, FE_PASSWORD_MESSAGE.0, |buf| {
        buf.put_slice(mechanism.as_bytes());
        buf.put_u8(b'0');

        let data = scram.message();
        buf.put_u32(data.len() as u32);
        buf.put_slice(data);
    });
    stream.flush().await?;

    loop {
        // wait for SASLContinue or SASLFinal.
        match stream.read_auth_message().await? {
            (AUTH_SASL_CONT, data) => scram.update(data).await?,
            (AUTH_SASL_FINAL, data) => {
                scram.finish(data)?;
                break;
            }
            (tag, msg) => {
                tracing::error!(
                    "compute responded with unexpected auth message with tag[{tag}]: {}",
                    String::from_utf8_lossy(msg)
                );
                return Err(PostgresError::InvalidAuthMessage);
            }
        }

        stream.write_raw(0, FE_PASSWORD_MESSAGE.0, |buf| {
            buf.put_slice(scram.message());
        });
        stream.flush().await?;
    }

    match stream.read_auth_message().await? {
        (AUTH_OK, _) => {}
        (tag, msg) => {
            tracing::error!(
                "compute responded with unexpected auth message with tag[{tag}]: {}",
                String::from_utf8_lossy(msg)
            );
            return Err(PostgresError::InvalidAuthMessage);
        }
    }

    Ok(stream)
}

/// Wrapper for upgrading raw streams into secure streams.
pub enum Stream<S> {
    /// We always begin with a raw stream,
    /// which may then be upgraded into a secure stream.
    Raw { raw: S },
    Tls {
        /// We box [`TlsStream`] since it can be quite large.
        tls: Box<TlsStream<S>>,
        /// Channel binding parameter
        tls_server_end_point: TlsServerEndPoint,
    },
}

impl<S: Unpin> Unpin for Stream<S> {}

impl<S> Stream<S> {
    pub(crate) fn tls_server_end_point(&self) -> TlsServerEndPoint {
        match self {
            Stream::Raw { .. } => TlsServerEndPoint::Undefined,
            Stream::Tls {
                tls_server_end_point,
                ..
            } => *tls_server_end_point,
        }
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncRead for Stream<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut task::Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> task::Poll<io::Result<()>> {
        match &mut *self {
            Self::Raw { raw } => Pin::new(raw).poll_read(context, buf),
            Self::Tls { tls, .. } => Pin::new(tls).poll_read(context, buf),
        }
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncWrite for Stream<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut task::Context<'_>,
        buf: &[u8],
    ) -> task::Poll<io::Result<usize>> {
        match &mut *self {
            Self::Raw { raw } => Pin::new(raw).poll_write(context, buf),
            Self::Tls { tls, .. } => Pin::new(tls).poll_write(context, buf),
        }
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        context: &mut task::Context<'_>,
    ) -> task::Poll<io::Result<()>> {
        match &mut *self {
            Self::Raw { raw } => Pin::new(raw).poll_flush(context),
            Self::Tls { tls, .. } => Pin::new(tls).poll_flush(context),
        }
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        context: &mut task::Context<'_>,
    ) -> task::Poll<io::Result<()>> {
        match &mut *self {
            Self::Raw { raw } => Pin::new(raw).poll_shutdown(context),
            Self::Tls { tls, .. } => Pin::new(tls).poll_shutdown(context),
        }
    }
}
