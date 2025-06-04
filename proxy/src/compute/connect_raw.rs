use core::task;
use std::io;
use std::pin::Pin;
use std::sync::Arc;

use crate::stream::{PostgresError, PqStream};
use crate::tls::TlsServerEndPoint;
use bytes::BufMut;
use futures::{FutureExt, TryFutureExt};
use postgres_client::config::{AuthKeys, Host, SslMode};
use postgres_client::{Config, config};
use postgres_protocol::authentication::sasl::{self, ChannelBinding, ScramSha256};
use postgres_protocol::authentication::sasl::{SCRAM_SHA_256, SCRAM_SHA_256_PLUS};
use rustls::pki_types::{DnsName, ServerName};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_rustls::client::TlsStream;

use super::{ConnectionError, TlsError};

pub async fn connect_raw<S>(
    stream: S,
    tls: Arc<rustls::ClientConfig>,
    config: &Config,
) -> Result<PqStream<Stream<S>>, ConnectionError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let Host::Tcp(host) = config.get_host();
    let stream = connect_tls(stream, config.get_ssl_mode(), tls, host).await?;

    let mut stream = PqStream::new_startup(stream, &config.server_params);
    authenticate(&mut stream, config).await?;

    Ok(stream)
}

pub async fn connect_tls<S>(
    mut stream: S,
    mode: SslMode,
    tls: Arc<rustls::ClientConfig>,
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

    let tls = tokio_rustls::TlsConnector::from(tls)
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

async fn authenticate<S>(
    stream: &mut PqStream<Stream<S>>,
    config: &Config,
) -> Result<(), PostgresError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let cb_mode = config.get_channel_binding();

    let channel_binding = match stream.get_ref().tls_server_end_point() {
        TlsServerEndPoint::Sha256(h) if cb_mode != config::ChannelBinding::Disable => Some(h),
        TlsServerEndPoint::Undefined if cb_mode == config::ChannelBinding::Require => {
            tracing::error!("SCRAM_SHA_256_PLUS is required but we're not using TLS");
            return Err(PostgresError::Io(io::Error::other("TLS not supported")));
        }
        _ => None,
    };

    // TODO: rather than checking for SASL, maybe we can just assume it.
    // With SCRAM_SHA_256 if we're not using TLS,
    // and SCRAM_SHA_256_PLUS if we are using TLS.
    // Problem: when testing locally, we don't use scram, so we cannot assume it.

    stream.flush().await?;

    let (channel_binding, mechanism) = match stream.read_auth_message().await? {
        // Ok.
        (0, _) => return Ok(()),
        // SASL
        (10, mechanisms) => {
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

            if !has_scram_plus && cb_mode == config::ChannelBinding::Require {
                tracing::error!("SCRAM_SHA_256_PLUS is required but not supported by compute");
                return Err(PostgresError::InvalidAuthMessage);
            }

            match channel_binding {
                Some(h) if has_scram_plus => (
                    ChannelBinding::tls_server_end_point(h.to_vec()),
                    SCRAM_SHA_256_PLUS,
                ),
                Some(_) => (sasl::ChannelBinding::unrequested(), SCRAM_SHA_256),
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

    let mut scram = if let Some(AuthKeys::ScramSha256(keys)) = config.get_auth_keys() {
        ScramSha256::new_with_keys(keys, channel_binding)
    } else if let Some(password) = config.get_password() {
        // We only touch passwords when it comes to console-redirect.
        ScramSha256::new(password, channel_binding)
    } else {
        // local_proxy does not set credentials, since it relies on trust and expects an OK message above
        tracing::error!("compute requested SASL auth, but there are no credentials available",);
        return Err(PostgresError::InvalidAuthMessage);
    };

    stream.write_raw(0, b'p', |buf| {
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
            (11, data) => scram.update(data).await?,
            (12, data) => {
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

        stream.write_raw(0, b'p', |buf| buf.put_slice(scram.message()));
        stream.flush().await?;
    }

    match stream.read_auth_message().await? {
        (0, _) => Ok(()),
        (tag, msg) => {
            tracing::error!(
                "compute responded with unexpected auth message with tag[{tag}]: {}",
                String::from_utf8_lossy(msg)
            );
            Err(PostgresError::InvalidAuthMessage)
        }
    }
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
