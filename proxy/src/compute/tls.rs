use futures::FutureExt;
use postgres_client::config::SslMode;
use postgres_client::maybe_tls_stream::MaybeTlsStream;
use postgres_client::tls::{MakeTlsConnect, TlsConnect};
use rustls::pki_types::InvalidDnsNameError;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::proxy::retry::CouldRetry;

#[derive(Debug, Error)]
pub enum TlsError {
    #[error("{0}")]
    Dns(#[from] InvalidDnsNameError),
    #[error("{0}")]
    Connection(#[from] std::io::Error),
    #[error("TLS required but not provided")]
    Required,
}

impl CouldRetry for TlsError {
    fn could_retry(&self) -> bool {
        match self {
            TlsError::Dns(_) => false,
            TlsError::Connection(err) => err.could_retry(),
            // perhaps compute didn't realise it supports TLS?
            TlsError::Required => true,
        }
    }
}

pub async fn connect_tls<S, T>(
    mut stream: S,
    mode: SslMode,
    tls: &T,
    host: &str,
) -> Result<MaybeTlsStream<S, T::Stream>, TlsError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
    T: MakeTlsConnect<
            S,
            Error = InvalidDnsNameError,
            TlsConnect: TlsConnect<S, Error = std::io::Error, Future: Send>,
        >,
{
    match mode {
        SslMode::Disable => return Ok(MaybeTlsStream::Raw(stream)),
        SslMode::Prefer | SslMode::Require => {}
    }

    // These are the bytes for <https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-SSLREQUEST>
    stream
        .write_all(&[0, 0, 0, 8, 0x0f4, 0xd2, 0x16, 0x2f])
        .await?;
    stream.flush().await?;

    // we expect back either `S` or `N` as a single byte.
    let mut res = *b"0";
    stream.read_exact(&mut res).await?;

    if res != *b"S" {
        if SslMode::Require == mode {
            return Err(TlsError::Required);
        }

        return Ok(MaybeTlsStream::Raw(stream));
    }

    Ok(MaybeTlsStream::Tls(
        tls.make_tls_connect(host)?.connect(stream).boxed().await?,
    ))
}
