use futures::FutureExt;
use postgres_client::config::SslMode;
use postgres_client::maybe_tls_stream::MaybeTlsStream;
use postgres_client::tls::{MakeTlsConnect, TlsConnect};
use rustls::pki_types::InvalidDnsNameError;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::pqproto::request_tls;
use crate::proxy::retry::CouldRetry;

#[derive(Debug, Error)]
pub enum TlsError {
    #[error(transparent)]
    Dns(#[from] InvalidDnsNameError),
    #[error(transparent)]
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

    if !request_tls(&mut stream).await? {
        if SslMode::Require == mode {
            return Err(TlsError::Required);
        }

        return Ok(MaybeTlsStream::Raw(stream));
    }

    Ok(MaybeTlsStream::Tls(
        tls.make_tls_connect(host)?.connect(stream).boxed().await?,
    ))
}
