use std::convert::TryFrom;
use std::sync::Arc;

use postgres_client::tls::MakeTlsConnect;
use rustls::pki_types::{InvalidDnsNameError, ServerName};
use tokio::io::{AsyncRead, AsyncWrite};

use crate::config::ComputeConfig;

mod private {
    use std::future::Future;
    use std::io;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use postgres_client::tls::{ChannelBinding, TlsConnect};
    use rustls::pki_types::ServerName;
    use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
    use tokio_rustls::TlsConnector;
    use tokio_rustls::client::TlsStream;

    use crate::tls::TlsServerEndPoint;

    pub struct TlsConnectFuture<S> {
        inner: tokio_rustls::Connect<S>,
    }

    impl<S> Future for TlsConnectFuture<S>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        type Output = io::Result<RustlsStream<S>>;

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            Pin::new(&mut self.inner)
                .poll(cx)
                .map_ok(|s| RustlsStream(Box::new(s)))
        }
    }

    pub struct RustlsConnect(pub RustlsConnectData);

    pub struct RustlsConnectData {
        pub hostname: ServerName<'static>,
        pub connector: TlsConnector,
    }

    impl<S> TlsConnect<S> for RustlsConnect
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        type Stream = RustlsStream<S>;
        type Error = io::Error;
        type Future = TlsConnectFuture<S>;

        fn connect(self, stream: S) -> Self::Future {
            TlsConnectFuture {
                inner: self.0.connector.connect(self.0.hostname, stream),
            }
        }
    }

    pub struct RustlsStream<S>(Box<TlsStream<S>>);

    impl<S> postgres_client::tls::TlsStream for RustlsStream<S>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        fn channel_binding(&self) -> ChannelBinding {
            let (_, session) = self.0.get_ref();
            match session.peer_certificates() {
                Some([cert, ..]) => TlsServerEndPoint::new(cert)
                    .ok()
                    .and_then(|cb| match cb {
                        TlsServerEndPoint::Sha256(hash) => Some(hash),
                        TlsServerEndPoint::Undefined => None,
                    })
                    .map_or_else(ChannelBinding::none, |hash| {
                        ChannelBinding::tls_server_end_point(hash.to_vec())
                    }),
                _ => ChannelBinding::none(),
            }
        }
    }

    impl<S> AsyncRead for RustlsStream<S>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<tokio::io::Result<()>> {
            Pin::new(&mut self.0).poll_read(cx, buf)
        }
    }

    impl<S> AsyncWrite for RustlsStream<S>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<tokio::io::Result<usize>> {
            Pin::new(&mut self.0).poll_write(cx, buf)
        }

        fn poll_flush(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<tokio::io::Result<()>> {
            Pin::new(&mut self.0).poll_flush(cx)
        }

        fn poll_shutdown(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<tokio::io::Result<()>> {
            Pin::new(&mut self.0).poll_shutdown(cx)
        }
    }
}

impl<S> MakeTlsConnect<S> for ComputeConfig
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type Stream = private::RustlsStream<S>;
    type TlsConnect = private::RustlsConnect;
    type Error = InvalidDnsNameError;

    fn make_tls_connect(&self, hostname: &str) -> Result<Self::TlsConnect, Self::Error> {
        make_tls_connect(&self.tls, hostname)
    }
}

pub fn make_tls_connect(
    tls: &Arc<rustls::ClientConfig>,
    hostname: &str,
) -> Result<private::RustlsConnect, InvalidDnsNameError> {
    ServerName::try_from(hostname).map(|dns_name| {
        private::RustlsConnect(private::RustlsConnectData {
            hostname: dns_name.to_owned(),
            connector: tls.clone().into(),
        })
    })
}
