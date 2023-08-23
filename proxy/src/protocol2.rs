//! Proxy Protocol V2 implementation

use std::{
    future::poll_fn,
    future::Future,
    io,
    net::SocketAddr,
    pin::{pin, Pin},
    task::{ready, Context, Poll},
};

use bytes::{Buf, BytesMut};
use hyper::server::conn::{AddrIncoming, AddrStream};
use pin_project_lite::pin_project;
use tls_listener::AsyncAccept;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, ReadBuf};

pin_project! {
    pub struct ProxyProtocolAccept {
        #[pin]
        pub incoming: AddrIncoming,
    }
}

pin_project! {
    pub struct WithClientIp<T> {
        #[pin]
        pub inner: T,
        buf: BytesMut,
        tlv_bytes: usize,
        state: ProxyParse,
    }
}

#[derive(Clone, PartialEq, Debug)]
enum ProxyParse {
    NotStarted,

    Finished(SocketAddr),
    None,
}

impl<T: AsyncWrite> AsyncWrite for WithClientIp<T> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        self.project().inner.poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        self.project().inner.poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        self.project().inner.poll_shutdown(cx)
    }
}

impl<T> WithClientIp<T> {
    pub fn new(inner: T) -> Self {
        WithClientIp {
            inner,
            buf: BytesMut::with_capacity(128),
            tlv_bytes: 0,
            state: ProxyParse::NotStarted,
        }
    }

    pub fn client_socket(&self) -> Option<SocketAddr> {
        match self.state {
            ProxyParse::Finished(socket) => Some(socket),
            _ => None,
        }
    }
}

impl<T: AsyncRead + Unpin> WithClientIp<T> {
    pub async fn wait_for_socket(&mut self) -> io::Result<Option<SocketAddr>> {
        let mut pin = Pin::new(self);
        poll_fn(|cx| pin.as_mut().poll_client_ip(cx)).await
    }
}

/// Proxy Protocol Version 2 Header
const HEADER: [u8; 12] = [
    0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51, 0x55, 0x49, 0x54, 0x0A,
];
const IPV4: u8 = 1;
const IPV6: u8 = 2;

impl<T: AsyncRead> WithClientIp<T> {
    fn poll_client_ip(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<Option<SocketAddr>>> {
        while self.buf.len() <= 16 {
            let mut this = self.as_mut().project();
            let bytes_read = pin!(this.inner.read_buf(this.buf)).poll(cx)?;

            // exit for bad header
            let len = usize::min(self.buf.len(), HEADER.len());
            if self.buf[..len] != HEADER[..len] {
                *self.as_mut().project().state = ProxyParse::None;
                return Poll::Ready(Ok(None));
            }

            // if no more bytes available then exit
            if ready!(bytes_read) == 0 {
                *self.as_mut().project().state = ProxyParse::None;
                return Poll::Ready(Ok(None));
            };
        }

        let family = self.buf[13] >> 4;
        let ip_bytes = match family {
            // 2 IPV4s and 2 ports
            IPV4 => (4 + 2) * 2,
            // 2 IPV6s and 2 ports
            IPV6 => (16 + 2) * 2,
            _ => 0,
        };

        while self.buf.len() < 16 + ip_bytes {
            let mut this = self.as_mut().project();
            if ready!(pin!(this.inner.read_buf(this.buf)).poll(cx)?) == 0 {
                *self.as_mut().project().state = ProxyParse::None;
                return Poll::Ready(Ok(None));
            }
        }

        let length = u16::from_be_bytes(self.buf[14..16].try_into().unwrap()) as usize;

        let this = self.as_mut().project();
        let socket = match family {
            // 2 IPV4s and 2 ports
            IPV4 if length >= ip_bytes => {
                *this.tlv_bytes = length - ip_bytes;
                let buf = this.buf.split_to(16 + ip_bytes);
                let src_addr: [u8; 4] = buf[16..20].try_into().unwrap();
                let src_port = u16::from_be_bytes(buf[24..26].try_into().unwrap());
                Some(SocketAddr::from((src_addr, src_port)))
            }
            IPV6 if length >= ip_bytes => {
                *this.tlv_bytes = length - ip_bytes;
                let buf = this.buf.split_to(16 + ip_bytes);
                let src_addr: [u8; 16] = buf[16..32].try_into().unwrap();
                let src_port = u16::from_be_bytes(buf[48..50].try_into().unwrap());
                Some(SocketAddr::from((src_addr, src_port)))
            }
            _ => None,
        };

        let discard = usize::min(*this.tlv_bytes, this.buf.len());
        *this.tlv_bytes -= discard;
        this.buf.advance(discard);

        Poll::Ready(Ok(socket))
    }
}

impl<T: AsyncRead> AsyncRead for WithClientIp<T> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if let ProxyParse::NotStarted = self.state {
            let ip = ready!(self.as_mut().poll_client_ip(cx)?);
            match ip {
                Some(x) => *self.as_mut().project().state = ProxyParse::Finished(x),
                None => *self.as_mut().project().state = ProxyParse::None,
            }
        }

        let mut this = self.project();

        while *this.tlv_bytes > 0 {
            // we know that this.buf is empty
            debug_assert_eq!(this.buf.len(), 0);

            let n = ready!(pin!(this.inner.read_buf(this.buf)).poll(cx)?);
            let tlv_bytes_read = usize::min(n, *this.tlv_bytes);
            *this.tlv_bytes -= tlv_bytes_read;
            this.buf.advance(tlv_bytes_read);
        }

        if !this.buf.is_empty() {
            // we know that tlv_bytes is 0
            debug_assert_eq!(*this.tlv_bytes, 0);

            let write = usize::min(this.buf.len(), buf.remaining());
            let slice = this.buf.split_to(write).freeze();
            buf.put_slice(&slice);

            return Poll::Ready(Ok(()));
        }

        if this.buf.is_empty() {
            this.inner.poll_read(cx, buf)
        } else {
            Poll::Ready(Ok(()))
        }
    }
}

impl AsyncAccept for ProxyProtocolAccept {
    type Connection = WithClientIp<AddrStream>;

    type Error = io::Error;

    fn poll_accept(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Self::Connection, Self::Error>>> {
        let conn = ready!(self.project().incoming.poll_accept(cx)?);
        let Some(conn) = conn else {
            return Poll::Ready(None);
        };

        Poll::Ready(Some(Ok(WithClientIp::new(conn))))
    }
}

#[cfg(test)]
mod tests {
    use std::pin::pin;

    use tokio::io::AsyncReadExt;

    use crate::protocol2::{ProxyParse, WithClientIp};

    #[tokio::test]
    async fn test_ipv4() {
        let header = super::HEADER
            // Proxy command, Inet << 4 | Stream
            .chain([1u8, (1 << 4) | 1].as_slice())
            // 12 + 3 bytes
            .chain([0, 15].as_slice())
            // src ip
            .chain([127, 0, 0, 1].as_slice())
            // dst ip
            .chain([192, 168, 0, 1].as_slice())
            // src port
            .chain([255, 255].as_slice())
            // dst port
            .chain([1, 1].as_slice())
            // TLV
            .chain([1, 2, 3].as_slice());

        let extra_data = [0x55; 256];

        let mut read = pin!(WithClientIp::new(header.chain(extra_data.as_slice())));

        let mut bytes = vec![];
        read.read_to_end(&mut bytes).await.unwrap();

        assert_eq!(bytes, extra_data);
        assert_eq!(
            read.state,
            ProxyParse::Finished(([127, 0, 0, 1], 65535).into())
        );
    }

    #[tokio::test]
    async fn test_invalid() {
        let data = [0x55; 256];

        let mut read = pin!(WithClientIp::new(data.as_slice()));

        let mut bytes = vec![];
        read.read_to_end(&mut bytes).await.unwrap();
        assert_eq!(bytes, data);
        assert_eq!(read.state, ProxyParse::None);
    }

    #[tokio::test]
    async fn test_short() {
        let data = [0x55; 10];

        let mut read = pin!(WithClientIp::new(data.as_slice()));

        let mut bytes = vec![];
        read.read_to_end(&mut bytes).await.unwrap();
        assert_eq!(bytes, data);
        assert_eq!(read.state, ProxyParse::None);
    }

    #[tokio::test]
    async fn test_large_tlv() {
        let tlv = [0x55; 512];
        let len = (12 + tlv.len() as u16).to_be_bytes();

        let header = super::HEADER
            // Proxy command, Inet << 4 | Stream
            .chain([1u8, (1 << 4) | 1].as_slice())
            // 12 + 3 bytes
            .chain(len.as_slice())
            // src ip
            .chain([55, 56, 57, 58].as_slice())
            // dst ip
            .chain([192, 168, 0, 1].as_slice())
            // src port
            .chain([255, 255].as_slice())
            // dst port
            .chain([1, 1].as_slice())
            // TLV
            .chain(tlv.as_slice());

        let extra_data = [0xaa; 256];

        let mut read = pin!(WithClientIp::new(header.chain(extra_data.as_slice())));

        let mut bytes = vec![];
        read.read_to_end(&mut bytes).await.unwrap();

        assert_eq!(bytes, extra_data);
        assert_eq!(
            read.state,
            ProxyParse::Finished(([55, 56, 57, 58], 65535).into())
        );
    }
}
