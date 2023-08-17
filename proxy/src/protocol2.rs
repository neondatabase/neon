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
        state: ProxyParse,
    }
}

#[derive(Clone, Copy, PartialEq, Debug)]
enum ProxyParse {
    NotStarted,
    FoundHeader,
    Header {
        _command: u8,
        family: u8,
        length: u16,
    },

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

impl<T: AsyncRead> WithClientIp<T> {
    fn fill_buf(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        len: usize,
    ) -> Poll<io::Result<Option<BytesMut>>> {
        let mut this = self.project();
        this.buf.reserve(len);
        while this.buf.len() < len {
            // read_buf is cancel safe
            if ready!(pin!(this.inner.read_buf(this.buf)).poll(cx)?) == 0 {
                return Poll::Ready(Ok(None));
            }
        }
        Poll::Ready(Ok(Some(this.buf.split_to(len))))
    }

    fn poll_client_ip(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<Option<SocketAddr>>> {
        loop {
            match self.state {
                ProxyParse::NotStarted => {
                    let Some(header) = ready!(self.as_mut().fill_buf(cx, 12)?) else {
                        *self.as_mut().project().state = ProxyParse::None;
                        continue;
                    };

                    let this = self.as_mut().project();
                    *this.state = if *header == HEADER {
                        ProxyParse::FoundHeader
                    } else {
                        // header is incorrect. add bytes back
                        // this should be zero cost
                        let suffix = std::mem::replace(this.buf, header);
                        this.buf.unsplit(suffix);

                        ProxyParse::None
                    };
                }
                ProxyParse::FoundHeader => {
                    let Some(mut bytes) = ready!(self.as_mut().fill_buf(cx, 4)?) else {
                        let this = self.as_mut().project();
                        *this.state = ProxyParse::None;
                        continue;
                    };
                    let _command = bytes.get_u8();
                    let family = bytes.get_u8();
                    let length = bytes.get_u16();
                    *self.as_mut().project().state = ProxyParse::Header {
                        _command,
                        family,
                        length,
                    };
                }
                ProxyParse::Header {
                    _command,
                    family,
                    length,
                } => {
                    const IPV4: u8 = 1;
                    const IPV6: u8 = 2;

                    let Some(mut bytes) = ready!(self.as_mut().fill_buf(cx, length as usize)?)
                    else {
                        let this = self.as_mut().project();
                        *this.state = ProxyParse::None;
                        continue;
                    };

                    let this = self.as_mut().project();
                    *this.state = match family >> 4 {
                        // 2 IPV4s and 2 ports
                        IPV4 if length >= (4 + 2) * 2 => {
                            let mut src_addr = [0; 4];
                            let mut dst_addr = [0; 4];
                            bytes.copy_to_slice(&mut src_addr);
                            bytes.copy_to_slice(&mut dst_addr);
                            let src_port = bytes.get_u16();
                            let _dst_port = bytes.get_u16();
                            ProxyParse::Finished(SocketAddr::from((src_addr, src_port)))
                        }
                        IPV6 if length >= (16 + 2) * 2 => {
                            let mut src_addr = [0; 16];
                            let mut dst_addr = [0; 16];
                            bytes.copy_to_slice(&mut src_addr);
                            bytes.copy_to_slice(&mut dst_addr);
                            let src_port = bytes.get_u16();
                            let _dst_port = bytes.get_u16();
                            ProxyParse::Finished(SocketAddr::from((src_addr, src_port)))
                        }
                        _ => ProxyParse::None,
                    };
                }
                ProxyParse::Finished(ip) => break Poll::Ready(Ok(Some(ip))),
                ProxyParse::None => break Poll::Ready(Ok(None)),
            }
        }
    }
}

impl<T: AsyncRead> AsyncRead for WithClientIp<T> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.state {
            ProxyParse::Finished(_) | ProxyParse::None => None,
            _ => ready!(self.as_mut().poll_client_ip(cx)?),
        };

        let this = self.project();

        let write = usize::min(this.buf.len(), buf.remaining());
        let slice = this.buf.split_to(write).freeze();
        buf.put_slice(&slice);
        if buf.remaining() > 0 {
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

        let extra_data = [0x55; 256];

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
