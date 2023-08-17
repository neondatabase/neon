//! Proxy Protocol V2 implementation

use std::{
    net::SocketAddr,
    pin::Pin,
    task::{ready, Context, Poll}, future::poll_fn,
};

use bytes::{Buf, BufMut, BytesMut};
use hyper::server::conn::{AddrIncoming, AddrStream};
use pin_project_lite::pin_project;
use tls_listener::AsyncAccept;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

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
    ) -> Poll<Result<usize, std::io::Error>> {
        self.project().inner.poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        self.project().inner.poll_flush(cx)
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
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
    pub async fn wait_for_socket(&mut self) -> std::io::Result<Option<SocketAddr>> {
        let mut pin = Pin::new(self);
        poll_fn(|cx| pin.as_mut().poll_client_ip(cx)).await
    }
}

impl<T: AsyncRead> WithClientIp<T> {
    fn fill_buf(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        len: usize,
    ) -> Poll<std::io::Result<BytesMut>> {
        let mut this = self.project();
        while this.buf.len() < len {
            let mut buf2 = ReadBuf::uninit(this.buf.spare_capacity_mut());
            ready!(this.inner.as_mut().poll_read(cx, &mut buf2)?);
            let filled = buf2.filled().len();
            // TODO: can this be done safely?
            // SAFETY: buf2 says we have filled this number of bytes
            unsafe { this.buf.advance_mut(filled) }
        }
        Poll::Ready(Ok(this.buf.split_to(len)))
    }

    fn poll_client_ip(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<Option<SocketAddr>>> {
        loop {
            match self.state {
                ProxyParse::NotStarted => {
                    let header = [
                        0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51, 0x55, 0x49, 0x54, 0x0A,
                    ];
                    let bytes = ready!(self.as_mut().fill_buf(cx, 12)?);
                    let this = self.as_mut().project();
                    if *bytes == header {
                        *this.state = ProxyParse::FoundHeader;
                    } else {
                        *this.state = ProxyParse::None;
                        // header is incorrect. add bytes back
                        // this should be zero cost
                        let suffix = std::mem::replace(this.buf, bytes);
                        this.buf.unsplit(suffix);
                    }
                }
                ProxyParse::FoundHeader => {
                    let mut bytes = ready!(self.as_mut().fill_buf(cx, 4)?);
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

                    let mut bytes = ready!(self.as_mut().fill_buf(cx, length as usize)?);

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
    ) -> Poll<std::io::Result<()>> {
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

    type Error = std::io::Error;

    fn poll_accept(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Self::Connection, Self::Error>>> {
        let conn = ready!(self.project().incoming.poll_accept(cx)?);
        let Some(conn) = conn else { return Poll::Ready(None) };

        Poll::Ready(Some(Ok(WithClientIp::new(conn))))
    }
}

#[cfg(test)]
mod tests {
    use std::pin::pin;

    use std::io::Cursor;
    use tokio::io::AsyncReadExt;

    use crate::protocol2::{ProxyParse, WithClientIp};

    #[tokio::test]
    async fn test_ipv4() {
        let data = [
            // proxy protocol header
            0x0D,
            0x0A,
            0x0D,
            0x0A,
            0x00,
            0x0D,
            0x0A,
            0x51,
            0x55,
            0x49,
            0x54,
            0x0A,
            // Proxy command
            1u8,
            // Inet << 4 | Stream
            (1 << 4) | 1,
            // Length beyond this: 12
            // Let's throw in a TLV with no data; 3 bytes.
            0,
            15,
            // Source IP
            127,
            0,
            0,
            1,
            // Destination IP
            192,
            168,
            0,
            1,
            // Source port
            // 65535 = [255, 255]
            255,
            255,
            // Destination port
            // 257 = [1, 1]
            1,
            1,
            // TLV
            69,
            0,
            0,
        ];
        let extra_data = [0x55; 256];

        let mut read = pin!(WithClientIp::new(
            Cursor::new(data).chain(Cursor::new(extra_data))
        ));

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

        let mut read = pin!(WithClientIp::new(Cursor::new(data)));

        let mut bytes = vec![];
        read.read_to_end(&mut bytes).await.unwrap();
        assert_eq!(bytes, data);
        assert_eq!(read.state, ProxyParse::None);
    }
}
