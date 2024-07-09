//! Proxy Protocol V2 implementation

use std::{
    io,
    net::SocketAddr,
    pin::Pin,
    task::{Context, Poll},
};

use bytes::BytesMut;
use pin_project_lite::pin_project;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, ReadBuf};

pin_project! {
    /// A chained [`AsyncRead`] with [`AsyncWrite`] passthrough
    pub struct ChainRW<T> {
        #[pin]
        pub inner: T,
        buf: BytesMut,
    }
}

impl<T: AsyncWrite> AsyncWrite for ChainRW<T> {
    #[inline]
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        self.project().inner.poll_write(cx, buf)
    }

    #[inline]
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        self.project().inner.poll_flush(cx)
    }

    #[inline]
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        self.project().inner.poll_shutdown(cx)
    }

    #[inline]
    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<Result<usize, io::Error>> {
        self.project().inner.poll_write_vectored(cx, bufs)
    }

    #[inline]
    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
}

/// Proxy Protocol Version 2 Header
const HEADER: [u8; 12] = [
    0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51, 0x55, 0x49, 0x54, 0x0A,
];

pub async fn read_proxy_protocol<T: AsyncRead + Unpin>(
    mut read: T,
) -> std::io::Result<(ChainRW<T>, Option<SocketAddr>)> {
    let mut buf = BytesMut::with_capacity(128);
    while buf.len() < 16 {
        let bytes_read = read.read_buf(&mut buf).await?;

        // exit for bad header
        let len = usize::min(buf.len(), HEADER.len());
        if buf[..len] != HEADER[..len] {
            return Ok((ChainRW { inner: read, buf }, None));
        }

        // if no more bytes available then exit
        if bytes_read == 0 {
            return Ok((ChainRW { inner: read, buf }, None));
        };
    }

    let header = buf.split_to(16);

    // The next byte (the 13th one) is the protocol version and command.
    // The highest four bits contains the version. As of this specification, it must
    // always be sent as \x2 and the receiver must only accept this value.
    let vc = header[12];
    let version = vc >> 4;
    let command = vc & 0b1111;
    if version != 2 {
        return Err(io::Error::new(
            io::ErrorKind::Other,
            "invalid proxy protocol version. expected version 2",
        ));
    }
    match command {
        // the connection was established on purpose by the proxy
        // without being relayed. The connection endpoints are the sender and the
        // receiver. Such connections exist when the proxy sends health-checks to the
        // server. The receiver must accept this connection as valid and must use the
        // real connection endpoints and discard the protocol block including the
        // family which is ignored.
        0 => {}
        // the connection was established on behalf of another node,
        // and reflects the original connection endpoints. The receiver must then use
        // the information provided in the protocol block to get original the address.
        1 => {}
        // other values are unassigned and must not be emitted by senders. Receivers
        // must drop connections presenting unexpected values here.
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "invalid proxy protocol command. expected local (0) or proxy (1)",
            ))
        }
    };

    // The 14th byte contains the transport protocol and address family. The highest 4
    // bits contain the address family, the lowest 4 bits contain the protocol.
    let ft = header[13];
    let address_length = match ft {
        // - \x11 : TCP over IPv4 : the forwarded connection uses TCP over the AF_INET
        //   protocol family. Address length is 2*4 + 2*2 = 12 bytes.
        // - \x12 : UDP over IPv4 : the forwarded connection uses UDP over the AF_INET
        //   protocol family. Address length is 2*4 + 2*2 = 12 bytes.
        0x11 | 0x12 => 12,
        // - \x21 : TCP over IPv6 : the forwarded connection uses TCP over the AF_INET6
        //   protocol family. Address length is 2*16 + 2*2 = 36 bytes.
        // - \x22 : UDP over IPv6 : the forwarded connection uses UDP over the AF_INET6
        //   protocol family. Address length is 2*16 + 2*2 = 36 bytes.
        0x21 | 0x22 => 36,
        // unspecified or unix stream. ignore the addresses
        _ => 0,
    };

    // The 15th and 16th bytes is the address length in bytes in network endian order.
    // It is used so that the receiver knows how many address bytes to skip even when
    // it does not implement the presented protocol. Thus the length of the protocol
    // header in bytes is always exactly 16 + this value. When a sender presents a
    // LOCAL connection, it should not present any address so it sets this field to
    // zero. Receivers MUST always consider this field to skip the appropriate number
    // of bytes and must not assume zero is presented for LOCAL connections. When a
    // receiver accepts an incoming connection showing an UNSPEC address family or
    // protocol, it may or may not decide to log the address information if present.
    let remaining_length = u16::from_be_bytes(header[14..16].try_into().unwrap());
    if remaining_length < address_length {
        return Err(io::Error::new(
            io::ErrorKind::Other,
            "invalid proxy protocol length. not enough to fit requested IP addresses",
        ));
    }
    drop(header);

    while buf.len() < remaining_length as usize {
        if read.read_buf(&mut buf).await? == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "stream closed while waiting for proxy protocol addresses",
            ));
        }
    }

    // Starting from the 17th byte, addresses are presented in network byte order.
    // The address order is always the same :
    //   - source layer 3 address in network byte order
    //   - destination layer 3 address in network byte order
    //   - source layer 4 address if any, in network byte order (port)
    //   - destination layer 4 address if any, in network byte order (port)
    let addresses = buf.split_to(remaining_length as usize);
    let socket = match address_length {
        12 => {
            let src_addr: [u8; 4] = addresses[0..4].try_into().unwrap();
            let src_port = u16::from_be_bytes(addresses[8..10].try_into().unwrap());
            Some(SocketAddr::from((src_addr, src_port)))
        }
        36 => {
            let src_addr: [u8; 16] = addresses[0..16].try_into().unwrap();
            let src_port = u16::from_be_bytes(addresses[32..34].try_into().unwrap());
            Some(SocketAddr::from((src_addr, src_port)))
        }
        _ => None,
    };

    Ok((ChainRW { inner: read, buf }, socket))
}

impl<T: AsyncRead> AsyncRead for ChainRW<T> {
    #[inline]
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.buf.is_empty() {
            self.project().inner.poll_read(cx, buf)
        } else {
            self.read_from_buf(buf)
        }
    }
}

impl<T: AsyncRead> ChainRW<T> {
    #[cold]
    fn read_from_buf(self: Pin<&mut Self>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        debug_assert!(!self.buf.is_empty());
        let this = self.project();

        let write = usize::min(this.buf.len(), buf.remaining());
        let slice = this.buf.split_to(write).freeze();
        buf.put_slice(&slice);

        // reset the allocation so it can be freed
        if this.buf.is_empty() {
            *this.buf = BytesMut::new();
        }

        Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
mod tests {
    use tokio::io::AsyncReadExt;

    use crate::protocol2::read_proxy_protocol;

    #[tokio::test]
    async fn test_ipv4() {
        let header = super::HEADER
            // Proxy command, IPV4 | TCP
            .chain([(2 << 4) | 1, (1 << 4) | 1].as_slice())
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

        let (mut read, addr) = read_proxy_protocol(header.chain(extra_data.as_slice()))
            .await
            .unwrap();

        let mut bytes = vec![];
        read.read_to_end(&mut bytes).await.unwrap();

        assert_eq!(bytes, extra_data);
        assert_eq!(addr, Some(([127, 0, 0, 1], 65535).into()));
    }

    #[tokio::test]
    async fn test_ipv6() {
        let header = super::HEADER
            // Proxy command, IPV6 | UDP
            .chain([(2 << 4) | 1, (2 << 4) | 2].as_slice())
            // 36 + 3 bytes
            .chain([0, 39].as_slice())
            // src ip
            .chain([15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0].as_slice())
            // dst ip
            .chain([0, 15, 1, 14, 2, 13, 3, 12, 4, 11, 5, 10, 6, 9, 7, 8].as_slice())
            // src port
            .chain([1, 1].as_slice())
            // dst port
            .chain([255, 255].as_slice())
            // TLV
            .chain([1, 2, 3].as_slice());

        let extra_data = [0x55; 256];

        let (mut read, addr) = read_proxy_protocol(header.chain(extra_data.as_slice()))
            .await
            .unwrap();

        let mut bytes = vec![];
        read.read_to_end(&mut bytes).await.unwrap();

        assert_eq!(bytes, extra_data);
        assert_eq!(
            addr,
            Some(([15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0], 257).into())
        );
    }

    #[tokio::test]
    async fn test_invalid() {
        let data = [0x55; 256];

        let (mut read, addr) = read_proxy_protocol(data.as_slice()).await.unwrap();

        let mut bytes = vec![];
        read.read_to_end(&mut bytes).await.unwrap();
        assert_eq!(bytes, data);
        assert_eq!(addr, None);
    }

    #[tokio::test]
    async fn test_short() {
        let data = [0x55; 10];

        let (mut read, addr) = read_proxy_protocol(data.as_slice()).await.unwrap();

        let mut bytes = vec![];
        read.read_to_end(&mut bytes).await.unwrap();
        assert_eq!(bytes, data);
        assert_eq!(addr, None);
    }

    #[tokio::test]
    async fn test_large_tlv() {
        let tlv = vec![0x55; 32768];
        let len = (12 + tlv.len() as u16).to_be_bytes();

        let header = super::HEADER
            // Proxy command, Inet << 4 | Stream
            .chain([(2 << 4) | 1, (1 << 4) | 1].as_slice())
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

        let (mut read, addr) = read_proxy_protocol(header.chain(extra_data.as_slice()))
            .await
            .unwrap();

        let mut bytes = vec![];
        read.read_to_end(&mut bytes).await.unwrap();

        assert_eq!(bytes, extra_data);
        assert_eq!(addr, Some(([55, 56, 57, 58], 65535).into()));
    }
}
