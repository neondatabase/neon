//! Proxy Protocol V2 implementation
//! Compatible with <https://www.haproxy.org/download/3.1/doc/proxy-protocol.txt>

use core::fmt;
use std::io;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};

use bytes::Buf;
use smol_str::SmolStr;
use strum_macros::FromRepr;
use tokio::io::{AsyncRead, AsyncReadExt};
use zerocopy::{FromBytes, Immutable, KnownLayout, Unaligned, network_endian};

/// Proxy Protocol Version 2 Header
const SIGNATURE: [u8; 12] = [
    0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51, 0x55, 0x49, 0x54, 0x0A,
];

const LOCAL_V2: u8 = 0x20;
const PROXY_V2: u8 = 0x21;

const TCP_OVER_IPV4: u8 = 0x11;
const UDP_OVER_IPV4: u8 = 0x12;
const TCP_OVER_IPV6: u8 = 0x21;
const UDP_OVER_IPV6: u8 = 0x22;

#[derive(PartialEq, Eq, Clone, Debug)]
pub struct ConnectionInfo {
    pub addr: SocketAddr,
    pub extra: Option<ConnectionInfoExtra>,
}

#[derive(PartialEq, Eq, Clone, Debug)]
pub enum ConnectHeader {
    Local,
    Proxy(ConnectionInfo),
}

impl fmt::Display for ConnectionInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.extra {
            None => self.addr.ip().fmt(f),
            Some(ConnectionInfoExtra::Aws { vpce_id }) => {
                write!(f, "vpce_id[{vpce_id:?}]:addr[{}]", self.addr.ip())
            }
            Some(ConnectionInfoExtra::Azure { link_id }) => {
                write!(f, "link_id[{link_id}]:addr[{}]", self.addr.ip())
            }
        }
    }
}

#[derive(PartialEq, Eq, Clone, Debug)]
pub enum ConnectionInfoExtra {
    Aws { vpce_id: SmolStr },
    Azure { link_id: u32 },
}

pub(crate) async fn read_proxy_protocol<T: AsyncRead + Unpin>(
    mut read: T,
) -> std::io::Result<(T, ConnectHeader)> {
    let mut header = [0; size_of::<ProxyProtocolV2Header>()];
    read.read_exact(&mut header).await?;
    let header: ProxyProtocolV2Header = zerocopy::transmute!(header);
    if header.signature != SIGNATURE {
        return Err(std::io::Error::other("invalid proxy protocol header"));
    }

    let mut payload = vec![0; usize::from(header.len.get())];
    read.read_exact(&mut payload).await?;

    let res = process_proxy_payload(header, &payload)?;
    Ok((read, res))
}

fn process_proxy_payload(
    header: ProxyProtocolV2Header,
    mut payload: &[u8],
) -> std::io::Result<ConnectHeader> {
    match header.version_and_command {
        // the connection was established on purpose by the proxy
        // without being relayed. The connection endpoints are the sender and the
        // receiver. Such connections exist when the proxy sends health-checks to the
        // server. The receiver must accept this connection as valid and must use the
        // real connection endpoints and discard the protocol block including the
        // family which is ignored.
        LOCAL_V2 => return Ok(ConnectHeader::Local),
        // the connection was established on behalf of another node,
        // and reflects the original connection endpoints. The receiver must then use
        // the information provided in the protocol block to get original the address.
        PROXY_V2 => {}
        // other values are unassigned and must not be emitted by senders. Receivers
        // must drop connections presenting unexpected values here.
        _ => {
            return Err(io::Error::other(format!(
                "invalid proxy protocol command 0x{:02X}. expected local (0x20) or proxy (0x21)",
                header.version_and_command
            )));
        }
    }

    let size_err =
        "invalid proxy protocol length. payload not large enough to fit requested IP addresses";
    let addr = match header.protocol_and_family {
        TCP_OVER_IPV4 | UDP_OVER_IPV4 => {
            let addr = payload
                .try_get::<ProxyProtocolV2HeaderV4>()
                .ok_or_else(|| io::Error::other(size_err))?;

            SocketAddr::from((addr.src_addr.get(), addr.src_port.get()))
        }
        TCP_OVER_IPV6 | UDP_OVER_IPV6 => {
            let addr = payload
                .try_get::<ProxyProtocolV2HeaderV6>()
                .ok_or_else(|| io::Error::other(size_err))?;

            SocketAddr::from((addr.src_addr.get(), addr.src_port.get()))
        }
        // unspecified or unix stream. ignore the addresses
        _ => {
            return Err(io::Error::other(
                "invalid proxy protocol address family/transport protocol.",
            ));
        }
    };

    let mut extra = None;

    while let Some(mut tlv) = read_tlv(&mut payload) {
        match Pp2Kind::from_repr(tlv.kind) {
            Some(Pp2Kind::Aws) => {
                if tlv.value.is_empty() {
                    tracing::warn!("invalid aws tlv: no subtype");
                }
                let subtype = tlv.value.get_u8();
                match Pp2AwsType::from_repr(subtype) {
                    Some(Pp2AwsType::VpceId) => match std::str::from_utf8(tlv.value) {
                        Ok(s) => {
                            extra = Some(ConnectionInfoExtra::Aws { vpce_id: s.into() });
                        }
                        Err(e) => {
                            tracing::warn!("invalid aws vpce id: {e}");
                        }
                    },
                    None => {
                        tracing::warn!("unknown aws tlv: subtype={subtype}");
                    }
                }
            }
            Some(Pp2Kind::Azure) => {
                if tlv.value.is_empty() {
                    tracing::warn!("invalid azure tlv: no subtype");
                }
                let subtype = tlv.value.get_u8();
                match Pp2AzureType::from_repr(subtype) {
                    Some(Pp2AzureType::PrivateEndpointLinkId) => {
                        if tlv.value.len() != 4 {
                            tracing::warn!("invalid azure link_id: {:?}", tlv.value);
                        }
                        extra = Some(ConnectionInfoExtra::Azure {
                            link_id: tlv.value.get_u32_le(),
                        });
                    }
                    None => {
                        tracing::warn!("unknown azure tlv: subtype={subtype}");
                    }
                }
            }
            Some(kind) => {
                tracing::debug!("unused tlv[{kind:?}]: {:?}", tlv.value);
            }
            None => {
                tracing::debug!("unknown tlv: {tlv:?}");
            }
        }
    }

    Ok(ConnectHeader::Proxy(ConnectionInfo { addr, extra }))
}

#[derive(FromRepr, Debug, Copy, Clone)]
#[repr(u8)]
enum Pp2Kind {
    // The following are defined by https://www.haproxy.org/download/3.1/doc/proxy-protocol.txt
    // we don't use these but it would be interesting to know what's available
    Alpn = 0x01,
    Authority = 0x02,
    Crc32C = 0x03,
    Noop = 0x04,
    UniqueId = 0x05,
    Ssl = 0x20,
    NetNs = 0x30,

    /// <https://docs.aws.amazon.com/elasticloadbalancing/latest/network/edit-target-group-attributes.html#proxy-protocol>
    Aws = 0xEA,

    /// <https://learn.microsoft.com/en-us/azure/private-link/private-link-service-overview#getting-connection-information-using-tcp-proxy-v2>
    Azure = 0xEE,
}

#[derive(FromRepr, Debug, Copy, Clone)]
#[repr(u8)]
enum Pp2AwsType {
    VpceId = 0x01,
}

#[derive(FromRepr, Debug, Copy, Clone)]
#[repr(u8)]
enum Pp2AzureType {
    PrivateEndpointLinkId = 0x01,
}

#[derive(Debug)]
struct Tlv<'a> {
    kind: u8,
    value: &'a [u8],
}

fn read_tlv<'a>(b: &mut &'a [u8]) -> Option<Tlv<'a>> {
    let tlv_header = b.try_get::<TlvHeader>()?;
    let len = usize::from(tlv_header.len.get());
    Some(Tlv {
        kind: tlv_header.kind,
        value: b.split_off(..len)?,
    })
}

trait BufExt: Sized {
    fn try_get<T: FromBytes>(&mut self) -> Option<T>;
}
impl BufExt for &[u8] {
    fn try_get<T: FromBytes>(&mut self) -> Option<T> {
        let (res, rest) = T::read_from_prefix(self).ok()?;
        *self = rest;
        Some(res)
    }
}

#[derive(FromBytes, KnownLayout, Immutable, Unaligned, Copy, Clone)]
#[repr(C, packed)]
struct ProxyProtocolV2Header {
    signature: [u8; 12],
    version_and_command: u8,
    protocol_and_family: u8,
    len: network_endian::U16,
}

#[derive(FromBytes, KnownLayout, Immutable, Unaligned, Copy, Clone)]
#[repr(C, packed)]
struct ProxyProtocolV2HeaderV4 {
    src_addr: NetworkEndianIpv4,
    dst_addr: NetworkEndianIpv4,
    src_port: network_endian::U16,
    dst_port: network_endian::U16,
}

#[derive(FromBytes, KnownLayout, Immutable, Unaligned, Copy, Clone)]
#[repr(C, packed)]
struct ProxyProtocolV2HeaderV6 {
    src_addr: NetworkEndianIpv6,
    dst_addr: NetworkEndianIpv6,
    src_port: network_endian::U16,
    dst_port: network_endian::U16,
}

#[derive(FromBytes, KnownLayout, Immutable, Unaligned, Copy, Clone)]
#[repr(C, packed)]
struct TlvHeader {
    kind: u8,
    len: network_endian::U16,
}

#[derive(FromBytes, KnownLayout, Immutable, Unaligned, Copy, Clone)]
#[repr(transparent)]
struct NetworkEndianIpv4(network_endian::U32);
impl NetworkEndianIpv4 {
    #[inline]
    fn get(self) -> Ipv4Addr {
        Ipv4Addr::from_bits(self.0.get())
    }
}

#[derive(FromBytes, KnownLayout, Immutable, Unaligned, Copy, Clone)]
#[repr(transparent)]
struct NetworkEndianIpv6(network_endian::U128);
impl NetworkEndianIpv6 {
    #[inline]
    fn get(self) -> Ipv6Addr {
        Ipv6Addr::from_bits(self.0.get())
    }
}

#[cfg(test)]
mod tests {
    use tokio::io::AsyncReadExt;

    use crate::protocol2::{
        ConnectHeader, LOCAL_V2, PROXY_V2, TCP_OVER_IPV4, UDP_OVER_IPV6, read_proxy_protocol,
    };

    #[tokio::test]
    async fn test_ipv4() {
        let header = super::SIGNATURE
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

        let (mut read, info) = read_proxy_protocol(header.chain(extra_data.as_slice()))
            .await
            .unwrap();

        let mut bytes = vec![];
        read.read_to_end(&mut bytes).await.unwrap();

        assert_eq!(bytes, extra_data);

        let ConnectHeader::Proxy(info) = info else {
            panic!()
        };
        assert_eq!(info.addr, ([127, 0, 0, 1], 65535).into());
    }

    #[tokio::test]
    async fn test_ipv6() {
        let header = super::SIGNATURE
            // Proxy command, IPV6 | UDP
            .chain([PROXY_V2, UDP_OVER_IPV6].as_slice())
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

        let (mut read, info) = read_proxy_protocol(header.chain(extra_data.as_slice()))
            .await
            .unwrap();

        let mut bytes = vec![];
        read.read_to_end(&mut bytes).await.unwrap();

        assert_eq!(bytes, extra_data);

        let ConnectHeader::Proxy(info) = info else {
            panic!()
        };
        assert_eq!(
            info.addr,
            ([15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0], 257).into()
        );
    }

    #[tokio::test]
    #[should_panic = "invalid proxy protocol header"]
    async fn test_invalid() {
        let data = [0x55; 256];

        read_proxy_protocol(data.as_slice()).await.unwrap();
    }

    #[tokio::test]
    #[should_panic = "early eof"]
    async fn test_short() {
        let data = [0x55; 10];

        read_proxy_protocol(data.as_slice()).await.unwrap();
    }

    #[tokio::test]
    async fn test_large_tlv() {
        let tlv = vec![0x55; 32768];
        let tlv_len = (tlv.len() as u16).to_be_bytes();
        let len = (12 + 3 + tlv.len() as u16).to_be_bytes();

        let header = super::SIGNATURE
            // Proxy command, Inet << 4 | Stream
            .chain([PROXY_V2, TCP_OVER_IPV4].as_slice())
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
            .chain([255].as_slice())
            .chain(tlv_len.as_slice())
            .chain(tlv.as_slice());

        let extra_data = [0xaa; 256];

        let (mut read, info) = read_proxy_protocol(header.chain(extra_data.as_slice()))
            .await
            .unwrap();

        let mut bytes = vec![];
        read.read_to_end(&mut bytes).await.unwrap();

        assert_eq!(bytes, extra_data);

        let ConnectHeader::Proxy(info) = info else {
            panic!()
        };
        assert_eq!(info.addr, ([55, 56, 57, 58], 65535).into());
    }

    #[tokio::test]
    async fn test_local() {
        let len = 0u16.to_be_bytes();
        let header = super::SIGNATURE
            .chain([LOCAL_V2, 0x00].as_slice())
            .chain(len.as_slice());

        let extra_data = [0xaa; 256];

        let (mut read, info) = read_proxy_protocol(header.chain(extra_data.as_slice()))
            .await
            .unwrap();

        let mut bytes = vec![];
        read.read_to_end(&mut bytes).await.unwrap();

        assert_eq!(bytes, extra_data);

        let ConnectHeader::Local = info else { panic!() };
    }
}
