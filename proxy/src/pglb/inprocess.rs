//! Contains types for in-process QUIC connections with `quinn`.

#![allow(dead_code, reason = "work in progress")]

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::{fmt, io};

use futures::task::AtomicWaker;
use quinn::crypto::rustls::{QuicClientConfig, QuicServerConfig};
use quinn::{AsyncUdpSocket, UdpPoller, udp};
use ringbuf::traits::{Consumer, Producer, Split};
use ringbuf::{CachingCons, CachingProd, HeapRb};

/// Special ALPN for in-process connections to avoid conflicts with real PGLB.
const ALPN_PGLB_INPROCESS: &[u8] = b"pglb-inprocess";
const MTU: u16 = 4096;
const PGLB_PORT: u16 = 0x879d;
const RECEIVE_BUFFER_DATAGRAM_COUNT: usize = 16;
// quinn will pre-allocate objects for every allowed stream. Keep this reasonable.
const MAX_CONCURRENT_BIDI_STREAMS: u32 = 200;
// disallow unidirectional streams.
const MAX_CONCURRENT_UNI_STREAMS: u32 = 0;

// TODO: remove encryption for in-process connections. Or try to use AES128
// cipher suite.
pub fn create_server_endpoint(
    server_socket: InProcessUdpSocket,
    mut server_crypto: rustls::ServerConfig,
) -> anyhow::Result<quinn::Endpoint> {
    let mut endpoint_config = quinn::EndpointConfig::default();
    endpoint_config
        .max_udp_payload_size(MTU)
        .expect("valid const MTU");

    server_crypto.alpn_protocols = vec![ALPN_PGLB_INPROCESS.into()];

    let mut server_config =
        quinn::ServerConfig::with_crypto(Arc::new(QuicServerConfig::try_from(server_crypto)?));

    let transport_config = Arc::get_mut(&mut server_config.transport).expect("no other clone");
    transport_config.max_concurrent_bidi_streams(MAX_CONCURRENT_BIDI_STREAMS.into());
    transport_config.max_concurrent_uni_streams(MAX_CONCURRENT_UNI_STREAMS.into());
    transport_config.mtu_discovery_config(None);
    transport_config.initial_mtu(MTU);
    transport_config.min_mtu(MTU);

    let server = quinn::Endpoint::new_with_abstract_socket(
        endpoint_config,
        Some(server_config),
        Arc::new(server_socket),
        Arc::new(quinn::TokioRuntime),
    )?;

    Ok(server)
}

pub fn create_client_endpoint(
    client_socket: InProcessUdpSocket,
    mut client_crypto: rustls::ClientConfig,
) -> anyhow::Result<quinn::Endpoint> {
    let mut endpoint_config = quinn::EndpointConfig::default();
    endpoint_config
        .max_udp_payload_size(MTU)
        .expect("valid const MTU");

    let mut client = quinn::Endpoint::new_with_abstract_socket(
        endpoint_config,
        None,
        Arc::new(client_socket),
        Arc::new(quinn::TokioRuntime),
    )?;

    client_crypto.alpn_protocols = vec![ALPN_PGLB_INPROCESS.into()];

    let mut transport_config = quinn::TransportConfig::default();
    transport_config.mtu_discovery_config(None);
    transport_config.initial_mtu(MTU);
    transport_config.min_mtu(MTU);

    let mut client_config =
        quinn::ClientConfig::new(Arc::new(QuicClientConfig::try_from(client_crypto)?));
    client_config.transport_config(Arc::new(transport_config));
    client.set_default_client_config(client_config);

    Ok(client)
}

struct Datagram<const MSS: usize> {
    len: usize,
    buf: [u8; MSS],
    ecn: Option<udp::EcnCodepoint>,
}

impl<const MSS: usize> Datagram<MSS> {
    fn new(data: &[u8], ecn: Option<udp::EcnCodepoint>) -> io::Result<Self> {
        let mut buf = [0u8; MSS];
        buf.get_mut(..data.len())
            .ok_or(io::Error::new(
                io::ErrorKind::InvalidInput,
                "too much data for datagram",
            ))?
            .copy_from_slice(data);

        Ok(Self {
            len: data.len(),
            buf,
            ecn,
        })
    }
}

struct DatagramQueue<const MSS: usize> {
    sender: Mutex<CachingProd<Arc<HeapRb<Datagram<MSS>>>>>,
    // TODO: does not need to be a Mutex, could be a TryLock.
    receiver: Mutex<CachingCons<Arc<HeapRb<Datagram<MSS>>>>>,
    sibling_waker: Arc<ReceiveWaker>,
}

impl<const MSS: usize> DatagramQueue<MSS> {
    fn new(
        sender: CachingProd<Arc<HeapRb<Datagram<MSS>>>>,
        receiver: CachingCons<Arc<HeapRb<Datagram<MSS>>>>,
    ) -> Self {
        Self {
            sender: Mutex::new(sender),
            receiver: Mutex::new(receiver),
            sibling_waker: Arc::new(ReceiveWaker::new()),
        }
    }

    fn send(&self, packet: &[u8], ecn: Option<udp::EcnCodepoint>) -> io::Result<()> {
        if packet.len() > MSS {
            // TODO: can only happen when there's confusion about MTU size. Maybe panic?
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "send: packet too large",
            ));
        }

        let mut sender = self.sender.lock().expect("poisoned");
        if sender.try_push(Datagram::new(packet, ecn)?).is_err() {
            // Drop the packet.
            return Ok(());
        }

        self.sibling_waker.wake();

        Ok(())
    }

    fn recv(&self, buf: &mut [u8], ecn: &mut Option<udp::EcnCodepoint>) -> io::Result<usize> {
        let mut receiver = self.receiver.lock().expect("poisoned");

        if let Some(dgram) = receiver.try_peek() {
            let len = dgram.len;
            if len > buf.len() {
                // TODO: can only happen when there's confusion about MTU size. Maybe panic?
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "recv: buffer too small",
                ));
            }
            buf[..len].copy_from_slice(&dgram.buf[..len]);
            *ecn = dgram.ecn;
            receiver.skip(1);
            Ok(len)
        } else {
            Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "recv: no datagram available",
            ))
        }
    }
}

struct ReceiveWaker {
    waker: AtomicWaker,
}

impl ReceiveWaker {
    const fn new() -> Self {
        ReceiveWaker {
            waker: AtomicWaker::new(),
        }
    }

    #[inline]
    fn register(&self, cx: &mut Context<'_>) {
        if let Some(waker) = self.waker.take() {
            assert!(waker.will_wake(cx.waker()), "waker of unexpected task");
        }
        self.waker.register(cx.waker());
    }

    #[inline]
    fn wake(&self) {
        self.waker.wake();
    }
}

/// An implementation of [`quinn::AsyncUdpSocket`] for in-process connections.
pub struct InProcessUdpSocket {
    // MTU == MSS
    queue: DatagramQueue<{ MTU as usize }>,
    recv_waker: Arc<ReceiveWaker>,
    local_addr: SocketAddr,
    remote_addr: SocketAddr,
}

impl fmt::Debug for InProcessUdpSocket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InProcessUdpSocket")
            .field("local_addr", &self.local_addr)
            .finish_non_exhaustive()
    }
}

impl InProcessUdpSocket {
    pub fn new_pair() -> (Self, Self) {
        let (sender_a, receiver_a) = HeapRb::new(RECEIVE_BUFFER_DATAGRAM_COUNT).split();
        let (sender_b, receiver_b) = HeapRb::new(RECEIVE_BUFFER_DATAGRAM_COUNT).split();

        let queue_a = DatagramQueue::new(sender_a, receiver_b);
        let queue_b = DatagramQueue::new(sender_b, receiver_a);

        let waker_b = Arc::clone(&queue_a.sibling_waker);
        let waker_a = Arc::clone(&queue_b.sibling_waker);

        let addr_a = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), PGLB_PORT);
        let addr_b = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2)), PGLB_PORT);

        let socket_a = InProcessUdpSocket {
            queue: queue_a,
            recv_waker: waker_a,
            local_addr: addr_a,
            remote_addr: addr_b,
        };

        let socket_b = InProcessUdpSocket {
            queue: queue_b,
            recv_waker: waker_b,
            local_addr: addr_b,
            remote_addr: addr_a,
        };

        (socket_a, socket_b)
    }
}

impl AsyncUdpSocket for InProcessUdpSocket {
    #[inline]
    fn create_io_poller(self: Arc<Self>) -> Pin<Box<dyn UdpPoller>> {
        // We don't have a send buffer, just a receive buffer. We accept any
        // sends and drop packets not fitting into the receive buffer.
        // TODO: let's see if this works well in prod.
        Box::pin(AlwaysReadyUdpPoller)
    }

    fn try_send(&self, transmit: &udp::Transmit<'_>) -> io::Result<()> {
        if transmit.src_ip.is_some_and(|ip| ip != self.local_addr.ip()) {
            return Err(io::Error::new(
                io::ErrorKind::AddrNotAvailable,
                "try_send: address not available",
            ));
        }
        if transmit.destination != self.remote_addr {
            // Instead of dropping the packet, we return an error.
            // We could even consider panicking here.
            return Err(io::Error::new(
                io::ErrorKind::HostUnreachable,
                "try_send: host unreachable",
            ));
        }
        if let Some(segment_size) = transmit.segment_size {
            if segment_size != transmit.contents.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "try_send: segmented transmit contents",
                ));
            }
        }

        self.queue.send(transmit.contents, transmit.ecn)?;

        Ok(())
    }

    fn poll_recv(
        &self,
        cx: &mut Context<'_>,
        bufs: &mut [io::IoSliceMut<'_>],
        meta: &mut [udp::RecvMeta],
    ) -> Poll<io::Result<usize>> {
        if bufs.is_empty() {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "poll_recv: no buffer",
            )));
        }

        let buf = &mut bufs[0];
        let mut ecn = None;
        let len = match self.queue.recv(buf, &mut ecn) {
            Ok(len) => len,
            Err(e) => {
                if e.kind() == io::ErrorKind::WouldBlock {
                    self.recv_waker.register(cx);
                    return Poll::Pending;
                }
                return Poll::Ready(Err(e));
            }
        };

        meta[0] = udp::RecvMeta {
            addr: self.remote_addr,
            len,
            stride: len,
            ecn,
            dst_ip: Some(self.local_addr.ip()),
        };

        Poll::Ready(Ok(1))
    }

    #[inline(always)]
    fn local_addr(&self) -> io::Result<SocketAddr> {
        Ok(self.local_addr)
    }

    #[inline(always)]
    fn max_transmit_segments(&self) -> usize {
        1
    }

    #[inline(always)]
    fn max_receive_segments(&self) -> usize {
        1
    }

    #[inline(always)]
    fn may_fragment(&self) -> bool {
        false
    }
}

/// A UDP poller implementation that always returns ready to write.
#[derive(Copy, Clone, Debug)]
struct AlwaysReadyUdpPoller;

impl UdpPoller for AlwaysReadyUdpPoller {
    #[inline(always)]
    fn poll_writable(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
mod tests {
    use std::task::Waker;

    use quinn::{RecvStream, SendStream};
    use ringbuf::traits::Observer;
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::proxy::tests::generate_certs;

    #[test]
    fn test_datagram_queue_send_recv() {
        let (sender, receiver) = HeapRb::new(4).split();
        let queue = DatagramQueue::<4>::new(sender, receiver);

        assert!(queue.sender.try_lock().unwrap().is_empty());
        assert_eq!(queue.send(&[1, 2, 3, 4], None).ok(), Some(()));
        assert!(!queue.sender.try_lock().unwrap().is_full());
        assert_eq!(queue.send(&[5, 6, 7, 8], None).ok(), Some(()));
        assert!(!queue.sender.try_lock().unwrap().is_full());
        assert_eq!(queue.send(&[9, 10, 11, 12], None).ok(), Some(()));
        assert!(!queue.sender.try_lock().unwrap().is_full());
        assert_eq!(queue.send(&[13, 14, 15, 16], None).ok(), Some(()));
        assert!(queue.sender.try_lock().unwrap().is_full());

        // dropped packet
        assert_eq!(queue.send(&[17, 18, 19, 20], None).ok(), Some(()));

        let mut buf = [0; 4];
        let mut ecn = None;
        assert_eq!(queue.recv(&mut buf, &mut ecn).expect("recv"), 4);
        assert_eq!(buf, [1, 2, 3, 4]);
        assert_eq!(ecn, None);

        assert_eq!(queue.recv(&mut buf, &mut ecn).expect("recv"), 4);
        assert_eq!(buf, [5, 6, 7, 8]);

        assert_eq!(queue.recv(&mut buf, &mut ecn).expect("recv"), 4);
        assert_eq!(buf, [9, 10, 11, 12]);

        assert_eq!(queue.recv(&mut buf, &mut ecn).expect("recv"), 4);
        assert_eq!(buf, [13, 14, 15, 16]);

        assert_eq!(
            queue.recv(&mut buf, &mut ecn).expect_err("recv").kind(),
            io::ErrorKind::WouldBlock
        );
    }

    #[test]
    fn test_inprocessudpsocket_send_recv() {
        let (a, b) = InProcessUdpSocket::new_pair();

        let tx = udp::Transmit {
            destination: b.local_addr().unwrap(),
            ecn: None,
            contents: &[1, 2, 3, 4],
            segment_size: Some(4),
            src_ip: Some(a.local_addr().unwrap().ip()),
        };

        let res = a.try_send(&tx);
        assert!(res.is_ok(), "try_send: {res:?}");

        let mut cx = Context::from_waker(Waker::noop());
        let mut buf = [0u8; 10];
        let mut iobufs = [io::IoSliceMut::new(&mut buf)];
        let mut metas = [udp::RecvMeta::default()];

        match b.poll_recv(&mut cx, &mut iobufs, &mut metas) {
            Poll::Ready(Ok(1)) => assert_eq!(buf, [1, 2, 3, 4, 0, 0, 0, 0, 0, 0]),
            res => panic!("recv: {res:?}"),
        }
    }

    #[tokio::test]
    async fn test_quinn_with_inprocessudpsockets() {
        tracing_subscriber::fmt::fmt()
            .with_writer(io::stderr)
            .with_max_level(tracing::Level::DEBUG)
            .init();

        let (server_socket, client_socket) = InProcessUdpSocket::new_pair();

        let (ca, cert, key) = generate_certs("pglb.localhost", "pglb.localhost").unwrap();

        let server_crypto = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![cert], key)
            .unwrap();

        let server = create_server_endpoint(server_socket, server_crypto).unwrap();
        let server_addr = server.local_addr().unwrap();

        let cancel = CancellationToken::new();
        let server_cancel = cancel.clone();
        let server_task = tokio::spawn(start_echo_server(cancel.clone(), server));

        let mut root_certs = rustls::RootCertStore::empty();
        root_certs.add(ca).unwrap();
        let client_crypto = rustls::ClientConfig::builder()
            .with_root_certificates(root_certs)
            .with_no_client_auth();

        let client = create_client_endpoint(client_socket, client_crypto).unwrap();

        let connect = client.connect(server_addr, "pglb.localhost").unwrap();
        let conn = connect.await.unwrap();

        let (mut send, mut recv) = conn.open_bi().await.unwrap();

        send.write_all(b"hello, pglb!\n").await.unwrap();
        send.finish().unwrap();
        drop(send);
        let mut buf = [0u8; MTU as usize];
        let n = recv.read(&mut buf).await.unwrap().unwrap();

        server_cancel.cancel();
        server_task.await.unwrap().unwrap();

        assert_eq!(&buf[..n], b"hello, pglb!\n");
    }

    async fn start_echo_server(
        cancel: CancellationToken,
        endpoint: quinn::Endpoint,
    ) -> anyhow::Result<()> {
        loop {
            tokio::select! {
                () = cancel.cancelled() => break,
                incoming = endpoint.accept() => {
                    let Some(incoming) = incoming else {
                        break;
                    };
                    tokio::spawn(handle_echo_connection(cancel.clone(), incoming));
                }
            }
        }
        Ok(())
    }

    async fn handle_echo_connection(
        cancel: CancellationToken,
        incoming: quinn::Incoming,
    ) -> anyhow::Result<()> {
        let conn = incoming.accept()?.await?;
        loop {
            tokio::select! {
                () = cancel.cancelled() => break,
                stream = conn.accept_bi() => {
                    let Ok(stream) = stream else {
                        break;
                    };
                    tokio::spawn(handle_echo_stream(cancel.clone(), stream));
                }
            }
        }
        Ok(())
    }

    async fn handle_echo_stream(
        _cancel: CancellationToken,
        (mut send, mut recv): (SendStream, RecvStream),
    ) -> anyhow::Result<()> {
        let mut buf = vec![0u8; MTU as usize].into_boxed_slice();
        while let Some(n) = recv.read(&mut buf).await? {
            send.write_all(&buf[..n]).await?;
        }
        Ok(())
    }
}
