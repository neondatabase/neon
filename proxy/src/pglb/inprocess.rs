use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::{fmt, io};

use futures::task::AtomicWaker;
use quinn::crypto::rustls::{QuicClientConfig, QuicServerConfig};
use quinn::{AsyncUdpSocket, UdpPoller, udp};
use ringbuf::traits::{Consumer, Producer, Split};
use ringbuf::{CachingCons, CachingProd, HeapRb};
use rustls::pki_types;
use try_lock::TryLock;

// TODO: remove encryption for in-process connections.
pub fn create_server_endpoint(
    server_socket: InProcessUdpSocket,
    cert: pki_types::CertificateDer<'static>,
    key: pki_types::PrivateKeyDer<'static>,
) -> anyhow::Result<quinn::Endpoint> {
    let mut server_crypto = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert], key)?;
    server_crypto.alpn_protocols = vec![b"pglb".into()];
    server_crypto.key_log = Arc::new(rustls::KeyLogFile::new());

    let mut server_config =
        quinn::ServerConfig::with_crypto(Arc::new(QuicServerConfig::try_from(server_crypto)?));
    let transport_config = Arc::get_mut(&mut server_config.transport).expect("no other clone");
    transport_config.max_concurrent_uni_streams(0_u8.into());

    let server = quinn::Endpoint::new_with_abstract_socket(
        quinn::EndpointConfig::default(),
        Some(server_config),
        Arc::new(server_socket),
        Arc::new(quinn::TokioRuntime),
    )?;

    Ok(server)
}

pub fn create_client_endpoint(
    client_socket: InProcessUdpSocket,
    ca: pki_types::CertificateDer<'static>,
) -> anyhow::Result<quinn::Endpoint> {
    let mut client = quinn::Endpoint::new_with_abstract_socket(
        quinn::EndpointConfig::default(),
        None,
        Arc::new(client_socket),
        Arc::new(quinn::TokioRuntime),
    )?;

    let mut root_certs = rustls::RootCertStore::empty();
    root_certs.add(ca)?;

    let mut client_crypto = rustls::ClientConfig::builder()
        .with_root_certificates(root_certs)
        .with_no_client_auth();
    client_crypto.alpn_protocols = vec![b"pglb".into()];
    client_crypto.key_log = Arc::new(rustls::KeyLogFile::new());

    let client_config =
        quinn::ClientConfig::new(Arc::new(QuicClientConfig::try_from(client_crypto)?));
    client.set_default_client_config(client_config);

    Ok(client)
}

struct Datagram<const MSS: usize> {
    len: usize,
    buf: [u8; MSS],
}

struct DatagramQueue<const MSS: usize> {
    sender: TryLock<CachingProd<Arc<HeapRb<Datagram<MSS>>>>>,
    receiver: TryLock<CachingCons<Arc<HeapRb<Datagram<MSS>>>>>,
    receiver_waker: Arc<ReceiverWaker>,
}

impl<const MSS: usize> DatagramQueue<MSS> {
    fn new(
        sender: CachingProd<Arc<HeapRb<Datagram<MSS>>>>,
        receiver: CachingCons<Arc<HeapRb<Datagram<MSS>>>>,
    ) -> Self {
        Self {
            sender: TryLock::new(sender),
            receiver: TryLock::new(receiver),
            receiver_waker: Arc::new(ReceiverWaker::new()),
        }
    }

    #[tracing::instrument(level = "debug", skip_all, err, fields(packet = packet.len()))]
    fn send(&self, packet: &[u8]) -> io::Result<()> {
        if packet.len() > MSS {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "send: packet too large",
            ));
        }

        let mut sender = self.sender.try_lock().expect("no other thread");

        let s = sender.vacant_slices_mut().0;
        if s.is_empty() {
            // Drop the packet.
            return Ok(());
        }

        // SAFETY: the ring buffer contains byte arrays that we initialize here
        // and the advance write index can by incremented by 1 because we checked
        // that there is at least one vacant slot in the ring buffer.
        unsafe {
            let dgram = s[0].assume_init_mut();
            dgram.len = packet.len();
            dgram.buf[..packet.len()].copy_from_slice(packet);
            sender.advance_write_index(1);
        }

        self.receiver_waker.signal();

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all, ret, err, fields(buf = buf.len()))]
    fn recv(&self, buf: &mut [u8]) -> io::Result<usize> {
        let mut receiver = self.receiver.try_lock().expect("no other thread");

        if let Some(dgram) = receiver.try_peek() {
            let len = dgram.len;
            if len > buf.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "recv: buffer too small",
                ));
            }
            buf[..len].copy_from_slice(&dgram.buf[..len]);
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

struct ReceiverWaker {
    waker: AtomicWaker,
}

impl ReceiverWaker {
    fn new() -> Self {
        ReceiverWaker {
            waker: AtomicWaker::new(),
        }
    }

    #[tracing::instrument(level = "debug", skip_all)]
    fn register(&self, cx: &mut Context<'_>) {
        self.waker.register(cx.waker());
    }

    #[tracing::instrument(level = "debug", skip_all)]
    fn signal(&self) {
        self.waker.wake();
    }
}

pub struct InProcessUdpSocket {
    queue: DatagramQueue<65536>,
    sibling_receiver_waker: Arc<ReceiverWaker>,
    local_addr: SocketAddr,
    remote_addr: SocketAddr,
}

impl fmt::Debug for InProcessUdpSocket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InProcessUdpSocket")
            .field("sockaddr", &self.local_addr)
            .finish_non_exhaustive()
    }
}

impl InProcessUdpSocket {
    pub fn new_pair() -> (Self, Self) {
        let (sender_a, receiver_a) = HeapRb::new(16).split();
        let (sender_b, receiver_b) = HeapRb::new(16).split();

        let addr_a = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 111)), 11111);
        let addr_b = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 222)), 22222);

        let queue_a = DatagramQueue::new(sender_a, receiver_b);
        let queue_b = DatagramQueue::new(sender_b, receiver_a);

        let receiver_waker_a = Arc::clone(&queue_a.receiver_waker);
        let receiver_waker_b = Arc::clone(&queue_b.receiver_waker);

        let socket_a = InProcessUdpSocket {
            queue: queue_a,
            sibling_receiver_waker: receiver_waker_b,
            local_addr: addr_a,
            remote_addr: addr_b,
        };

        let socket_b = InProcessUdpSocket {
            queue: queue_b,
            sibling_receiver_waker: receiver_waker_a,
            local_addr: addr_b,
            remote_addr: addr_a,
        };

        (socket_a, socket_b)
    }
}

impl AsyncUdpSocket for InProcessUdpSocket {
    #[tracing::instrument(level = "debug", skip_all)]
    fn create_io_poller(self: Arc<Self>) -> Pin<Box<dyn UdpPoller>> {
        Box::pin(AlwaysReadyUdpPoller)
    }

    #[tracing::instrument(level = "debug", skip_all, err)]
    fn try_send(&self, transmit: &udp::Transmit<'_>) -> io::Result<()> {
        if transmit.src_ip.is_some_and(|ip| ip != self.local_addr.ip()) {
            return Err(io::Error::new(
                io::ErrorKind::AddrNotAvailable,
                "try_send: address not available",
            ));
        }
        if transmit.destination != self.remote_addr {
            // TODO: or drop the packet?
            return Err(io::Error::new(
                io::ErrorKind::HostUnreachable,
                "try_send: host unreachable",
            ));
        }
        self.queue.send(transmit.contents)?;
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all, ret)]
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
        if bufs.len() > 1 {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "poll_recv: multiple buffers",
            )));
        }

        let buf = &mut bufs[0];
        let len = match self.queue.recv(buf) {
            Ok(len) => len,
            Err(e) => {
                if e.kind() == io::ErrorKind::WouldBlock {
                    self.sibling_receiver_waker.register(cx);
                    return Poll::Pending;
                }
                return Poll::Ready(Err(e));
            }
        };

        meta[0] = udp::RecvMeta {
            addr: self.remote_addr,
            len,
            stride: len,
            ecn: None,
            dst_ip: Some(self.local_addr.ip()),
        };

        Poll::Ready(Ok(len))
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        Ok(self.local_addr)
    }

    fn max_transmit_segments(&self) -> usize {
        1
    }

    fn max_receive_segments(&self) -> usize {
        1
    }

    fn may_fragment(&self) -> bool {
        false
    }
}

#[derive(Debug)]
struct AlwaysReadyUdpPoller;

impl UdpPoller for AlwaysReadyUdpPoller {
    #[tracing::instrument(level = "debug", skip_all)]
    fn poll_writable(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<io::Result<()>> {
        // TODO: let's see if this works well in prod.
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

        assert_eq!(queue.sender.try_lock().unwrap().is_empty(), true);
        assert_eq!(queue.send(&[1, 2, 3, 4]).ok(), Some(()));
        assert_eq!(queue.sender.try_lock().unwrap().is_full(), false);
        assert_eq!(queue.send(&[5, 6, 7, 8]).ok(), Some(()));
        assert_eq!(queue.sender.try_lock().unwrap().is_full(), false);
        assert_eq!(queue.send(&[9, 10, 11, 12]).ok(), Some(()));
        assert_eq!(queue.sender.try_lock().unwrap().is_full(), false);
        assert_eq!(queue.send(&[13, 14, 15, 16]).ok(), Some(()));
        assert_eq!(queue.sender.try_lock().unwrap().is_full(), true);

        // dropped packet
        assert_eq!(queue.send(&[17, 18, 19, 20]).ok(), Some(()));

        let mut buf = [0; 4];
        assert_eq!(queue.recv(&mut buf).expect("recv"), 4);
        assert_eq!(buf, [1, 2, 3, 4]);

        assert_eq!(queue.recv(&mut buf).expect("recv"), 4);
        assert_eq!(buf, [5, 6, 7, 8]);

        assert_eq!(queue.recv(&mut buf).expect("recv"), 4);
        assert_eq!(buf, [9, 10, 11, 12]);

        assert_eq!(queue.recv(&mut buf).expect("recv"), 4);
        assert_eq!(buf, [13, 14, 15, 16]);

        assert_eq!(
            queue.recv(&mut buf).expect_err("recv").kind(),
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

        assert_eq!(a.try_send(&tx).ok(), Some(()));

        let mut cx = Context::from_waker(&Waker::noop());
        let mut buf = [0u8; 10];
        let mut iobufs = [io::IoSliceMut::new(&mut buf)];
        let mut metas = [udp::RecvMeta::default()];

        match b.poll_recv(&mut cx, &mut iobufs, &mut metas) {
            Poll::Ready(Ok(4)) => assert_eq!(buf, [1, 2, 3, 4, 0, 0, 0, 0, 0, 0]),
            _ => panic!("recv"),
        }
    }

    #[tokio::test]
    async fn test_quinn_with_inprocessudpsockets() {
        let (server_socket, client_socket) = InProcessUdpSocket::new_pair();

        let (ca, cert, key) = generate_certs("pglb.localhost", "pglb.localhost").unwrap();

        let server = create_server_endpoint(server_socket, cert, key).unwrap();
        let server_addr = server.local_addr().unwrap();

        let cancel = CancellationToken::new();
        let server_cancel = cancel.clone();
        let server_task = tokio::spawn(start_echo_server(cancel.clone(), server));

        let client = create_client_endpoint(client_socket, ca).unwrap();

        let connect = client.connect(server_addr, "pglb.localhost").unwrap();
        let conn = connect.await.unwrap();

        let (mut send, mut recv) = conn.open_bi().await.unwrap();

        send.write_all(b"hello, pglb!\n").await.unwrap();
        send.finish().unwrap();
        drop(send);
        let mut buf = [0u8; 64];
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
                _ = cancel.cancelled() => break,
                incoming = endpoint.accept() => {
                    let Some(incoming) = incoming else {
                        break;
                    };
                    tokio::spawn(handle_echo_incoming(cancel.clone(), incoming));
                }
            }
        }
        Ok(())
    }

    async fn handle_echo_incoming(
        cancel: CancellationToken,
        incoming: quinn::Incoming,
    ) -> anyhow::Result<()> {
        let conn = incoming.accept()?.await?;
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
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
        let mut buf = [0u8; 64 * 1024];
        while let Some(n) = recv.read(&mut buf).await? {
            send.write_all(&buf[..n]).await?;
        }
        Ok(())
    }
}
