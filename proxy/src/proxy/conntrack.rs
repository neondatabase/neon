use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::SystemTime;
use std::{fmt, io};

use pin_project_lite::pin_project;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ConnId(usize);

#[derive(Default)]
pub struct ConnectionTracking {
    conns: clashmap::ClashMap<ConnId, (ConnectionState, SystemTime)>,
}

impl ConnectionTracking {
    pub fn new_tracker(self: &Arc<Self>) -> ConnectionTracker<Arc<Self>> {
        let conn_id = self.new_conn_id();
        ConnectionTracker::new(conn_id, Arc::clone(self))
    }

    fn new_conn_id(&self) -> ConnId {
        static NEXT_ID: AtomicUsize = AtomicUsize::new(0);
        let id = ConnId(NEXT_ID.fetch_add(1, Ordering::Relaxed));
        self.conns
            .insert(id, (ConnectionState::Idle, SystemTime::now()));
        id
    }

    fn update(&self, conn_id: ConnId, new_state: ConnectionState) {
        let new_timestamp = SystemTime::now();
        let old_state = self.conns.insert(conn_id, (new_state, new_timestamp));

        if let Some((old_state, _old_timestamp)) = old_state {
            tracing::debug!(?conn_id, %old_state, %new_state, "conntrack: update");
        } else {
            tracing::debug!(?conn_id, %new_state, "conntrack: update");
        }
    }

    fn remove(&self, conn_id: ConnId) {
        if let Some((_, (old_state, _old_timestamp))) = self.conns.remove(&conn_id) {
            tracing::debug!(?conn_id, %old_state, "conntrack: remove");
        }
    }
}

impl StateChangeObserver for Arc<ConnectionTracking> {
    type ConnId = ConnId;
    fn change(
        &self,
        conn_id: Self::ConnId,
        _old_state: ConnectionState,
        new_state: ConnectionState,
    ) {
        match new_state {
            ConnectionState::Init
            | ConnectionState::Idle
            | ConnectionState::Transaction
            | ConnectionState::Busy
            | ConnectionState::Unknown => self.update(conn_id, new_state),
            ConnectionState::Closed => self.remove(conn_id),
        }
    }
}

/// Called by `ConnectionTracker` whenever the `ConnectionState` changed.
pub trait StateChangeObserver {
    /// Identifier of the connection passed back on state change.
    type ConnId: Copy;
    /// Called iff the connection's state changed.
    fn change(&self, conn_id: Self::ConnId, old_state: ConnectionState, new_state: ConnectionState);
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum ConnectionState {
    #[default]
    Init = 0,
    Idle = 1,
    Transaction = 2,
    Busy = 3,
    Closed = 4,
    Unknown = 5,
}

impl ConnectionState {
    const fn into_repr(self) -> u8 {
        self as u8
    }

    const fn from_repr(value: u8) -> Option<Self> {
        Some(match value {
            0 => Self::Init,
            1 => Self::Idle,
            2 => Self::Transaction,
            3 => Self::Busy,
            4 => Self::Closed,
            5 => Self::Unknown,
            _ => return None,
        })
    }
}

impl fmt::Display for ConnectionState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            ConnectionState::Init => f.write_str("init"),
            ConnectionState::Idle => f.write_str("idle"),
            ConnectionState::Transaction => f.write_str("transaction"),
            ConnectionState::Busy => f.write_str("busy"),
            ConnectionState::Closed => f.write_str("closed"),
            ConnectionState::Unknown => f.write_str("unknown"),
        }
    }
}

/// Stores the `ConnectionState`. Used by ConnectionTracker to avoid needing
/// mutable references.
#[derive(Debug, Default)]
struct AtomicConnectionState(AtomicU8);

impl AtomicConnectionState {
    fn set(&self, state: ConnectionState) {
        self.0.store(state.into_repr(), Ordering::Relaxed);
    }

    fn get(&self) -> ConnectionState {
        ConnectionState::from_repr(self.0.load(Ordering::Relaxed)).expect("only valid variants")
    }
}

/// Tracks the `ConnectionState` of a connection by inspecting the frontend and
/// backend stream and reacting to specific messages. Used in combination with
/// two `TrackedStream`s.
pub struct ConnectionTracker<SCO: StateChangeObserver> {
    state: AtomicConnectionState,
    observer: SCO,
    conn_id: SCO::ConnId,
}

impl<SCO: StateChangeObserver> Drop for ConnectionTracker<SCO> {
    fn drop(&mut self) {
        self.observer
            .change(self.conn_id, self.state.get(), ConnectionState::Closed);
    }
}

impl<SCO: StateChangeObserver> ConnectionTracker<SCO> {
    pub fn new(conn_id: SCO::ConnId, observer: SCO) -> Self {
        ConnectionTracker {
            conn_id,
            state: AtomicConnectionState::default(),
            observer,
        }
    }

    pub fn frontend_message_tag(&self, tag: Tag) {
        self.update_state(|old_state| Self::state_from_frontend_tag(old_state, tag));
    }

    pub fn backend_message_tag(&self, tag: Tag) {
        self.update_state(|old_state| Self::state_from_backend_tag(old_state, tag));
    }

    fn update_state(&self, new_state_fn: impl FnOnce(ConnectionState) -> ConnectionState) {
        let old_state = self.state.get();
        let new_state = new_state_fn(old_state);
        if old_state != new_state {
            self.observer.change(self.conn_id, old_state, new_state);
            self.state.set(new_state);
        }
    }

    fn state_from_frontend_tag(_old_state: ConnectionState, fe_tag: Tag) -> ConnectionState {
        // Most activity from the client puts connection into busy state.
        // Only the server can put a connection back into idle state.
        match fe_tag {
            Tag::Start | Tag::ReadyForQuery(_) | Tag::Message(_) => ConnectionState::Busy,
            Tag::End => ConnectionState::Closed,
            Tag::Lost => ConnectionState::Unknown,
        }
    }

    fn state_from_backend_tag(old_state: ConnectionState, be_tag: Tag) -> ConnectionState {
        match be_tag {
            // Check for RFQ and put connection into idle or idle in transaction state.
            Tag::ReadyForQuery(b'I') => ConnectionState::Idle,
            Tag::ReadyForQuery(b'T') => ConnectionState::Transaction,
            Tag::ReadyForQuery(b'E') => ConnectionState::Transaction,
            // We can't put a connection into idle state for unknown RFQ status.
            Tag::ReadyForQuery(_) => ConnectionState::Unknown,
            // Ignore out-fo message from the server.
            Tag::NOTICE | Tag::NOTIFICATION_RESPONSE | Tag::PARAMETER_STATUS => old_state,
            // All other activity from server puts connection into busy state.
            Tag::Start | Tag::Message(_) => ConnectionState::Busy,

            Tag::End => ConnectionState::Closed,
            Tag::Lost => ConnectionState::Unknown,
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Tag {
    Message(u8),
    ReadyForQuery(u8),
    Start,
    End,
    Lost,
}

impl Tag {
    const READY_FOR_QUERY: Tag = Tag::Message(b'Z');
    const NOTICE: Tag = Tag::Message(b'N');
    const NOTIFICATION_RESPONSE: Tag = Tag::Message(b'A');
    const PARAMETER_STATUS: Tag = Tag::Message(b'S');
}

impl fmt::Display for Tag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Tag::Start => f.write_str("start"),
            Tag::End => f.write_str("end"),
            Tag::Lost => f.write_str("lost"),
            Tag::Message(tag) => write!(f, "'{}'", tag as char),
            Tag::ReadyForQuery(status) => write!(f, "ReadyForQuery:'{}'", status as char),
        }
    }
}

pub trait TagObserver {
    fn observe(&mut self, tag: Tag);
}

impl<F: FnMut(Tag)> TagObserver for F {
    fn observe(&mut self, tag: Tag) {
        (self)(tag);
    }
}

pin_project! {
    pub struct TrackedStream<S, TO> {
        #[pin]
        stream: S,
        scanner: StreamScanner<TO>,
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin, TO: TagObserver> TrackedStream<S, TO> {
    pub const fn new(stream: S, midstream: bool, observer: TO) -> Self {
        TrackedStream {
            stream,
            scanner: StreamScanner::new(midstream, observer),
        }
    }
}

impl<S: AsyncRead + Unpin, TO: TagObserver> AsyncRead for TrackedStream<S, TO> {
    #[inline]
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.project();
        let old_len = buf.filled().len();
        match this.stream.poll_read(cx, buf) {
            Poll::Ready(Ok(())) => {
                let new_len = buf.filled().len();
                this.scanner.scan_bytes(&buf.filled()[old_len..new_len]);
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<S: AsyncWrite + Unpin, TO> AsyncWrite for TrackedStream<S, TO> {
    #[inline(always)]
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.project().stream.poll_write(cx, buf)
    }

    #[inline(always)]
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.project().stream.poll_flush(cx)
    }

    #[inline(always)]
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.project().stream.poll_shutdown(cx)
    }
}

#[derive(Debug)]
struct StreamScanner<TO> {
    observer: TO,
    state: StreamScannerState,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum StreamScannerState {
    /// Initial state when no message has been read and we are looling for a
    /// message without a tag.
    Start,
    /// Read a message tag.
    Tag,
    /// Read the length bytes and calculate the total length.
    Length {
        tag: Tag,
        /// Number of bytes missing to know the full length of the message: 0..=4
        length_bytes_missing: usize,
        /// Total length of the message (without tag) that is calculated as we
        /// read the bytes for the length.
        calculated_length: usize,
    },
    /// Read (= skip) the payload.
    Payload {
        tag: Tag,
        /// If this is the first time payload bytes are read. Important when
        /// inspecting specific messages, like ReadyForQuery.
        first: bool,
        /// Number of payload bytes left to read before looking for a new tag.
        bytes_to_skip: usize,
    },
    /// Stream was terminated.
    End,
    /// Stream ended up in a lost state. We only stop tracking the stream, not
    /// interrupt it.
    Lost,
}

impl<TO: TagObserver> StreamScanner<TO> {
    const fn new(midstream: bool, observer: TO) -> Self {
        StreamScanner {
            observer,
            state: if midstream {
                StreamScannerState::Tag
            } else {
                StreamScannerState::Start
            },
        }
    }
}

impl<TO: TagObserver> StreamScanner<TO> {
    fn scan_bytes(&mut self, mut buf: &[u8]) {
        use StreamScannerState as S;

        if matches!(self.state, S::End | S::Lost) {
            return;
        }
        if buf.is_empty() {
            match self.state {
                S::Start | S::Tag => {
                    self.observer.observe(Tag::End);
                    self.state = S::End;
                    return;
                }
                S::Length { .. } | S::Payload { .. } => {
                    self.observer.observe(Tag::Lost);
                    self.state = S::Lost;
                    return;
                }
                S::End | S::Lost => unreachable!(),
            }
        }

        while !buf.is_empty() {
            match self.state {
                S::Start => {
                    self.state = S::Length {
                        tag: Tag::Start,
                        length_bytes_missing: 4,
                        calculated_length: 0,
                    };
                }

                S::Tag => {
                    let tag = buf.first().copied().expect("buf not empty");
                    buf = &buf[1..];

                    self.state = S::Length {
                        tag: Tag::Message(tag),
                        length_bytes_missing: 4,
                        calculated_length: 0,
                    };
                }

                S::Length {
                    tag,
                    mut length_bytes_missing,
                    mut calculated_length,
                } => {
                    let consume = length_bytes_missing.min(buf.len());

                    let (length_bytes, remainder) = buf.split_at(consume);
                    for b in length_bytes {
                        calculated_length <<= 8;
                        calculated_length |= *b as usize;
                    }
                    buf = remainder;

                    length_bytes_missing -= consume;
                    if length_bytes_missing == 0 {
                        let Some(bytes_to_skip) = calculated_length.checked_sub(4) else {
                            self.observer.observe(Tag::Lost);
                            self.state = S::Lost;
                            return;
                        };

                        if bytes_to_skip == 0 {
                            self.observer.observe(tag);
                            self.state = S::Tag;
                        } else {
                            self.state = S::Payload {
                                tag,
                                first: true,
                                bytes_to_skip,
                            };
                        }
                    } else {
                        self.state = S::Length {
                            tag,
                            length_bytes_missing,
                            calculated_length,
                        };
                    }
                }

                S::Payload {
                    tag,
                    first,
                    mut bytes_to_skip,
                } => {
                    let consume = bytes_to_skip.min(buf.len());
                    bytes_to_skip -= consume;
                    if bytes_to_skip == 0 {
                        if tag == Tag::READY_FOR_QUERY && first && consume == 1 {
                            let status = buf.first().copied().expect("buf not empty");
                            self.observer.observe(Tag::ReadyForQuery(status));
                        } else {
                            self.observer.observe(tag);
                        }
                        self.state = S::Tag;
                    } else {
                        self.state = S::Payload {
                            tag,
                            first: false,
                            bytes_to_skip,
                        };
                    }
                    buf = &buf[consume..];
                }

                S::End | S::Lost => unreachable!(),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::pin::pin;
    use std::rc::Rc;
    use std::sync::{Arc, Mutex};

    use tokio::io::{AsyncReadExt, BufReader};

    use super::*;

    #[test]
    fn test_stream_scanner() {
        let tags = Rc::new(RefCell::new(Vec::new()));
        let observer_tags = tags.clone();
        let observer = move |tag| {
            observer_tags.borrow_mut().push(tag);
        };
        let mut scanner = StreamScanner::new(false, observer);

        scanner.scan_bytes(&[0, 0]);
        assert_eq!(tags.borrow().as_slice(), &[]);
        assert_eq!(
            scanner.state,
            StreamScannerState::Length {
                tag: Tag::Start,
                length_bytes_missing: 2,
                calculated_length: 0,
            }
        );

        scanner.scan_bytes(&[0x01, 0x01, 0x00]);
        assert_eq!(tags.borrow().as_slice(), &[]);
        assert_eq!(
            scanner.state,
            StreamScannerState::Payload {
                tag: Tag::Start,
                first: false,
                bytes_to_skip: 0x00000101 - 4 - 1,
            }
        );

        scanner.scan_bytes(vec![0; 0x00000101 - 4 - 1 - 1].as_slice());
        assert_eq!(tags.borrow().as_slice(), &[]);
        assert_eq!(
            scanner.state,
            StreamScannerState::Payload {
                tag: Tag::Start,
                first: false,
                bytes_to_skip: 1,
            }
        );

        scanner.scan_bytes(&[0x00, b'A', 0x00, 0x00, 0x00, 0x08]);
        assert_eq!(tags.borrow().as_slice(), &[Tag::Start]);
        assert_eq!(
            scanner.state,
            StreamScannerState::Payload {
                tag: Tag::Message(b'A'),
                first: true,
                bytes_to_skip: 4,
            }
        );

        scanner.scan_bytes(&[0, 0, 0, 0]);
        assert_eq!(tags.borrow().as_slice(), &[Tag::Start, Tag::Message(b'A')]);
        assert_eq!(scanner.state, StreamScannerState::Tag);

        scanner.scan_bytes(&[b'Z', 0x00, 0x00, 0x00, 0x05, b'T']);
        assert_eq!(
            tags.borrow().as_slice(),
            &[Tag::Start, Tag::Message(b'A'), Tag::ReadyForQuery(b'T')]
        );
        assert_eq!(scanner.state, StreamScannerState::Tag);

        scanner.scan_bytes(&[]);
        assert_eq!(
            tags.borrow().as_slice(),
            &[
                Tag::Start,
                Tag::Message(b'A'),
                Tag::ReadyForQuery(b'T'),
                Tag::End
            ]
        );
        assert_eq!(scanner.state, StreamScannerState::End);
    }

    #[tokio::test]
    async fn test_connection_tracker() {
        let transitions: Arc<Mutex<Vec<(ConnectionState, ConnectionState)>>> = Arc::default();
        struct Observer(Arc<Mutex<Vec<(ConnectionState, ConnectionState)>>>);
        impl StateChangeObserver for Observer {
            type ConnId = usize;
            fn change(
                &self,
                conn_id: Self::ConnId,
                old_state: ConnectionState,
                new_state: ConnectionState,
            ) {
                assert_eq!(conn_id, 42);
                self.0.lock().unwrap().push((old_state, new_state));
            }
        }
        let tracker = ConnectionTracker::new(42, Observer(transitions.clone()));

        let stream = TestStream::new(
            &[
                0, 0, 0, 4, // Init
                b'Z', 0, 0, 0, 5, b'I', // Init -> Idle
                b'x', 0, 0, 0, 4, // Idle -> Busy
                b'Z', 0, 0, 0, 5, b'I', // Busy -> Idle
            ][..],
        );
        // AsyncRead
        let mut stream = TrackedStream::new(stream, false, |tag| tracker.backend_message_tag(tag));

        let mut readbuf = [0; 2];
        let n = stream.read_exact(&mut readbuf).await.unwrap();
        assert_eq!(n, 2);
        assert_eq!(&readbuf, &[0, 0,]);
        assert!(transitions.lock().unwrap().is_empty());

        let mut readbuf = [0; 2];
        let n = stream.read_exact(&mut readbuf).await.unwrap();
        assert_eq!(n, 2);
        assert_eq!(&readbuf, &[0, 4]);
        assert_eq!(
            transitions.lock().unwrap().as_slice(),
            &[(ConnectionState::Init, ConnectionState::Busy)]
        );

        let mut readbuf = [0; 6];
        let n = stream.read_exact(&mut readbuf).await.unwrap();
        assert_eq!(n, 6);
        assert_eq!(&readbuf, &[b'Z', 0, 0, 0, 5, b'I']);
        assert_eq!(
            transitions.lock().unwrap().as_slice(),
            &[
                (ConnectionState::Init, ConnectionState::Busy),
                (ConnectionState::Busy, ConnectionState::Idle),
            ]
        );

        let mut readbuf = [0; 5];
        let n = stream.read_exact(&mut readbuf).await.unwrap();
        assert_eq!(n, 5);
        assert_eq!(&readbuf, &[b'x', 0, 0, 0, 4]);
        assert_eq!(
            transitions.lock().unwrap().as_slice(),
            &[
                (ConnectionState::Init, ConnectionState::Busy),
                (ConnectionState::Busy, ConnectionState::Idle),
                (ConnectionState::Idle, ConnectionState::Busy),
            ]
        );

        let mut readbuf = [0; 6];
        let n = stream.read_exact(&mut readbuf).await.unwrap();
        assert_eq!(n, 6);
        assert_eq!(&readbuf, &[b'Z', 0, 0, 0, 5, b'I']);
        assert_eq!(
            transitions.lock().unwrap().as_slice(),
            &[
                (ConnectionState::Init, ConnectionState::Busy),
                (ConnectionState::Busy, ConnectionState::Idle),
                (ConnectionState::Idle, ConnectionState::Busy),
                (ConnectionState::Busy, ConnectionState::Idle),
            ]
        );
    }

    struct TestStream {
        stream: BufReader<&'static [u8]>,
    }
    impl TestStream {
        fn new(data: &'static [u8]) -> Self {
            TestStream {
                stream: BufReader::new(data),
            }
        }
    }
    impl AsyncRead for TestStream {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            pin!(&mut self.stream).poll_read(cx, buf)
        }
    }
    impl AsyncWrite for TestStream {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<Result<usize, io::Error>> {
            Poll::Ready(Ok(buf.len()))
        }
        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
            Poll::Ready(Ok(()))
        }
        fn poll_shutdown(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), io::Error>> {
            Poll::Ready(Ok(()))
        }
    }
}
