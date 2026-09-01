use std::future::poll_fn;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll, ready};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tracing::info;

#[derive(Debug)]
enum TransferState {
    Running(CopyBuffer),
    ShuttingDown(u64),
    Done(u64),
}

impl TransferState {
    fn is_buffer_empty_and_flushed(&self) -> bool {
        match self {
            TransferState::Running(buf) => buf.is_empty_and_flushed(),
            TransferState::ShuttingDown(_) | TransferState::Done(_) => true,
        }
    }
}

#[derive(Debug)]
pub(crate) enum ErrorDirection {
    Read(io::Error),
    Write(io::Error),
}

impl ErrorSource {
    fn from_client(err: ErrorDirection) -> ErrorSource {
        match err {
            ErrorDirection::Read(client) => Self::Client(client),
            ErrorDirection::Write(compute) => Self::Compute(compute),
        }
    }
    fn from_compute(err: ErrorDirection) -> ErrorSource {
        match err {
            ErrorDirection::Write(client) => Self::Client(client),
            ErrorDirection::Read(compute) => Self::Compute(compute),
        }
    }
}

#[derive(Debug)]
pub enum ErrorSource {
    Client(io::Error),
    Compute(io::Error),
}

fn transfer_one_direction<A, B>(
    cx: &mut Context<'_>,
    state: &mut TransferState,
    r: &mut A,
    w: &mut B,
) -> Poll<Result<u64, ErrorDirection>>
where
    A: AsyncRead + AsyncWrite + Unpin + ?Sized,
    B: AsyncRead + AsyncWrite + Unpin + ?Sized,
{
    let mut r = Pin::new(r);
    let mut w = Pin::new(w);
    loop {
        match state {
            TransferState::Running(buf) => {
                let count = ready!(buf.poll_copy(cx, r.as_mut(), w.as_mut()))?;
                *state = TransferState::ShuttingDown(count);
            }
            TransferState::ShuttingDown(count) => {
                ready!(w.as_mut().poll_shutdown(cx)).map_err(ErrorDirection::Write)?;
                *state = TransferState::Done(*count);
            }
            TransferState::Done(count) => return Poll::Ready(Ok(*count)),
        }
    }
}

/// Like `transfer_one_direction`, but never calls `poll_shutdown` on the writer
/// when the reader hits EOF. Used in the pooled bidirectional copy for the
/// client→compute direction, because shutting down the compute write half
/// (`shutdown(SHUT_WR)`) breaks the connection for pool reuse: Postgres
/// reacts to the FIN by closing the backend, and subsequent reads hit
/// `ENOTCONN`.
fn transfer_one_direction_no_shutdown<A, B>(
    cx: &mut Context<'_>,
    state: &mut TransferState,
    r: &mut A,
    w: &mut B,
) -> Poll<Result<u64, ErrorDirection>>
where
    A: AsyncRead + AsyncWrite + Unpin + ?Sized,
    B: AsyncRead + AsyncWrite + Unpin + ?Sized,
{
    let mut r = Pin::new(r);
    let mut w = Pin::new(w);
    loop {
        match state {
            TransferState::Running(buf) => {
                let count = ready!(buf.poll_copy(cx, r.as_mut(), w.as_mut()))?;
                *state = TransferState::Done(count);
            }
            TransferState::ShuttingDown(count) => {
                *state = TransferState::Done(*count);
            }
            TransferState::Done(count) => return Poll::Ready(Ok(*count)),
        }
    }
}

#[tracing::instrument(skip_all)]
pub async fn copy_bidirectional_client_compute<Client, Compute>(
    client: &mut Client,
    compute: &mut Compute,
) -> Result<(u64, u64), ErrorSource>
where
    Client: AsyncRead + AsyncWrite + Unpin + ?Sized,
    Compute: AsyncRead + AsyncWrite + Unpin + ?Sized,
{
    let mut client_to_compute = TransferState::Running(CopyBuffer::new());
    let mut compute_to_client = TransferState::Running(CopyBuffer::new());

    poll_fn(|cx| {
        let mut client_to_compute_result =
            transfer_one_direction(cx, &mut client_to_compute, client, compute)
                .map_err(ErrorSource::from_client)?;
        let mut compute_to_client_result =
            transfer_one_direction(cx, &mut compute_to_client, compute, client)
                .map_err(ErrorSource::from_compute)?;

        // Early termination checks from compute to client.
        if let TransferState::Done(_) = compute_to_client
            && let TransferState::Running(buf) = &client_to_compute
        {
            info!("Compute is done, terminate client");
            client_to_compute = TransferState::ShuttingDown(buf.amt);
            client_to_compute_result =
                transfer_one_direction(cx, &mut client_to_compute, client, compute)
                    .map_err(ErrorSource::from_client)?;
        }

        // Early termination checks from client to compute.
        if let TransferState::Done(_) = client_to_compute
            && let TransferState::Running(buf) = &compute_to_client
        {
            info!("Client is done, terminate compute");
            compute_to_client = TransferState::ShuttingDown(buf.amt);
            compute_to_client_result =
                transfer_one_direction(cx, &mut compute_to_client, compute, client)
                    .map_err(ErrorSource::from_compute)?;
        }

        let client_to_compute = ready!(client_to_compute_result);
        let compute_to_client = ready!(compute_to_client_result);
        Poll::Ready(Ok((client_to_compute, compute_to_client)))
    })
    .await
}

#[tracing::instrument(skip_all)]
pub async fn copy_bidirectional_client_compute_pooled<Client, Compute>(
    client: &mut Client,
    compute: &mut Compute,
) -> Result<(u64, u64), ErrorSource>
where
    Client: AsyncRead + AsyncWrite + Unpin + ?Sized,
    Compute: AsyncRead + AsyncWrite + Unpin + ?Sized,
{
    let mut filtered_client = TerminateFilter::new(&mut *client);

    let mut client_to_compute = TransferState::Running(CopyBuffer::new());
    let mut compute_to_client = TransferState::Running(CopyBuffer::new());

    let result: Result<(u64, u64), ErrorSource> = poll_fn(|cx| {
        // CRITICAL: check for clean Terminate BEFORE driving the state machines.
        // Once TerminateFilter has seen Terminate, we want to bail without
        // calling poll_shutdown on compute (which would close the pooled connection).
        if filtered_client.saw_terminate() {
            let c2c_count = match &client_to_compute {
                TransferState::Running(buf) => buf.amt,
                TransferState::ShuttingDown(n) | TransferState::Done(n) => *n,
            };
            let s2c_count = match &compute_to_client {
                TransferState::Running(buf) => buf.amt,
                TransferState::ShuttingDown(n) | TransferState::Done(n) => *n,
            };
            info!("clean client disconnect via Terminate; compute connection preserved");
            return Poll::Ready(Ok((c2c_count, s2c_count)));
        }

        // CRITICAL: never poll_shutdown(compute) — the connection must be
        // reusable in the pool. If the client closes (or sends Terminate),
        // we just stop forwarding without half-closing the compute socket.
        let client_to_compute_result = transfer_one_direction_no_shutdown(
            cx,
            &mut client_to_compute,
            &mut filtered_client,
            compute,
        )
        .map_err(ErrorSource::from_client)?;

        // Re-check after the read side has been driven; the filter may have just
        // observed Terminate during this poll.
        if filtered_client.saw_terminate() {
            let c2c_count = match &client_to_compute {
                TransferState::Running(buf) => buf.amt,
                TransferState::ShuttingDown(n) | TransferState::Done(n) => *n,
            };
            let s2c_count = match &compute_to_client {
                TransferState::Running(buf) => buf.amt,
                TransferState::ShuttingDown(n) | TransferState::Done(n) => *n,
            };
            info!("clean client disconnect via Terminate; compute connection preserved");
            return Poll::Ready(Ok((c2c_count, s2c_count)));
        }

        let compute_to_client_result =
            transfer_one_direction(cx, &mut compute_to_client, compute, &mut filtered_client)
                .map_err(ErrorSource::from_compute)?;

        // If compute closes first, this path runs and IS allowed to shut down the
        // client (since the compute connection is gone anyway and shouldn't be pooled).
        if let TransferState::Done(_) = compute_to_client
            && let TransferState::Running(buf) = &client_to_compute
        {
            info!("Compute is done, terminate client");
            client_to_compute = TransferState::ShuttingDown(buf.amt);
            // Note: we do NOT re-drive client_to_compute here because that would
            // call poll_shutdown(compute) and we've already established compute is done.
            // Just let the result fall through.
        }

        let c2c = ready!(client_to_compute_result);
        let s2c = ready!(compute_to_client_result);
        Poll::Ready(Ok((c2c, s2c)))
    })
    .await;

    result
}

/// Outcome of a single iteration of the transaction-mode pump.
#[derive(Debug, Clone, Copy)]
pub(crate) enum BoundaryReason {
    /// Compute sent ReadyForQuery; transaction status is in the byte.
    /// `b'I'` = idle (compute can be returned to pool), `b'T'` / `b'E'` =
    /// in/failed transaction (compute must be held).
    ReadyForQuery(u8),
    /// Client sent Terminate ('X'). Caller decides whether to release
    /// compute (last status was idle) or discard (mid-transaction).
    ClientTerminated,
    /// Compute closed the read side. The compute connection is gone;
    /// caller should propagate this as an error to the client.
    ComputeClosed,
}

/// Pump bytes between client and compute until a transaction boundary is
/// reached. Used by the transaction-mode multiplex loop. Never calls
/// `poll_shutdown` on the compute writer — the compute connection is
/// expected to live past this call (returned to the pool or held for
/// the next iteration of the inner loop).
#[tracing::instrument(skip_all)]
pub async fn copy_bidirectional_until_boundary<Client, Compute>(
    client: &mut Client,
    compute: &mut Compute,
) -> Result<BoundaryReason, ErrorSource>
where
    Client: AsyncRead + AsyncWrite + Unpin + ?Sized,
    Compute: AsyncRead + AsyncWrite + Unpin + ?Sized,
{
    let mut filtered_client = TerminateFilter::new(&mut *client);
    let mut watched_compute = ReadyForQueryWatcher::new(&mut *compute);

    let mut client_to_compute = TransferState::Running(CopyBuffer::new());
    let mut compute_to_client = TransferState::Running(CopyBuffer::new());

    poll_fn(|cx| {
        if filtered_client.saw_terminate() {
            return Poll::Ready(Ok(BoundaryReason::ClientTerminated));
        }

        // If compute already produced ReadyForQuery, do not read more client
        // bytes into this backend. First drain the response bytes that caused
        // the boundary to the current client; only then may the caller decide
        // whether this backend is safe to return to the pool.
        if watched_compute.ready_for_query_pending() {
            let _ = transfer_one_direction_no_shutdown(
                cx,
                &mut compute_to_client,
                &mut watched_compute,
                &mut filtered_client,
            )
            .map_err(ErrorSource::from_compute)?;

            if compute_to_client.is_buffer_empty_and_flushed() {
                ready!(Pin::new(&mut filtered_client).poll_flush(cx))
                    .map_err(ErrorSource::Client)?;
                let status = watched_compute.last_status().unwrap_or(b'?');
                watched_compute.take_ready_for_query();
                return Poll::Ready(Ok(BoundaryReason::ReadyForQuery(status)));
            }

            return Poll::Pending;
        }

        // Drive client → compute. Never shut down compute here.
        let _ = transfer_one_direction_no_shutdown(
            cx,
            &mut client_to_compute,
            &mut filtered_client,
            &mut watched_compute,
        )
        .map_err(ErrorSource::from_client)?;

        if filtered_client.saw_terminate() {
            return Poll::Ready(Ok(BoundaryReason::ClientTerminated));
        }

        // Drive compute → client through the watcher.
        let _ = transfer_one_direction_no_shutdown(
            cx,
            &mut compute_to_client,
            &mut watched_compute,
            &mut filtered_client,
        )
        .map_err(ErrorSource::from_compute)?;

        if watched_compute.ready_for_query_pending()
            && compute_to_client.is_buffer_empty_and_flushed()
        {
            ready!(Pin::new(&mut filtered_client).poll_flush(cx)).map_err(ErrorSource::Client)?;
            let status = watched_compute.last_status().unwrap_or(b'?');
            watched_compute.take_ready_for_query();
            return Poll::Ready(Ok(BoundaryReason::ReadyForQuery(status)));
        }

        if let TransferState::Done(_) = compute_to_client {
            return Poll::Ready(Ok(BoundaryReason::ComputeClosed));
        }

        Poll::Pending
    })
    .await
}

#[derive(Debug)]
pub(super) struct CopyBuffer {
    read_done: bool,
    need_flush: bool,
    pos: usize,
    cap: usize,
    amt: u64,
    buf: Box<[u8]>,
}
const DEFAULT_BUF_SIZE: usize = 1024;

impl CopyBuffer {
    pub(super) fn new() -> Self {
        Self {
            read_done: false,
            need_flush: false,
            pos: 0,
            cap: 0,
            amt: 0,
            buf: vec![0; DEFAULT_BUF_SIZE].into_boxed_slice(),
        }
    }

    fn is_empty_and_flushed(&self) -> bool {
        self.pos == self.cap && !self.need_flush
    }

    fn poll_fill_buf<R>(
        &mut self,
        cx: &mut Context<'_>,
        reader: Pin<&mut R>,
    ) -> Poll<io::Result<()>>
    where
        R: AsyncRead + ?Sized,
    {
        let me = &mut *self;
        let mut buf = ReadBuf::new(&mut me.buf);
        buf.set_filled(me.cap);

        let res = reader.poll_read(cx, &mut buf);
        if let Poll::Ready(Ok(())) = res {
            let filled_len = buf.filled().len();
            me.read_done = me.cap == filled_len;
            me.cap = filled_len;
        }
        res
    }

    fn poll_write_buf<R, W>(
        &mut self,
        cx: &mut Context<'_>,
        mut reader: Pin<&mut R>,
        mut writer: Pin<&mut W>,
    ) -> Poll<Result<usize, ErrorDirection>>
    where
        R: AsyncRead + ?Sized,
        W: AsyncWrite + ?Sized,
    {
        let me = &mut *self;
        match writer.as_mut().poll_write(cx, &me.buf[me.pos..me.cap]) {
            Poll::Pending => {
                // Top up the buffer towards full if we can read a bit more
                // data - this should improve the chances of a large write
                if !me.read_done && me.cap < me.buf.len() {
                    ready!(me.poll_fill_buf(cx, reader.as_mut())).map_err(ErrorDirection::Read)?;
                }
                Poll::Pending
            }
            res @ Poll::Ready(_) => res.map_err(ErrorDirection::Write),
        }
    }

    pub(super) fn poll_copy<R, W>(
        &mut self,
        cx: &mut Context<'_>,
        mut reader: Pin<&mut R>,
        mut writer: Pin<&mut W>,
    ) -> Poll<Result<u64, ErrorDirection>>
    where
        R: AsyncRead + ?Sized,
        W: AsyncWrite + ?Sized,
    {
        loop {
            // If there is some space left in our buffer, then we try to read some
            // data to continue, thus maximizing the chances of a large write.
            if self.cap < self.buf.len() && !self.read_done {
                match self.poll_fill_buf(cx, reader.as_mut()) {
                    Poll::Ready(Ok(())) => (),
                    Poll::Ready(Err(err)) => return Poll::Ready(Err(ErrorDirection::Read(err))),
                    Poll::Pending => {
                        // Ignore pending reads when our buffer is not empty, because
                        // we can try to write data immediately.
                        if self.pos == self.cap {
                            // Try flushing when the reader has no progress to avoid deadlock
                            // when the reader depends on buffered writer.
                            if self.need_flush {
                                ready!(writer.as_mut().poll_flush(cx))
                                    .map_err(ErrorDirection::Write)?;
                                self.need_flush = false;
                            }

                            return Poll::Pending;
                        }
                    }
                }
            }

            // If our buffer has some data, let's write it out!
            while self.pos < self.cap {
                let i = ready!(self.poll_write_buf(cx, reader.as_mut(), writer.as_mut()))?;
                if i == 0 {
                    return Poll::Ready(Err(ErrorDirection::Write(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "write zero byte into writer",
                    ))));
                }
                self.pos += i;
                self.amt += i as u64;
                self.need_flush = true;
            }

            // If pos larger than cap, this loop will never stop.
            // In particular, user's wrong poll_write implementation returning
            // incorrect written length may lead to thread blocking.
            debug_assert!(
                self.pos <= self.cap,
                "writer returned length larger than input slice"
            );

            // All data has been written, the buffer can be considered empty again
            self.pos = 0;
            self.cap = 0;

            // If we've written all the data and we've seen EOF, flush out the
            // data and finish the transfer.
            if self.read_done {
                ready!(writer.as_mut().poll_flush(cx)).map_err(ErrorDirection::Write)?;
                return Poll::Ready(Ok(self.amt));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;

    fn command_complete(tag: &[u8]) -> Vec<u8> {
        let mut msg = Vec::with_capacity(5 + tag.len() + 1);
        msg.push(b'C');
        msg.extend_from_slice(&((tag.len() + 1 + 4) as u32).to_be_bytes());
        msg.extend_from_slice(tag);
        msg.push(0);
        msg
    }

    fn ready_for_query(status: u8) -> [u8; 6] {
        [b'Z', 0, 0, 0, 5, status]
    }

    #[tokio::test]
    async fn test_client_to_compute() {
        let (mut client_client, mut client_proxy) = tokio::io::duplex(8); // Create a mock duplex stream
        let (mut compute_proxy, mut compute_client) = tokio::io::duplex(32); // Create a mock duplex stream

        // Simulate 'a' finishing while there's still data for 'b'
        client_client.write_all(b"hello").await.unwrap();
        client_client.shutdown().await.unwrap();
        compute_client.write_all(b"Neon").await.unwrap();
        compute_client.shutdown().await.unwrap();

        let result = copy_bidirectional_client_compute(&mut client_proxy, &mut compute_proxy)
            .await
            .unwrap();

        // Assert correct transferred amounts
        let (client_to_compute_count, compute_to_client_count) = result;
        assert_eq!(client_to_compute_count, 5); // 'hello' was transferred
        assert_eq!(compute_to_client_count, 4); // response only partially transferred or not at all
    }

    #[tokio::test]
    async fn test_compute_to_client() {
        let (mut client_client, mut client_proxy) = tokio::io::duplex(32); // Create a mock duplex stream
        let (mut compute_proxy, mut compute_client) = tokio::io::duplex(8); // Create a mock duplex stream

        // Simulate 'a' finishing while there's still data for 'b'
        compute_client.write_all(b"hello").await.unwrap();
        compute_client.shutdown().await.unwrap();
        client_client
            .write_all(b"Neon Serverless Postgres")
            .await
            .unwrap();

        let result = copy_bidirectional_client_compute(&mut client_proxy, &mut compute_proxy)
            .await
            .unwrap();

        // Assert correct transferred amounts
        let (client_to_compute_count, compute_to_client_count) = result;
        assert_eq!(compute_to_client_count, 5); // 'hello' was transferred
        assert!(client_to_compute_count <= 8); // response only partially transferred or not at all
    }

    #[tokio::test]
    async fn transaction_boundary_waits_for_ready_for_query_to_reach_client() {
        let command = command_complete(b"BEGIN");
        let ready = ready_for_query(b'T');
        let mut response = command.clone();
        response.extend_from_slice(&ready);

        let (mut client_peer, mut client_proxy) = tokio::io::duplex(1);
        let (mut compute_proxy, mut compute_peer) = tokio::io::duplex(1024);

        compute_peer.write_all(&response).await.unwrap();

        let pump = tokio::spawn(async move {
            copy_bidirectional_until_boundary(&mut client_proxy, &mut compute_proxy).await
        });

        let mut got_command = vec![0; command.len()];
        client_peer.read_exact(&mut got_command).await.unwrap();
        assert_eq!(got_command, command);

        tokio::task::yield_now().await;
        assert!(
            !pump.is_finished(),
            "boundary returned before ReadyForQuery was written to the client"
        );

        let mut got_ready = [0; 6];
        client_peer.read_exact(&mut got_ready).await.unwrap();
        assert_eq!(got_ready, ready);

        let boundary = pump.await.unwrap().unwrap();
        assert!(matches!(boundary, BoundaryReason::ReadyForQuery(b'T')));
    }
}

pub(crate) struct TerminateFilter<R> {
    inner: R,
    state: FilterState,
    saw_terminate: bool,
}

enum FilterState {
    AwaitingHeader {
        header: [u8; 5],
        pos: usize,
    },
    InBody {
        remaining: usize,
        header: [u8; 5],
        header_emitted: usize,
    },
}

impl<R> TerminateFilter<R> {
    pub(crate) fn new(inner: R) -> Self {
        Self {
            inner,
            state: FilterState::AwaitingHeader {
                header: [0; 5],
                pos: 0,
            },
            saw_terminate: false,
        }
    }

    pub(crate) fn saw_terminate(&self) -> bool {
        self.saw_terminate
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for TerminateFilter<R> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        out: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        // Destructure self once so we have separate borrows of each field.
        // This is the standard Rust idiom for "split borrow" through a Pin.
        let this = self.get_mut();

        if this.saw_terminate {
            return Poll::Ready(Ok(()));
        }

        loop {
            match &mut this.state {
                FilterState::InBody { .. } => {
                    if let FilterState::InBody {
                        remaining: _,
                        header,
                        header_emitted,
                    } = &mut this.state
                    {
                        if *header_emitted < 5 {
                            if out.remaining() == 0 {
                                return Poll::Ready(Ok(()));
                            }
                            let need = 5 - *header_emitted;
                            let to_emit = need.min(out.remaining());
                            let start = *header_emitted;
                            out.put_slice(&header[start..start + to_emit]);
                            *header_emitted += to_emit;
                            if *header_emitted < 5 {
                                return Poll::Ready(Ok(()));
                            }
                        }
                    }
                    let FilterState::InBody { remaining, .. } = &mut this.state else {
                        unreachable!();
                    };
                    if *remaining == 0 {
                        this.state = FilterState::AwaitingHeader {
                            header: [0; 5],
                            pos: 0,
                        };
                        continue;
                    }
                    let to_read = (*remaining).min(out.remaining());
                    if to_read == 0 {
                        return Poll::Ready(Ok(()));
                    }
                    let initial_filled = out.filled().len();
                    let mut limited = out.take(to_read);
                    match Pin::new(&mut this.inner).poll_read(cx, &mut limited) {
                        Poll::Ready(Ok(())) => {
                            let n = limited.filled().len();
                            if n == 0 {
                                return Poll::Ready(Ok(()));
                            }
                            out.set_filled(initial_filled + n);
                            *remaining -= n;
                            return Poll::Ready(Ok(()));
                        }
                        other => return other,
                    }
                }
                FilterState::AwaitingHeader { header, pos } => {
                    let need = 5 - *pos;
                    let mut tmp = [0u8; 5];
                    let mut tmp_buf = ReadBuf::new(&mut tmp[..need]);
                    match Pin::new(&mut this.inner).poll_read(cx, &mut tmp_buf) {
                        Poll::Ready(Ok(())) => {
                            let n = tmp_buf.filled().len();
                            if n == 0 {
                                if *pos > 0 {
                                    let to_emit = (*pos).min(out.remaining());
                                    out.put_slice(&header[..to_emit]);
                                }
                                return Poll::Ready(Ok(()));
                            }
                            header[*pos..*pos + n].copy_from_slice(&tmp[..n]);
                            *pos += n;
                            if *pos < 5 {
                                continue;
                            }
                            let tag = header[0];
                            let len =
                                u32::from_be_bytes([header[1], header[2], header[3], header[4]])
                                    as usize;
                            if tag == b'X' && len == 4 {
                                tracing::info!(
                                    "TerminateFilter: intercepted Postgres Terminate; \
                                     compute connection preserved"
                                );
                                this.saw_terminate = true;
                                return Poll::Ready(Ok(()));
                            }
                            // Save header bytes locally before mutating this.state.
                            let header_copy = *header;
                            let body_len = len.saturating_sub(4);
                            let to_emit = 5.min(out.remaining());
                            out.put_slice(&header_copy[..to_emit]);
                            this.state = FilterState::InBody {
                                remaining: body_len,
                                header: header_copy,
                                header_emitted: to_emit,
                            };
                            return Poll::Ready(Ok(()));
                        }
                        other => return other,
                    }
                }
            }
        }
    }
}

impl<R: AsyncRead + AsyncWrite + Unpin> tokio::io::AsyncWrite for TerminateFilter<R> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

/// Transparently pass bytes through from the compute side while parsing
/// Postgres backend protocol message headers. Tracks the last-seen
/// `ReadyForQuery` (`'Z'`) transaction-status byte (`'I'` idle, `'T'` in-tx,
/// `'E'` failed-tx). The transaction-mode pump uses this to decide when a
/// transaction boundary has been reached and the compute connection can be
/// returned to the pool.
pub(crate) struct ReadyForQueryWatcher<R> {
    inner: R,
    state: WatchState,
    last_status: Option<u8>,
    saw_ready_for_query: bool,
}

enum WatchState {
    AwaitingHeader {
        header: [u8; 5],
        pos: usize,
    },
    InBody {
        tag: u8,
        remaining: usize,
        body_seen: usize,
        header: [u8; 5],
        header_emitted: usize,
    },
}

impl<R> ReadyForQueryWatcher<R> {
    pub(crate) fn new(inner: R) -> Self {
        Self {
            inner,
            state: WatchState::AwaitingHeader {
                header: [0; 5],
                pos: 0,
            },
            last_status: None,
            saw_ready_for_query: false,
        }
    }

    pub(crate) fn last_status(&self) -> Option<u8> {
        self.last_status
    }

    pub(crate) fn ready_for_query_pending(&self) -> bool {
        self.saw_ready_for_query
    }

    /// Returns true if a `ReadyForQuery` was observed since the last call
    /// (and clears the flag). The caller uses this as the "transaction
    /// boundary reached" signal in the multiplex loop.
    pub(crate) fn take_ready_for_query(&mut self) -> bool {
        std::mem::replace(&mut self.saw_ready_for_query, false)
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for ReadyForQueryWatcher<R> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        out: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        // Once a ReadyForQuery boundary is observed, pause reads until the
        // caller consumes it via take_ready_for_query(). This prevents the
        // boundary pump from over-reading into subsequent backend messages.
        if this.saw_ready_for_query {
            return Poll::Pending;
        }

        loop {
            match &mut this.state {
                WatchState::AwaitingHeader { header, pos } => {
                    let need = 5 - *pos;
                    let mut tmp = [0u8; 5];
                    let mut tmp_buf = ReadBuf::new(&mut tmp[..need]);
                    match Pin::new(&mut this.inner).poll_read(cx, &mut tmp_buf) {
                        Poll::Ready(Ok(())) => {
                            let n = tmp_buf.filled().len();
                            if n == 0 {
                                if *pos > 0 {
                                    let to_emit = (*pos).min(out.remaining());
                                    out.put_slice(&header[..to_emit]);
                                }
                                return Poll::Ready(Ok(()));
                            }
                            header[*pos..*pos + n].copy_from_slice(&tmp[..n]);
                            *pos += n;
                            if *pos < 5 {
                                continue;
                            }

                            let tag = header[0];
                            let len =
                                u32::from_be_bytes([header[1], header[2], header[3], header[4]])
                                    as usize;
                            let body_len = len.saturating_sub(4);

                            // bidir copy uses a 1024-byte read buffer, so
                            // out.remaining() is always >= 5 here in practice.
                            // We still cap the put_slice for safety.
                            let header_copy = *header;
                            let to_emit = 5.min(out.remaining());
                            out.put_slice(&header_copy[..to_emit]);

                            this.state = WatchState::InBody {
                                tag,
                                remaining: body_len,
                                body_seen: 0,
                                header: header_copy,
                                header_emitted: to_emit,
                            };

                            if body_len == 0 && to_emit == 5 {
                                this.state = WatchState::AwaitingHeader {
                                    header: [0; 5],
                                    pos: 0,
                                };
                            }

                            if out.remaining() == 0 || to_emit < 5 || body_len == 0 {
                                return Poll::Ready(Ok(()));
                            }

                            // Fall through to read body bytes in the same poll.
                            continue;
                        }
                        other => return other,
                    }
                }
                WatchState::InBody {
                    tag,
                    remaining,
                    body_seen,
                    header,
                    header_emitted,
                } => {
                    if *header_emitted < 5 {
                        if out.remaining() == 0 {
                            return Poll::Ready(Ok(()));
                        }
                        let need = 5 - *header_emitted;
                        let to_emit = need.min(out.remaining());
                        let start = *header_emitted;
                        out.put_slice(&header[start..start + to_emit]);
                        *header_emitted += to_emit;
                        if *header_emitted < 5 {
                            return Poll::Ready(Ok(()));
                        }
                        if *remaining == 0 {
                            this.state = WatchState::AwaitingHeader {
                                header: [0; 5],
                                pos: 0,
                            };
                            continue;
                        }
                    }

                    if *remaining == 0 {
                        this.state = WatchState::AwaitingHeader {
                            header: [0; 5],
                            pos: 0,
                        };
                        continue;
                    }
                    if out.remaining() == 0 {
                        return Poll::Ready(Ok(()));
                    }
                    let to_read = (*remaining).min(out.remaining());
                    let initial_filled = out.filled().len();
                    let mut limited = out.take(to_read);
                    match Pin::new(&mut this.inner).poll_read(cx, &mut limited) {
                        Poll::Ready(Ok(())) => {
                            let n = limited.filled().len();
                            if n == 0 {
                                return Poll::Ready(Ok(()));
                            }

                            // For ReadyForQuery, the body length must be 1 and
                            // the only byte is the transaction status.
                            let mut ready_for_query_status = None;
                            if *tag == b'Z' {
                                if *body_seen != 0 || *remaining != 1 || n != 1 {
                                    return Poll::Ready(Err(std::io::Error::new(
                                        std::io::ErrorKind::InvalidData,
                                        "malformed ReadyForQuery message",
                                    )));
                                }
                                ready_for_query_status = Some(limited.filled()[0]);
                            }

                            out.set_filled(initial_filled + n);
                            *remaining -= n;
                            *body_seen += n;
                            if let Some(status) = ready_for_query_status {
                                this.last_status = Some(status);
                                this.saw_ready_for_query = true;
                            }
                            return Poll::Ready(Ok(()));
                        }
                        other => return other,
                    }
                }
            }
        }
    }
}

impl<R: AsyncRead + AsyncWrite + Unpin> tokio::io::AsyncWrite for ReadyForQueryWatcher<R> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write(cx, buf)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}
