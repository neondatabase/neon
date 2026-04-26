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
    use tokio::io::AsyncWriteExt;

    use super::*;

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
}

pub(crate) struct TerminateFilter<R> {
    inner: R,
    state: FilterState,
    saw_terminate: bool
}


enum  FilterState {
    AwaitingHeader {header: [u8;5], pos: usize},
    InBody { remaining: usize}
}

impl<R> TerminateFilter<R> {
    pub(crate) fn new(inner: R) -> Self {
        Self {
            inner,
            state: FilterState::AwaitingHeader { header: [0;5], pos: 0 },
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
                FilterState::InBody { remaining } => {
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
                            let len = u32::from_be_bytes([
                                header[1], header[2], header[3], header[4],
                            ]) as usize;
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
                            if to_emit < 5 {
                                out.put_slice(&header_copy[..to_emit]);
                                this.state = FilterState::InBody { remaining: body_len };
                                return Poll::Ready(Ok(()));
                            }
                            out.put_slice(&header_copy[..5]);
                            this.state = FilterState::InBody { remaining: body_len };
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

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}