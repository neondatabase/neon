//! [`hyper-util`] offers an 'auto' connection to detect whether the connection should be HTTP1 or HTTP2.
//! There's a bug in this implementation where graceful shutdowns are not properly respected.

use futures::ready;
use hyper_util::rt::{TokioExecutor, TokioTimer};
use std::future::Future;
use std::marker::PhantomPinned;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::{io, marker::Unpin};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use bytes::Bytes;

use hyper1::server::conn::http1;
use hyper1::server::conn::http2;

use pin_project_lite::pin_project;

const H2_PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

/// Http1 or Http2 connection builder.
#[derive(Clone, Debug)]
pub struct Builder {
    pub http1: http1::Builder,
    pub http2: http2::Builder<TokioExecutor>,
}

impl Builder {
    /// Create a new auto connection builder.
    pub fn new() -> Self {
        let mut builder = Self {
            http1: http1::Builder::new(),
            http2: http2::Builder::new(TokioExecutor::new()),
        };

        builder.http1.timer(TokioTimer::new());
        builder.http2.timer(TokioTimer::new());

        builder
    }
}

#[derive(Copy, Clone)]
pub(crate) enum Version {
    H1,
    H2,
}

pub(crate) fn read_version<I>(io: I) -> ReadVersion<I>
where
    I: AsyncRead + Unpin,
{
    ReadVersion {
        io: Some(io),
        buf: [0; 24],
        filled: 0,
        version: Version::H2,
        _pin: PhantomPinned,
    }
}

pin_project! {
    pub(crate) struct ReadVersion<I> {
        io: Option<I>,
        buf: [u8; 24],
        // the amount of `buf` thats been filled
        filled: usize,
        version: Version,
        // Make this future `!Unpin` for compatibility with async trait methods.
        #[pin]
        _pin: PhantomPinned,
    }
}

impl<I> Future for ReadVersion<I>
where
    I: AsyncRead + Unpin,
{
    type Output = io::Result<(Version, Rewind<I>)>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        let mut buf = ReadBuf::new(&mut *this.buf);
        buf.set_filled(*this.filled);

        // We start as H2 and switch to H1 as soon as we don't have the preface.
        while buf.filled().len() < H2_PREFACE.len() {
            let len = buf.filled().len();
            ready!(Pin::new(this.io.as_mut().unwrap()).poll_read(cx, &mut buf))?;
            *this.filled = buf.filled().len();

            // We starts as H2 and switch to H1 when we don't get the preface.
            if buf.filled().len() == len
                || buf.filled()[len..] != H2_PREFACE[len..buf.filled().len()]
            {
                *this.version = Version::H1;
                break;
            }
        }

        let io = this.io.take().unwrap();
        let buf = buf.filled().to_vec();
        Poll::Ready(Ok((
            *this.version,
            Rewind::new_buffered(io, Bytes::from(buf)),
        )))
    }
}

/// Combine a buffer with an IO, rewinding reads to use the buffer.
#[derive(Debug)]
pub(crate) struct Rewind<T> {
    pre: Option<Bytes>,
    inner: T,
}

impl<T> Rewind<T> {
    pub(crate) fn new_buffered(io: T, buf: Bytes) -> Self {
        Rewind {
            pre: Some(buf),
            inner: io,
        }
    }

    pub fn into_inner(self) -> (Bytes, T) {
        (self.pre.unwrap_or_default(), self.inner)
    }
}

impl<T> AsyncRead for Rewind<T>
where
    T: AsyncRead + Unpin,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if let Some(prefix) = self.pre.take() {
            // If there are no remaining bytes, let the bytes get dropped.
            if !prefix.is_empty() {
                let copy_len = std::cmp::min(prefix.len(), buf.remaining());
                buf.put_slice(&prefix[..copy_len]);
                // Put back what's left
                if !prefix.is_empty() {
                    self.pre = Some(prefix);
                }

                return Poll::Ready(Ok(()));
            }
        }
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl<T> AsyncWrite for Rewind<T>
where
    T: AsyncWrite + Unpin,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write_vectored(cx, bufs)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
}
