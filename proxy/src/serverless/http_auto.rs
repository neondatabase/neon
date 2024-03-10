//! [`hyper-util`] offers an 'auto' connection to detect whether the connection should be HTTP1 or HTTP2.
//! There's a bug in this implementation where graceful shutdowns are not properly respected.

use futures::ready;
use hyper1::body::Body;
use hyper1::rt::ReadBufCursor;
use hyper1::service::HttpService;
use hyper_util::rt::{TokioExecutor, TokioTimer};
use std::future::Future;
use std::marker::PhantomPinned;
use std::mem::MaybeUninit;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::{error::Error as StdError, io, marker::Unpin};

use ::http1::{Request, Response};
use bytes::{Buf, Bytes};
use hyper1::{
    body::Incoming,
    rt::{Read, ReadBuf, Write},
    service::Service,
};

use hyper1::server::conn::http1;
use hyper1::{rt::bounds::Http2ServerConnExec, server::conn::http2};

use pin_project_lite::pin_project;

type Error = Box<dyn std::error::Error + Send + Sync>;

type Result<T> = std::result::Result<T, Error>;

const H2_PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

/// Exactly equivalent to [`Http2ServerConnExec`].
pub trait HttpServerConnExec<A, B: Body>: Http2ServerConnExec<A, B> {}

impl<A, B: Body, T: Http2ServerConnExec<A, B>> HttpServerConnExec<A, B> for T {}

/// Http1 or Http2 connection builder.
#[derive(Clone, Debug)]
pub struct Builder {
    http1: http1::Builder,
    http2: http2::Builder<TokioExecutor>,
}

impl Builder {
    /// Create a new auto connection builder.
    ///
    /// `executor` parameter should be a type that implements
    /// [`Executor`](hyper::rt::Executor) trait.
    ///
    /// # Example
    ///
    /// ```
    /// use hyper_util::{
    ///     rt::TokioExecutor,
    ///     server::conn::auto,
    /// };
    ///
    /// auto::Builder::new(TokioExecutor::new());
    /// ```
    pub fn new() -> Self {
        let mut builder = Self {
            http1: http1::Builder::new(),
            http2: http2::Builder::new(TokioExecutor::new()),
        };

        builder.http1.timer(TokioTimer::new());
        builder.http2.timer(TokioTimer::new());

        builder
    }

    /// Bind a connection together with a [`Service`], with the ability to
    /// handle HTTP upgrades. This requires that the IO object implements
    /// `Send`.
    pub fn serve_connection_with_upgrades<I, S, B>(
        &self,
        io: I,
        service: S,
    ) -> UpgradeableConnection<'_, I, S>
    where
        S: Service<Request<Incoming>, Response = Response<B>>,
        S::Future: 'static,
        S::Error: Into<Box<dyn StdError + Send + Sync>>,
        B: Body + 'static,
        B::Error: Into<Box<dyn StdError + Send + Sync>>,
        I: Read + Write + Unpin + Send + 'static,
        TokioExecutor: HttpServerConnExec<S::Future, B>,
    {
        UpgradeableConnection {
            state: UpgradeableConnState::ReadVersion {
                read_version: read_version(io),
                builder: self,
                service: Some(service),
            },
        }
    }

    /// Bind a HTTP2 connection together with a [`Service`]. This requires that the IO object implements `Send`.
    pub fn serve_http2_connection<I, S, B>(
        &self,
        io: I,
        service: S,
    ) -> UpgradeableConnection<'_, I, S>
    where
        S: Service<Request<Incoming>, Response = Response<B>>,
        S::Future: 'static,
        S::Error: Into<Box<dyn StdError + Send + Sync>>,
        B: Body + 'static,
        B::Error: Into<Box<dyn StdError + Send + Sync>>,
        I: Read + Write + Unpin + Send + 'static,
        TokioExecutor: HttpServerConnExec<S::Future, B>,
    {
        let conn = self.http2.serve_connection(Rewind::new(io), service);
        UpgradeableConnection {
            state: UpgradeableConnState::H2 { conn },
        }
    }

    /// Bind a HTTP2 connection together with a [`Service`]. This requires that the IO object implements `Send`.
    pub fn serve_http1_connection_with_upgrades<I, S, B>(
        &self,
        io: I,
        service: S,
    ) -> UpgradeableConnection<'_, I, S>
    where
        S: Service<Request<Incoming>, Response = Response<B>>,
        S::Future: 'static,
        S::Error: Into<Box<dyn StdError + Send + Sync>>,
        B: Body + 'static,
        B::Error: Into<Box<dyn StdError + Send + Sync>>,
        I: Read + Write + Unpin + Send + 'static,
        TokioExecutor: HttpServerConnExec<S::Future, B>,
    {
        let conn = self
            .http1
            .serve_connection(Rewind::new(io), service)
            .with_upgrades();
        UpgradeableConnection {
            state: UpgradeableConnState::H1 { conn },
        }
    }
}

#[derive(Copy, Clone)]
enum Version {
    H1,
    H2,
}

fn read_version<I>(io: I) -> ReadVersion<I>
where
    I: Read + Unpin,
{
    ReadVersion {
        io: Some(io),
        buf: [MaybeUninit::uninit(); 24],
        filled: 0,
        version: Version::H2,
        _pin: PhantomPinned,
    }
}

pin_project! {
    struct ReadVersion<I> {
        io: Option<I>,
        buf: [MaybeUninit<u8>; 24],
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
    I: Read + Unpin,
{
    type Output = io::Result<(Version, Rewind<I>)>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        let mut buf = ReadBuf::uninit(&mut *this.buf);
        // SAFETY: `this.filled` tracks how many bytes have been read (and thus initialized) and
        // we're only advancing by that many.
        unsafe {
            buf.unfilled().advance(*this.filled);
        };

        // We start as H2 and switch to H1 as soon as we don't have the preface.
        while buf.filled().len() < H2_PREFACE.len() {
            let len = buf.filled().len();
            ready!(Pin::new(this.io.as_mut().unwrap()).poll_read(cx, buf.unfilled()))?;
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

pin_project! {
    /// Connection future.
    pub struct Connection<'a, I, S>
    where
        S: HttpService<Incoming>,
    {
        #[pin]
        state: ConnState<'a, I, S>,
    }
}

type Http1Connection<I, S> = hyper1::server::conn::http1::Connection<Rewind<I>, S>;
type Http2Connection<I, S> = hyper1::server::conn::http2::Connection<Rewind<I>, S, TokioExecutor>;

pin_project! {
    #[project = ConnStateProj]
    enum ConnState<'a, I, S>
    where
        S: HttpService<Incoming>,
    {
        ReadVersion {
            #[pin]
            read_version: ReadVersion<I>,
            builder: &'a Builder,
            service: Option<S>,
        },
        H1 {
            #[pin]
            conn: Http1Connection<I, S>,
        },
        H2 {
            #[pin]
            conn: Http2Connection<I, S>,
        },
    }
}

impl<I, S, B> Future for Connection<'_, I, S>
where
    S: Service<Request<Incoming>, Response = Response<B>>,
    S::Future: 'static,
    S::Error: Into<Box<dyn StdError + Send + Sync>>,
    B: Body + 'static,
    B::Error: Into<Box<dyn StdError + Send + Sync>>,
    I: Read + Write + Unpin + 'static,
    TokioExecutor: HttpServerConnExec<S::Future, B>,
{
    type Output = Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            let mut this = self.as_mut().project();

            match this.state.as_mut().project() {
                ConnStateProj::ReadVersion {
                    read_version,
                    builder,
                    service,
                } => {
                    let (version, io) = ready!(read_version.poll(cx))?;
                    let service = service.take().unwrap();
                    match version {
                        Version::H1 => {
                            let conn = builder.http1.serve_connection(io, service);
                            this.state.set(ConnState::H1 { conn });
                        }
                        Version::H2 => {
                            let conn = builder.http2.serve_connection(io, service);
                            this.state.set(ConnState::H2 { conn });
                        }
                    }
                }
                ConnStateProj::H1 { conn } => {
                    return conn.poll(cx).map_err(Into::into);
                }
                ConnStateProj::H2 { conn } => {
                    return conn.poll(cx).map_err(Into::into);
                }
            }
        }
    }
}

pin_project! {
    /// Connection future.
    pub struct UpgradeableConnection<'a, I, S>
    where
        S: HttpService<Incoming>,
    {
        #[pin]
        state: UpgradeableConnState<'a, I, S>,
    }
}

type Http1UpgradeableConnection<I, S> = hyper1::server::conn::http1::UpgradeableConnection<I, S>;

pin_project! {
    #[project = UpgradeableConnStateProj]
    enum UpgradeableConnState<'a, I, S>
    where
        S: HttpService<Incoming>,
    {
        ReadVersion {
            #[pin]
            read_version: ReadVersion<I>,
            builder: &'a Builder,
            service: Option<S>,
        },
        H1 {
            #[pin]
            conn: Http1UpgradeableConnection<Rewind<I>, S>,
        },
        H2 {
            #[pin]
            conn: Http2Connection<I, S>,
        },
    }
}

impl<I, S, B> UpgradeableConnection<'_, I, S>
where
    S: HttpService<Incoming, ResBody = B>,
    S::Error: Into<Box<dyn StdError + Send + Sync>>,
    I: Read + Write + Unpin,
    B: Body + 'static,
    B::Error: Into<Box<dyn StdError + Send + Sync>>,
    TokioExecutor: HttpServerConnExec<S::Future, B>,
{
    /// Start a graceful shutdown process for this connection.
    ///
    /// This `UpgradeableConnection` should continue to be polled until shutdown can finish.
    ///
    /// # Note
    ///
    /// This should only be called while the `Connection` future is still nothing. pending. If
    /// called after `UpgradeableConnection::poll` has resolved, this does nothing.
    pub fn graceful_shutdown(self: Pin<&mut Self>) {
        match self.project().state.project() {
            UpgradeableConnStateProj::ReadVersion { .. } => {}
            UpgradeableConnStateProj::H1 { conn } => conn.graceful_shutdown(),
            UpgradeableConnStateProj::H2 { conn } => conn.graceful_shutdown(),
        }
    }
}

impl<I, S, B> Future for UpgradeableConnection<'_, I, S>
where
    S: Service<Request<Incoming>, Response = Response<B>>,
    S::Future: 'static,
    S::Error: Into<Box<dyn StdError + Send + Sync>>,
    B: Body + 'static,
    B::Error: Into<Box<dyn StdError + Send + Sync>>,
    I: Read + Write + Unpin + Send + 'static,
    TokioExecutor: HttpServerConnExec<S::Future, B>,
{
    type Output = Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            let mut this = self.as_mut().project();

            match this.state.as_mut().project() {
                UpgradeableConnStateProj::ReadVersion {
                    read_version,
                    builder,
                    service,
                } => {
                    let (version, io) = ready!(read_version.poll(cx))?;
                    let service = service.take().unwrap();
                    match version {
                        Version::H1 => {
                            let conn = builder.http1.serve_connection(io, service).with_upgrades();
                            this.state.set(UpgradeableConnState::H1 { conn });
                        }
                        Version::H2 => {
                            let conn = builder.http2.serve_connection(io, service);
                            this.state.set(UpgradeableConnState::H2 { conn });
                        }
                    }
                }
                UpgradeableConnStateProj::H1 { conn } => {
                    return conn.poll(cx).map_err(Into::into);
                }
                UpgradeableConnStateProj::H2 { conn } => {
                    return conn.poll(cx).map_err(Into::into);
                }
            }
        }
    }
}

/// Combine a buffer with an IO, rewinding reads to use the buffer.
#[derive(Debug)]
pub(crate) struct Rewind<T> {
    pre: Option<Bytes>,
    inner: T,
}

impl<T> Rewind<T> {
    pub(crate) fn new(io: T) -> Self {
        Rewind {
            pre: None,
            inner: io,
        }
    }

    pub(crate) fn new_buffered(io: T, buf: Bytes) -> Self {
        Rewind {
            pre: Some(buf),
            inner: io,
        }
    }
}

impl<T> Read for Rewind<T>
where
    T: Read + Unpin,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        mut buf: ReadBufCursor<'_>,
    ) -> Poll<io::Result<()>> {
        if let Some(mut prefix) = self.pre.take() {
            // If there are no remaining bytes, let the bytes get dropped.
            if !prefix.is_empty() {
                let copy_len = std::cmp::min(prefix.len(), remaining(&mut buf));
                // TODO: There should be a way to do following two lines cleaner...
                put_slice(&mut buf, &prefix[..copy_len]);
                prefix.advance(copy_len);
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

fn remaining(cursor: &mut ReadBufCursor<'_>) -> usize {
    // SAFETY:
    // We do not uninitialize any set bytes.
    unsafe { cursor.as_mut().len() }
}

// Copied from `ReadBufCursor::put_slice`.
// If that becomes public, we could ditch this.
fn put_slice(cursor: &mut ReadBufCursor<'_>, slice: &[u8]) {
    assert!(
        remaining(cursor) >= slice.len(),
        "buf.len() must fit in remaining()"
    );

    let amt = slice.len();

    // SAFETY:
    // the length is asserted above
    unsafe {
        cursor.as_mut()[..amt]
            .as_mut_ptr()
            .cast::<u8>()
            .copy_from_nonoverlapping(slice.as_ptr(), amt);
        cursor.advance(amt);
    }
}

impl<T> Write for Rewind<T>
where
    T: Write + Unpin,
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
