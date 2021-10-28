use pin_project_lite::pin_project;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::{io, task};

pin_project! {
    /// We use this future to mark certain methods
    /// as callable in both sync and async modes.
    #[repr(transparent)]
    pub struct SyncFuture<S, T: Future> {
        #[pin]
        inner: T,
        _marker: PhantomData<S>,
    }
}

/// This wrapper lets us synchronously wait for inner future's completion
/// (see [`SyncFuture::wait`]) **provided that `S` implements [`SyncProof`]**.
/// For instance, `S` may be substituted with types implementing
/// [`tokio::io::AsyncRead`], but it's not the only viable option.
impl<S, T: Future> SyncFuture<S, T> {
    /// NOTE: caller should carefully pick a type for `S`,
    /// because we don't want to enable [`SyncFuture::wait`] when
    /// it's in fact impossible to run the future synchronously.
    /// Violation of this contract will not cause UB, but
    /// panics and async event loop freezes won't please you.
    ///
    /// Example:
    ///
    /// ```
    /// # use zenith_utils::sync::SyncFuture;
    /// # use std::future::Future;
    /// # use tokio::io::AsyncReadExt;
    /// #
    /// // Parse a pair of numbers from a stream
    /// pub fn parse_pair<Reader>(
    ///     stream: &mut Reader,
    /// ) -> SyncFuture<Reader, impl Future<Output = anyhow::Result<(u32, u64)>> + '_>
    /// where
    ///     Reader: tokio::io::AsyncRead + Unpin,
    /// {
    ///     // If `Reader` is a `SyncProof`, this will give caller
    ///     // an opportunity to use `SyncFuture::wait`, because
    ///     // `.await` will always result in `Poll::Ready`.
    ///     SyncFuture::new(async move {
    ///         let x = stream.read_u32().await?;
    ///         let y = stream.read_u64().await?;
    ///         Ok((x, y))
    ///     })
    /// }
    /// ```
    pub fn new(inner: T) -> Self {
        Self {
            inner,
            _marker: PhantomData,
        }
    }
}

impl<S, T: Future> Future for SyncFuture<S, T> {
    type Output = T::Output;

    /// In async code, [`SyncFuture`] behaves like a regular wrapper.
    #[inline(always)]
    fn poll(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> task::Poll<Self::Output> {
        self.project().inner.poll(cx)
    }
}

/// Postulates that we can call [`SyncFuture::wait`].
/// If implementer is also a [`Future`], it should always
/// return [`task::Poll::Ready`] from [`Future::poll`].
///
/// Each implementation should document which futures
/// specifically are being declared sync-proof.
pub trait SyncPostulate {}

impl<T: SyncPostulate> SyncPostulate for &T {}
impl<T: SyncPostulate> SyncPostulate for &mut T {}

impl<P: SyncPostulate, T: Future> SyncFuture<P, T> {
    /// Synchronously wait for future completion.
    pub fn wait(mut self) -> T::Output {
        const RAW_WAKER: task::RawWaker = task::RawWaker::new(
            std::ptr::null(),
            &task::RawWakerVTable::new(
                |_| RAW_WAKER,
                |_| panic!("SyncFuture: failed to wake"),
                |_| panic!("SyncFuture: failed to wake by ref"),
                |_| { /* drop is no-op */ },
            ),
        );

        // SAFETY: We never move `self` during this call;
        // furthermore, it will be dropped in the end regardless of panics
        let this = unsafe { Pin::new_unchecked(&mut self) };

        // SAFETY: This waker doesn't do anything apart from panicking
        let waker = unsafe { task::Waker::from_raw(RAW_WAKER) };
        let context = &mut task::Context::from_waker(&waker);

        match this.poll(context) {
            task::Poll::Ready(res) => res,
            _ => panic!("SyncFuture: unexpected pending!"),
        }
    }
}

/// This wrapper turns any [`std::io::Read`] into a blocking [`tokio::io::AsyncRead`],
/// which lets us abstract over sync & async readers in methods returning [`SyncFuture`].
/// NOTE: you **should not** use this in async code.
#[repr(transparent)]
pub struct AsyncishRead<T: io::Read + Unpin>(pub T);

/// This lets us call [`SyncFuture<AsyncishRead<_>, _>::wait`],
/// and allows the future to await on any of the [`AsyncRead`]
/// and [`AsyncReadExt`] methods on `AsyncishRead`.
impl<T: io::Read + Unpin> SyncPostulate for AsyncishRead<T> {}

impl<T: io::Read + Unpin> tokio::io::AsyncRead for AsyncishRead<T> {
    #[inline(always)]
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> task::Poll<io::Result<()>> {
        task::Poll::Ready(
            // `Read::read` will block, meaning we don't need a real event loop!
            self.0
                .read(buf.initialize_unfilled())
                .map(|sz| buf.advance(sz)),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    // async helper(stream: &mut impl AsyncRead) -> io::Result<u32>
    fn bytes_add<Reader>(
        stream: &mut Reader,
    ) -> SyncFuture<Reader, impl Future<Output = io::Result<u32>> + '_>
    where
        Reader: tokio::io::AsyncRead + Unpin,
    {
        SyncFuture::new(async move {
            let a = stream.read_u32().await?;
            let b = stream.read_u32().await?;
            Ok(a + b)
        })
    }

    #[test]
    fn test_sync() {
        let bytes = [100u32.to_be_bytes(), 200u32.to_be_bytes()].concat();
        let res = bytes_add(&mut AsyncishRead(&mut &bytes[..]))
            .wait()
            .unwrap();
        assert_eq!(res, 300);
    }

    // We need a single-threaded executor for this test
    #[tokio::test(flavor = "current_thread")]
    async fn test_async() {
        let (mut tx, mut rx) = tokio::net::UnixStream::pair().unwrap();

        let write = async move {
            tx.write_u32(100).await?;
            tx.write_u32(200).await?;
            Ok(())
        };

        let (res, ()) = tokio::try_join!(bytes_add(&mut rx), write).unwrap();
        assert_eq!(res, 300);
    }
}
