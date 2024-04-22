use bytes::BytesMut;
use tokio_epoll_uring::{BoundedBuf, IoBuf, Slice};

/// A trait for doing owned-buffer write IO.
/// Think [`tokio::io::AsyncWrite`] but with owned buffers.
pub trait OwnedAsyncWriter {
    async fn write_all<B: BoundedBuf<Buf = Buf>, Buf: IoBuf + Send>(
        &mut self,
        buf: B,
    ) -> std::io::Result<(usize, B::Buf)>;
}

pub trait Buffer {
    const BUFFER_SIZE: usize;

    fn len(&self) -> usize;
    fn clear(&mut self);
    fn extend_from_slice(&mut self, other: &[u8]);
}

pub struct BytesMutBuffer<const BUFFER_SIZE: usize> {
    buf: BytesMut,
}

/// SAFETY: just forwards to the pre-existing impl for BytesMut
unsafe impl<const BUFFER_SIZE: usize> IoBuf for BytesMutBuffer<BUFFER_SIZE> {
    fn stable_ptr(&self) -> *const u8 {
        IoBuf::stable_ptr(&self.buf)
    }

    fn bytes_init(&self) -> usize {
        IoBuf::bytes_init(&self.buf)
    }

    fn bytes_total(&self) -> usize {
        IoBuf::bytes_total(&self.buf)
    }
}

impl<const BUFFER_SIZE: usize> Buffer for BytesMutBuffer<BUFFER_SIZE> {
    const BUFFER_SIZE: usize = BUFFER_SIZE;

    fn len(&self) -> usize {
        self.buf.len()
    }

    fn clear(&mut self) {
        self.buf.clear()
    }

    fn extend_from_slice(&mut self, other: &[u8]) {
        self.buf.extend_from_slice(other)
    }
}

impl<const BUFFER_SIZE: usize> BytesMutBuffer<BUFFER_SIZE> {
    pub fn new() -> Self {
        BytesMutBuffer {
            buf: BytesMut::with_capacity(BUFFER_SIZE),
        }
    }
}

/// A wrapper aorund an [`OwnedAsyncWriter`] that batches smaller writers
/// into `BUFFER_SIZE`-sized writes.
///
/// # Passthrough Of Large Writers
///
/// Buffered writes larger than the `BUFFER_SIZE` cause the internal
/// buffer to be flushed, even if it is not full yet. Then, the large
/// buffered write is passed through to the unerlying [`OwnedAsyncWriter`].
///
/// This pass-through is generally beneficial for throughput, but if
/// the storage backend of the [`OwnedAsyncWriter`] is a shared resource,
/// unlimited large writes may cause latency or fairness issues.
///
/// In such cases, a different implementation that always buffers in memory
/// may be preferable.
pub struct BufferedWriter<B, W> {
    writer: W,
    // invariant: always remains Some(buf)
    // with buf.capacity() == BUFFER_SIZE except
    // - while IO is ongoing => goes back to Some() once the IO completed successfully
    // - after an IO error => stays `None` forever
    // In these exceptional cases, it's `None`.
    buf: Option<B>,
}

impl<B, W> BufferedWriter<B, W>
where
    B: Buffer + IoBuf + Send,
    W: OwnedAsyncWriter,
{
    pub fn new(writer: W, buffer: B) -> Self {
        Self {
            writer,
            buf: Some(buffer),
        }
    }

    pub fn as_inner(&self) -> &W {
        &self.writer
    }

    /// Panics if used after any of the write paths returned an error
    pub fn inspect_buffer(&self) -> &B {
        self.buf.as_ref().expect("must not use after an error")
    }

    pub async fn flush_and_into_inner(mut self) -> std::io::Result<W> {
        self.flush().await?;
        let Self { buf, writer } = self;
        assert!(buf.is_some());
        Ok(writer)
    }

    pub async fn write_buffered<S: IoBuf>(&mut self, chunk: Slice<S>) -> std::io::Result<(usize, S)>
    where
        S: IoBuf + Send,
    {
        let chunk_len = chunk.len();
        // avoid memcpy for the middle of the chunk
        if chunk.len() >= B::BUFFER_SIZE {
            self.flush().await?;
            // do a big write, bypassing `buf`
            assert_eq!(
                self.buf
                    .as_ref()
                    .expect("must not use after an error")
                    .len(),
                0
            );
            let (nwritten, chunk) = self.writer.write_all(chunk).await?;
            assert_eq!(nwritten, chunk_len);
            return Ok((nwritten, chunk));
        }
        // in-memory copy the < BUFFER_SIZED tail of the chunk
        assert!(chunk.len() < B::BUFFER_SIZE);
        let mut slice = &chunk[..];
        while !slice.is_empty() {
            let buf = self.buf.as_mut().expect("must not use after an error");
            let need = B::BUFFER_SIZE - buf.len();
            let have = slice.len();
            let n = std::cmp::min(need, have);
            buf.extend_from_slice(&slice[..n]);
            slice = &slice[n..];
            if buf.len() >= B::BUFFER_SIZE {
                assert_eq!(buf.len(), B::BUFFER_SIZE);
                self.flush().await?;
            }
        }
        assert!(slice.is_empty(), "by now we should have drained the chunk");
        Ok((chunk_len, chunk.into_inner()))
    }

    /// Strictly less performant variant of [`Self::write_buffered`] that allows writing borrowed data.
    ///
    /// It is less performant because we always have to copy the borrowed data into the internal buffer
    /// before we can do the IO. The [`Self::write_buffered`] can avoid this, which is more performant
    /// for large writes.
    pub async fn write_buffered_borrowed(&mut self, mut chunk: &[u8]) -> std::io::Result<usize> {
        let chunk_len = chunk.len();
        while !chunk.is_empty() {
            let buf = self.buf.as_mut().expect("must not use after an error");
            let need = B::BUFFER_SIZE - buf.len();
            let have = chunk.len();
            let n = std::cmp::min(need, have);
            buf.extend_from_slice(&chunk[..n]);
            chunk = &chunk[n..];
            if buf.len() >= B::BUFFER_SIZE {
                assert_eq!(buf.len(), B::BUFFER_SIZE);
                self.flush().await?;
            }
        }
        Ok(chunk_len)
    }

    async fn flush(&mut self) -> std::io::Result<()> {
        let buf = self.buf.take().expect("must not use after an error");
        if buf.bytes_init() == 0 {
            self.buf = Some(buf);
            return std::io::Result::Ok(());
        }
        let buf_len = buf.len();
        let (nwritten, mut buf) = self.writer.write_all(buf).await?;
        assert_eq!(nwritten, buf_len);
        buf.clear();
        self.buf = Some(buf);
        Ok(())
    }
}

impl OwnedAsyncWriter for Vec<u8> {
    async fn write_all<B: BoundedBuf<Buf = Buf>, Buf: IoBuf + Send>(
        &mut self,
        buf: B,
    ) -> std::io::Result<(usize, B::Buf)> {
        let nbytes = buf.bytes_init();
        if nbytes == 0 {
            return Ok((0, Slice::into_inner(buf.slice_full())));
        }
        let buf = buf.slice(0..nbytes);
        self.extend_from_slice(&buf[..]);
        Ok((buf.len(), Slice::into_inner(buf)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct RecorderWriter {
        writes: Vec<Vec<u8>>,
    }
    impl OwnedAsyncWriter for RecorderWriter {
        async fn write_all<B: BoundedBuf<Buf = Buf>, Buf: IoBuf + Send>(
            &mut self,
            buf: B,
        ) -> std::io::Result<(usize, B::Buf)> {
            let nbytes = buf.bytes_init();
            if nbytes == 0 {
                self.writes.push(vec![]);
                return Ok((0, Slice::into_inner(buf.slice_full())));
            }
            let buf = buf.slice(0..nbytes);
            self.writes.push(Vec::from(&buf[..]));
            Ok((buf.len(), Slice::into_inner(buf)))
        }
    }

    macro_rules! write {
        ($writer:ident, $data:literal) => {{
            $writer
                .write_buffered(::bytes::Bytes::from_static($data).slice_full())
                .await?;
        }};
    }

    #[tokio::test]
    async fn test_buffered_writes_only() -> std::io::Result<()> {
        let recorder = RecorderWriter::default();
        let mut writer = BufferedWriter::new(recorder, BytesMutBuffer::<2>::new());
        write!(writer, b"a");
        write!(writer, b"b");
        write!(writer, b"c");
        write!(writer, b"d");
        write!(writer, b"e");
        let recorder = writer.flush_and_into_inner().await?;
        assert_eq!(
            recorder.writes,
            vec![Vec::from(b"ab"), Vec::from(b"cd"), Vec::from(b"e")]
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_passthrough_writes_only() -> std::io::Result<()> {
        let recorder = RecorderWriter::default();
        let mut writer = BufferedWriter::new(recorder, BytesMutBuffer::<2>::new());
        write!(writer, b"abc");
        write!(writer, b"de");
        write!(writer, b"");
        write!(writer, b"fghijk");
        let recorder = writer.flush_and_into_inner().await?;
        assert_eq!(
            recorder.writes,
            vec![Vec::from(b"abc"), Vec::from(b"de"), Vec::from(b"fghijk")]
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_passthrough_write_with_nonempty_buffer() -> std::io::Result<()> {
        let recorder = RecorderWriter::default();
        let mut writer = BufferedWriter::new(recorder, BytesMutBuffer::<2>::new());
        write!(writer, b"a");
        write!(writer, b"bc");
        write!(writer, b"d");
        write!(writer, b"e");
        let recorder = writer.flush_and_into_inner().await?;
        assert_eq!(
            recorder.writes,
            vec![Vec::from(b"a"), Vec::from(b"bc"), Vec::from(b"de")]
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_write_all_borrowed_always_goes_through_buffer() -> std::io::Result<()> {
        let recorder = RecorderWriter::default();
        let mut writer = BufferedWriter::new(recorder, BytesMutBuffer::<2>::new());

        writer.write_buffered_borrowed(b"abc").await?;
        writer.write_buffered_borrowed(b"d").await?;
        writer.write_buffered_borrowed(b"e").await?;
        writer.write_buffered_borrowed(b"fg").await?;
        writer.write_buffered_borrowed(b"hi").await?;
        writer.write_buffered_borrowed(b"j").await?;
        writer.write_buffered_borrowed(b"klmno").await?;

        let recorder = writer.flush_and_into_inner().await?;
        assert_eq!(
            recorder.writes,
            {
                let expect: &[&[u8]] = &[b"ab", b"cd", b"ef", b"gh", b"ij", b"kl", b"mn", b"o"];
                expect
            }
            .iter()
            .map(|v| v[..].to_vec())
            .collect::<Vec<_>>()
        );
        Ok(())
    }
}
