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
pub struct BufferedWriter<const BUFFER_SIZE: usize, W> {
    writer: W,
    // invariant: always remains Some(buf)
    // with buf.capacity() == BUFFER_SIZE except
    // - while IO is ongoing => goes back to Some() once the IO completed successfully
    // - after an IO error => stays `None` forever
    // In these exceptional cases, it's `None`.
    buf: Option<BytesMut>,
}

impl<const BUFFER_SIZE: usize, W> BufferedWriter<BUFFER_SIZE, W>
where
    W: OwnedAsyncWriter,
{
    pub fn new(writer: W) -> Self {
        Self {
            writer,
            buf: Some(BytesMut::with_capacity(BUFFER_SIZE)),
        }
    }

    pub async fn flush_and_into_inner(mut self) -> std::io::Result<W> {
        self.flush().await?;
        let Self { buf, writer } = self;
        assert!(buf.is_some());
        Ok(writer)
    }

    pub async fn write_buffered<B: IoBuf>(&mut self, chunk: Slice<B>) -> std::io::Result<()>
    where
        B: IoBuf + Send,
    {
        // avoid memcpy for the middle of the chunk
        if chunk.len() >= BUFFER_SIZE {
            self.flush().await?;
            // do a big write, bypassing `buf`
            assert_eq!(
                self.buf
                    .as_ref()
                    .expect("must not use after an error")
                    .len(),
                0
            );
            let chunk_len = chunk.len();
            let (nwritten, chunk) = self.writer.write_all(chunk).await?;
            assert_eq!(nwritten, chunk_len);
            drop(chunk);
            return Ok(());
        }
        // in-memory copy the < BUFFER_SIZED tail of the chunk
        assert!(chunk.len() < BUFFER_SIZE);
        let mut chunk = &chunk[..];
        while !chunk.is_empty() {
            let buf = self.buf.as_mut().expect("must not use after an error");
            let need = BUFFER_SIZE - buf.len();
            let have = chunk.len();
            let n = std::cmp::min(need, have);
            buf.extend_from_slice(&chunk[..n]);
            chunk = &chunk[n..];
            if buf.len() >= BUFFER_SIZE {
                assert_eq!(buf.len(), BUFFER_SIZE);
                self.flush().await?;
            }
        }
        assert!(chunk.is_empty(), "by now we should have drained the chunk");
        Ok(())
    }

    async fn flush(&mut self) -> std::io::Result<()> {
        let buf = self.buf.take().expect("must not use after an error");
        if buf.is_empty() {
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
        let mut writer = BufferedWriter::<2, _>::new(recorder);
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
        let mut writer = BufferedWriter::<2, _>::new(recorder);
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
        let mut writer = BufferedWriter::<2, _>::new(recorder);
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
}
