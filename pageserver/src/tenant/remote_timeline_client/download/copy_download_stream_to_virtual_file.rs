use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use tokio_epoll_uring::{BoundedBuf, IoBuf, Slice};

use crate::virtual_file::VirtualFile;

/// Equivalent to `tokio::io::copy_buf` but with an
/// owned-buffers-style Write API, as required by VirtualFile / tokio-epoll-uring.
pub async fn copy(
    src: &mut remote_storage::DownloadStream,
    dst: &mut VirtualFile,
) -> std::io::Result<u64> {
    // TODO: use vectored write (writev) once supported by tokio-epoll-uring.
    // There's chunks_vectored() on the stream.

    let writer = SizeTrackingWriter {
        dst,
        bytes_amount: 0,
    };
    let mut writer = BypassableBufferedWriter::<
        { crate::tenant::remote_timeline_client::BUFFER_SIZE },
        _,
    >::new(writer);
    while let Some(res) = src.next().await {
        let chunk = match res {
            Ok(chunk) => chunk,
            Err(e) => return Err(e),
        };
        writer.write_buffered(chunk).await?;
    }
    let size_tracking = writer.flush_and_into_inner().await?;
    Ok(size_tracking.bytes_amount)
}

struct SizeTrackingWriter<'f> {
    dst: &'f mut VirtualFile,
    bytes_amount: u64,
}

impl<'f> Writer for SizeTrackingWriter<'f> {
    async fn write_all<B: BoundedBuf<Buf = Buf>, Buf: IoBuf + Send>(
        &mut self,
        buf: B,
    ) -> std::io::Result<(usize, B::Buf)> {
        let (buf, res) = self.dst.write_all(buf).await;
        let nwritten = res?;
        self.bytes_amount += u64::try_from(nwritten).unwrap();
        Ok((nwritten, buf))
    }
}

trait Writer {
    async fn write_all<B: BoundedBuf<Buf = Buf>, Buf: IoBuf + Send>(
        &mut self,
        buf: B,
    ) -> std::io::Result<(usize, B::Buf)>;
}

struct BypassableBufferedWriter<const BUFFER_SIZE: usize, W> {
    writer: W,
    buf: Option<BytesMut>,
}

impl<const BUFFER_SIZE: usize, W> BypassableBufferedWriter<BUFFER_SIZE, W>
where
    W: Writer,
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

    pub async fn write_buffered(&mut self, chunk: Bytes) -> std::io::Result<()> {
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

impl Writer for Vec<u8> {
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
    impl Writer for RecorderWriter {
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

    #[tokio::test]
    async fn test_buffered_writes_only() -> std::io::Result<()> {
        let recorder = RecorderWriter::default();
        let mut writer = BypassableBufferedWriter::<2, _>::new(recorder);
        writer.write_buffered(Bytes::from_static(b"a")).await?;
        writer.write_buffered(Bytes::from_static(b"b")).await?;
        writer.write_buffered(Bytes::from_static(b"c")).await?;
        writer.write_buffered(Bytes::from_static(b"d")).await?;
        writer.write_buffered(Bytes::from_static(b"e")).await?;
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
        let mut writer = BypassableBufferedWriter::<2, _>::new(recorder);
        writer.write_buffered(Bytes::from_static(b"abc")).await?;
        writer.write_buffered(Bytes::from_static(b"de")).await?;
        writer.write_buffered(Bytes::from_static(b"")).await?;
        writer.write_buffered(Bytes::from_static(b"fghijk")).await?;
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
        let mut writer = BypassableBufferedWriter::<2, _>::new(recorder);
        writer.write_buffered(Bytes::from_static(b"a")).await?;
        writer.write_buffered(Bytes::from_static(b"bc")).await?;
        writer.write_buffered(Bytes::from_static(b"d")).await?;
        writer.write_buffered(Bytes::from_static(b"e")).await?;
        let recorder = writer.flush_and_into_inner().await?;
        assert_eq!(
            recorder.writes,
            vec![Vec::from(b"a"), Vec::from(b"bc"), Vec::from(b"de")]
        );
        Ok(())
    }
}
