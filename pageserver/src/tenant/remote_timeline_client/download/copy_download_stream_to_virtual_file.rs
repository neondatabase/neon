use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use tokio_epoll_uring::{BoundedBuf, IoBuf};

use crate::{tenant::remote_timeline_client::BUFFER_SIZE, virtual_file::VirtualFile};

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
    let mut writer = BypassableBufferedWriter::new(writer);
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

struct BypassableBufferedWriter<W> {
    writer: W,
    buf: Option<BytesMut>,
}

impl<W> BypassableBufferedWriter<W>
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
