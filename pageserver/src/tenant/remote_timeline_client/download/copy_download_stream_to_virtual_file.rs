use bytes::BytesMut;
use futures::StreamExt;

use crate::{tenant::remote_timeline_client::BUFFER_SIZE, virtual_file::VirtualFile};

/// Equivalent to `tokio::io::copy_buf` but with an
/// owned-buffers-style Write API, as required by VirtualFile / tokio-epoll-uring.
pub async fn copy(
    src: &mut remote_storage::DownloadStream,
    dst: &mut VirtualFile,
) -> std::io::Result<u64> {
    // TODO: use vectored write (writev) once supported by tokio-epoll-uring.
    // There's chunks_vectored()
    let mut buf = BytesMut::with_capacity(BUFFER_SIZE);
    let mut bytes_amount: u64 = 0;
    while let Some(chunk) = src.next().await {
        let mut chunk = match chunk {
            Ok(chunk) => chunk,
            Err(e) => return Err(e),
        };

        if buf.len() + chunk.len() > BUFFER_SIZE {
            {
                // flush buf
                let res;
                let buf_pre_write = buf.len();
                (buf, res) = dst.write_all(buf).await;
                let nwritten = res?;
                assert_eq!(nwritten, buf_pre_write);
                buf.clear();
                bytes_amount += u64::try_from(nwritten).unwrap();
            }
        }

        // avoid memcpy for the middle of the chunk
        if chunk.len() >= BUFFER_SIZE {
            // do a big write
            let res;
            let buf_pre_write = chunk.len();
            (chunk, res) = dst.write_all(chunk).await;
            let nwritten = res?;
            assert_eq!(nwritten, buf_pre_write);
            bytes_amount += u64::try_from(nwritten).unwrap();
            drop(chunk);
            continue;
        }

        // in-memory copy the < BUFFER_SIZED tail of the chunk
        assert!(chunk.len() < BUFFER_SIZE);
        let mut chunk = &chunk[..];
        while !chunk.is_empty() {
            let need = BUFFER_SIZE - buf.len();
            let have = chunk.len();
            let n = std::cmp::min(need, have);
            buf.extend_from_slice(&chunk[..n]);
            chunk = &chunk[n..];
            if buf.len() >= BUFFER_SIZE {
                assert_eq!(buf.len(), BUFFER_SIZE);
                {
                    // flush buf
                    let res;
                    let buf_pre_write = buf.len();
                    (buf, res) = dst.write_all(buf).await;
                    let nwritten = res?;
                    assert_eq!(nwritten, buf_pre_write);
                    buf.clear();
                    bytes_amount += u64::try_from(nwritten).unwrap();
                }
            }
        }
        assert!(chunk.is_empty(), "by now we should have drained the chunk");
    }
    {
        // flush buf
        let res;
        let buf_pre_write = buf.len();
        (buf, res) = dst.write_all(buf).await;
        let nwritten = res?;
        assert_eq!(nwritten, buf_pre_write);
        buf.clear();
        bytes_amount += u64::try_from(nwritten).unwrap();
    }
    Ok(bytes_amount)
}
