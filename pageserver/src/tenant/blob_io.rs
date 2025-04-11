//!
//! Functions for reading and writing variable-sized "blobs".
//!
//! Each blob begins with a 1- or 4-byte length field, followed by the
//! actual data. If the length is smaller than 128 bytes, the length
//! is written as a one byte. If it's larger than that, the length
//! is written as a four-byte integer, in big-endian, with the high
//! bit set. This way, we can detect whether it's 1- or 4-byte header
//! by peeking at the first byte. For blobs larger than 128 bits,
//! we also specify three reserved bits, only one of the three bit
//! patterns is currently in use (0b011) and signifies compression
//! with zstd.
//!
//! len <  128: 0XXXXXXX
//! len >= 128: 1CCCXXXX XXXXXXXX XXXXXXXX XXXXXXXX
//!
use std::cmp::min;
use std::sync::Arc;

use async_compression::Level;
use bytes::{BufMut, BytesMut};
use pageserver_api::models::ImageCompressionAlgorithm;
use tokio::io::AsyncWriteExt;
use tokio_epoll_uring::IoBuf;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::context::RequestContext;
use crate::page_cache::PAGE_SZ;
use crate::tenant::block_io::BlockCursor;
use crate::virtual_file::IoBufferMut;
use crate::virtual_file::VirtualFile;
use crate::virtual_file::owned_buffers_io::io_buf_ext::{FullSlice, IoBufExt};
use crate::virtual_file::owned_buffers_io::write::{BufferedWriter, FlushTaskError};

#[derive(Copy, Clone, Debug)]
pub struct CompressionInfo {
    pub written_compressed: bool,
    pub compressed_size: Option<usize>,
}

#[derive(Debug, thiserror::Error)]
pub enum WriteBlobError {
    #[error(transparent)]
    Flush(FlushTaskError),
    #[error("blob too large ({len} bytes)")]
    BlobTooLarge { len: usize },
}

impl BlockCursor<'_> {
    /// Read a blob into a new buffer.
    pub async fn read_blob(
        &self,
        offset: u64,
        ctx: &RequestContext,
    ) -> Result<Vec<u8>, std::io::Error> {
        let mut buf = Vec::new();
        self.read_blob_into_buf(offset, &mut buf, ctx).await?;
        Ok(buf)
    }
    /// Read blob into the given buffer. Any previous contents in the buffer
    /// are overwritten.
    pub async fn read_blob_into_buf(
        &self,
        offset: u64,
        dstbuf: &mut Vec<u8>,
        ctx: &RequestContext,
    ) -> Result<(), std::io::Error> {
        let mut blknum = (offset / PAGE_SZ as u64) as u32;
        let mut off = (offset % PAGE_SZ as u64) as usize;

        let mut buf = self.read_blk(blknum, ctx).await?;

        // peek at the first byte, to determine if it's a 1- or 4-byte length
        let first_len_byte = buf[off];
        let len: usize = if first_len_byte < 0x80 {
            // 1-byte length header
            off += 1;
            first_len_byte as usize
        } else {
            // 4-byte length header
            let mut len_buf = [0u8; 4];
            let thislen = PAGE_SZ - off;
            if thislen < 4 {
                // it is split across two pages
                len_buf[..thislen].copy_from_slice(&buf[off..PAGE_SZ]);
                blknum += 1;
                buf = self.read_blk(blknum, ctx).await?;
                len_buf[thislen..].copy_from_slice(&buf[0..4 - thislen]);
                off = 4 - thislen;
            } else {
                len_buf.copy_from_slice(&buf[off..off + 4]);
                off += 4;
            }
            let bit_mask = if self.read_compressed {
                !LEN_COMPRESSION_BIT_MASK
            } else {
                0x7f
            };
            len_buf[0] &= bit_mask;
            u32::from_be_bytes(len_buf) as usize
        };
        let compression_bits = first_len_byte & LEN_COMPRESSION_BIT_MASK;

        let mut tmp_buf = Vec::new();
        let buf_to_write;
        let compression = if compression_bits <= BYTE_UNCOMPRESSED || !self.read_compressed {
            if compression_bits > BYTE_UNCOMPRESSED {
                warn!("reading key above future limit ({len} bytes)");
            }
            buf_to_write = dstbuf;
            None
        } else if compression_bits == BYTE_ZSTD {
            buf_to_write = &mut tmp_buf;
            Some(dstbuf)
        } else {
            let error = std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("invalid compression byte {compression_bits:x}"),
            );
            return Err(error);
        };

        buf_to_write.clear();
        buf_to_write.reserve(len);

        // Read the payload
        let mut remain = len;
        while remain > 0 {
            let mut page_remain = PAGE_SZ - off;
            if page_remain == 0 {
                // continue on next page
                blknum += 1;
                buf = self.read_blk(blknum, ctx).await?;
                off = 0;
                page_remain = PAGE_SZ;
            }
            let this_blk_len = min(remain, page_remain);
            buf_to_write.extend_from_slice(&buf[off..off + this_blk_len]);
            remain -= this_blk_len;
            off += this_blk_len;
        }

        if let Some(dstbuf) = compression {
            if compression_bits == BYTE_ZSTD {
                let mut decoder = async_compression::tokio::write::ZstdDecoder::new(dstbuf);
                decoder.write_all(buf_to_write).await?;
                decoder.flush().await?;
            } else {
                unreachable!("already checked above")
            }
        }

        Ok(())
    }
}

/// Reserved bits for length and compression
pub(super) const LEN_COMPRESSION_BIT_MASK: u8 = 0xf0;

/// The maximum size of blobs we support. The highest few bits
/// are reserved for compression and other further uses.
pub(crate) const MAX_SUPPORTED_BLOB_LEN: usize = 0x0fff_ffff;

pub(super) const BYTE_UNCOMPRESSED: u8 = 0x80;
pub(super) const BYTE_ZSTD: u8 = BYTE_UNCOMPRESSED | 0x10;

/// A wrapper of `VirtualFile` that allows users to write blobs.
///
/// If a `BlobWriter` is dropped, the internal buffer will be
/// discarded. You need to call [`Self::into_inner`]
/// manually before dropping.
pub struct BlobWriter {
    /// We do tiny writes for the length headers; they need to be in an owned buffer;
    io_buf: Option<BytesMut>,
    writer: BufferedWriter<IoBufferMut, VirtualFile>,
    offset: u64,
}

impl BlobWriter {
    pub fn new(
        file: Arc<VirtualFile>,
        start_offset: u64,
        gate: &utils::sync::gate::Gate,
        cancel: CancellationToken,
        ctx: &RequestContext,
        flush_task_span: tracing::Span,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            io_buf: Some(BytesMut::new()),
            writer: BufferedWriter::new(
                file,
                start_offset,
                || IoBufferMut::with_capacity(Self::CAPACITY),
                gate.enter()?,
                cancel,
                ctx,
                flush_task_span,
            ),
            offset: start_offset,
        })
    }

    pub fn size(&self) -> u64 {
        self.offset
    }

    const CAPACITY: usize = 64 * 1024;

    /// Writes `src_buf` to the file at the current offset.
    async fn write_all<Buf: IoBuf + Send>(
        &mut self,
        src_buf: FullSlice<Buf>,
        ctx: &RequestContext,
    ) -> (FullSlice<Buf>, Result<(), FlushTaskError>) {
        let res = self
            .writer
            // TODO: why are we taking a FullSlice if we're going to pass a borrow downstack?
            // Can remove all the complexity around owned buffers upstack
            .write_buffered_borrowed(&src_buf, ctx)
            .await
            .map(|len| {
                self.offset += len as u64;
            });

        (src_buf, res)
    }

    /// Write a blob of data. Returns the offset that it was written to,
    /// which can be used to retrieve the data later.
    pub async fn write_blob<Buf: IoBuf + Send>(
        &mut self,
        srcbuf: FullSlice<Buf>,
        ctx: &RequestContext,
    ) -> (FullSlice<Buf>, Result<u64, WriteBlobError>) {
        let (buf, res) = self
            .write_blob_maybe_compressed(srcbuf, ctx, ImageCompressionAlgorithm::Disabled)
            .await;
        (buf, res.map(|(off, _compression_info)| off))
    }

    /// Write a blob of data. Returns the offset that it was written to,
    /// which can be used to retrieve the data later.
    pub(crate) async fn write_blob_maybe_compressed<Buf: IoBuf + Send>(
        &mut self,
        srcbuf: FullSlice<Buf>,
        ctx: &RequestContext,
        algorithm: ImageCompressionAlgorithm,
    ) -> (
        FullSlice<Buf>,
        Result<(u64, CompressionInfo), WriteBlobError>,
    ) {
        let offset = self.offset;
        let mut compression_info = CompressionInfo {
            written_compressed: false,
            compressed_size: None,
        };

        let len = srcbuf.len();

        let mut io_buf = self.io_buf.take().expect("we always put it back below");
        io_buf.clear();
        let mut compressed_buf = None;
        let ((io_buf_slice, hdr_res), srcbuf) = async {
            if len < 128 {
                // Short blob. Write a 1-byte length header
                io_buf.put_u8(len as u8);
                let (slice, res) = self.write_all(io_buf.slice_len(), ctx).await;
                let res = res.map_err(WriteBlobError::Flush);
                ((slice, res), srcbuf)
            } else {
                // Write a 4-byte length header
                if len > MAX_SUPPORTED_BLOB_LEN {
                    return (
                        (
                            io_buf.slice_len(),
                            Err(WriteBlobError::BlobTooLarge { len }),
                        ),
                        srcbuf,
                    );
                }
                let (high_bit_mask, len_written, srcbuf) = match algorithm {
                    ImageCompressionAlgorithm::Zstd { level } => {
                        let mut encoder = if let Some(level) = level {
                            async_compression::tokio::write::ZstdEncoder::with_quality(
                                Vec::new(),
                                Level::Precise(level.into()),
                            )
                        } else {
                            async_compression::tokio::write::ZstdEncoder::new(Vec::new())
                        };
                        encoder.write_all(&srcbuf[..]).await.unwrap();
                        encoder.shutdown().await.unwrap();
                        let compressed = encoder.into_inner();
                        compression_info.compressed_size = Some(compressed.len());
                        if compressed.len() < len {
                            compression_info.written_compressed = true;
                            let compressed_len = compressed.len();
                            compressed_buf = Some(compressed);
                            (BYTE_ZSTD, compressed_len, srcbuf)
                        } else {
                            (BYTE_UNCOMPRESSED, len, srcbuf)
                        }
                    }
                    ImageCompressionAlgorithm::Disabled => (BYTE_UNCOMPRESSED, len, srcbuf),
                };
                let mut len_buf = (len_written as u32).to_be_bytes();
                assert_eq!(len_buf[0] & 0xf0, 0);
                len_buf[0] |= high_bit_mask;
                io_buf.extend_from_slice(&len_buf[..]);
                let (slice, res) = self.write_all(io_buf.slice_len(), ctx).await;
                let res = res.map_err(WriteBlobError::Flush);
                ((slice, res), srcbuf)
            }
        }
        .await;
        self.io_buf = Some(io_buf_slice.into_raw_slice().into_inner());
        match hdr_res {
            Ok(_) => (),
            Err(e) => return (srcbuf, Err(e)),
        }
        let (srcbuf, res) = if let Some(compressed_buf) = compressed_buf {
            let (_buf, res) = self.write_all(compressed_buf.slice_len(), ctx).await;
            (srcbuf, res)
        } else {
            self.write_all(srcbuf, ctx).await
        };
        let res = res.map_err(WriteBlobError::Flush);
        (srcbuf, res.map(|_| (offset, compression_info)))
    }

    /// Access the underlying `VirtualFile`.
    ///
    /// This function flushes the internal buffer before giving access
    /// to the underlying `VirtualFile`.
    ///
    /// The caller can use the `handle_tail` function to change the tail of the buffer before flushing it to disk.
    /// The buffer will not be flushed to disk if handle_tail returns `None`.
    pub async fn into_inner(
        self,
        handle_tail: impl FnMut(IoBufferMut) -> Option<IoBufferMut>,
    ) -> Result<VirtualFile, FlushTaskError> {
        let (_, file) = self.writer.shutdown(handle_tail).await?;
        Ok(file)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use camino::Utf8PathBuf;
    use camino_tempfile::Utf8TempDir;
    use rand::{Rng, SeedableRng};
    use tracing::info_span;

    use super::*;
    use crate::context::DownloadBehavior;
    use crate::task_mgr::TaskKind;
    use crate::tenant::block_io::BlockReaderRef;

    async fn round_trip_test(blobs: &[Vec<u8>]) -> anyhow::Result<()> {
        round_trip_test_compressed(blobs, false).await
    }

    pub(crate) async fn write_maybe_compressed(
        blobs: &[Vec<u8>],
        compression: bool,
        ctx: &RequestContext,
    ) -> anyhow::Result<(Utf8TempDir, Utf8PathBuf, Vec<u64>)> {
        let temp_dir = camino_tempfile::tempdir()?;
        let pathbuf = temp_dir.path().join("file");
        let gate = utils::sync::gate::Gate::default();
        let cancel = CancellationToken::new();

        // Write part (in block to drop the file)
        let mut offsets = Vec::new();
        {
            let file = Arc::new(VirtualFile::create_v2(pathbuf.as_path(), ctx).await?);
            let mut wtr =
                BlobWriter::new(file, 0, &gate, cancel.clone(), ctx, info_span!("test")).unwrap();
            for blob in blobs.iter() {
                let (_, res) = if compression {
                    let res = wtr
                        .write_blob_maybe_compressed(
                            blob.clone().slice_len(),
                            ctx,
                            ImageCompressionAlgorithm::Zstd { level: Some(1) },
                        )
                        .await;
                    (res.0, res.1.map(|(off, _)| off))
                } else {
                    wtr.write_blob(blob.clone().slice_len(), ctx).await
                };
                let offs = res?;
                offsets.push(offs);
            }
            wtr.into_inner(|mut buf| {
                use crate::virtual_file::owned_buffers_io::write::Buffer;

                let len = buf.pending();
                let cap = buf.cap();

                // pad zeros to the next io alignment requirement.
                // TODO: this is actually padding to next PAGE_SZ multiple, but only if the buffer capacity is larger than that.
                // We can't let the fact that we do direct IO, or the buffer capacity, dictate the on-disk format we write here.
                // Need to find a better API that allows writing the format we intend to.
                let count = len.next_multiple_of(PAGE_SZ).min(cap) - len;
                buf.extend_with(0, count);

                Some(buf)
            })
            .await?; // TODO: this here is the problem with the tests: we're dropping the tail end
        }
        Ok((temp_dir, pathbuf, offsets))
    }

    async fn round_trip_test_compressed(
        blobs: &[Vec<u8>],
        compression: bool,
    ) -> anyhow::Result<()> {
        let ctx =
            RequestContext::new(TaskKind::UnitTest, DownloadBehavior::Error).with_scope_unit_test();
        let (_temp_dir, pathbuf, offsets) =
            write_maybe_compressed(blobs, compression, &ctx).await?;

        println!("Done writing!");
        let file = VirtualFile::open_v2(pathbuf, &ctx).await?;
        let rdr = BlockReaderRef::VirtualFile(&file);
        let rdr = BlockCursor::new_with_compression(rdr, compression);
        for (idx, (blob, offset)) in blobs.iter().zip(offsets.iter()).enumerate() {
            let blob_read = rdr.read_blob(*offset, &ctx).await?;
            assert_eq!(
                blob, &blob_read,
                "mismatch for idx={idx} at offset={offset}"
            );
        }
        Ok(())
    }

    pub(crate) fn random_array(len: usize) -> Vec<u8> {
        let mut rng = rand::thread_rng();
        (0..len).map(|_| rng.r#gen()).collect::<_>()
    }

    #[tokio::test]
    async fn test_one() -> anyhow::Result<()> {
        let blobs = &[vec![12, 21, 22]];
        round_trip_test(blobs).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_hello_simple() -> anyhow::Result<()> {
        let blobs = &[
            vec![0, 1, 2, 3],
            b"Hello, World!".to_vec(),
            Vec::new(),
            b"foobar".to_vec(),
        ];
        round_trip_test(blobs).await?;
        round_trip_test_compressed(blobs, true).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_really_big_array() -> anyhow::Result<()> {
        let blobs = &[
            b"test".to_vec(),
            random_array(10 * PAGE_SZ),
            b"hello".to_vec(),
            random_array(66 * PAGE_SZ),
            vec![0xf3; 24 * PAGE_SZ],
            b"foobar".to_vec(),
        ];
        round_trip_test(blobs).await?;
        round_trip_test_compressed(blobs, true).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_arrays_inc() -> anyhow::Result<()> {
        let blobs = (0..PAGE_SZ / 8)
            .map(|v| random_array(v * 16))
            .collect::<Vec<_>>();
        round_trip_test(&blobs).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_arrays_random_size() -> anyhow::Result<()> {
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let blobs = (0..1024)
            .map(|_| {
                let mut sz: u16 = rng.r#gen();
                // Make 50% of the arrays small
                if rng.r#gen() {
                    sz &= 63;
                }
                random_array(sz.into())
            })
            .collect::<Vec<_>>();
        round_trip_test(&blobs).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_arrays_page_boundary() -> anyhow::Result<()> {
        let blobs = &[
            random_array(PAGE_SZ - 4),
            random_array(PAGE_SZ - 4),
            random_array(PAGE_SZ - 4),
        ];
        round_trip_test(blobs).await?;
        Ok(())
    }
}
