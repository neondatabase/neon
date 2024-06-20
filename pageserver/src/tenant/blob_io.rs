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
use async_compression::Level;
use bytes::{BufMut, BytesMut};
use pageserver_api::models::ImageCompressionAlgorithm;
use tokio::io::AsyncWriteExt;
use tokio_epoll_uring::{BoundedBuf, IoBuf, Slice};

use crate::context::RequestContext;
use crate::page_cache::PAGE_SZ;
use crate::tenant::block_io::BlockCursor;
use crate::virtual_file::VirtualFile;
use std::cmp::min;
use std::io::{Error, ErrorKind};

impl<'a> BlockCursor<'a> {
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
            len_buf[0] &= 0x0f;
            u32::from_be_bytes(len_buf) as usize
        };
        let compression_bits = first_len_byte & 0xf0;

        let mut tmp_buf = Vec::new();
        let buf_to_write;
        let compression = if compression_bits <= BYTE_UNCOMPRESSED {
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

const BYTE_UNCOMPRESSED: u8 = 0x80;
const BYTE_ZSTD: u8 = BYTE_UNCOMPRESSED | 0x10;

/// A wrapper of `VirtualFile` that allows users to write blobs.
///
/// If a `BlobWriter` is dropped, the internal buffer will be
/// discarded. You need to call [`flush_buffer`](Self::flush_buffer)
/// manually before dropping.
pub struct BlobWriter<const BUFFERED: bool> {
    inner: VirtualFile,
    offset: u64,
    /// A buffer to save on write calls, only used if BUFFERED=true
    buf: Vec<u8>,
    /// We do tiny writes for the length headers; they need to be in an owned buffer;
    io_buf: Option<BytesMut>,
}

impl<const BUFFERED: bool> BlobWriter<BUFFERED> {
    pub fn new(inner: VirtualFile, start_offset: u64) -> Self {
        Self {
            inner,
            offset: start_offset,
            buf: Vec::with_capacity(Self::CAPACITY),
            io_buf: Some(BytesMut::new()),
        }
    }

    pub fn size(&self) -> u64 {
        self.offset
    }

    const CAPACITY: usize = if BUFFERED { 64 * 1024 } else { 0 };

    /// Writes the given buffer directly to the underlying `VirtualFile`.
    /// You need to make sure that the internal buffer is empty, otherwise
    /// data will be written in wrong order.
    #[inline(always)]
    async fn write_all_unbuffered<B: BoundedBuf<Buf = Buf>, Buf: IoBuf + Send>(
        &mut self,
        src_buf: B,
        ctx: &RequestContext,
    ) -> (B::Buf, Result<(), Error>) {
        let (src_buf, res) = self.inner.write_all(src_buf, ctx).await;
        let nbytes = match res {
            Ok(nbytes) => nbytes,
            Err(e) => return (src_buf, Err(e)),
        };
        self.offset += nbytes as u64;
        (src_buf, Ok(()))
    }

    #[inline(always)]
    /// Flushes the internal buffer to the underlying `VirtualFile`.
    pub async fn flush_buffer(&mut self, ctx: &RequestContext) -> Result<(), Error> {
        let buf = std::mem::take(&mut self.buf);
        let (mut buf, res) = self.inner.write_all(buf, ctx).await;
        res?;
        buf.clear();
        self.buf = buf;
        Ok(())
    }

    #[inline(always)]
    /// Writes as much of `src_buf` into the internal buffer as it fits
    fn write_into_buffer(&mut self, src_buf: &[u8]) -> usize {
        let remaining = Self::CAPACITY - self.buf.len();
        let to_copy = src_buf.len().min(remaining);
        self.buf.extend_from_slice(&src_buf[..to_copy]);
        self.offset += to_copy as u64;
        to_copy
    }

    /// Internal, possibly buffered, write function
    async fn write_all<B: BoundedBuf<Buf = Buf>, Buf: IoBuf + Send>(
        &mut self,
        src_buf: B,
        ctx: &RequestContext,
    ) -> (B::Buf, Result<(), Error>) {
        if !BUFFERED {
            assert!(self.buf.is_empty());
            return self.write_all_unbuffered(src_buf, ctx).await;
        }
        let remaining = Self::CAPACITY - self.buf.len();
        let src_buf_len = src_buf.bytes_init();
        if src_buf_len == 0 {
            return (Slice::into_inner(src_buf.slice_full()), Ok(()));
        }
        let mut src_buf = src_buf.slice(0..src_buf_len);
        // First try to copy as much as we can into the buffer
        if remaining > 0 {
            let copied = self.write_into_buffer(&src_buf);
            src_buf = src_buf.slice(copied..);
        }
        // Then, if the buffer is full, flush it out
        if self.buf.len() == Self::CAPACITY {
            if let Err(e) = self.flush_buffer(ctx).await {
                return (Slice::into_inner(src_buf), Err(e));
            }
        }
        // Finally, write the tail of src_buf:
        // If it wholly fits into the buffer without
        // completely filling it, then put it there.
        // If not, write it out directly.
        let src_buf = if !src_buf.is_empty() {
            assert_eq!(self.buf.len(), 0);
            if src_buf.len() < Self::CAPACITY {
                let copied = self.write_into_buffer(&src_buf);
                // We just verified above that src_buf fits into our internal buffer.
                assert_eq!(copied, src_buf.len());
                Slice::into_inner(src_buf)
            } else {
                let (src_buf, res) = self.write_all_unbuffered(src_buf, ctx).await;
                if let Err(e) = res {
                    return (src_buf, Err(e));
                }
                src_buf
            }
        } else {
            Slice::into_inner(src_buf)
        };
        (src_buf, Ok(()))
    }

    /// Write a blob of data. Returns the offset that it was written to,
    /// which can be used to retrieve the data later.
    pub async fn write_blob<B: BoundedBuf<Buf = Buf>, Buf: IoBuf + Send>(
        &mut self,
        srcbuf: B,
        ctx: &RequestContext,
    ) -> (B::Buf, Result<u64, Error>) {
        self.write_blob_maybe_compressed(srcbuf, ctx, None).await
    }

    /// Write a blob of data. Returns the offset that it was written to,
    /// which can be used to retrieve the data later.
    pub async fn write_blob_maybe_compressed<B: BoundedBuf<Buf = Buf>, Buf: IoBuf + Send>(
        &mut self,
        srcbuf: B,
        ctx: &RequestContext,
        algorithm: Option<ImageCompressionAlgorithm>,
    ) -> (B::Buf, Result<u64, Error>) {
        let offset = self.offset;

        let len = srcbuf.bytes_init();

        let mut io_buf = self.io_buf.take().expect("we always put it back below");
        io_buf.clear();
        let mut compressed_buf = None;
        let ((io_buf, hdr_res), srcbuf) = async {
            if len < 128 {
                // Short blob. Write a 1-byte length header
                io_buf.put_u8(len as u8);
                (
                    self.write_all(io_buf, ctx).await,
                    srcbuf.slice(..).into_inner(),
                )
            } else {
                // Write a 4-byte length header
                if len > 0x0fff_ffff {
                    return (
                        (
                            io_buf,
                            Err(Error::new(
                                ErrorKind::Other,
                                format!("blob too large ({len} bytes)"),
                            )),
                        ),
                        srcbuf.slice(..).into_inner(),
                    );
                }
                let (high_bit_mask, len_written, srcbuf) = match algorithm {
                    Some(ImageCompressionAlgorithm::Zstd { level }) => {
                        let mut encoder = if let Some(level) = level {
                            async_compression::tokio::write::ZstdEncoder::with_quality(
                                Vec::new(),
                                Level::Precise(level.into()),
                            )
                        } else {
                            async_compression::tokio::write::ZstdEncoder::new(Vec::new())
                        };
                        let slice = srcbuf.slice(..);
                        encoder.write_all(&slice[..]).await.unwrap();
                        encoder.shutdown().await.unwrap();
                        let compressed = encoder.into_inner();
                        if compressed.len() < len {
                            let compressed_len = compressed.len();
                            compressed_buf = Some(compressed);
                            (BYTE_ZSTD, compressed_len, slice.into_inner())
                        } else {
                            (BYTE_UNCOMPRESSED, len, slice.into_inner())
                        }
                    }
                    None => (BYTE_UNCOMPRESSED, len, srcbuf.slice(..).into_inner()),
                };
                let mut len_buf = (len_written as u32).to_be_bytes();
                assert_eq!(len_buf[0] & 0xf0, 0);
                len_buf[0] |= high_bit_mask;
                io_buf.extend_from_slice(&len_buf[..]);
                (self.write_all(io_buf, ctx).await, srcbuf)
            }
        }
        .await;
        self.io_buf = Some(io_buf);
        match hdr_res {
            Ok(_) => (),
            Err(e) => return (Slice::into_inner(srcbuf.slice(..)), Err(e)),
        }
        let (srcbuf, res) = if let Some(compressed_buf) = compressed_buf {
            let (_buf, res) = self.write_all(compressed_buf, ctx).await;
            (Slice::into_inner(srcbuf.slice(..)), res)
        } else {
            self.write_all(srcbuf, ctx).await
        };
        (srcbuf, res.map(|_| offset))
    }
}

impl BlobWriter<true> {
    /// Access the underlying `VirtualFile`.
    ///
    /// This function flushes the internal buffer before giving access
    /// to the underlying `VirtualFile`.
    pub async fn into_inner(mut self, ctx: &RequestContext) -> Result<VirtualFile, Error> {
        self.flush_buffer(ctx).await?;
        Ok(self.inner)
    }

    /// Access the underlying `VirtualFile`.
    ///
    /// Unlike [`into_inner`](Self::into_inner), this doesn't flush
    /// the internal buffer before giving access.
    pub fn into_inner_no_flush(self) -> VirtualFile {
        self.inner
    }
}

impl BlobWriter<false> {
    /// Access the underlying `VirtualFile`.
    pub fn into_inner(self) -> VirtualFile {
        self.inner
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{context::DownloadBehavior, task_mgr::TaskKind, tenant::block_io::BlockReaderRef};
    use rand::{Rng, SeedableRng};

    async fn round_trip_test<const BUFFERED: bool>(blobs: &[Vec<u8>]) -> Result<(), Error> {
        round_trip_test_compressed::<BUFFERED, 0>(blobs).await
    }

    async fn round_trip_test_compressed<const BUFFERED: bool, const COMPRESSION: u8>(
        blobs: &[Vec<u8>],
    ) -> Result<(), Error> {
        let temp_dir = camino_tempfile::tempdir()?;
        let pathbuf = temp_dir.path().join("file");
        let ctx = RequestContext::new(TaskKind::UnitTest, DownloadBehavior::Error);

        // Write part (in block to drop the file)
        let mut offsets = Vec::new();
        {
            let file = VirtualFile::create(pathbuf.as_path(), &ctx).await?;
            let mut wtr = BlobWriter::<BUFFERED>::new(file, 0);
            for blob in blobs.iter() {
                let (_, res) = match COMPRESSION {
                    0 => wtr.write_blob(blob.clone(), &ctx).await,
                    1 => {
                        wtr.write_blob_maybe_compressed(
                            blob.clone(),
                            &ctx,
                            Some(ImageCompressionAlgorithm::ZstdLow),
                        )
                        .await
                    }
                    _ => unreachable!("Invalid compression {COMPRESSION}"),
                };
                let offs = res?;
                offsets.push(offs);
            }
            // Write out one page worth of zeros so that we can
            // read again with read_blk
            let (_, res) = wtr.write_blob(vec![0; PAGE_SZ], &ctx).await;
            let offs = res?;
            println!("Writing final blob at offs={offs}");
            wtr.flush_buffer(&ctx).await?;
        }

        let file = VirtualFile::open(pathbuf.as_path(), &ctx).await?;
        let rdr = BlockReaderRef::VirtualFile(&file);
        let rdr = BlockCursor::new(rdr);
        for (idx, (blob, offset)) in blobs.iter().zip(offsets.iter()).enumerate() {
            let blob_read = rdr.read_blob(*offset, &ctx).await?;
            assert_eq!(
                blob, &blob_read,
                "mismatch for idx={idx} at offset={offset}"
            );
        }
        Ok(())
    }

    fn random_array(len: usize) -> Vec<u8> {
        let mut rng = rand::thread_rng();
        (0..len).map(|_| rng.gen()).collect::<_>()
    }

    #[tokio::test]
    async fn test_one() -> Result<(), Error> {
        let blobs = &[vec![12, 21, 22]];
        round_trip_test::<false>(blobs).await?;
        round_trip_test::<true>(blobs).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_hello_simple() -> Result<(), Error> {
        let blobs = &[
            vec![0, 1, 2, 3],
            b"Hello, World!".to_vec(),
            Vec::new(),
            b"foobar".to_vec(),
        ];
        round_trip_test::<false>(blobs).await?;
        round_trip_test::<true>(blobs).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_really_big_array() -> Result<(), Error> {
        let blobs = &[
            b"test".to_vec(),
            random_array(10 * PAGE_SZ),
            b"hello".to_vec(),
            random_array(66 * PAGE_SZ),
            vec![0xf3; 24 * PAGE_SZ],
            b"foobar".to_vec(),
        ];
        round_trip_test::<false>(blobs).await?;
        round_trip_test::<true>(blobs).await?;
        round_trip_test_compressed::<false, 1>(blobs).await?;
        round_trip_test_compressed::<true, 1>(blobs).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_arrays_inc() -> Result<(), Error> {
        let blobs = (0..PAGE_SZ / 8)
            .map(|v| random_array(v * 16))
            .collect::<Vec<_>>();
        round_trip_test::<false>(&blobs).await?;
        round_trip_test::<true>(&blobs).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_arrays_random_size() -> Result<(), Error> {
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let blobs = (0..1024)
            .map(|_| {
                let mut sz: u16 = rng.gen();
                // Make 50% of the arrays small
                if rng.gen() {
                    sz &= 63;
                }
                random_array(sz.into())
            })
            .collect::<Vec<_>>();
        round_trip_test::<false>(&blobs).await?;
        round_trip_test::<true>(&blobs).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_arrays_page_boundary() -> Result<(), Error> {
        let blobs = &[
            random_array(PAGE_SZ - 4),
            random_array(PAGE_SZ - 4),
            random_array(PAGE_SZ - 4),
        ];
        round_trip_test::<false>(blobs).await?;
        round_trip_test::<true>(blobs).await?;
        Ok(())
    }
}
