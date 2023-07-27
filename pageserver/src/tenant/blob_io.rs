//!
//! Functions for reading and writing variable-sized "blobs".
//!
//! Each blob begins with a 1- or 4-byte length field, followed by the
//! actual data. If the length is smaller than 128 bytes, the length
//! is written as a one byte. If it's larger than that, the length
//! is written as a four-byte integer, in big-endian, with the high
//! bit set. This way, we can detect whether it's 1- or 4-byte header
//! by peeking at the first byte.
//!
//! len <  128: 0XXXXXXX
//! len >= 128: 1XXXXXXX XXXXXXXX XXXXXXXX XXXXXXXX
//!
use crate::page_cache::PAGE_SZ;
use crate::tenant::block_io::{BlockCursor, BlockReader};
use std::cmp::min;
use std::io::{Error, ErrorKind};

impl<R> BlockCursor<R>
where
    R: BlockReader,
{
    /// Read a blob into a new buffer.
    pub fn read_blob(&self, offset: u64) -> Result<Vec<u8>, std::io::Error> {
        let mut buf = Vec::new();
        self.read_blob_into_buf(offset, &mut buf)?;
        Ok(buf)
    }
    /// Read blob into the given buffer. Any previous contents in the buffer
    /// are overwritten.
    pub fn read_blob_into_buf(
        &self,
        offset: u64,
        dstbuf: &mut Vec<u8>,
    ) -> Result<(), std::io::Error> {
        let mut blknum = (offset / PAGE_SZ as u64) as u32;
        let mut off = (offset % PAGE_SZ as u64) as usize;

        let mut buf = self.read_blk(blknum)?;

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
                buf = self.read_blk(blknum)?;
                len_buf[thislen..].copy_from_slice(&buf[0..4 - thislen]);
                off = 4 - thislen;
            } else {
                len_buf.copy_from_slice(&buf[off..off + 4]);
                off += 4;
            }
            len_buf[0] &= 0x7f;
            u32::from_be_bytes(len_buf) as usize
        };

        dstbuf.clear();
        dstbuf.reserve(len);

        // Read the payload
        let mut remain = len;
        while remain > 0 {
            let mut page_remain = PAGE_SZ - off;
            if page_remain == 0 {
                // continue on next page
                blknum += 1;
                buf = self.read_blk(blknum)?;
                off = 0;
                page_remain = PAGE_SZ;
            }
            let this_blk_len = min(remain, page_remain);
            dstbuf.extend_from_slice(&buf[off..off + this_blk_len]);
            remain -= this_blk_len;
            off += this_blk_len;
        }
        Ok(())
    }
}

///
/// Abstract trait for a data sink that you can write blobs to.
///
pub trait BlobWriter {
    /// Write a blob of data. Returns the offset that it was written to,
    /// which can be used to retrieve the data later.
    fn write_blob(&mut self, srcbuf: &[u8]) -> Result<u64, Error>;
}

///
/// An implementation of BlobWriter to write blobs to anything that
/// implements std::io::Write.
///
pub struct WriteBlobWriter<W>
where
    W: std::io::Write,
{
    inner: W,
    offset: u64,
}

impl<W> WriteBlobWriter<W>
where
    W: std::io::Write,
{
    pub fn new(inner: W, start_offset: u64) -> Self {
        WriteBlobWriter {
            inner,
            offset: start_offset,
        }
    }

    pub fn size(&self) -> u64 {
        self.offset
    }

    /// Access the underlying Write object.
    ///
    /// NOTE: WriteBlobWriter keeps track of the current write offset. If
    /// you write something directly to the inner Write object, it makes the
    /// internally tracked 'offset' to go out of sync. So don't do that.
    pub fn into_inner(self) -> W {
        self.inner
    }
}

impl<W> BlobWriter for WriteBlobWriter<W>
where
    W: std::io::Write,
{
    fn write_blob(&mut self, srcbuf: &[u8]) -> Result<u64, Error> {
        let offset = self.offset;

        if srcbuf.len() < 128 {
            // Short blob. Write a 1-byte length header
            let len_buf = srcbuf.len() as u8;
            self.inner.write_all(&[len_buf])?;
            self.offset += 1;
        } else {
            // Write a 4-byte length header
            if srcbuf.len() > 0x7fff_ffff {
                return Err(Error::new(
                    ErrorKind::Other,
                    format!("blob too large ({} bytes)", srcbuf.len()),
                ));
            }
            let mut len_buf = ((srcbuf.len()) as u32).to_be_bytes();
            len_buf[0] |= 0x80;
            self.inner.write_all(&len_buf)?;
            self.offset += 4;
        }
        self.inner.write_all(srcbuf)?;
        self.offset += srcbuf.len() as u64;
        Ok(offset)
    }
}
