//!
//! Functions for reading and writing variable-sized "blobs".
//!
//! Each blob begins with a 4-byte length, followed by the actual data.
//!
use crate::layered_repository::block_io::{BlockCursor, BlockReader};
use crate::page_cache::PAGE_SZ;
use std::cmp::min;
use std::io::Error;

/// For reading
pub trait BlobCursor {
    fn read_blob(&mut self, offset: u64) -> Result<Vec<u8>, std::io::Error> {
        let mut buf = Vec::new();
        self.read_blob_into_buf(offset, &mut buf)?;
        Ok(buf)
    }

    fn read_blob_into_buf(
        &mut self,
        offset: u64,
        dstbuf: &mut Vec<u8>,
    ) -> Result<(), std::io::Error>;
}

impl<'a, R> BlobCursor for BlockCursor<R>
where
    R: BlockReader,
{
    fn read_blob_into_buf(
        &mut self,
        offset: u64,
        dstbuf: &mut Vec<u8>,
    ) -> Result<(), std::io::Error> {
        let mut blknum = (offset / PAGE_SZ as u64) as u32;
        let mut off = (offset % PAGE_SZ as u64) as usize;

        let mut buf = self.read_blk(blknum)?;

        // read length
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
        let len = u32::from_ne_bytes(len_buf) as usize;

        dstbuf.clear();

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

pub trait BlobWriter {
    fn write_blob(&mut self, srcbuf: &[u8]) -> Result<u64, Error>;
}

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
        self.inner
            .write_all(&((srcbuf.len()) as u32).to_ne_bytes())?;
        self.inner.write_all(srcbuf)?;
        self.offset += 4 + srcbuf.len() as u64;
        Ok(offset)
    }
}
