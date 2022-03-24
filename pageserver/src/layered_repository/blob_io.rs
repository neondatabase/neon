// Utilities for reading and writing Values
use crate::layered_repository::block_io::DiskBlockReader;
use crate::page_cache::PAGE_SZ;
use std::cmp::min;
use std::io::{Error, Write};

pub trait BlobReader: DiskBlockReader {
    fn read_blob(&self, offset: u64) -> Result<Vec<u8>, std::io::Error> {
        let mut buf = Vec::new();
        self.read_blob_into_buf(offset, &mut buf)?;
        Ok(buf)
    }

    fn read_blob_into_buf(&self, offset: u64, dstbuf: &mut Vec<u8>) -> Result<(), std::io::Error> {
        let blknum = (offset / PAGE_SZ as u64) as u32;
        let off = (offset % PAGE_SZ as u64) as usize;

        assert!(off <= PAGE_SZ - 4);

        let buf = self.read_blk(blknum)?;

        // read length
        let mut len_buf = [0u8; 4];
        len_buf.copy_from_slice(&buf.as_ref()[off..off + 4]);
        let len = u32::from_ne_bytes(len_buf) as usize;

        dstbuf.clear();

        // read the blob. It might not all fit on the page
        let mut blknum = blknum;
        let mut off = off + 4;
        let mut remain = len;
        let mut buf = buf;
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
            dstbuf.extend_from_slice(&buf.as_ref()[off..off + this_blk_len]);
            remain -= this_blk_len;
            off += this_blk_len;
        }
        Ok(())
    }
}

pub trait BlobWriter {
    fn write_blob(&mut self, buf: &[u8]) -> Result<u64, Error>;
}

pub struct BlobWrite<W: Write> {
    pub writer: W,
    pub offset: u64,
}
impl<W: Write> BlobWrite<W> {
    pub fn new(writer: W, start_offset: u64) -> Self {
        BlobWrite {
            writer,
            offset: start_offset,
        }
    }
}
impl<W: Write> BlobWriter for BlobWrite<W> {
    fn write_blob(&mut self, buf: &[u8]) -> Result<u64, Error> {
        {
            let page_remain = PAGE_SZ - (self.offset as usize % PAGE_SZ);
            if page_remain < 4 {
                const PADDING: [u8; 4] = [0u8; 4];
                self.writer.write_all(&PADDING[..page_remain])?;
                self.offset += page_remain as u64;
            }
        }

        let offset = self.offset;

        // write the 'length' field
        let lenbuf = u32::to_ne_bytes(buf.len() as u32);
        self.writer.write_all(&lenbuf)?;
        self.writer.write_all(buf)?;

        self.offset += 4 + buf.len() as u64;

        Ok(offset)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{thread_rng, RngCore};

    #[derive(Clone, Default)]
    struct TestDisk {
        disk: Vec<u8>,
    }
    impl TestDisk {
        fn new() -> Self {
            Self::default()
        }
    }
    impl DiskBlockReader for TestDisk {
        type Lease = std::rc::Rc<[u8; PAGE_SZ]>;

        fn read_blk(&self, blknum: u32) -> Result<Self::Lease, std::io::Error> {
            let offset = blknum as usize * PAGE_SZ;
            let end_offset = min(self.disk.len(), offset + PAGE_SZ);
            let mut buf = [0u8; PAGE_SZ];
            buf[..(end_offset - offset)].copy_from_slice(&self.disk[offset..end_offset]);
            Ok(std::rc::Rc::new(buf))
        }
    }
    impl BlobReader for TestDisk {}
    impl Write for TestDisk {
        fn write(&mut self, buf: &[u8]) -> std::result::Result<usize, std::io::Error> {
            self.disk.extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::result::Result<(), std::io::Error> {
            Ok(())
        }
    }

    #[test]
    fn test_blob_io() -> Result<(), Error> {
        let disk = TestDisk::new();

        let mut blob_writer = BlobWrite::new(disk, 0);

        let pos_foo = blob_writer.write_blob(b"foo")?;
        assert_eq!(b"foo", blob_writer.writer.read_blob(pos_foo)?.as_slice());
        let pos_bar = blob_writer.write_blob(b"bar")?;
        assert_eq!(b"foo", blob_writer.writer.read_blob(pos_foo)?.as_slice());
        assert_eq!(b"bar", blob_writer.writer.read_blob(pos_bar)?.as_slice());

        let mut blobs = Vec::new();
        for i in 0..10000 {
            let data = Vec::from(format!("blob{}", i).as_bytes());
            let pos = blob_writer.write_blob(&data)?;
            blobs.push((pos, data));
        }

        // Test a large blob that spans multiple pages
        let mut large_data = Vec::new();
        large_data.resize(20000, 0);
        thread_rng().fill_bytes(&mut large_data);
        let pos_large = blob_writer.write_blob(&large_data)?;

        let disk = blob_writer.writer;

        assert_eq!(b"foo", disk.read_blob(pos_foo)?.as_slice());
        assert_eq!(b"bar", disk.read_blob(pos_bar)?.as_slice());

        for (pos, expected) in blobs {
            assert_eq!(disk.read_blob(pos)?, expected);
        }

        // test large blob
        let result = disk.read_blob(pos_large)?;
        assert_eq!(result, large_data);

        Ok(())
    }
}
