// Utilities for reading and writing Values
use crate::layered_repository::disk_btree::DiskBlockWriter;
use crate::page_cache::PAGE_SZ;
use bytes::Bytes;
use std::io::{Error, Write};
use std::os::unix::fs::FileExt;

pub fn read_blob_buf<F: FileExt>(file: &F, off: u64, buf: &mut Vec<u8>) -> Result<usize, Error> {
    // read length
    let mut len_buf = [0u8; 4];
    file.read_exact_at(&mut len_buf, off)?;

    let len = u32::from_ne_bytes(len_buf) as usize;

    assert!(len < 1000000);

    buf.resize(len, 0);
    file.read_exact_at(&mut buf.as_mut_slice(), off + 4)?;

    Ok(len)
}

pub fn read_blob<F: FileExt>(file: &F, off: u64) -> Result<Vec<u8>, Error> {
    let mut buf: Vec<u8> = Vec::new();
    let _ = read_blob_buf(file, off, &mut buf);
    Ok(buf)
}

pub fn write_blob<W: Write>(writer: &mut W, buf: &[u8]) -> Result<u64, Error> {
    let val_len = buf.len() as u32;

    // write the 'length' field
    let lenbuf = u32::to_ne_bytes(val_len);

    writer.write_all(&lenbuf)?;
    writer.write_all(buf)?;

    Ok(4 + val_len as u64)
}

pub struct BlockBuf {
    pub blocks: Vec<Bytes>,
}

pub const ALL_ZEROS: [u8; PAGE_SZ] = [0u8; PAGE_SZ];

impl DiskBlockWriter for BlockBuf {
    fn write_blk(&mut self, buf: Bytes) -> Result<u32, std::io::Error> {
        assert!(buf.len() == PAGE_SZ);
        let blknum = self.blocks.len();
        self.blocks.push(buf);
        tracing::info!("buffered block {}", blknum);
        Ok(blknum as u32)
    }
}

impl BlockBuf {
    pub fn new() -> Self {
        BlockBuf { blocks: Vec::new() }
    }

    pub fn size(&self) -> u64 {
        (self.blocks.len() * PAGE_SZ) as u64
    }
}
