use crate::layered_repository::disk_btree;
use crate::virtual_file::VirtualFile;

use lazy_static::lazy_static;
use std::cmp::min;
use std::os::unix::fs::FileExt;
use std::sync::atomic::AtomicU64;

use crate::page_cache;
use crate::page_cache::ReadBufResult;
use crate::page_cache::PAGE_SZ;

lazy_static! {
    ///
    /// This is the global cache of file descriptors (File objects).
    ///
    static ref NEXT_ID: AtomicU64 = AtomicU64::new(1);
}

pub struct BlockyReader {
    file_id: u64,
    file: VirtualFile,
}

impl BlockyReader {
    fn new_id() -> u64 {
        NEXT_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }

    pub fn new(file: VirtualFile) -> Self {
        BlockyReader {
            file_id: Self::new_id(),
            file,
        }
    }

    pub fn fill_buffer(&self, buf: &mut [u8], blkno: u32) -> Result<(), std::io::Error> {
        let mut off = 0;
        while off < PAGE_SZ {
            let n = self
                .file
                .read_at(&mut buf[off..], blkno as u64 * PAGE_SZ as u64 + off as u64)?;

            if n == 0 {
                // Reached EOF. Fill the rest of the buffer with zeros.
                const ZERO_BUF: [u8; PAGE_SZ] = [0u8; PAGE_SZ];

                buf[off..].copy_from_slice(&ZERO_BUF[off..]);
                break;
            }

            off += n as usize;
        }
        Ok(())
    }
}

impl disk_btree::DiskBlockReader for &BlockyReader {
    type Lease = page_cache::PageReadGuard<'static>;

    fn read_blk(&self, blknum: u32) -> Result<Self::Lease, std::io::Error> {
        // Look up the right page
        let cache = page_cache::get();
        loop {
            match cache.read_blocky_buf(self.file_id, blknum) {
                ReadBufResult::Found(guard) => return Ok(guard),
                ReadBufResult::NotFound(mut write_guard) => {
                    // Read the page from disk into the buffer
                    use std::ops::DerefMut;
                    self.fill_buffer(write_guard.deref_mut(), blknum)?;
                    write_guard.mark_valid();

                    // Swap for read lock
                    continue;
                }
            };
        }
    }
}

impl FileExt for BlockyReader {
    fn read_at(&self, dstbuf: &mut [u8], offset: u64) -> Result<usize, std::io::Error> {
        let blknum = (offset / PAGE_SZ as u64) as u32;
        let off = (offset % PAGE_SZ as u64) as usize;

        use crate::layered_repository::disk_btree::DiskBlockReader;
        let buf = self.read_blk(blknum)?;
        let srcbuf = &buf[off..];

        let len = min(srcbuf.len(), dstbuf.len());
        dstbuf[..len].copy_from_slice(&srcbuf[..len]);
        Ok(len)
    }

    fn write_at(&self, _: &[u8], _: u64) -> Result<usize, std::io::Error> {
        panic!()
    }
}

pub struct OffsetBlockReader<'a, R>
where
    R: disk_btree::DiskBlockReader,
{
    start_blk: u32,
    reader: &'a R,
}

impl<R> disk_btree::DiskBlockReader for OffsetBlockReader<'_, R>
where
    R: disk_btree::DiskBlockReader,
{
    type Lease = R::Lease;

    fn read_blk(&self, blknum: u32) -> Result<Self::Lease, std::io::Error> {
        self.reader.read_blk(self.start_blk + blknum)
    }
}

impl<'a, R> OffsetBlockReader<'a, R>
where
    R: disk_btree::DiskBlockReader,
{
    pub fn new(start_blk: u32, reader: &'a R) -> Self {
        OffsetBlockReader { start_blk, reader }
    }
}
