use crate::layered_repository::block_io::DiskBlockReader;
use crate::virtual_file::VirtualFile;

use crate::layered_repository::blob_io::BlobReader;
use lazy_static::lazy_static;
use std::ops::DerefMut;
use std::os::unix::fs::FileExt;
use std::sync::atomic::AtomicU64;

use crate::page_cache;
use crate::page_cache::ReadBufResult;
use crate::page_cache::PAGE_SZ;

lazy_static! {
    static ref NEXT_ID: AtomicU64 = AtomicU64::new(1);
}

/// An adapter to allow reading from a virtual file, using the page cache.
///
/// The file is assumed to be immutable. We don't provide any functions for
/// modifying the file, nor for invalidating the cache if it is modified.
pub struct BlockyReader {
    /// Unique ID of this file, used in the page cache.
    file_id: u64,
    file: VirtualFile,
}

impl BlobReader for BlockyReader {}

impl BlockyReader {
    pub fn new(file: VirtualFile) -> Self {
        let file_id = NEXT_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        BlockyReader { file_id, file }
    }

    // Read a page from the underlying file into given buffer.
    fn fill_buffer(&self, buf: &mut [u8], blkno: u32) -> Result<(), std::io::Error> {
        assert!(buf.len() == PAGE_SZ);
        self.file.read_exact_at(buf, blkno as u64 * PAGE_SZ as u64)
    }
}

impl DiskBlockReader for BlockyReader {
    type Lease = page_cache::PageReadGuard<'static>;

    fn read_blk(&self, blknum: u32) -> Result<Self::Lease, std::io::Error> {
        // Look up the right page
        let cache = page_cache::get();
        loop {
            match cache.read_blocky_buf(self.file_id, blknum) {
                ReadBufResult::Found(guard) => return Ok(guard),
                ReadBufResult::NotFound(mut write_guard) => {
                    // Read the page from disk into the buffer
                    self.fill_buffer(write_guard.deref_mut(), blknum)?;
                    write_guard.mark_valid();

                    // Swap for read lock
                    continue;
                }
            };
        }
    }
}
