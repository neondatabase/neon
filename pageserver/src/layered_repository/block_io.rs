//!
//! Low-level Block-oriented I/O functions
//!
//!
//!

use crate::page_cache;
use crate::page_cache::{ReadBufResult, PAGE_SZ};
use lazy_static::lazy_static;
use std::ops::{Deref, DerefMut};
use std::os::unix::fs::FileExt;
use std::sync::atomic::AtomicU64;

/// This is implemented by anything that can read 8 kB (PAGE_SZ)
/// blocks, using the page cache
///
/// There are currently two implementations: EphemeralFile, and FileBlockReader
/// below.
pub trait BlockReader {
    type BlockLease: Deref<Target = [u8; PAGE_SZ]> + 'static;

    ///
    /// Read a block. Returns a "lease" object that can be used to
    /// access to the contents of the page. (For the page cache, the
    /// lease object represents a lock on the buffer.)
    ///
    fn read_blk(&self, blknum: u32) -> Result<Self::BlockLease, std::io::Error>;

    ///
    /// Create a new "cursor" for reading from this reader.
    ///
    /// A cursor caches the last accessed page, allowing for faster
    /// access if the same block is accessed repeatedly.
    fn block_cursor(&self) -> BlockCursor<&Self>
    where
        Self: Sized,
    {
        BlockCursor::new(self)
    }
}

impl<B> BlockReader for &B
where
    B: BlockReader,
{
    type BlockLease = B::BlockLease;

    fn read_blk(&self, blknum: u32) -> Result<Self::BlockLease, std::io::Error> {
        (*self).read_blk(blknum)
    }
}

///
/// A "cursor" for efficiently reading multiple pages from a BlockReader
///
/// A cursor caches the last accessed page, allowing for faster access if the
/// same block is accessed repeatedly.
///
/// You can access the last page with `*cursor`. 'read_blk' returns 'self', so
/// that in many cases you can use a BlockCursor as a drop-in replacement for
/// the underlying BlockReader. For example:
///
/// ```no_run
/// # use pageserver::layered_repository::block_io::{BlockReader, FileBlockReader};
/// # let reader: FileBlockReader<std::fs::File> = todo!();
/// let cursor = reader.block_cursor();
/// let buf = cursor.read_blk(1);
/// // do stuff with 'buf'
/// let buf = cursor.read_blk(2);
/// // do stuff with 'buf'
/// ```
///
pub struct BlockCursor<R>
where
    R: BlockReader,
{
    reader: R,
    /// last accessed page
    cache: Option<(u32, R::BlockLease)>,
}

impl<R> BlockCursor<R>
where
    R: BlockReader,
{
    pub fn new(reader: R) -> Self {
        BlockCursor {
            reader,
            cache: None,
        }
    }

    pub fn read_blk(&mut self, blknum: u32) -> Result<&Self, std::io::Error> {
        // Fast return if this is the same block as before
        if let Some((cached_blk, _buf)) = &self.cache {
            if *cached_blk == blknum {
                return Ok(self);
            }
        }

        // Read the block from the underlying reader, and cache it
        self.cache = None;
        let buf = self.reader.read_blk(blknum)?;
        self.cache = Some((blknum, buf));

        Ok(self)
    }
}

impl<R> Deref for BlockCursor<R>
where
    R: BlockReader,
{
    type Target = [u8; PAGE_SZ];

    fn deref(&self) -> &<Self as Deref>::Target {
        &self.cache.as_ref().unwrap().1
    }
}

lazy_static! {
    static ref NEXT_ID: AtomicU64 = AtomicU64::new(1);
}

/// An adapter for reading a (virtual) file using the page cache.
///
/// The file is assumed to be immutable. This doesn't provide any functions
/// for modifying the file, nor for invalidating the cache if it is modified.
pub struct FileBlockReader<F> {
    pub file: F,

    /// Unique ID of this file, used as key in the page cache.
    file_id: u64,
}

impl<F> FileBlockReader<F>
where
    F: FileExt,
{
    pub fn new(file: F) -> Self {
        let file_id = NEXT_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        FileBlockReader { file_id, file }
    }

    /// Read a page from the underlying file into given buffer.
    fn fill_buffer(&self, buf: &mut [u8], blkno: u32) -> Result<(), std::io::Error> {
        assert!(buf.len() == PAGE_SZ);
        self.file.read_exact_at(buf, blkno as u64 * PAGE_SZ as u64)
    }
}

impl<F> BlockReader for FileBlockReader<F>
where
    F: FileExt,
{
    type BlockLease = page_cache::PageReadGuard<'static>;

    fn read_blk(&self, blknum: u32) -> Result<Self::BlockLease, std::io::Error> {
        // Look up the right page
        let cache = page_cache::get();
        loop {
            match cache.read_immutable_buf(self.file_id, blknum) {
                ReadBufResult::Found(guard) => break Ok(guard),
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
