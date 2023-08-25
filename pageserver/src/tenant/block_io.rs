//!
//! Low-level Block-oriented I/O functions
//!

use super::ephemeral_file::EphemeralFile;
use super::storage_layer::delta_layer::{Adapter, DeltaLayerInner};
use crate::page_cache::{self, PageReadGuard, ReadBufResult, PAGE_SZ};
use crate::virtual_file::VirtualFile;
use bytes::Bytes;
use std::fs::File;
use std::ops::{Deref, DerefMut};
use std::os::unix::fs::FileExt;

/// This is implemented by anything that can read 8 kB (PAGE_SZ)
/// blocks, using the page cache
///
/// There are currently two implementations: EphemeralFile, and FileBlockReader
/// below.
pub trait BlockReader {
    ///
    /// Create a new "cursor" for reading from this reader.
    ///
    /// A cursor caches the last accessed page, allowing for faster
    /// access if the same block is accessed repeatedly.
    fn block_cursor(&self) -> BlockCursor<'_>;
}

impl<B> BlockReader for &B
where
    B: BlockReader,
{
    fn block_cursor(&self) -> BlockCursor<'_> {
        (*self).block_cursor()
    }
}

/// Reference to an in-memory copy of an immutable on-disk block.
pub enum BlockLease<'a> {
    PageReadGuard(PageReadGuard<'static>),
    EphemeralFileMutableTail(&'a [u8; PAGE_SZ]),
    #[cfg(test)]
    Rc(std::rc::Rc<[u8; PAGE_SZ]>),
}

impl From<PageReadGuard<'static>> for BlockLease<'static> {
    fn from(value: PageReadGuard<'static>) -> BlockLease<'static> {
        BlockLease::PageReadGuard(value)
    }
}

#[cfg(test)]
impl<'a> From<std::rc::Rc<[u8; PAGE_SZ]>> for BlockLease<'a> {
    fn from(value: std::rc::Rc<[u8; PAGE_SZ]>) -> Self {
        BlockLease::Rc(value)
    }
}

impl<'a> Deref for BlockLease<'a> {
    type Target = [u8; PAGE_SZ];

    fn deref(&self) -> &Self::Target {
        match self {
            BlockLease::PageReadGuard(v) => v.deref(),
            BlockLease::EphemeralFileMutableTail(v) => v,
            #[cfg(test)]
            BlockLease::Rc(v) => v.deref(),
        }
    }
}

/// Provides the ability to read blocks from different sources,
/// similar to using traits for this purpose.
///
/// Unlike traits, we also support the read function to be async though.
pub(crate) enum BlockReaderRef<'a> {
    FileBlockReaderVirtual(&'a FileBlockReader<VirtualFile>),
    FileBlockReaderFile(&'a FileBlockReader<std::fs::File>),
    EphemeralFile(&'a EphemeralFile),
    Adapter(Adapter<&'a DeltaLayerInner>),
    #[cfg(test)]
    TestDisk(&'a super::disk_btree::tests::TestDisk),
}

impl<'a> BlockReaderRef<'a> {
    #[inline(always)]
    fn read_blk(&self, blknum: u32) -> Result<BlockLease, std::io::Error> {
        use BlockReaderRef::*;
        match self {
            FileBlockReaderVirtual(r) => r.read_blk(blknum),
            FileBlockReaderFile(r) => r.read_blk(blknum),
            EphemeralFile(r) => r.read_blk(blknum),
            Adapter(r) => r.read_blk(blknum),
            #[cfg(test)]
            TestDisk(r) => r.read_blk(blknum),
        }
    }
}

///
/// A "cursor" for efficiently reading multiple pages from a BlockReader
///
/// You can access the last page with `*cursor`. 'read_blk' returns 'self', so
/// that in many cases you can use a BlockCursor as a drop-in replacement for
/// the underlying BlockReader. For example:
///
/// ```no_run
/// # use pageserver::tenant::block_io::{BlockReader, FileBlockReader};
/// # let reader: FileBlockReader<std::fs::File> = unimplemented!("stub");
/// let cursor = reader.block_cursor();
/// let buf = cursor.read_blk(1);
/// // do stuff with 'buf'
/// let buf = cursor.read_blk(2);
/// // do stuff with 'buf'
/// ```
///
pub struct BlockCursor<'a> {
    reader: BlockReaderRef<'a>,
}

impl<'a> BlockCursor<'a> {
    pub(crate) fn new(reader: BlockReaderRef<'a>) -> Self {
        BlockCursor { reader }
    }
    // Needed by cli
    pub fn new_fileblockreader_virtual(reader: &'a FileBlockReader<VirtualFile>) -> Self {
        BlockCursor {
            reader: BlockReaderRef::FileBlockReaderVirtual(reader),
        }
    }

    /// Read a block.
    ///
    /// Returns a "lease" object that can be used to
    /// access to the contents of the page. (For the page cache, the
    /// lease object represents a lock on the buffer.)
    #[inline(always)]
    pub fn read_blk(&self, blknum: u32) -> Result<BlockLease, std::io::Error> {
        self.reader.read_blk(blknum)
    }
}

/// An adapter for reading a (virtual) file using the page cache.
///
/// The file is assumed to be immutable. This doesn't provide any functions
/// for modifying the file, nor for invalidating the cache if it is modified.
pub struct FileBlockReader<F> {
    pub file: F,

    /// Unique ID of this file, used as key in the page cache.
    file_id: page_cache::FileId,
}

impl<F> FileBlockReader<F>
where
    F: FileExt,
{
    pub fn new(file: F) -> Self {
        let file_id = page_cache::next_file_id();

        FileBlockReader { file_id, file }
    }

    /// Read a page from the underlying file into given buffer.
    fn fill_buffer(&self, buf: &mut [u8], blkno: u32) -> Result<(), std::io::Error> {
        assert!(buf.len() == PAGE_SZ);
        self.file.read_exact_at(buf, blkno as u64 * PAGE_SZ as u64)
    }
    /// Read a block.
    ///
    /// Returns a "lease" object that can be used to
    /// access to the contents of the page. (For the page cache, the
    /// lease object represents a lock on the buffer.)
    pub fn read_blk(&self, blknum: u32) -> Result<BlockLease, std::io::Error> {
        let cache = page_cache::get();
        loop {
            match cache
                .read_immutable_buf(self.file_id, blknum)
                .map_err(|e| {
                    std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Failed to read immutable buf: {e:#}"),
                    )
                })? {
                ReadBufResult::Found(guard) => break Ok(guard.into()),
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

impl BlockReader for FileBlockReader<File> {
    fn block_cursor(&self) -> BlockCursor<'_> {
        BlockCursor::new(BlockReaderRef::FileBlockReaderFile(self))
    }
}

impl BlockReader for FileBlockReader<VirtualFile> {
    fn block_cursor(&self) -> BlockCursor<'_> {
        BlockCursor::new(BlockReaderRef::FileBlockReaderVirtual(self))
    }
}

///
/// Trait for block-oriented output
///
pub trait BlockWriter {
    ///
    /// Write a page to the underlying storage.
    ///
    /// 'buf' must be of size PAGE_SZ. Returns the block number the page was
    /// written to.
    ///
    fn write_blk(&mut self, buf: Bytes) -> Result<u32, std::io::Error>;
}

///
/// A simple in-memory buffer of blocks.
///
pub struct BlockBuf {
    pub blocks: Vec<Bytes>,
}
impl BlockWriter for BlockBuf {
    fn write_blk(&mut self, buf: Bytes) -> Result<u32, std::io::Error> {
        assert!(buf.len() == PAGE_SZ);
        let blknum = self.blocks.len();
        self.blocks.push(buf);
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
impl Default for BlockBuf {
    fn default() -> Self {
        Self::new()
    }
}
