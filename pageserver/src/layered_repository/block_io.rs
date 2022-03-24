use crate::page_cache::PAGE_SZ;
use bytes::Bytes;

///FIXME
/// The implementation is generic in that it doesn't know about the
/// page cache or other page server infrastructure. That makes it
/// easier to write unit tests.  The search functions use
/// DiskBlockReader to access the underylying storage.  The real
/// implementation used by the image and delta layers is in
/// 'blocky_reader.rs', and it uses the page cache. The unit tests
/// simulate a disk with a simple in-memory vector of pages.
///
pub trait DiskBlockReader {
    type Lease: AsRef<[u8; PAGE_SZ]>;

    ///
    /// Read a block. Returns a "lease" object that can be used to
    /// access to the contents of the page. (For the page cache, the
    /// lease object represents a lock on the buffer.)
    ///
    fn read_blk(&self, blknum: u32) -> Result<Self::Lease, std::io::Error>;
}

///
/// Abstraction for writing a block, when building the tree.
///
pub trait DiskBlockWriter {
    ///
    /// Write 'buf' to a block on disk. 'buf' must be PAGE_SZ bytes long.
    /// Returns the block number that the block was written to. It can be
    /// used to access the page later, when reading the tree.
    ///
    fn write_blk(&mut self, buf: Bytes) -> Result<u32, std::io::Error>;
}

pub struct OffsetBlockReader<'a, R>
where
    R: DiskBlockReader,
{
    start_blk: u32,
    reader: &'a R,
}

impl<R> DiskBlockReader for OffsetBlockReader<'_, R>
where
    R: DiskBlockReader,
{
    type Lease = R::Lease;

    fn read_blk(&self, blknum: u32) -> Result<Self::Lease, std::io::Error> {
        self.reader.read_blk(self.start_blk + blknum)
    }
}

impl<'a, R> OffsetBlockReader<'a, R>
where
    R: DiskBlockReader,
{
    pub fn new(start_blk: u32, reader: &'a R) -> Self {
        OffsetBlockReader { start_blk, reader }
    }
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
