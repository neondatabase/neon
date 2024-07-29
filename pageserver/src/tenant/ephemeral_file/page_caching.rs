//! Wrapper around [`super::zero_padded_read_write::RW`] that uses the
//! [`crate::page_cache`] to serve reads that need to go to the underlying [`VirtualFile`].

use crate::context::RequestContext;
use crate::page_cache::{self, PAGE_SZ};
use crate::virtual_file::owned_buffers_io::io_buf_ext::FullSlice;
use crate::virtual_file::VirtualFile;

use std::io::{self, ErrorKind};
use std::ops::{Deref, Range};
use tokio_epoll_uring::BoundedBuf;
use tracing::*;

use super::zero_padded_read_write;

/// See module-level comment.
pub struct RW {
    page_cache_file_id: page_cache::FileId,
    rw: super::zero_padded_read_write::RW<PreWarmingWriter>,
    /// Gate guard is held on as long as we need to do operations in the path (delete on drop).
    _gate_guard: utils::sync::gate::GateGuard,
}

/// Result of [`RW::read_page`].
pub(crate) enum ReadResult<'a> {
    EphemeralFileMutableTail(PageBuf, &'a [u8; PAGE_SZ]),
    Owned(PageBuf),
}

impl ReadResult<'_> {
    pub(crate) fn contents(&self) -> &[u8; PAGE_SZ] {
        match self {
            ReadResult::EphemeralFileMutableTail(_, buf) => buf,
            ReadResult::Owned(buf) => buf.deref(),
        }
    }
    pub(crate) fn into_page_buf(self) -> PageBuf {
        match self {
            ReadResult::EphemeralFileMutableTail(buf, _) => buf,
            ReadResult::Owned(buf) => buf,
        }
    }
}

pub(crate) struct PageBuf(Box<[u8; PAGE_SZ]>);

impl From<Box<[u8; PAGE_SZ]>> for PageBuf {
    fn from(buf: Box<[u8; PAGE_SZ]>) -> Self {
        Self(buf)
    }
}

impl Deref for PageBuf {
    type Target = [u8; PAGE_SZ];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

// Safety: `PageBuf` is a fixed-size buffer that is zero-initialized.
unsafe impl tokio_epoll_uring::IoBuf for PageBuf {
    fn stable_ptr(&self) -> *const u8 {
        self.0.as_ptr()
    }

    fn bytes_init(&self) -> usize {
        self.0.len()
    }

    fn bytes_total(&self) -> usize {
        self.0.len()
    }
}

// Safety: the `&mut self` guarantees no aliasing. `set_init` is safe
// because the buffer is always fully initialized.
unsafe impl tokio_epoll_uring::IoBufMut for PageBuf {
    fn stable_mut_ptr(&mut self) -> *mut u8 {
        self.0.as_mut_ptr()
    }

    unsafe fn set_init(&mut self, pos: usize) {
        // this is a no-op because the buffer is always fully initialized
        assert!(pos <= self.0.len());
    }
}

impl RW {
    pub fn new(file: VirtualFile, _gate_guard: utils::sync::gate::GateGuard) -> Self {
        let page_cache_file_id = page_cache::next_file_id();
        Self {
            page_cache_file_id,
            rw: super::zero_padded_read_write::RW::new(PreWarmingWriter::new(file)),
            _gate_guard,
        }
    }

    pub fn page_cache_file_id(&self) -> page_cache::FileId {
        self.page_cache_file_id
    }

    pub(crate) async fn write_all_borrowed(
        &mut self,
        srcbuf: &[u8],
        ctx: &RequestContext,
    ) -> Result<(), io::Error> {
        // It doesn't make sense to proactively fill the page cache on the Pageserver write path
        // because Compute is unlikely to access recently written data.
        self.rw.write_all_borrowed(srcbuf, ctx).await.map(|_| ())
    }

    pub(crate) fn bytes_written(&self) -> u32 {
        self.rw.bytes_written()
    }

    /// Load all blocks that can be read via [`Self::read_page`] into a contiguous memory buffer.
    ///
    /// This includes the blocks that aren't yet flushed to disk by the internal buffered writer.
    /// The last block is zero-padded to [`PAGE_SZ`], so, the returned buffer is always a multiple of [`PAGE_SZ`].
    pub(super) async fn load_to_vec(&self, ctx: &RequestContext) -> Result<Vec<u8>, io::Error> {
        // round up to the next PAGE_SZ multiple, required by blob_io
        let size = {
            let s = usize::try_from(self.bytes_written()).unwrap();
            if s % PAGE_SZ == 0 {
                s
            } else {
                s.checked_add(PAGE_SZ - (s % PAGE_SZ)).unwrap()
            }
        };
        let vec = Vec::with_capacity(size);

        // read from disk what we've already flushed
        let writer = self.rw.as_writer();
        let flushed_range = writer.written_range();
        let mut vec = writer
            .file
            .read_exact_at(
                vec.slice(0..(flushed_range.end - flushed_range.start)),
                u64::try_from(flushed_range.start).unwrap(),
                ctx,
            )
            .await?
            .into_inner();

        // copy from in-memory buffer what we haven't flushed yet but would return when accessed via read_blk
        let buffered = self.rw.get_tail_zero_padded();
        vec.extend_from_slice(buffered);
        assert_eq!(vec.len(), size);
        assert_eq!(vec.len() % PAGE_SZ, 0);
        Ok(vec)
    }

    pub(crate) async fn read_page(
        &self,
        blknum: u32,
        buf: PageBuf,
        ctx: &RequestContext,
    ) -> Result<ReadResult, io::Error> {
        match self.rw.read_blk(blknum).await? {
            zero_padded_read_write::ReadResult::NeedsReadFromWriter { writer } => {
                let buf = writer
                    .file
                    .read_exact_at(buf.slice_full(), blknum as u64 * PAGE_SZ as u64, ctx)
                    .await
                    .map(|slice| slice.into_inner())?;
                Ok(ReadResult::Owned(buf))
            }
            zero_padded_read_write::ReadResult::ServedFromZeroPaddedMutableTail {
                buffer: tail_ref,
            } => Ok(ReadResult::EphemeralFileMutableTail(buf, tail_ref)),
        }
    }
}

impl Drop for RW {
    fn drop(&mut self) {
        // There might still be pages in the [`crate::page_cache`] for this file.
        // We leave them there, [`crate::page_cache::PageCache::find_victim`] will evict them when needed.

        // unlink the file
        // we are clear to do this, because we have entered a gate
        let res = std::fs::remove_file(&self.rw.as_writer().file.path);
        if let Err(e) = res {
            if e.kind() != std::io::ErrorKind::NotFound {
                // just never log the not found errors, we cannot do anything for them; on detach
                // the tenant directory is already gone.
                //
                // not found files might also be related to https://github.com/neondatabase/neon/issues/2442
                error!(
                    "could not remove ephemeral file '{}': {}",
                    self.rw.as_writer().file.path,
                    e
                );
            }
        }
    }
}

struct PreWarmingWriter {
    nwritten_blocks: u32,
    file: VirtualFile,
}

impl PreWarmingWriter {
    fn new(file: VirtualFile) -> Self {
        Self {
            nwritten_blocks: 0,
            file,
        }
    }

    /// Return the byte range within `file` that has been written though `write_all`.
    ///
    /// The returned range would be invalidated by another `write_all`. To prevent that, we capture `&_`.
    fn written_range(&self) -> (impl Deref<Target = Range<usize>> + '_) {
        let nwritten_blocks = usize::try_from(self.nwritten_blocks).unwrap();
        struct Wrapper(Range<usize>);
        impl Deref for Wrapper {
            type Target = Range<usize>;
            fn deref(&self) -> &Range<usize> {
                &self.0
            }
        }
        Wrapper(0..nwritten_blocks * PAGE_SZ)
    }
}

impl crate::virtual_file::owned_buffers_io::write::OwnedAsyncWriter for PreWarmingWriter {
    async fn write_all<Buf: tokio_epoll_uring::IoBuf + Send>(
        &mut self,
        buf: FullSlice<Buf>,
        ctx: &RequestContext,
    ) -> std::io::Result<(usize, FullSlice<Buf>)> {
        let buflen = buf.len();
        assert_eq!(
            buflen % PAGE_SZ,
            0,
            "{buflen} ; we know TAIL_SZ is a PAGE_SZ multiple, and write_buffered_borrowed is used"
        );

        // Do the IO.
        let buf = match self.file.write_all(buf, ctx).await {
            (buf, Ok(nwritten)) => {
                assert_eq!(nwritten, buflen);
                buf
            }
            (_, Err(e)) => {
                return Err(std::io::Error::new(
                    ErrorKind::Other,
                    // order error before path because path is long and error is short
                    format!(
                        "ephemeral_file: write_blob: write-back tail self.nwritten_blocks={}, buflen={}, {:#}: {}",
                        self.nwritten_blocks, buflen, e, self.file.path,
                    ),
                ));
            }
        };

        let nblocks = buflen / PAGE_SZ;
        let nblocks32 = u32::try_from(nblocks).unwrap();
        self.nwritten_blocks = self.nwritten_blocks.checked_add(nblocks32).unwrap();
        Ok((buflen, buf))
    }
}
