//! Wrapper around [`super::zero_padded_read_write::RW`] that uses the
//! [`crate::page_cache`] to serve reads that need to go to the underlying [`VirtualFile`].
//!
//! Subject to removal in <https://github.com/neondatabase/neon/pull/8537>

use crate::context::RequestContext;
use crate::page_cache::{self, PAGE_SZ};
use crate::tenant::block_io::BlockLease;
use crate::virtual_file::owned_buffers_io::util::size_tracking_writer;
use crate::virtual_file::VirtualFile;

use std::io::{self};
use tokio_epoll_uring::BoundedBuf;
use tracing::*;

use super::zero_padded_read_write;

/// See module-level comment.
pub struct RW {
    page_cache_file_id: page_cache::FileId,
    rw: super::zero_padded_read_write::RW<size_tracking_writer::Writer<VirtualFile>>,
    /// Gate guard is held on as long as we need to do operations in the path (delete on drop).
    _gate_guard: utils::sync::gate::GateGuard,
}

impl RW {
    pub fn new(file: VirtualFile, _gate_guard: utils::sync::gate::GateGuard) -> Self {
        let page_cache_file_id = page_cache::next_file_id();
        Self {
            page_cache_file_id,
            rw: super::zero_padded_read_write::RW::new(size_tracking_writer::Writer::new(file)),
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
    ) -> Result<usize, io::Error> {
        // It doesn't make sense to proactively fill the page cache on the Pageserver write path
        // because Compute is unlikely to access recently written data.
        self.rw.write_all_borrowed(srcbuf, ctx).await
    }

    pub(crate) fn bytes_written(&self) -> u64 {
        self.rw.bytes_written()
    }

    /// Load all blocks that can be read via [`Self::read_blk`] into a contiguous memory buffer.
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
        let file_size_tracking_writer = self.rw.as_writer();
        let flushed_range = 0..usize::try_from(file_size_tracking_writer.bytes_written()).unwrap();
        let mut vec = file_size_tracking_writer
            .as_inner()
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

    pub(crate) async fn read_blk(
        &self,
        blknum: u32,
        ctx: &RequestContext,
    ) -> Result<BlockLease, io::Error> {
        match self.rw.read_blk(blknum).await? {
            zero_padded_read_write::ReadResult::NeedsReadFromWriter { writer } => {
                let cache = page_cache::get();
                match cache
                    .read_immutable_buf(self.page_cache_file_id, blknum, ctx)
                    .await
                    .map_err(|e| {
                        std::io::Error::new(
                            std::io::ErrorKind::Other,
                            // order path before error because error is anyhow::Error => might have many contexts
                            format!(
                                "ephemeral file: read immutable page #{}: {}: {:#}",
                                blknum,
                                self.rw.as_writer().as_inner().path,
                                e,
                            ),
                        )
                    })? {
                    page_cache::ReadBufResult::Found(guard) => {
                        return Ok(BlockLease::PageReadGuard(guard))
                    }
                    page_cache::ReadBufResult::NotFound(write_guard) => {
                        let write_guard = writer
                            .as_inner()
                            .read_exact_at_page(write_guard, blknum as u64 * PAGE_SZ as u64, ctx)
                            .await?;
                        let read_guard = write_guard.mark_valid();
                        return Ok(BlockLease::PageReadGuard(read_guard));
                    }
                }
            }
            zero_padded_read_write::ReadResult::ServedFromZeroPaddedMutableTail { buffer } => {
                Ok(BlockLease::EphemeralFileMutableTail(buffer))
            }
        }
    }
}

impl Drop for RW {
    fn drop(&mut self) {
        // There might still be pages in the [`crate::page_cache`] for this file.
        // We leave them there, [`crate::page_cache::PageCache::find_victim`] will evict them when needed.

        // unlink the file
        // we are clear to do this, because we have entered a gate
        let path = &self.rw.as_writer().as_inner().path;
        let res = std::fs::remove_file(path);
        if let Err(e) = res {
            if e.kind() != std::io::ErrorKind::NotFound {
                // just never log the not found errors, we cannot do anything for them; on detach
                // the tenant directory is already gone.
                //
                // not found files might also be related to https://github.com/neondatabase/neon/issues/2442
                error!("could not remove ephemeral file '{path}': {e}");
            }
        }
    }
}
