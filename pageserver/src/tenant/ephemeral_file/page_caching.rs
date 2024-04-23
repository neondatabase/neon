//! Wrapper around [`super::zero_padded_read_write::RW`] that uses the
//! [`crate::page_cache`] to serve reads that need to go to the underlying [`VirtualFile`].

use crate::context::RequestContext;
use crate::page_cache::{self, PAGE_SZ};
use crate::tenant::block_io::BlockLease;
use crate::virtual_file::VirtualFile;

use std::io;
use tracing::*;

use super::zero_padded_read_write;

/// See module-level comment.
pub struct RW {
    page_cache_file_id: page_cache::FileId,
    rw: super::zero_padded_read_write::RW,
}

impl RW {
    pub fn new(file: VirtualFile) -> Self {
        Self {
            page_cache_file_id: page_cache::next_file_id(),
            rw: super::zero_padded_read_write::RW::new(file),
        }
    }

    pub fn page_cache_file_id(&self) -> page_cache::FileId {
        self.page_cache_file_id
    }

    pub(crate) async fn write_all_borrowed(&mut self, srcbuf: &[u8]) -> Result<usize, io::Error> {
        // It doesn't make sense to proactively fill the page cache on the Pageserver write path
        // because Compute is unlikely to access recently written data.
        self.rw.write_all_borrowed(srcbuf).await
    }

    pub(crate) fn bytes_written(&self) -> u64 {
        self.rw.bytes_written()
    }

    pub(crate) async fn read_blk(
        &self,
        blknum: u32,
        ctx: &RequestContext,
    ) -> Result<BlockLease, io::Error> {
        match self.rw.read_blk(blknum).await? {
            zero_padded_read_write::ReadResult::NeedsReadFromVirtualFile { virtual_file } => {
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
                                self.rw.as_inner_virtual_file().path,
                                e,
                            ),
                        )
                    })? {
                    page_cache::ReadBufResult::Found(guard) => {
                        return Ok(BlockLease::PageReadGuard(guard))
                    }
                    page_cache::ReadBufResult::NotFound(write_guard) => {
                        let write_guard = virtual_file
                            .read_exact_at_page(write_guard, blknum as u64 * PAGE_SZ as u64)
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
        let res = std::fs::remove_file(&self.rw.as_inner_virtual_file().path);
        if let Err(e) = res {
            if e.kind() != std::io::ErrorKind::NotFound {
                // just never log the not found errors, we cannot do anything for them; on detach
                // the tenant directory is already gone.
                //
                // not found files might also be related to https://github.com/neondatabase/neon/issues/2442
                error!(
                    "could not remove ephemeral file '{}': {}",
                    self.rw.as_inner_virtual_file().path,
                    e
                );
            }
        }
    }
}
