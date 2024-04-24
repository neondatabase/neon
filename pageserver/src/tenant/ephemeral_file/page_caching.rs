//! Wrapper around [`super::zero_padded_read_write::RW`] that uses the
//! [`crate::page_cache`] to serve reads that need to go to the underlying [`VirtualFile`].

use crate::context::RequestContext;
use crate::page_cache::{self, PAGE_SZ};
use crate::tenant::block_io::BlockLease;
use crate::virtual_file::VirtualFile;

use std::io;
use tokio_epoll_uring::BoundedBuf;
use tracing::*;

use super::zero_padded_read_write;

/// See module-level comment.
pub struct RW {
    page_cache_file_id: page_cache::FileId,
    rw: super::zero_padded_read_write::RW<PageCachingWriter>,
}

impl RW {
    pub fn new(file: VirtualFile) -> Self {
        let page_cache_file_id = page_cache::next_file_id();
        Self {
            page_cache_file_id,
            rw: super::zero_padded_read_write::RW::new(PageCachingWriter::new(
                page_cache_file_id,
                file,
            )),
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
                                self.rw.as_writer().file.path,
                                e,
                            ),
                        )
                    })? {
                    page_cache::ReadBufResult::Found(guard) => {
                        return Ok(BlockLease::PageReadGuard(guard))
                    }
                    page_cache::ReadBufResult::NotFound(write_guard) => {
                        let write_guard = writer
                            .file
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

struct PageCachingWriter {
    nwritten_blocks: u32,
    page_cache_file_id: page_cache::FileId,
    file: VirtualFile,
}

impl PageCachingWriter {
    fn new(page_cache_file_id: page_cache::FileId, file: VirtualFile) -> Self {
        Self {
            nwritten_blocks: 0,
            page_cache_file_id,
            file,
        }
    }
}

impl crate::virtual_file::owned_buffers_io::write::OwnedAsyncWriter for PageCachingWriter {
    async fn write_all<
        B: tokio_epoll_uring::BoundedBuf<Buf = Buf>,
        Buf: tokio_epoll_uring::IoBuf + Send,
    >(
        &mut self,
        buf: B,
    ) -> std::io::Result<(usize, B::Buf)> {
        let buf = buf.slice(..); // TODO: review this is correct
        let check_bounds_stuff_works = if cfg!(debug_assertions) {
            let to_write: &[u8] = &*buf;
            let mut pre = Vec::with_capacity(to_write.len());
            pre.extend_from_slice(&to_write);
            Some(pre)
        } else {
            None
        };
        let saved_bounds = buf.bounds();
        let (n, iobuf) = crate::virtual_file::owned_buffers_io::write::OwnedAsyncWriter::write_all(
            &mut self.file,
            buf,
        )
        .await?;
        let buf = tokio_epoll_uring::Slice::from_buf_bounds(iobuf, saved_bounds);
        if cfg!(debug_assertions) {
            assert_eq!(check_bounds_stuff_works.as_deref().unwrap(), &*buf);
        }

        assert_eq!(n % PAGE_SZ, 0, "{n}");
        let nblocks = n / PAGE_SZ;
        let nblocks32 = u32::try_from(nblocks).unwrap();
        let cache = page_cache::get();
        let ctx = RequestContext::new(
            crate::task_mgr::TaskKind::Todo,
            crate::context::DownloadBehavior::Error,
        );
        for blknum_in_buffer in 0..nblocks {
            let blk_in_buffer = &buf[blknum_in_buffer * PAGE_SZ..(blknum_in_buffer + 1) * PAGE_SZ];
            match cache
                .read_immutable_buf(
                    self.page_cache_file_id,
                    self.nwritten_blocks
                        .checked_add(blknum_in_buffer as u32)
                        .unwrap(),
                    &ctx,
                )
                .await
            {
                Err(e) => {
                    warn!("ephemeral_file: error evicting page for warm-up: {e:#}");
                }
                Ok(v) => match v {
                    page_cache::ReadBufResult::Found(guard) => {
                        debug_assert_eq!(blk_in_buffer, &*guard);
                    }
                    page_cache::ReadBufResult::NotFound(mut write_guard) => {
                        write_guard.copy_from_slice(blk_in_buffer);
                        let _ = write_guard.mark_valid();
                    }
                },
            }
        }
        self.nwritten_blocks = self.nwritten_blocks.checked_add(nblocks32).unwrap();
        Ok((n, buf.into_inner()))
    }
}
