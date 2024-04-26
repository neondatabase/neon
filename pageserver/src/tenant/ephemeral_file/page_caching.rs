//! Wrapper around [`super::zero_padded_read_write::RW`] that uses the
//! [`crate::page_cache`] to serve reads that need to go to the underlying [`VirtualFile`].

use crate::context::RequestContext;
use crate::page_cache::{self, PAGE_SZ};
use crate::tenant::block_io::BlockLease;
use crate::virtual_file::VirtualFile;

use once_cell::sync::Lazy;
use std::io::{self, ErrorKind};
use tokio_epoll_uring::BoundedBuf;
use tracing::*;

use super::zero_padded_read_write;

/// See module-level comment.
pub struct RW {
    page_cache_file_id: page_cache::FileId,
    rw: super::zero_padded_read_write::RW<PreWarmingWriter>,
}

impl RW {
    pub fn new(file: VirtualFile) -> Self {
        let page_cache_file_id = page_cache::next_file_id();
        Self {
            page_cache_file_id,
            rw: super::zero_padded_read_write::RW::new(PreWarmingWriter::new(
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

struct PreWarmingWriter {
    nwritten_blocks: u32,
    page_cache_file_id: page_cache::FileId,
    file: VirtualFile,
}

impl PreWarmingWriter {
    fn new(page_cache_file_id: page_cache::FileId, file: VirtualFile) -> Self {
        Self {
            nwritten_blocks: 0,
            page_cache_file_id,
            file,
        }
    }
}

impl crate::virtual_file::owned_buffers_io::write::OwnedAsyncWriter for PreWarmingWriter {
    async fn write_all<
        B: tokio_epoll_uring::BoundedBuf<Buf = Buf>,
        Buf: tokio_epoll_uring::IoBuf + Send,
    >(
        &mut self,
        buf: B,
    ) -> std::io::Result<(usize, B::Buf)> {
        let buf = buf.slice(..);
        let saved_bounds = buf.bounds(); // save for reconstructing the Slice from iobuf after the IO is done
        let check_bounds_stuff_works = if cfg!(test) && cfg!(debug_assertions) {
            Some(buf.to_vec())
        } else {
            None
        };
        let buflen = buf.len();
        assert_eq!(
            buflen % PAGE_SZ,
            0,
            "{buflen} ; we know TAIL_SZ is a PAGE_SZ multiple, and write_buffered_borrowed is used"
        );

        // Do the IO.
        let iobuf = match self.file.write_all(buf).await {
            (iobuf, Ok(nwritten)) => {
                assert_eq!(nwritten, buflen);
                iobuf
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

        // Reconstruct the Slice (the write path consumed the Slice and returned us the underlying IoBuf)
        let buf = tokio_epoll_uring::Slice::from_buf_bounds(iobuf, saved_bounds);
        if let Some(check_bounds_stuff_works) = check_bounds_stuff_works {
            assert_eq!(&check_bounds_stuff_works, &*buf);
        }

        // Pre-warm page cache with the contents.
        // At least in isolated bulk ingest benchmarks (test_bulk_insert.py), the pre-warming
        // benefits the code that writes InMemoryLayer=>L0 layers.
        let nblocks = buflen / PAGE_SZ;
        let nblocks32 = u32::try_from(nblocks).unwrap();
        let cache = page_cache::get();
        static CTX: Lazy<RequestContext> = Lazy::new(|| {
            RequestContext::new(
                crate::task_mgr::TaskKind::EphemeralFilePreWarmPageCache,
                crate::context::DownloadBehavior::Error,
            )
        });
        for blknum_in_buffer in 0..nblocks {
            let blk_in_buffer = &buf[blknum_in_buffer * PAGE_SZ..(blknum_in_buffer + 1) * PAGE_SZ];
            let blknum = self
                .nwritten_blocks
                .checked_add(blknum_in_buffer as u32)
                .unwrap();
            match cache
                .read_immutable_buf(self.page_cache_file_id, blknum, &CTX)
                .await
            {
                Err(e) => {
                    error!("ephemeral_file write_blob failed to get immutable buf to pre-warm page cache: {e:?}");
                    // fail gracefully, it's not the end of the world if we can't pre-warm the cache here
                }
                Ok(v) => match v {
                    page_cache::ReadBufResult::Found(_guard) => {
                        // This function takes &mut self, so, it shouldn't be possible to reach this point.
                        unreachable!("we just wrote block {blknum} to the VirtualFile, which is owned by Self, \
                                      and this function takes &mut self, so, no concurrent read_blk is possible");
                    }
                    page_cache::ReadBufResult::NotFound(mut write_guard) => {
                        write_guard.copy_from_slice(blk_in_buffer);
                        let _ = write_guard.mark_valid();
                    }
                },
            }
        }
        self.nwritten_blocks = self.nwritten_blocks.checked_add(nblocks32).unwrap();
        Ok((buflen, buf.into_inner()))
    }
}
