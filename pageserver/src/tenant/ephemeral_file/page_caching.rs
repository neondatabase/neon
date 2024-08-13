//! Wrapper around [`super::zero_padded_read_write::RW`] that uses the
//! [`crate::page_cache`] to serve reads that need to go to the underlying [`VirtualFile`].

use crate::context::RequestContext;
use crate::page_cache::{self, PAGE_SZ};
use crate::tenant::block_io::BlockLease;
use crate::virtual_file::VirtualFile;

use once_cell::sync::Lazy;
use std::io::{self, ErrorKind};
use std::ops::{Deref, Range};
use tokio_epoll_uring::{BoundedBuf, Slice};
use tracing::*;

use super::zero_padded_read_write;

/// See module-level comment.
pub struct RW {
    page_cache_file_id: page_cache::FileId,
    rw: super::zero_padded_read_write::RW<PreWarmingWriter>,
    /// Gate guard is held on as long as we need to do operations in the path (delete on drop).
    _gate_guard: utils::sync::gate::GateGuard,
}

/// When we flush a block to the underlying [`crate::virtual_file::VirtualFile`],
/// should we pre-warm the [`crate::page_cache`] with the contents?
#[derive(Clone, Copy)]
pub enum PrewarmOnWrite {
    Yes,
    No,
}

impl RW {
    pub fn new(
        file: VirtualFile,
        prewarm_on_write: PrewarmOnWrite,
        _gate_guard: utils::sync::gate::GateGuard,
    ) -> Self {
        let page_cache_file_id = page_cache::next_file_id();
        Self {
            page_cache_file_id,
            rw: super::zero_padded_read_write::RW::new(PreWarmingWriter::new(
                page_cache_file_id,
                file,
                prewarm_on_write,
            )),
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
    prewarm_on_write: PrewarmOnWrite,
    nwritten_blocks: u32,
    page_cache_file_id: page_cache::FileId,
    file: VirtualFile,
}

impl PreWarmingWriter {
    fn new(
        page_cache_file_id: page_cache::FileId,
        file: VirtualFile,
        prewarm_on_write: PrewarmOnWrite,
    ) -> Self {
        Self {
            prewarm_on_write,
            nwritten_blocks: 0,
            page_cache_file_id,
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
        buf: Slice<Buf>,
        ctx: &RequestContext,
    ) -> std::io::Result<(usize, Slice<Buf>)> {
        assert_eq!(buf.bytes_init(), buf.bytes_total());

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

        if matches!(self.prewarm_on_write, PrewarmOnWrite::Yes) {
            // Pre-warm page cache with the contents.
            // At least in isolated bulk ingest benchmarks (test_bulk_insert.py), the pre-warming
            // benefits the code that writes InMemoryLayer=>L0 layers.

            let cache = page_cache::get();
            static CTX: Lazy<RequestContext> = Lazy::new(|| {
                RequestContext::new(
                    crate::task_mgr::TaskKind::EphemeralFilePreWarmPageCache,
                    crate::context::DownloadBehavior::Error,
                )
            });
            for blknum_in_buffer in 0..nblocks {
                let blk_in_buffer =
                    &buf[blknum_in_buffer * PAGE_SZ..(blknum_in_buffer + 1) * PAGE_SZ];
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
        }

        self.nwritten_blocks = self.nwritten_blocks.checked_add(nblocks32).unwrap();
        Ok((buflen, buf))
    }
}
