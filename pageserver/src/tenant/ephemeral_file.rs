//! Implementation of append-only file data structure
//! used to keep in-memory layers spilled on disk.

use crate::config::PageServerConf;
use crate::context::RequestContext;
use crate::page_cache::{self, PAGE_SZ};
use crate::tenant::block_io::{BlockCursor, BlockLease, BlockReader};
use crate::virtual_file::owned_buffers_io::write::OwnedAsyncWriter;
use crate::virtual_file::{self, owned_buffers_io, VirtualFile};
use camino::Utf8PathBuf;
use pageserver_api::shard::TenantShardId;

use std::io;
use std::sync::atomic::AtomicU64;
use tracing::*;
use utils::id::TimelineId;

pub struct EphemeralFile {
    page_cache_file_id: page_cache::FileId,

    _tenant_shard_id: TenantShardId,
    _timeline_id: TimelineId,
    /// We sandwich the buffered writer between two size-tracking writers.
    /// This allows us to "elegantly" track in-memory bytes vs flushed bytes,
    /// enabling [`Self::read_blk`] to determine whether to read from the
    /// buffered writer's buffer, versus going to the VirtualFile.
    ///
    /// TODO: longer-term, we probably wand to get rid of this in favor
    /// of a double-buffering scheme. See this commit's commit message
    /// and git history for what we had before this sandwich, it might be useful.
    file: owned_buffers_io::util::size_tracking_writer::Writer<
        owned_buffers_io::write::BufferedWriter<
            { Self::TAIL_SZ },
            owned_buffers_io::util::size_tracking_writer::Writer<VirtualFile>,
        >,
    >,
}

impl EphemeralFile {
    const TAIL_SZ: usize = 64 * 1024;

    pub async fn create(
        conf: &PageServerConf,
        tenant_shard_id: TenantShardId,
        timeline_id: TimelineId,
    ) -> Result<EphemeralFile, io::Error> {
        static NEXT_FILENAME: AtomicU64 = AtomicU64::new(1);
        let filename_disambiguator =
            NEXT_FILENAME.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let filename = conf
            .timeline_path(&tenant_shard_id, &timeline_id)
            .join(Utf8PathBuf::from(format!(
                "ephemeral-{filename_disambiguator}"
            )));

        let file = VirtualFile::open_with_options(
            &filename,
            virtual_file::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true),
        )
        .await?;
        let file = owned_buffers_io::util::size_tracking_writer::Writer::new(file);
        let file = owned_buffers_io::write::BufferedWriter::new(file);
        let file = owned_buffers_io::util::size_tracking_writer::Writer::new(file);

        Ok(EphemeralFile {
            page_cache_file_id: page_cache::next_file_id(),
            _tenant_shard_id: tenant_shard_id,
            _timeline_id: timeline_id,
            file,
        })
    }

    pub(crate) fn len(&self) -> u64 {
        self.file.bytes_written()
    }

    pub(crate) fn id(&self) -> page_cache::FileId {
        self.page_cache_file_id
    }

    pub(crate) async fn read_blk(
        &self,
        blknum: u32,
        ctx: &RequestContext,
    ) -> Result<BlockLease, io::Error> {
        let buffered_offset = self.file.bytes_written();
        let flushed_offset = self.file.as_inner().as_inner().bytes_written();
        assert!(buffered_offset >= flushed_offset);
        let read_offset = (blknum as u64) * (PAGE_SZ as u64);

        assert_eq!(
            flushed_offset % (PAGE_SZ as u64),
            0,
            "we need this in the logic below, because it assumes the page isn't spread across flushed part and in-memory buffer"
        );

        if read_offset < flushed_offset {
            assert!(
                read_offset + (PAGE_SZ as u64) <= flushed_offset,
                "this impl can't deal with pages spread across flushed & buffered part"
            );
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
                            self.file.as_inner().as_inner().as_inner().path,
                            e,
                        ),
                    )
                })? {
                page_cache::ReadBufResult::Found(guard) => {
                    return Ok(BlockLease::PageReadGuard(guard))
                }
                page_cache::ReadBufResult::NotFound(write_guard) => {
                    let write_guard = self
                        .file
                        .as_inner()
                        .as_inner()
                        .as_inner()
                        .read_exact_at_page(write_guard, blknum as u64 * PAGE_SZ as u64)
                        .await?;
                    let read_guard = write_guard.mark_valid();
                    return Ok(BlockLease::PageReadGuard(read_guard));
                }
            };
        } else {
            let read_until_offset = read_offset + (PAGE_SZ as u64);
            if !(0..buffered_offset).contains(&read_until_offset) {
                // The blob_io code relies on the reader allowing reads past
                // the end of what was written, up to end of the current PAGE_SZ chunk.
                // This is a relict of the past where we would get a pre-zeroed page from the page cache.
                //
                // DeltaLayer probably has the same issue, not sure why it needs no special treatment.
                let nbytes_past_end = read_until_offset.checked_sub(buffered_offset).unwrap();
                if nbytes_past_end >= (PAGE_SZ as u64) {
                    // TODO: treat this as error. Pre-existing issue before this patch.
                    panic!(
                        "return IO error: read past end of file: read=0x{read_offset:x} buffered=0x{buffered_offset:x} flushed=0x{flushed_offset}"
                    )
                }
            }
            let buffer: &[u8; Self::TAIL_SZ] = self.file.as_inner().inspect_buffer();
            let read_offset_in_buffer = read_offset
                .checked_sub(flushed_offset)
                .expect("would have taken `if` branch instead of this one");

            let read_offset_in_buffer = usize::try_from(read_offset_in_buffer).unwrap();
            let page = &buffer[read_offset_in_buffer..(read_offset_in_buffer + PAGE_SZ)];
            Ok(BlockLease::EphemeralFileMutableTail(
                page.try_into()
                    .expect("the slice above got it as page-size slice"),
            ))
        }
    }

    pub(crate) async fn write_blob(
        &mut self,
        srcbuf: &[u8],
        _ctx: &RequestContext,
    ) -> Result<u64, io::Error> {
        let pos = self.file.bytes_written();

        // Write the length field
        if srcbuf.len() < 0x80 {
            // short one-byte length header
            let len_buf = [srcbuf.len() as u8];

            self.file.write_all_borrowed(&len_buf).await?;
        } else {
            let mut len_buf = u32::to_be_bytes(srcbuf.len() as u32);
            len_buf[0] |= 0x80;
            self.file.write_all_borrowed(&len_buf).await?;
        }

        // Write the payload
        self.file.write_all_borrowed(srcbuf).await?;

        // TODO: bring back pre-warming of page cache, using another sandwich layer

        Ok(pos)
    }
}

/// Does the given filename look like an ephemeral file?
pub fn is_ephemeral_file(filename: &str) -> bool {
    if let Some(rest) = filename.strip_prefix("ephemeral-") {
        rest.parse::<u32>().is_ok()
    } else {
        false
    }
}

impl Drop for EphemeralFile {
    fn drop(&mut self) {
        // There might still be pages in the [`crate::page_cache`] for this file.
        // We leave them there, [`crate::page_cache::PageCache::find_victim`] will evict them when needed.

        // unlink the file
        let res = std::fs::remove_file(&self.file.as_inner().as_inner().as_inner().path);
        if let Err(e) = res {
            if e.kind() != std::io::ErrorKind::NotFound {
                // just never log the not found errors, we cannot do anything for them; on detach
                // the tenant directory is already gone.
                //
                // not found files might also be related to https://github.com/neondatabase/neon/issues/2442
                error!(
                    "could not remove ephemeral file '{}': {}",
                    self.file.as_inner().as_inner().as_inner().path,
                    e
                );
            }
        }
    }
}

impl BlockReader for EphemeralFile {
    fn block_cursor(&self) -> super::block_io::BlockCursor<'_> {
        BlockCursor::new(super::block_io::BlockReaderRef::EphemeralFile(self))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::DownloadBehavior;
    use crate::task_mgr::TaskKind;
    use crate::tenant::block_io::BlockReaderRef;
    use rand::{thread_rng, RngCore};
    use std::fs;
    use std::str::FromStr;

    fn harness(
        test_name: &str,
    ) -> Result<
        (
            &'static PageServerConf,
            TenantShardId,
            TimelineId,
            RequestContext,
        ),
        io::Error,
    > {
        let repo_dir = PageServerConf::test_repo_dir(test_name);
        let _ = fs::remove_dir_all(&repo_dir);
        let conf = PageServerConf::dummy_conf(repo_dir);
        // Make a static copy of the config. This can never be free'd, but that's
        // OK in a test.
        let conf: &'static PageServerConf = Box::leak(Box::new(conf));

        let tenant_shard_id = TenantShardId::from_str("11000000000000000000000000000000").unwrap();
        let timeline_id = TimelineId::from_str("22000000000000000000000000000000").unwrap();
        fs::create_dir_all(conf.timeline_path(&tenant_shard_id, &timeline_id))?;

        let ctx = RequestContext::new(TaskKind::UnitTest, DownloadBehavior::Error);

        Ok((conf, tenant_shard_id, timeline_id, ctx))
    }

    #[tokio::test]
    async fn test_ephemeral_blobs() -> Result<(), io::Error> {
        let (conf, tenant_id, timeline_id, ctx) = harness("ephemeral_blobs")?;

        let mut file = EphemeralFile::create(conf, tenant_id, timeline_id).await?;

        let pos_foo = file.write_blob(b"foo", &ctx).await?;
        assert_eq!(
            b"foo",
            file.block_cursor()
                .read_blob(pos_foo, &ctx)
                .await?
                .as_slice()
        );
        let pos_bar = file.write_blob(b"bar", &ctx).await?;
        assert_eq!(
            b"foo",
            file.block_cursor()
                .read_blob(pos_foo, &ctx)
                .await?
                .as_slice()
        );
        assert_eq!(
            b"bar",
            file.block_cursor()
                .read_blob(pos_bar, &ctx)
                .await?
                .as_slice()
        );

        let mut blobs = Vec::new();
        for i in 0..10000 {
            let data = Vec::from(format!("blob{}", i).as_bytes());
            let pos = file.write_blob(&data, &ctx).await?;
            blobs.push((pos, data));
        }
        // also test with a large blobs
        for i in 0..100 {
            let data = format!("blob{}", i).as_bytes().repeat(100);
            let pos = file.write_blob(&data, &ctx).await?;
            blobs.push((pos, data));
        }

        let cursor = BlockCursor::new(BlockReaderRef::EphemeralFile(&file));
        for (pos, expected) in blobs {
            let actual = cursor.read_blob(pos, &ctx).await?;
            assert_eq!(actual, expected);
        }

        // Test a large blob that spans multiple pages
        let mut large_data = vec![0; 20000];
        thread_rng().fill_bytes(&mut large_data);
        let pos_large = file.write_blob(&large_data, &ctx).await?;
        let result = file.block_cursor().read_blob(pos_large, &ctx).await?;
        assert_eq!(result, large_data);

        Ok(())
    }
}
