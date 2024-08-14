//! Implementation of append-only file data structure
//! used to keep in-memory layers spilled on disk.

use crate::config::PageServerConf;
use crate::context::RequestContext;
use crate::page_cache;
use crate::virtual_file::owned_buffers_io::slice::SliceMutExt;
use crate::virtual_file::owned_buffers_io::util::size_tracking_writer;
use crate::virtual_file::owned_buffers_io::write::Buffer;
use crate::virtual_file::{self, owned_buffers_io, VirtualFile};
use anyhow::Context;
use bytes::BytesMut;
use camino::Utf8PathBuf;
use pageserver_api::shard::TenantShardId;
use tokio_epoll_uring::{BoundedBuf, IoBufMut, Slice};
use tracing::error;

use std::io;
use std::sync::atomic::AtomicU64;
use utils::id::TimelineId;

pub struct EphemeralFile {
    _tenant_shard_id: TenantShardId,
    _timeline_id: TimelineId,
    page_cache_file_id: page_cache::FileId,
    bytes_written: u32,
    buffered_writer: owned_buffers_io::write::BufferedWriter<
        BytesMut,
        size_tracking_writer::Writer<VirtualFile>,
    >,
    /// Gate guard is held on as long as we need to do operations in the path (delete on drop)
    _gate_guard: utils::sync::gate::GateGuard,
}

use super::storage_layer::inmemory_layer::InMemoryLayerIndexValue;

const TAIL_SZ: usize = 64 * 1024;

impl EphemeralFile {
    pub async fn create(
        conf: &PageServerConf,
        tenant_shard_id: TenantShardId,
        timeline_id: TimelineId,
        gate_guard: utils::sync::gate::GateGuard,
        ctx: &RequestContext,
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
            ctx,
        )
        .await?;

        let page_cache_file_id = page_cache::next_file_id(); // XXX get rid, we're not page-caching anymore

        Ok(EphemeralFile {
            _tenant_shard_id: tenant_shard_id,
            _timeline_id: timeline_id,
            page_cache_file_id,
            bytes_written: 0,
            buffered_writer: owned_buffers_io::write::BufferedWriter::new(
                size_tracking_writer::Writer::new(file),
                BytesMut::with_capacity(TAIL_SZ),
            ),
            _gate_guard: gate_guard,
        })
    }
}

impl Drop for EphemeralFile {
    fn drop(&mut self) {
        // unlink the file
        // we are clear to do this, because we have entered a gate
        let path = &self.buffered_writer.as_inner().as_inner().path;
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

impl EphemeralFile {
    pub(crate) fn len(&self) -> u32 {
        self.bytes_written
    }

    pub(crate) fn page_cache_file_id(&self) -> page_cache::FileId {
        self.page_cache_file_id
    }

    pub(crate) async fn load_to_vec(&self, ctx: &RequestContext) -> Result<Vec<u8>, io::Error> {
        let size = usize::try_from(self.len()).unwrap();
        let vec = Vec::with_capacity(size);

        // read from disk what we've already flushed
        let file_size_tracker = self.buffered_writer.as_inner();
        let flushed_offset = usize::try_from(file_size_tracker.bytes_written()).unwrap();
        let flushed_range = 0..flushed_offset;
        let file: &VirtualFile = file_size_tracker.as_inner();
        let mut vec = file
            .read_exact_at(
                vec.slice(0..(flushed_range.end - flushed_range.start)),
                u64::try_from(flushed_range.start).unwrap(),
                ctx,
            )
            .await?
            .into_inner();

        // copy from in-memory buffer what we haven't flushed yet but would return when accessed via read_blk
        let buffer = self.buffered_writer.inspect_buffer();
        let buffered = &buffer[0..buffer.pending()];
        vec.extend_from_slice(buffered);
        assert_eq!(vec.len(), size);
        Ok(vec)
    }

    /// Fill dst will dst.bytes_total() bytes from the bytes written to the buffered writer from offset `start` and later.
    /// If `dst` is larger than the available bytes, the read will be short.
    /// The read will never be short for other reasons.
    /// The number of bytes read into `dst` is returned as part of the result tuple.
    /// No guarantees are made about the remaining bytes in `dst`, i.e., assume their contents are random.
    pub(crate) async fn read_at_to_end<B: IoBufMut + Send>(
        &self,
        start: u32,
        dst: Slice<B>,
        ctx: &RequestContext,
    ) -> std::io::Result<(Slice<B>, usize)> {
        let file_size_tracking_writer = self.buffered_writer.as_inner();
        let flushed_offset = u32::try_from(file_size_tracking_writer.bytes_written())
            .expect("we don't allow writing more than u32::MAX bytes");

        let buffer = self.buffered_writer.inspect_buffer();
        let buffered = &buffer[0..buffer.pending()];

        let dst_cap = u32::try_from(dst.bytes_total())
            .with_context(|| {
                format!(
                    "read_aligned: dst.bytes_total() is too large: {}",
                    dst.len()
                )
            })
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        let end = {
            let mut end = start
                .checked_add(dst_cap)
                .with_context(|| {
                    format!("read_aligned: offset + dst.bytes_total() is too large: {start} + {dst_cap}",)
                })
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
            if end > self.bytes_written {
                end = self.bytes_written;
            }
            end
        };

        // inclusive, exclusive
        #[derive(Debug)]
        struct Range(u32, u32);
        impl Range {
            fn len(&self) -> u32 {
                if self.0 > self.1 {
                    0
                } else {
                    self.1 - self.0
                }
            }
        }
        let written_range = Range(start, std::cmp::min(end, flushed_offset));
        let buffered_range = Range(std::cmp::max(start, flushed_offset), end);

        let dst = if written_range.len() > 0 {
            let file: &VirtualFile = file_size_tracking_writer.as_inner();
            let bounds = dst.bounds();
            let slice = file
                .read_exact_at(
                    dst.slice(0..written_range.len() as usize),
                    start as u64,
                    ctx,
                )
                .await?;
            Slice::from_buf_bounds(Slice::into_inner(slice), bounds)
        } else {
            dst
        };

        let dst = if buffered_range.len() > 0 {
            let offset_in_buffer =
                usize::try_from(buffered_range.0.checked_sub(flushed_offset).unwrap()).unwrap();
            let to_copy =
                &buffered[offset_in_buffer..(offset_in_buffer + buffered_range.len() as usize)];
            let bounds = dst.bounds();
            let mut view = dst.slice(
                written_range.len() as usize
                    ..written_range.len() as usize + buffered_range.len() as usize,
            );
            view.as_mut_rust_slice_full_zeroed()
                .copy_from_slice(to_copy);
            Slice::from_buf_bounds(Slice::into_inner(view), bounds)
        } else {
            dst
        };

        // TODO: in debug mode, randomize the remaining bytes in `dst` to catch bugs

        Ok((dst, (end - start) as usize))
    }

    pub(crate) async fn write_blob(
        &mut self,
        buf: &[u8],
        will_init: bool,
        ctx: &RequestContext,
    ) -> Result<InMemoryLayerIndexValue, io::Error> {
        let pos = self.bytes_written;
        let len = u32::try_from(buf.len()).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::Other,
                anyhow::anyhow!(
                    "EphemeralFile::write_blob value too large: {}: {e}",
                    buf.len()
                ),
            )
        })?;
        pos.checked_add(len).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "EphemeralFile::write_blob: overflow",
            )
        })?;

        self.buffered_writer
            .write_buffered_borrowed(buf, ctx)
            .await?;
        self.bytes_written += len;

        Ok(InMemoryLayerIndexValue {
            pos,
            len,
            will_init,
        })
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::DownloadBehavior;
    use crate::task_mgr::TaskKind;
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
    async fn ephemeral_file_holds_gate_open() {
        const FOREVER: std::time::Duration = std::time::Duration::from_secs(5);

        let (conf, tenant_id, timeline_id, ctx) =
            harness("ephemeral_file_holds_gate_open").unwrap();

        let gate = utils::sync::gate::Gate::default();

        let file = EphemeralFile::create(conf, tenant_id, timeline_id, gate.enter().unwrap(), &ctx)
            .await
            .unwrap();

        let mut closing = tokio::task::spawn(async move {
            gate.close().await;
        });

        // gate is entered until the ephemeral file is dropped
        // do not start paused tokio-epoll-uring has a sleep loop
        tokio::time::pause();
        tokio::time::timeout(FOREVER, &mut closing)
            .await
            .expect_err("closing cannot complete before dropping");

        // this is a requirement of the reset_tenant functionality: we have to be able to restart a
        // tenant fast, and for that, we need all tenant_dir operations be guarded by entering a gate
        drop(file);

        tokio::time::timeout(FOREVER, &mut closing)
            .await
            .expect("closing completes right away")
            .expect("closing does not panic");
    }
}
