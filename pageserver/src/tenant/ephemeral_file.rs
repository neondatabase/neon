//! Implementation of append-only file data structure
//! used to keep in-memory layers spilled on disk.

use crate::assert_u64_eq_usize::{U64IsUsize, UsizeIsU64};
use crate::config::PageServerConf;
use crate::context::RequestContext;
use crate::page_cache;
use crate::tenant::storage_layer::inmemory_layer::vectored_dio_read::File;
use crate::virtual_file::owned_buffers_io::slice::SliceMutExt;
use crate::virtual_file::owned_buffers_io::util::size_tracking_writer;
use crate::virtual_file::owned_buffers_io::write::Buffer;
use crate::virtual_file::{self, owned_buffers_io, VirtualFile};
use bytes::BytesMut;
use camino::Utf8PathBuf;
use num_traits::Num;
use pageserver_api::shard::TenantShardId;
use tokio_epoll_uring::{BoundedBuf, Slice};
use tracing::error;

use std::io;
use std::sync::atomic::AtomicU64;
use utils::id::TimelineId;

pub struct EphemeralFile {
    _tenant_shard_id: TenantShardId,
    _timeline_id: TimelineId,
    page_cache_file_id: page_cache::FileId,
    bytes_written: u64,
    buffered_writer: owned_buffers_io::write::BufferedWriter<
        BytesMut,
        size_tracking_writer::Writer<VirtualFile>,
    >,
    /// Gate guard is held on as long as we need to do operations in the path (delete on drop)
    _gate_guard: utils::sync::gate::GateGuard,
}

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
        let path = self.buffered_writer.as_inner().as_inner().path();
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
    pub(crate) fn len(&self) -> u64 {
        self.bytes_written
    }

    pub(crate) fn page_cache_file_id(&self) -> page_cache::FileId {
        self.page_cache_file_id
    }

    pub(crate) async fn load_to_vec(&self, ctx: &RequestContext) -> Result<Vec<u8>, io::Error> {
        let size = self.len().into_usize();
        let vec = Vec::with_capacity(size);
        let (slice, nread) = self.read_exact_at_eof_ok(0, vec.slice_full(), ctx).await?;
        assert_eq!(nread, size);
        let vec = slice.into_inner();
        assert_eq!(vec.len(), nread);
        assert_eq!(vec.capacity(), size, "we shouldn't be reallocating");
        Ok(vec)
    }

    /// Returns the offset at which the first byte of the input was written, for use
    /// in constructing indices over the written value.
    ///
    /// Panics if the write is short because there's no way we can recover from that.
    /// TODO: make upstack handle this as an error.
    pub(crate) async fn write_raw(
        &mut self,
        srcbuf: &[u8],
        ctx: &RequestContext,
    ) -> std::io::Result<u64> {
        let pos = self.bytes_written;

        let new_bytes_written = pos.checked_add(srcbuf.len().into_u64()).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::Other,
                format!(
                    "write would grow EphemeralFile beyond u64::MAX: len={pos} writen={srcbuf_len}",
                    srcbuf_len = srcbuf.len(),
                ),
            )
        })?;

        // Write the payload
        let nwritten = self
            .buffered_writer
            .write_buffered_borrowed(srcbuf, ctx)
            .await?;
        assert_eq!(
            nwritten,
            srcbuf.len(),
            "buffered writer has no short writes"
        );

        self.bytes_written = new_bytes_written;

        Ok(pos)
    }
}

impl super::storage_layer::inmemory_layer::vectored_dio_read::File for EphemeralFile {
    async fn read_exact_at_eof_ok<'a, 'b, B: tokio_epoll_uring::IoBufMut + Send>(
        &'b self,
        start: u64,
        dst: tokio_epoll_uring::Slice<B>,
        ctx: &'a RequestContext,
    ) -> std::io::Result<(tokio_epoll_uring::Slice<B>, usize)> {
        let file_size_tracking_writer = self.buffered_writer.as_inner();
        let flushed_offset = file_size_tracking_writer.bytes_written();

        let buffer = self.buffered_writer.inspect_buffer();
        let buffered = &buffer[0..buffer.pending()];

        let dst_cap = dst.bytes_total().into_u64();
        let end = {
            // saturating_add is correct here because the max file size is u64::MAX, so,
            // if start + dst.len() > u64::MAX, then we know it will be a short read
            let mut end: u64 = start.saturating_add(dst_cap);
            if end > self.bytes_written {
                end = self.bytes_written;
            }
            end
        };

        // inclusive, exclusive
        #[derive(Debug)]
        struct Range<N>(N, N);
        impl<N: Num + Clone + Copy + PartialOrd + Ord> Range<N> {
            fn len(&self) -> N {
                if self.0 > self.1 {
                    N::zero()
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
                .read_exact_at(dst.slice(0..written_range.len().into_usize()), start, ctx)
                .await?;
            Slice::from_buf_bounds(Slice::into_inner(slice), bounds)
        } else {
            dst
        };

        let dst = if buffered_range.len() > 0 {
            let offset_in_buffer = buffered_range
                .0
                .checked_sub(flushed_offset)
                .unwrap()
                .into_usize();
            let to_copy =
                &buffered[offset_in_buffer..(offset_in_buffer + buffered_range.len().into_usize())];
            let bounds = dst.bounds();
            let mut view = dst.slice({
                let start = written_range.len().into_usize();
                let end = start
                    .checked_add(buffered_range.len().into_usize())
                    .unwrap();
                start..end
            });
            view.as_mut_rust_slice_full_zeroed()
                .copy_from_slice(to_copy);
            Slice::from_buf_bounds(Slice::into_inner(view), bounds)
        } else {
            dst
        };

        // TODO: in debug mode, randomize the remaining bytes in `dst` to catch bugs

        Ok((dst, (end - start).into_usize()))
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
    use rand::Rng;

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

    #[tokio::test]
    async fn test_ephemeral_file_basics() {
        let (conf, tenant_id, timeline_id, ctx) = harness("test_ephemeral_file_basics").unwrap();

        let gate = utils::sync::gate::Gate::default();

        let mut file =
            EphemeralFile::create(conf, tenant_id, timeline_id, gate.enter().unwrap(), &ctx)
                .await
                .unwrap();

        let cap = file.buffered_writer.inspect_buffer().capacity();

        let write_nbytes = cap + cap / 2;

        let content: Vec<u8> = rand::thread_rng()
            .sample_iter(rand::distributions::Standard)
            .take(write_nbytes)
            .collect();

        let mut value_offsets = Vec::new();
        for i in 0..write_nbytes {
            let off = file.write_raw(&content[i..i + 1], &ctx).await.unwrap();
            value_offsets.push(off);
        }

        assert!(file.len() as usize == write_nbytes);
        for i in 0..write_nbytes {
            assert_eq!(value_offsets[i], i.into_u64());
            let buf = Vec::with_capacity(1);
            let (buf_slice, nread) = file
                .read_exact_at_eof_ok(i.into_u64(), buf.slice_full(), &ctx)
                .await
                .unwrap();
            let buf = buf_slice.into_inner();
            assert_eq!(nread, 1);
            assert_eq!(&buf, &content[i..i + 1]);
        }

        let file_contents =
            std::fs::read(file.buffered_writer.as_inner().as_inner().path()).unwrap();
        assert_eq!(file_contents, &content[0..cap]);

        let buffer_contents = file.buffered_writer.inspect_buffer();
        assert_eq!(buffer_contents, &content[cap..write_nbytes]);
    }

    #[tokio::test]
    async fn test_flushes_do_happen() {
        let (conf, tenant_id, timeline_id, ctx) = harness("test_flushes_do_happen").unwrap();

        let gate = utils::sync::gate::Gate::default();

        let mut file =
            EphemeralFile::create(conf, tenant_id, timeline_id, gate.enter().unwrap(), &ctx)
                .await
                .unwrap();

        let cap = file.buffered_writer.inspect_buffer().capacity();

        let content: Vec<u8> = rand::thread_rng()
            .sample_iter(rand::distributions::Standard)
            .take(cap + cap / 2)
            .collect();

        file.write_raw(&content, &ctx).await.unwrap();

        // assert the state is as this test expects it to be
        assert_eq!(
            &file.load_to_vec(&ctx).await.unwrap(),
            &content[0..cap + cap / 2]
        );
        let md = file
            .buffered_writer
            .as_inner()
            .as_inner()
            .path()
            .metadata()
            .unwrap();
        assert_eq!(
            md.len(),
            cap.into_u64(),
            "buffered writer does one write if we write 1.5x buffer capacity"
        );
        assert_eq!(
            &file.buffered_writer.inspect_buffer()[0..cap / 2],
            &content[cap..cap + cap / 2]
        );
    }

    #[tokio::test]
    async fn test_read_split_across_file_and_buffer() {
        // This test exercises the logic on the read path that splits the logical read
        // into a read from the flushed part (= the file) and a copy from the buffered writer's buffer.
        //
        // This test build on the assertions in test_flushes_do_happen

        let (conf, tenant_id, timeline_id, ctx) =
            harness("test_read_split_across_file_and_buffer").unwrap();

        let gate = utils::sync::gate::Gate::default();

        let mut file =
            EphemeralFile::create(conf, tenant_id, timeline_id, gate.enter().unwrap(), &ctx)
                .await
                .unwrap();

        let cap = file.buffered_writer.inspect_buffer().capacity();

        let content: Vec<u8> = rand::thread_rng()
            .sample_iter(rand::distributions::Standard)
            .take(cap + cap / 2)
            .collect();

        file.write_raw(&content, &ctx).await.unwrap();

        let test_read = |start: usize, len: usize| {
            let file = &file;
            let ctx = &ctx;
            let content = &content;
            async move {
                let (buf, nread) = file
                    .read_exact_at_eof_ok(
                        start.into_u64(),
                        Vec::with_capacity(len).slice_full(),
                        ctx,
                    )
                    .await
                    .unwrap();
                assert_eq!(nread, len);
                assert_eq!(&buf.into_inner(), &content[start..(start + len)]);
            }
        };

        // completely within the file range
        assert!(20 < cap, "test assumption");
        test_read(10, 10).await;
        // border onto edge of file
        test_read(cap - 10, 10).await;
        // read across file and buffer
        test_read(cap - 10, 20).await;
        // stay from start of buffer
        test_read(cap, 10).await;
        // completely within buffer
        test_read(cap + 10, 10).await;
    }
}
