//! Implementation of append-only file data structure
//! used to keep in-memory layers spilled on disk.

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use camino::Utf8PathBuf;
use num_traits::Num;
use pageserver_api::shard::TenantShardId;
use tokio_epoll_uring::{BoundedBuf, Slice};
use tokio_util::sync::CancellationToken;
use tracing::{error, info_span};
use utils::id::TimelineId;
use utils::sync::gate::GateGuard;

use crate::assert_u64_eq_usize::{U64IsUsize, UsizeIsU64};
use crate::config::PageServerConf;
use crate::context::RequestContext;
use crate::page_cache;
use crate::tenant::storage_layer::inmemory_layer::GlobalResourceUnits;
use crate::tenant::storage_layer::inmemory_layer::vectored_dio_read::File;
use crate::virtual_file::owned_buffers_io::io_buf_aligned::IoBufAlignedMut;
use crate::virtual_file::owned_buffers_io::slice::SliceMutExt;
use crate::virtual_file::owned_buffers_io::write::{Buffer, FlushTaskError};
use crate::virtual_file::{self, IoBufferMut, TempVirtualFile, VirtualFile, owned_buffers_io};

use self::owned_buffers_io::write::OwnedAsyncWriter;

pub struct EphemeralFile {
    _tenant_shard_id: TenantShardId,
    _timeline_id: TimelineId,
    page_cache_file_id: page_cache::FileId,
    file: TempVirtualFileCoOwnedByEphemeralFileAndBufferedWriter,

    buffered_writer: tokio::sync::RwLock<BufferedWriter>,

    bytes_written: AtomicU64,

    resource_units: std::sync::Mutex<GlobalResourceUnits>,
}

type BufferedWriter = owned_buffers_io::write::BufferedWriter<
    IoBufferMut,
    TempVirtualFileCoOwnedByEphemeralFileAndBufferedWriter,
>;

/// A TempVirtualFile that is co-owned by the [`EphemeralFile`]` and [`BufferedWriter`].
///
/// (Actually [`BufferedWriter`] internally is just a client to a background flush task.
/// The co-ownership is between [`EphemeralFile`] and that flush task.)
///
/// Co-ownership allows us to serve reads for data that has already been flushed by the [`BufferedWriter`].
#[derive(Debug, Clone)]
struct TempVirtualFileCoOwnedByEphemeralFileAndBufferedWriter {
    inner: Arc<TempVirtualFile>,
}

const TAIL_SZ: usize = 64 * 1024;

impl EphemeralFile {
    pub async fn create(
        conf: &PageServerConf,
        tenant_shard_id: TenantShardId,
        timeline_id: TimelineId,
        gate: &utils::sync::gate::Gate,
        cancel: &CancellationToken,
        ctx: &RequestContext,
    ) -> anyhow::Result<EphemeralFile> {
        // TempVirtualFile requires us to never reuse a filename while an old
        // instance of TempVirtualFile created with that filename is not done dropping yet.
        // So, we use a monotonic counter to disambiguate the filenames.
        static NEXT_TEMP_DISAMBIGUATOR: AtomicU64 = AtomicU64::new(1);
        let filename_disambiguator =
            NEXT_TEMP_DISAMBIGUATOR.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let filename = conf
            .timeline_path(&tenant_shard_id, &timeline_id)
            .join(Utf8PathBuf::from(format!(
                "ephemeral-{filename_disambiguator}"
            )));

        let file = TempVirtualFileCoOwnedByEphemeralFileAndBufferedWriter::new(
            VirtualFile::open_with_options_v2(
                &filename,
                virtual_file::OpenOptions::new()
                    .create_new(true)
                    .read(true)
                    .write(true),
                ctx,
            )
            .await?,
            gate.enter()?,
        );

        let page_cache_file_id = page_cache::next_file_id(); // XXX get rid, we're not page-caching anymore

        Ok(EphemeralFile {
            _tenant_shard_id: tenant_shard_id,
            _timeline_id: timeline_id,
            page_cache_file_id,
            file: file.clone(),
            buffered_writer: tokio::sync::RwLock::new(BufferedWriter::new(
                file,
                0,
                || IoBufferMut::with_capacity(TAIL_SZ),
                gate.enter()?,
                cancel.child_token(),
                ctx,
                info_span!(parent: None, "ephemeral_file_buffered_writer", tenant_id=%tenant_shard_id.tenant_id, shard_id=%tenant_shard_id.shard_slug(), timeline_id=%timeline_id, path = %filename),
            )),
            bytes_written: AtomicU64::new(0),
            resource_units: std::sync::Mutex::new(GlobalResourceUnits::new()),
        })
    }
}

impl TempVirtualFileCoOwnedByEphemeralFileAndBufferedWriter {
    fn new(file: VirtualFile, gate_guard: GateGuard) -> Self {
        Self {
            inner: Arc::new(TempVirtualFile::new(file, gate_guard)),
        }
    }
}

impl OwnedAsyncWriter for TempVirtualFileCoOwnedByEphemeralFileAndBufferedWriter {
    fn write_all_at<Buf: owned_buffers_io::io_buf_aligned::IoBufAligned + Send>(
        &self,
        buf: owned_buffers_io::io_buf_ext::FullSlice<Buf>,
        offset: u64,
        ctx: &RequestContext,
    ) -> impl std::future::Future<
        Output = (
            owned_buffers_io::io_buf_ext::FullSlice<Buf>,
            std::io::Result<()>,
        ),
    > + Send {
        self.inner.write_all_at(buf, offset, ctx)
    }

    fn set_len(
        &self,
        len: u64,
        ctx: &RequestContext,
    ) -> impl Future<Output = std::io::Result<()>> + Send {
        self.inner.set_len(len, ctx)
    }
}

impl std::ops::Deref for TempVirtualFileCoOwnedByEphemeralFileAndBufferedWriter {
    type Target = VirtualFile;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum EphemeralFileWriteError {
    #[error("cancelled")]
    Cancelled,
}

impl EphemeralFile {
    pub(crate) fn len(&self) -> u64 {
        // TODO(vlad): The value returned here is not always correct if
        // we have more than one concurrent writer. Writes are always
        // sequenced, but we could grab the buffered writer lock if we wanted
        // to.
        self.bytes_written.load(Ordering::Acquire)
    }

    pub(crate) fn page_cache_file_id(&self) -> page_cache::FileId {
        self.page_cache_file_id
    }

    pub(crate) async fn load_to_io_buf(
        &self,
        ctx: &RequestContext,
    ) -> Result<IoBufferMut, io::Error> {
        let size = self.len().into_usize();
        let buf = IoBufferMut::with_capacity(size);
        let (slice, nread) = self.read_exact_at_eof_ok(0, buf.slice_full(), ctx).await?;
        assert_eq!(nread, size);
        let buf = slice.into_inner();
        assert_eq!(buf.len(), nread);
        assert_eq!(buf.capacity(), size, "we shouldn't be reallocating");
        Ok(buf)
    }

    /// Returns the offset at which the first byte of the input was written, for use
    /// in constructing indices over the written value.
    ///
    /// Panics if the write is short because there's no way we can recover from that.
    /// TODO: make upstack handle this as an error.
    pub(crate) async fn write_raw(
        &self,
        srcbuf: &[u8],
        ctx: &RequestContext,
    ) -> Result<u64, EphemeralFileWriteError> {
        let (pos, control) = self.write_raw_controlled(srcbuf, ctx).await?;
        if let Some(control) = control {
            control.release().await;
        }
        Ok(pos)
    }

    async fn write_raw_controlled(
        &self,
        srcbuf: &[u8],
        ctx: &RequestContext,
    ) -> Result<(u64, Option<owned_buffers_io::write::FlushControl>), EphemeralFileWriteError> {
        let mut writer = self.buffered_writer.write().await;

        let (nwritten, control) = writer
            .write_buffered_borrowed_controlled(srcbuf, ctx)
            .await
            .map_err(|e| match e {
                FlushTaskError::Cancelled => EphemeralFileWriteError::Cancelled,
            })?;
        assert_eq!(
            nwritten,
            srcbuf.len(),
            "buffered writer has no short writes"
        );

        // There's no realistic risk of overflow here. We won't have exabytes sized files on disk.
        let pos = self
            .bytes_written
            .fetch_add(srcbuf.len().into_u64(), Ordering::AcqRel);

        let mut resource_units = self.resource_units.lock().unwrap();
        resource_units.maybe_publish_size(self.bytes_written.load(Ordering::Relaxed));

        Ok((pos, control))
    }

    pub(crate) fn tick(&self) -> Option<u64> {
        let mut resource_units = self.resource_units.lock().unwrap();
        let len = self.bytes_written.load(Ordering::Relaxed);
        resource_units.publish_size(len)
    }
}

impl super::storage_layer::inmemory_layer::vectored_dio_read::File for EphemeralFile {
    async fn read_exact_at_eof_ok<B: IoBufAlignedMut + Send>(
        &self,
        start: u64,
        mut dst: tokio_epoll_uring::Slice<B>,
        ctx: &RequestContext,
    ) -> std::io::Result<(tokio_epoll_uring::Slice<B>, usize)> {
        // We will fill the slice in back to front. Hence, we need
        // the slice to be fully initialized.
        // TODO(vlad): Is there a nicer way of doing this?
        dst.as_mut_rust_slice_full_zeroed();

        let writer = self.buffered_writer.read().await;

        // Read bytes written while under lock. This is a hack to deal with concurrent
        // writes updating the number of bytes written. `bytes_written` is not DIO alligned
        // but we may end the read there.
        //
        // TODO(vlad): Feels like there's a nicer path where we align the end if it
        // shoots over the end of the file.
        let bytes_written = self.bytes_written.load(Ordering::Acquire);

        let dst_cap = dst.bytes_total().into_u64();
        let end = {
            // saturating_add is correct here because the max file size is u64::MAX, so,
            // if start + dst.len() > u64::MAX, then we know it will be a short read
            let mut end: u64 = start.saturating_add(dst_cap);
            if end > bytes_written {
                end = bytes_written;
            }
            end
        };

        let submitted_offset = writer.bytes_submitted();
        let maybe_flushed = writer.inspect_maybe_flushed();

        let mutable = match writer.inspect_mutable() {
            Some(mutable) => &mutable[0..mutable.pending()],
            None => {
                // Timeline::cancel and hence buffered writer flush was cancelled.
                // Remain read-available while timeline is shutting down.
                &[]
            }
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

        let (written_range, maybe_flushed_range) = {
            if maybe_flushed.is_some() {
                // [       written       ][ maybe_flushed ][    mutable    ]
                //                                         ^
                //                                 `submitted_offset`
                // <++++++ on disk +++++++????????????????>
                (
                    Range(
                        start,
                        std::cmp::min(end, submitted_offset.saturating_sub(TAIL_SZ as u64)),
                    ),
                    Range(
                        std::cmp::max(start, submitted_offset.saturating_sub(TAIL_SZ as u64)),
                        std::cmp::min(end, submitted_offset),
                    ),
                )
            } else {
                // [       written                        ][    mutable    ]
                //                                         ^
                //                                 `submitted_offset`
                // <++++++ on disk +++++++++++++++++++++++>
                (
                    Range(start, std::cmp::min(end, submitted_offset)),
                    // zero len
                    Range(submitted_offset, u64::MIN),
                )
            }
        };

        let mutable_range = Range(std::cmp::max(start, submitted_offset), end);

        // There are three sources from which we might have to read data:
        // 1. The file itself
        // 2. The buffer which contains changes currently being flushed
        // 3. The buffer which contains chnages yet to be flushed
        //
        // For better concurrency, we do them in reverse order: perform the in-memory
        // reads while holding the writer lock, drop the writer lock and read from the
        // file if required.

        let dst = if mutable_range.len() > 0 {
            let offset_in_buffer = mutable_range
                .0
                .checked_sub(submitted_offset)
                .unwrap()
                .into_usize();
            let to_copy =
                &mutable[offset_in_buffer..(offset_in_buffer + mutable_range.len().into_usize())];
            let bounds = dst.bounds();
            let mut view = dst.slice({
                let start =
                    written_range.len().into_usize() + maybe_flushed_range.len().into_usize();
                let end = start.checked_add(mutable_range.len().into_usize()).unwrap();
                start..end
            });
            view.as_mut_rust_slice_full_zeroed()
                .copy_from_slice(to_copy);
            Slice::from_buf_bounds(Slice::into_inner(view), bounds)
        } else {
            dst
        };

        let dst = if maybe_flushed_range.len() > 0 {
            let offset_in_buffer = maybe_flushed_range
                .0
                .checked_sub(submitted_offset.saturating_sub(TAIL_SZ as u64))
                .unwrap()
                .into_usize();
            // Checked previously the buffer is Some.
            let maybe_flushed = maybe_flushed.unwrap();
            let to_copy = &maybe_flushed
                [offset_in_buffer..(offset_in_buffer + maybe_flushed_range.len().into_usize())];
            let bounds = dst.bounds();
            let mut view = dst.slice({
                let start = written_range.len().into_usize();
                let end = start
                    .checked_add(maybe_flushed_range.len().into_usize())
                    .unwrap();
                start..end
            });
            view.as_mut_rust_slice_full_zeroed()
                .copy_from_slice(to_copy);
            Slice::from_buf_bounds(Slice::into_inner(view), bounds)
        } else {
            dst
        };

        drop(writer);

        let dst = if written_range.len() > 0 {
            let bounds = dst.bounds();
            let slice = self
                .file
                .read_exact_at(dst.slice(0..written_range.len().into_usize()), start, ctx)
                .await?;
            Slice::from_buf_bounds(Slice::into_inner(slice), bounds)
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
    use std::fs;
    use std::str::FromStr;

    use rand::Rng;

    use super::*;
    use crate::context::DownloadBehavior;
    use crate::task_mgr::TaskKind;

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

        let ctx =
            RequestContext::new(TaskKind::UnitTest, DownloadBehavior::Error).with_scope_unit_test();

        Ok((conf, tenant_shard_id, timeline_id, ctx))
    }

    #[tokio::test]
    async fn ephemeral_file_holds_gate_open() {
        const FOREVER: std::time::Duration = std::time::Duration::from_secs(5);

        let (conf, tenant_id, timeline_id, ctx) =
            harness("ephemeral_file_holds_gate_open").unwrap();

        let gate = utils::sync::gate::Gate::default();
        let cancel = CancellationToken::new();

        let file = EphemeralFile::create(conf, tenant_id, timeline_id, &gate, &cancel, &ctx)
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
        let cancel = CancellationToken::new();

        let file = EphemeralFile::create(conf, tenant_id, timeline_id, &gate, &cancel, &ctx)
            .await
            .unwrap();

        let writer = file.buffered_writer.read().await;
        let mutable = writer.mutable();
        let cap = mutable.capacity();
        let align = mutable.align();
        drop(writer);

        let write_nbytes = cap * 2 + cap / 2;

        let content: Vec<u8> = rand::thread_rng()
            .sample_iter(rand::distributions::Standard)
            .take(write_nbytes)
            .collect();

        let mut value_offsets = Vec::new();
        for range in (0..write_nbytes)
            .step_by(align)
            .map(|start| start..(start + align).min(write_nbytes))
        {
            let off = file.write_raw(&content[range], &ctx).await.unwrap();
            value_offsets.push(off);
        }

        assert_eq!(file.len() as usize, write_nbytes);
        for (i, range) in (0..write_nbytes)
            .step_by(align)
            .map(|start| start..(start + align).min(write_nbytes))
            .enumerate()
        {
            assert_eq!(value_offsets[i], range.start.into_u64());
            let buf = IoBufferMut::with_capacity(range.len());
            let (buf_slice, nread) = file
                .read_exact_at_eof_ok(range.start.into_u64(), buf.slice_full(), &ctx)
                .await
                .unwrap();
            let buf = buf_slice.into_inner();
            assert_eq!(nread, range.len());
            assert_eq!(&buf, &content[range]);
        }

        let file_contents = std::fs::read(file.file.path()).unwrap();
        assert!(file_contents == content[0..cap * 2]);

        let writer = file.buffered_writer.read().await;
        let maybe_flushed_buffer_contents = writer.inspect_maybe_flushed().unwrap();
        assert_eq!(&maybe_flushed_buffer_contents[..], &content[cap..cap * 2]);

        let mutable_buffer_contents = writer.mutable();
        assert_eq!(mutable_buffer_contents, &content[cap * 2..write_nbytes]);
    }

    #[tokio::test]
    async fn test_flushes_do_happen() {
        let (conf, tenant_id, timeline_id, ctx) = harness("test_flushes_do_happen").unwrap();

        let gate = utils::sync::gate::Gate::default();
        let cancel = CancellationToken::new();
        let file = EphemeralFile::create(conf, tenant_id, timeline_id, &gate, &cancel, &ctx)
            .await
            .unwrap();

        // mutable buffer and maybe_flushed buffer each has `cap` bytes.
        let writer = file.buffered_writer.read().await;
        let cap = writer.mutable().capacity();
        drop(writer);

        let content: Vec<u8> = rand::thread_rng()
            .sample_iter(rand::distributions::Standard)
            .take(cap * 2 + cap / 2)
            .collect();

        file.write_raw(&content, &ctx).await.unwrap();

        // assert the state is as this test expects it to be
        let load_io_buf_res = file.load_to_io_buf(&ctx).await.unwrap();
        assert_eq!(&load_io_buf_res[..], &content[0..cap * 2 + cap / 2]);
        let md = file.file.path().metadata().unwrap();
        assert_eq!(
            md.len(),
            2 * cap.into_u64(),
            "buffered writer requires one write to be flushed if we write 2.5x buffer capacity"
        );
        let writer = file.buffered_writer.read().await;
        assert_eq!(
            &writer.inspect_maybe_flushed().unwrap()[0..cap],
            &content[cap..cap * 2]
        );
        assert_eq!(
            &writer.mutable()[0..cap / 2],
            &content[cap * 2..cap * 2 + cap / 2]
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
        let cancel = CancellationToken::new();

        let file = EphemeralFile::create(conf, tenant_id, timeline_id, &gate, &cancel, &ctx)
            .await
            .unwrap();

        let writer = file.buffered_writer.read().await;
        let mutable = writer.mutable();
        let cap = mutable.capacity();
        let align = mutable.align();
        drop(writer);
        let content: Vec<u8> = rand::thread_rng()
            .sample_iter(rand::distributions::Standard)
            .take(cap * 2 + cap / 2)
            .collect();

        let (_, control) = file.write_raw_controlled(&content, &ctx).await.unwrap();

        let test_read = |start: usize, len: usize| {
            let file = &file;
            let ctx = &ctx;
            let content = &content;
            async move {
                let (buf, nread) = file
                    .read_exact_at_eof_ok(
                        start.into_u64(),
                        IoBufferMut::with_capacity(len).slice_full(),
                        ctx,
                    )
                    .await
                    .unwrap();
                assert_eq!(nread, len);
                assert_eq!(&buf.into_inner(), &content[start..(start + len)]);
            }
        };

        let test_read_all_offset_combinations = || {
            async move {
                test_read(align, align).await;
                // border onto edge of file
                test_read(cap - align, align).await;
                // read across file and buffer
                test_read(cap - align, 2 * align).await;
                // stay from start of maybe flushed buffer
                test_read(cap, align).await;
                // completely within maybe flushed buffer
                test_read(cap + align, align).await;
                // border onto edge of maybe flushed buffer.
                test_read(cap * 2 - align, align).await;
                // read across maybe flushed and mutable buffer
                test_read(cap * 2 - align, 2 * align).await;
                // read across three segments
                test_read(cap - align, cap + 2 * align).await;
                // completely within mutable buffer
                test_read(cap * 2 + align, align).await;
            }
        };

        // completely within the file range
        assert!(align < cap, "test assumption");
        assert!(cap % align == 0);

        // test reads at different flush stages.
        let not_started = control.unwrap().into_not_started();
        test_read_all_offset_combinations().await;
        let in_progress = not_started.ready_to_flush();
        test_read_all_offset_combinations().await;
        in_progress.wait_until_flush_is_done().await;
        test_read_all_offset_combinations().await;
    }
}
