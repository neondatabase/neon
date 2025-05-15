//! VirtualFile is like a normal File, but it's not bound directly to
//! a file descriptor.
//!
//! Instead, the file is opened when it's read from,
//! and if too many files are open globally in the system, least-recently
//! used ones are closed.
//!
//! To track which files have been recently used, we use the clock algorithm
//! with a 'recently_used' flag on each slot.
//!
//! This is similar to PostgreSQL's virtual file descriptor facility in
//! src/backend/storage/file/fd.c
//!
use std::fs::File;
use std::io::{Error, ErrorKind};
use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd, RawFd};
use std::sync::LazyLock;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};

use camino::{Utf8Path, Utf8PathBuf};
use once_cell::sync::OnceCell;
use owned_buffers_io::aligned_buffer::buffer::AlignedBuffer;
use owned_buffers_io::aligned_buffer::{AlignedBufferMut, AlignedSlice, ConstAlign};
use owned_buffers_io::io_buf_aligned::{IoBufAligned, IoBufAlignedMut};
use owned_buffers_io::io_buf_ext::FullSlice;
use pageserver_api::config::defaults::DEFAULT_IO_BUFFER_ALIGNMENT;
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use tokio::time::Instant;
use tokio_epoll_uring::{BoundedBuf, IoBuf, IoBufMut, Slice};

use self::owned_buffers_io::write::OwnedAsyncWriter;
use crate::assert_u64_eq_usize::UsizeIsU64;
use crate::context::RequestContext;
use crate::metrics::{STORAGE_IO_TIME_METRIC, StorageIoOperation};
use crate::page_cache::{PAGE_SZ, PageWriteGuard};

pub(crate) use api::IoMode;
pub(crate) use io_engine::IoEngineKind;
pub use io_engine::{
    FeatureTestResult as IoEngineFeatureTestResult, feature_test as io_engine_feature_test,
    io_engine_for_bench,
};
pub(crate) use metadata::Metadata;
pub(crate) use open_options::*;
pub use pageserver_api::models::virtual_file as api;
pub use temporary::TempVirtualFile;

pub(crate) mod io_engine;
mod metadata;
mod open_options;
mod temporary;
pub(crate) mod owned_buffers_io {
    //! Abstractions for IO with owned buffers.
    //!
    //! Not actually tied to [`crate::virtual_file`] specifically, but, it's the primary
    //! reason we need this abstraction.
    //!
    //! Over time, this could move into the `tokio-epoll-uring` crate, maybe `uring-common`,
    //! but for the time being we're proving out the primitives in the neon.git repo
    //! for faster iteration.

    pub(crate) mod aligned_buffer;
    pub(crate) mod io_buf_aligned;
    pub(crate) mod io_buf_ext;
    pub(crate) mod slice;
    pub(crate) mod write;
}

#[derive(Debug)]
pub struct VirtualFile {
    inner: VirtualFileInner,
    _mode: IoMode,
}

impl VirtualFile {
    /// Open a file in read-only mode. Like File::open.
    ///
    /// Insensitive to `virtual_file_io_mode` setting.
    pub async fn open<P: AsRef<Utf8Path>>(
        path: P,
        ctx: &RequestContext,
    ) -> Result<Self, std::io::Error> {
        let inner = VirtualFileInner::open(path, ctx).await?;
        Ok(VirtualFile {
            inner,
            _mode: IoMode::Buffered,
        })
    }

    /// Open a file in read-only mode. Like File::open.
    ///
    /// `O_DIRECT` will be enabled base on `virtual_file_io_mode`.
    pub async fn open_v2<P: AsRef<Utf8Path>>(
        path: P,
        ctx: &RequestContext,
    ) -> Result<Self, std::io::Error> {
        Self::open_with_options_v2(path.as_ref(), OpenOptions::new().read(true), ctx).await
    }

    /// `O_DIRECT` will be enabled base on `virtual_file_io_mode`.
    pub async fn open_with_options_v2<P: AsRef<Utf8Path>>(
        path: P,
        mut open_options: OpenOptions,
        ctx: &RequestContext,
    ) -> Result<Self, std::io::Error> {
        let mode = get_io_mode();
        let direct = match (mode, open_options.is_write()) {
            (IoMode::Buffered, _) => false,
            (IoMode::Direct, false) => true,
            (IoMode::Direct, true) => false,
            (IoMode::DirectRw, _) => true,
        };
        open_options = open_options.direct(direct);
        let inner = VirtualFileInner::open_with_options(path, open_options, ctx).await?;
        Ok(VirtualFile { inner, _mode: mode })
    }

    pub fn path(&self) -> &Utf8Path {
        self.inner.path.as_path()
    }

    pub async fn crashsafe_overwrite<B: BoundedBuf<Buf = Buf> + Send, Buf: IoBuf + Send>(
        final_path: Utf8PathBuf,
        tmp_path: Utf8PathBuf,
        content: B,
    ) -> std::io::Result<()> {
        VirtualFileInner::crashsafe_overwrite(final_path, tmp_path, content).await
    }

    pub async fn sync_all(&self) -> Result<(), Error> {
        if SYNC_MODE.load(std::sync::atomic::Ordering::Relaxed) == SyncMode::UnsafeNoSync as u8 {
            return Ok(());
        }
        self.inner.sync_all().await
    }

    pub async fn sync_data(&self) -> Result<(), Error> {
        if SYNC_MODE.load(std::sync::atomic::Ordering::Relaxed) == SyncMode::UnsafeNoSync as u8 {
            return Ok(());
        }
        self.inner.sync_data().await
    }

    pub async fn set_len(&self, len: u64, ctx: &RequestContext) -> Result<(), Error> {
        self.inner.set_len(len, ctx).await
    }

    pub async fn metadata(&self) -> Result<Metadata, Error> {
        self.inner.metadata().await
    }

    pub async fn read_exact_at<Buf>(
        &self,
        slice: Slice<Buf>,
        offset: u64,
        ctx: &RequestContext,
    ) -> Result<Slice<Buf>, Error>
    where
        Buf: IoBufAlignedMut + Send,
    {
        self.inner.read_exact_at(slice, offset, ctx).await
    }

    pub async fn read_exact_at_page(
        &self,
        page: PageWriteGuard<'static>,
        offset: u64,
        ctx: &RequestContext,
    ) -> Result<PageWriteGuard<'static>, Error> {
        self.inner.read_exact_at_page(page, offset, ctx).await
    }

    pub async fn write_all_at<Buf: IoBufAligned + Send>(
        &self,
        buf: FullSlice<Buf>,
        offset: u64,
        ctx: &RequestContext,
    ) -> (FullSlice<Buf>, Result<(), Error>) {
        self.inner.write_all_at(buf, offset, ctx).await
    }

    pub(crate) async fn read_to_string<P: AsRef<Utf8Path>>(
        path: P,
        ctx: &RequestContext,
    ) -> std::io::Result<String> {
        let file = VirtualFile::open(path, ctx).await?; // TODO: open_v2
        let mut buf = Vec::new();
        let mut tmp = vec![0; 128];
        let mut pos: u64 = 0;
        loop {
            let slice = tmp.slice(..128);
            let (slice, res) = file.inner.read_at(slice, pos, ctx).await;
            match res {
                Ok(0) => break,
                Ok(n) => {
                    pos += n as u64;
                    buf.extend_from_slice(&slice[..n]);
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
            tmp = slice.into_inner();
        }
        String::from_utf8(buf).map_err(|_| {
            std::io::Error::new(ErrorKind::InvalidData, "file contents are not valid UTF-8")
        })
    }
}

/// Indicates whether to enable fsync, fdatasync, or O_SYNC/O_DSYNC when writing
/// files. Switching this off is unsafe and only used for testing on machines
/// with slow drives.
#[repr(u8)]
pub enum SyncMode {
    Sync,
    UnsafeNoSync,
}

impl TryFrom<u8> for SyncMode {
    type Error = u8;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        Ok(match value {
            v if v == (SyncMode::Sync as u8) => SyncMode::Sync,
            v if v == (SyncMode::UnsafeNoSync as u8) => SyncMode::UnsafeNoSync,
            x => return Err(x),
        })
    }
}

///
/// A virtual file descriptor. You can use this just like std::fs::File, but internally
/// the underlying file is closed if the system is low on file descriptors,
/// and re-opened when it's accessed again.
///
/// Like with std::fs::File, multiple threads can read/write the file concurrently,
/// holding just a shared reference the same VirtualFile, using the read_at() / write_at()
/// functions from the FileExt trait. But the functions from the Read/Write/Seek traits
/// require a mutable reference, because they modify the "current position".
///
/// Each VirtualFile has a physical file descriptor in the global OPEN_FILES array, at the
/// slot that 'handle points to, if the underlying file is currently open. If it's not
/// currently open, the 'handle' can still point to the slot where it was last kept. The
/// 'tag' field is used to detect whether the handle still is valid or not.
///
#[derive(Debug)]
pub struct VirtualFileInner {
    /// Lazy handle to the global file descriptor cache. The slot that this points to
    /// might contain our File, or it may be empty, or it may contain a File that
    /// belongs to a different VirtualFile.
    handle: RwLock<SlotHandle>,

    /// File path and options to use to open it.
    ///
    /// Note: this only contains the options needed to re-open it. For example,
    /// if a new file is created, we only pass the create flag when it's initially
    /// opened, in the VirtualFile::create() function, and strip the flag before
    /// storing it here.
    pub path: Utf8PathBuf,
    open_options: OpenOptions,
}

#[derive(Debug, PartialEq, Clone, Copy)]
struct SlotHandle {
    /// Index into OPEN_FILES.slots
    index: usize,

    /// Value of 'tag' in the slot. If slot's tag doesn't match, then the slot has
    /// been recycled and no longer contains the FD for this virtual file.
    tag: u64,
}

/// OPEN_FILES is the global array that holds the physical file descriptors that
/// are currently open. Each slot in the array is protected by a separate lock,
/// so that different files can be accessed independently. The lock must be held
/// in write mode to replace the slot with a different file, but a read mode
/// is enough to operate on the file, whether you're reading or writing to it.
///
/// OPEN_FILES starts in uninitialized state, and it's initialized by
/// the virtual_file::init() function. It must be called exactly once at page
/// server startup.
static OPEN_FILES: OnceCell<OpenFiles> = OnceCell::new();

struct OpenFiles {
    slots: &'static [Slot],

    /// clock arm for the clock algorithm
    next: AtomicUsize,
}

struct Slot {
    inner: RwLock<SlotInner>,

    /// has this file been used since last clock sweep?
    recently_used: AtomicBool,
}

struct SlotInner {
    /// Counter that's incremented every time a different file is stored here.
    /// To avoid the ABA problem.
    tag: u64,

    /// the underlying file
    file: Option<OwnedFd>,
}

/// Impl of [`tokio_epoll_uring::IoBuf`] and [`tokio_epoll_uring::IoBufMut`] for [`PageWriteGuard`].
struct PageWriteGuardBuf {
    page: PageWriteGuard<'static>,
}
// Safety: the [`PageWriteGuard`] gives us exclusive ownership of the page cache slot,
// and the location remains stable even if [`Self`] or the [`PageWriteGuard`] is moved.
// Page cache pages are zero-initialized, so, wrt uninitialized memory we're good.
// (Page cache tracks separately whether the contents are valid, see `PageWriteGuard::mark_valid`.)
unsafe impl tokio_epoll_uring::IoBuf for PageWriteGuardBuf {
    fn stable_ptr(&self) -> *const u8 {
        self.page.as_ptr()
    }
    fn bytes_init(&self) -> usize {
        self.page.len()
    }
    fn bytes_total(&self) -> usize {
        self.page.len()
    }
}
// Safety: see above, plus: the ownership of [`PageWriteGuard`] means exclusive access,
// hence it's safe to hand out the `stable_mut_ptr()`.
unsafe impl tokio_epoll_uring::IoBufMut for PageWriteGuardBuf {
    fn stable_mut_ptr(&mut self) -> *mut u8 {
        self.page.as_mut_ptr()
    }

    unsafe fn set_init(&mut self, pos: usize) {
        // There shouldn't really be any reason to call this API since bytes_init() == bytes_total().
        assert!(pos <= self.page.len());
    }
}

impl OpenFiles {
    /// Find a slot to use, evicting an existing file descriptor if needed.
    ///
    /// On return, we hold a lock on the slot, and its 'tag' has been updated
    /// recently_used has been set. It's all ready for reuse.
    async fn find_victim_slot(&self) -> (SlotHandle, RwLockWriteGuard<SlotInner>) {
        //
        // Run the clock algorithm to find a slot to replace.
        //
        let num_slots = self.slots.len();
        let mut retries = 0;
        let mut slot;
        let mut slot_guard;
        let index;
        loop {
            let next = self.next.fetch_add(1, Ordering::AcqRel) % num_slots;
            slot = &self.slots[next];

            // If the recently_used flag on this slot is set, continue the clock
            // sweep. Otherwise try to use this slot. If we cannot acquire the
            // lock, also continue the clock sweep.
            //
            // We only continue in this manner for a while, though. If we loop
            // through the array twice without finding a victim, just pick the
            // next slot and wait until we can reuse it. This way, we avoid
            // spinning in the extreme case that all the slots are busy with an
            // I/O operation.
            if retries < num_slots * 2 {
                if !slot.recently_used.swap(false, Ordering::Release) {
                    if let Ok(guard) = slot.inner.try_write() {
                        slot_guard = guard;
                        index = next;
                        break;
                    }
                }
                retries += 1;
            } else {
                slot_guard = slot.inner.write().await;
                index = next;
                break;
            }
        }

        //
        // We now have the victim slot locked. If it was in use previously, close the
        // old file.
        //
        if let Some(old_file) = slot_guard.file.take() {
            // the normal path of dropping VirtualFile uses "close", use "close-by-replace" here to
            // distinguish the two.
            STORAGE_IO_TIME_METRIC
                .get(StorageIoOperation::CloseByReplace)
                .observe_closure_duration(|| drop(old_file));
        }

        // Prepare the slot for reuse and return it
        slot_guard.tag += 1;
        slot.recently_used.store(true, Ordering::Relaxed);
        (
            SlotHandle {
                index,
                tag: slot_guard.tag,
            },
            slot_guard,
        )
    }
}

/// Identify error types that should alwways terminate the process.  Other
/// error types may be elegible for retry.
pub(crate) fn is_fatal_io_error(e: &std::io::Error) -> bool {
    use nix::errno::Errno::*;
    match e.raw_os_error().map(nix::errno::from_i32) {
        Some(EIO) => {
            // Terminate on EIO because we no longer trust the device to store
            // data safely, or to uphold persistence guarantees on fsync.
            true
        }
        Some(EROFS) => {
            // Terminate on EROFS because a filesystem is usually remounted
            // readonly when it has experienced some critical issue, so the same
            // logic as EIO applies.
            true
        }
        Some(EACCES) => {
            // Terminate on EACCESS because we should always have permissions
            // for our own data dir: if we don't, then we can't do our job and
            // need administrative intervention to fix permissions.  Terminating
            // is the best way to make sure we stop cleanly rather than going
            // into infinite retry loops, and will make it clear to the outside
            // world that we need help.
            true
        }
        _ => {
            // Treat all other local file I/O errors are retryable.  This includes:
            // - ENOSPC: we stay up and wait for eviction to free some space
            // - EINVAL, EBADF, EBADFD: this is a code bug, not a filesystem/hardware issue
            // - WriteZero, Interrupted: these are used internally VirtualFile
            false
        }
    }
}

/// Call this when the local filesystem gives us an error with an external
/// cause: this includes EIO, EROFS, and EACCESS: all these indicate either
/// bad storage or bad configuration, and we can't fix that from inside
/// a running process.
pub(crate) fn on_fatal_io_error(e: &std::io::Error, context: &str) -> ! {
    let backtrace = std::backtrace::Backtrace::force_capture();
    tracing::error!("Fatal I/O error: {e}: {context})\n{backtrace}");
    std::process::abort();
}

pub(crate) trait MaybeFatalIo<T> {
    fn maybe_fatal_err(self, context: &str) -> std::io::Result<T>;
    fn fatal_err(self, context: &str) -> T;
}

impl<T> MaybeFatalIo<T> for std::io::Result<T> {
    /// Terminate the process if the result is an error of a fatal type, else pass it through
    ///
    /// This is appropriate for writes, where we typically want to die on EIO/ACCES etc, but
    /// not on ENOSPC.
    fn maybe_fatal_err(self, context: &str) -> std::io::Result<T> {
        if let Err(e) = &self {
            if is_fatal_io_error(e) {
                on_fatal_io_error(e, context);
            }
        }
        self
    }

    /// Terminate the process on any I/O error.
    ///
    /// This is appropriate for reads on files that we know exist: they should always work.
    fn fatal_err(self, context: &str) -> T {
        match self {
            Ok(v) => v,
            Err(e) => {
                on_fatal_io_error(&e, context);
            }
        }
    }
}

/// Observe duration for the given storage I/O operation
///
/// Unlike `observe_closure_duration`, this supports async,
/// where "support" means that we measure wall clock time.
macro_rules! observe_duration {
    ($op:expr, $($body:tt)*) => {{
        let instant = Instant::now();
        let result = $($body)*;
        let elapsed = instant.elapsed().as_secs_f64();
        STORAGE_IO_TIME_METRIC
            .get($op)
            .observe(elapsed);
        result
    }}
}

macro_rules! with_file {
    ($this:expr, $op:expr, | $ident:ident | $($body:tt)*) => {{
        let $ident = $this.lock_file().await?;
        observe_duration!($op, $($body)*)
    }};
    ($this:expr, $op:expr, | mut $ident:ident | $($body:tt)*) => {{
        let mut $ident = $this.lock_file().await?;
        observe_duration!($op, $($body)*)
    }};
}

impl VirtualFileInner {
    /// Open a file in read-only mode. Like File::open.
    pub async fn open<P: AsRef<Utf8Path>>(
        path: P,
        ctx: &RequestContext,
    ) -> Result<VirtualFileInner, std::io::Error> {
        Self::open_with_options(path.as_ref(), OpenOptions::new().read(true), ctx).await
    }

    /// Open a file with given options.
    ///
    /// Note: If any custom flags were set in 'open_options' through OpenOptionsExt,
    /// they will be applied also when the file is subsequently re-opened, not only
    /// on the first time. Make sure that's sane!
    pub async fn open_with_options<P: AsRef<Utf8Path>>(
        path: P,
        open_options: OpenOptions,
        _ctx: &RequestContext,
    ) -> Result<VirtualFileInner, std::io::Error> {
        let path = path.as_ref();
        let (handle, mut slot_guard) = get_open_files().find_victim_slot().await;

        // NB: there is also StorageIoOperation::OpenAfterReplace which is for the case
        // where our caller doesn't get to use the returned VirtualFile before its
        // slot gets re-used by someone else.
        let file = observe_duration!(StorageIoOperation::Open, {
            open_options.open(path.as_std_path()).await?
        });

        // Strip all options other than read and write.
        //
        // It would perhaps be nicer to check just for the read and write flags
        // explicitly, but OpenOptions doesn't contain any functions to read flags,
        // only to set them.
        let reopen_options = open_options
            .clone()
            .create(false)
            .create_new(false)
            .truncate(false);

        let vfile = VirtualFileInner {
            handle: RwLock::new(handle),
            path: path.to_owned(),
            open_options: reopen_options,
        };

        // TODO: Under pressure, it's likely the slot will get re-used and
        // the underlying file closed before they get around to using it.
        // => https://github.com/neondatabase/neon/issues/6065
        slot_guard.file.replace(file);

        Ok(vfile)
    }

    /// Async version of [`::utils::crashsafe::overwrite`].
    ///
    /// # NB:
    ///
    /// Doesn't actually use the [`VirtualFile`] file descriptor cache, but,
    /// it did at an earlier time.
    /// And it will use this module's [`io_engine`] in the near future, so, leaving it here.
    pub async fn crashsafe_overwrite<B: BoundedBuf<Buf = Buf> + Send, Buf: IoBuf + Send>(
        final_path: Utf8PathBuf,
        tmp_path: Utf8PathBuf,
        content: B,
    ) -> std::io::Result<()> {
        // TODO: use tokio_epoll_uring if configured as `io_engine`.
        // See https://github.com/neondatabase/neon/issues/6663

        tokio::task::spawn_blocking(move || {
            let slice_storage;
            let content_len = content.bytes_init();
            let content = if content.bytes_init() > 0 {
                slice_storage = Some(content.slice(0..content_len));
                slice_storage.as_deref().expect("just set it to Some()")
            } else {
                &[]
            };
            utils::crashsafe::overwrite(&final_path, &tmp_path, content)
                .maybe_fatal_err("crashsafe_overwrite")
        })
        .await
        .expect("blocking task is never aborted")
    }

    /// Call File::sync_all() on the underlying File.
    pub async fn sync_all(&self) -> Result<(), Error> {
        with_file!(self, StorageIoOperation::Fsync, |file_guard| {
            let (_file_guard, res) = io_engine::get().sync_all(file_guard).await;
            res.maybe_fatal_err("sync_all")
        })
    }

    /// Call File::sync_data() on the underlying File.
    pub async fn sync_data(&self) -> Result<(), Error> {
        with_file!(self, StorageIoOperation::Fsync, |file_guard| {
            let (_file_guard, res) = io_engine::get().sync_data(file_guard).await;
            res.maybe_fatal_err("sync_data")
        })
    }

    pub async fn metadata(&self) -> Result<Metadata, Error> {
        with_file!(self, StorageIoOperation::Metadata, |file_guard| {
            let (_file_guard, res) = io_engine::get().metadata(file_guard).await;
            res
        })
    }

    pub async fn set_len(&self, len: u64, _ctx: &RequestContext) -> Result<(), Error> {
        with_file!(self, StorageIoOperation::SetLen, |file_guard| {
            let (_file_guard, res) = io_engine::get().set_len(file_guard, len).await;
            res.maybe_fatal_err("set_len")
        })
    }

    /// Helper function internal to `VirtualFile` that looks up the underlying File,
    /// opens it and evicts some other File if necessary. The passed parameter is
    /// assumed to be a function available for the physical `File`.
    ///
    /// We are doing it via a macro as Rust doesn't support async closures that
    /// take on parameters with lifetimes.
    async fn lock_file(&self) -> Result<FileGuard, Error> {
        let open_files = get_open_files();

        let mut handle_guard = {
            // Read the cached slot handle, and see if the slot that it points to still
            // contains our File.
            //
            // We only need to hold the handle lock while we read the current handle. If
            // another thread closes the file and recycles the slot for a different file,
            // we will notice that the handle we read is no longer valid and retry.
            let mut handle = *self.handle.read().await;
            loop {
                // Check if the slot contains our File
                {
                    let slot = &open_files.slots[handle.index];
                    let slot_guard = slot.inner.read().await;
                    if slot_guard.tag == handle.tag && slot_guard.file.is_some() {
                        // Found a cached file descriptor.
                        slot.recently_used.store(true, Ordering::Relaxed);
                        return Ok(FileGuard { slot_guard });
                    }
                }

                // The slot didn't contain our File. We will have to open it ourselves,
                // but before that, grab a write lock on handle in the VirtualFile, so
                // that no other thread will try to concurrently open the same file.
                let handle_guard = self.handle.write().await;

                // If another thread changed the handle while we were not holding the lock,
                // then the handle might now be valid again. Loop back to retry.
                if *handle_guard != handle {
                    handle = *handle_guard;
                    continue;
                }
                break handle_guard;
            }
        };

        // We need to open the file ourselves. The handle in the VirtualFile is
        // now locked in write-mode. Find a free slot to put it in.
        let (handle, mut slot_guard) = open_files.find_victim_slot().await;

        // Re-open the physical file.
        // NB: we use StorageIoOperation::OpenAferReplace for this to distinguish this
        // case from StorageIoOperation::Open. This helps with identifying thrashing
        // of the virtual file descriptor cache.
        let file = observe_duration!(StorageIoOperation::OpenAfterReplace, {
            self.open_options.open(self.path.as_std_path()).await?
        });

        // Store the File in the slot and update the handle in the VirtualFile
        // to point to it.
        slot_guard.file.replace(file);

        *handle_guard = handle;

        Ok(FileGuard {
            slot_guard: slot_guard.downgrade(),
        })
    }

    /// Read the file contents in range `offset..(offset + slice.bytes_total())` into `slice[0..slice.bytes_total()]`.
    ///
    /// The returned `Slice<Buf>` is equivalent to the input `slice`, i.e., it's the same view into the same buffer.
    pub async fn read_exact_at<Buf>(
        &self,
        slice: Slice<Buf>,
        offset: u64,
        ctx: &RequestContext,
    ) -> Result<Slice<Buf>, Error>
    where
        Buf: IoBufAlignedMut + Send,
    {
        let assert_we_return_original_bounds = if cfg!(debug_assertions) {
            Some((slice.stable_ptr() as usize, slice.bytes_total()))
        } else {
            None
        };

        let original_bounds = slice.bounds();
        let (buf, res) =
            read_exact_at_impl(slice, offset, |buf, offset| self.read_at(buf, offset, ctx)).await;
        let res = res.map(|_| buf.slice(original_bounds));

        if let Some(original_bounds) = assert_we_return_original_bounds {
            if let Ok(slice) = &res {
                let returned_bounds = (slice.stable_ptr() as usize, slice.bytes_total());
                assert_eq!(original_bounds, returned_bounds);
            }
        }

        res
    }

    /// Like [`Self::read_exact_at`] but for [`PageWriteGuard`].
    pub async fn read_exact_at_page(
        &self,
        page: PageWriteGuard<'static>,
        offset: u64,
        ctx: &RequestContext,
    ) -> Result<PageWriteGuard<'static>, Error> {
        let buf = PageWriteGuardBuf { page }.slice_full();
        debug_assert_eq!(buf.bytes_total(), PAGE_SZ);
        self.read_exact_at(buf, offset, ctx)
            .await
            .map(|slice| slice.into_inner().page)
    }

    // Copied from https://doc.rust-lang.org/1.72.0/src/std/os/unix/fs.rs.html#219-235
    pub async fn write_all_at<Buf: IoBuf + Send>(
        &self,
        buf: FullSlice<Buf>,
        mut offset: u64,
        ctx: &RequestContext,
    ) -> (FullSlice<Buf>, Result<(), Error>) {
        let buf = buf.into_raw_slice();
        let bounds = buf.bounds();
        let restore =
            |buf: Slice<_>| FullSlice::must_new(Slice::from_buf_bounds(buf.into_inner(), bounds));
        let mut buf = buf;
        while !buf.is_empty() {
            let (tmp, res) = self.write_at(FullSlice::must_new(buf), offset, ctx).await;
            buf = tmp.into_raw_slice();
            match res {
                Ok(0) => {
                    return (
                        restore(buf),
                        Err(Error::new(
                            std::io::ErrorKind::WriteZero,
                            "failed to write whole buffer",
                        )),
                    );
                }
                Ok(n) => {
                    buf = buf.slice(n..);
                    offset += n as u64;
                }
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                Err(e) => return (restore(buf), Err(e)),
            }
        }
        (restore(buf), Ok(()))
    }

    pub(super) async fn read_at<Buf>(
        &self,
        buf: tokio_epoll_uring::Slice<Buf>,
        offset: u64,
        ctx: &RequestContext,
    ) -> (tokio_epoll_uring::Slice<Buf>, Result<usize, Error>)
    where
        Buf: tokio_epoll_uring::IoBufMut + Send,
    {
        self.validate_direct_io(
            Slice::stable_ptr(&buf).addr(),
            Slice::bytes_total(&buf),
            offset,
        );

        let file_guard = match self
            .lock_file()
            .await
            .maybe_fatal_err("lock_file inside VirtualFileInner::read_at")
        {
            Ok(file_guard) => file_guard,
            Err(e) => return (buf, Err(e)),
        };

        observe_duration!(StorageIoOperation::Read, {
            let ((_file_guard, buf), res) = io_engine::get().read_at(file_guard, offset, buf).await;
            let res = res.maybe_fatal_err("io_engine read_at inside VirtualFileInner::read_at");
            if let Ok(size) = res {
                ctx.io_size_metrics().read.add(size.into_u64());
            }
            (buf, res)
        })
    }

    async fn write_at<B: IoBuf + Send>(
        &self,
        buf: FullSlice<B>,
        offset: u64,
        ctx: &RequestContext,
    ) -> (FullSlice<B>, Result<usize, Error>) {
        self.validate_direct_io(buf.as_ptr().addr(), buf.len(), offset);

        let file_guard = match self.lock_file().await {
            Ok(file_guard) => file_guard,
            Err(e) => return (buf, Err(e)),
        };
        observe_duration!(StorageIoOperation::Write, {
            let ((_file_guard, buf), result) =
                io_engine::get().write_at(file_guard, offset, buf).await;
            let result = result.maybe_fatal_err("write_at");
            if let Ok(size) = result {
                ctx.io_size_metrics().write.add(size.into_u64());
            }
            (buf, result)
        })
    }

    /// Validate all reads and writes to adhere to the O_DIRECT requirements of our production systems.
    ///
    /// Validating it iin userspace sets a consistent bar, independent of what actual OS/filesystem/block device is in use.
    fn validate_direct_io(&self, addr: usize, size: usize, offset: u64) {
        // TODO: eventually enable validation in the builds we use in real environments like staging, preprod, and prod.
        if !(cfg!(feature = "testing") || cfg!(test)) {
            return;
        }
        if !self.open_options.is_direct() {
            return;
        }

        // Validate buffer memory alignment.
        //
        // What practically matters as of Linux 6.1 is bdev_dma_alignment()
        // which is practically between 512 and 4096.
        // On our production systems, the value is 512.
        // The IoBuffer/IoBufferMut hard-code that value.
        //
        // Because the alloctor might return _more_ aligned addresses than requested,
        // there is a chance that testing would not catch violations of a runtime requirement stricter than 512.
        {
            let requirement = 512;
            let remainder = addr % requirement;
            assert!(
                remainder == 0,
                "Direct I/O buffer must be aligned: buffer_addr=0x{addr:x} % 0x{requirement:x} = 0x{remainder:x}"
            );
        }

        // Validate offset alignment.
        //
        // We hard-code 512 throughout the code base.
        // So enforce just that and not anything more restrictive.
        // Even the shallowest testing will expose more restrictive requirements if those ever arise.
        {
            let requirement = 512;
            let remainder = offset % requirement;
            assert!(
                remainder == 0,
                "Direct I/O offset must be aligned: offset=0x{offset:x} % 0x{requirement:x} = 0x{remainder:x}"
            );
        }

        // Validate buffer size multiple requirement.
        //
        // The requirement in Linux 6.1 is bdev_logical_block_size().
        // On our production systems, that is 512.
        {
            let requirement = 512;
            let remainder = size % requirement;
            assert!(
                remainder == 0,
                "Direct I/O buffer size must be a multiple of {requirement}: size=0x{size:x} % 0x{requirement:x} = 0x{remainder:x}"
            );
        }
    }
}

// Adapted from https://doc.rust-lang.org/1.72.0/src/std/os/unix/fs.rs.html#117-135
pub async fn read_exact_at_impl<Buf, F, Fut>(
    mut buf: tokio_epoll_uring::Slice<Buf>,
    mut offset: u64,
    mut read_at: F,
) -> (Buf, std::io::Result<()>)
where
    Buf: IoBufMut + Send,
    F: FnMut(tokio_epoll_uring::Slice<Buf>, u64) -> Fut,
    Fut: std::future::Future<Output = (tokio_epoll_uring::Slice<Buf>, std::io::Result<usize>)>,
{
    while buf.bytes_total() != 0 {
        let res;
        (buf, res) = read_at(buf, offset).await;
        match res {
            Ok(0) => break,
            Ok(n) => {
                buf = buf.slice(n..);
                offset += n as u64;
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => return (buf.into_inner(), Err(e)),
        }
    }
    // NB: don't use `buf.is_empty()` here; it is from the
    // `impl Deref for Slice { Target = [u8] }`; the &[u8]
    // returned by it only covers the initialized portion of `buf`.
    // Whereas we're interested in ensuring that we filled the entire
    // buffer that the user passed in.
    if buf.bytes_total() != 0 {
        (
            buf.into_inner(),
            Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "failed to fill whole buffer",
            )),
        )
    } else {
        assert_eq!(buf.len(), buf.bytes_total());
        (buf.into_inner(), Ok(()))
    }
}

#[cfg(test)]
mod test_read_exact_at_impl {

    use std::collections::VecDeque;
    use std::sync::Arc;

    use tokio_epoll_uring::{BoundedBuf, BoundedBufMut};

    use super::read_exact_at_impl;

    struct Expectation {
        offset: u64,
        bytes_total: usize,
        result: std::io::Result<Vec<u8>>,
    }
    struct MockReadAt {
        expectations: VecDeque<Expectation>,
    }

    impl MockReadAt {
        async fn read_at(
            &mut self,
            mut buf: tokio_epoll_uring::Slice<Vec<u8>>,
            offset: u64,
        ) -> (tokio_epoll_uring::Slice<Vec<u8>>, std::io::Result<usize>) {
            let exp = self
                .expectations
                .pop_front()
                .expect("read_at called but we have no expectations left");
            assert_eq!(exp.offset, offset);
            assert_eq!(exp.bytes_total, buf.bytes_total());
            match exp.result {
                Ok(bytes) => {
                    assert!(bytes.len() <= buf.bytes_total());
                    buf.put_slice(&bytes);
                    (buf, Ok(bytes.len()))
                }
                Err(e) => (buf, Err(e)),
            }
        }
    }

    impl Drop for MockReadAt {
        fn drop(&mut self) {
            assert_eq!(self.expectations.len(), 0);
        }
    }

    #[tokio::test]
    async fn test_basic() {
        let buf = Vec::with_capacity(5).slice_full();
        let mock_read_at = Arc::new(tokio::sync::Mutex::new(MockReadAt {
            expectations: VecDeque::from(vec![Expectation {
                offset: 0,
                bytes_total: 5,
                result: Ok(vec![b'a', b'b', b'c', b'd', b'e']),
            }]),
        }));
        let (buf, res) = read_exact_at_impl(buf, 0, |buf, offset| {
            let mock_read_at = Arc::clone(&mock_read_at);
            async move { mock_read_at.lock().await.read_at(buf, offset).await }
        })
        .await;
        assert!(res.is_ok());
        assert_eq!(buf, vec![b'a', b'b', b'c', b'd', b'e']);
    }

    #[tokio::test]
    async fn test_empty_buf_issues_no_syscall() {
        let buf = Vec::new().slice_full();
        let mock_read_at = Arc::new(tokio::sync::Mutex::new(MockReadAt {
            expectations: VecDeque::new(),
        }));
        let (_buf, res) = read_exact_at_impl(buf, 0, |buf, offset| {
            let mock_read_at = Arc::clone(&mock_read_at);
            async move { mock_read_at.lock().await.read_at(buf, offset).await }
        })
        .await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_two_read_at_calls_needed_until_buf_filled() {
        let buf = Vec::with_capacity(4).slice_full();
        let mock_read_at = Arc::new(tokio::sync::Mutex::new(MockReadAt {
            expectations: VecDeque::from(vec![
                Expectation {
                    offset: 0,
                    bytes_total: 4,
                    result: Ok(vec![b'a', b'b']),
                },
                Expectation {
                    offset: 2,
                    bytes_total: 2,
                    result: Ok(vec![b'c', b'd']),
                },
            ]),
        }));
        let (buf, res) = read_exact_at_impl(buf, 0, |buf, offset| {
            let mock_read_at = Arc::clone(&mock_read_at);
            async move { mock_read_at.lock().await.read_at(buf, offset).await }
        })
        .await;
        assert!(res.is_ok());
        assert_eq!(buf, vec![b'a', b'b', b'c', b'd']);
    }

    #[tokio::test]
    async fn test_eof_before_buffer_full() {
        let buf = Vec::with_capacity(3).slice_full();
        let mock_read_at = Arc::new(tokio::sync::Mutex::new(MockReadAt {
            expectations: VecDeque::from(vec![
                Expectation {
                    offset: 0,
                    bytes_total: 3,
                    result: Ok(vec![b'a']),
                },
                Expectation {
                    offset: 1,
                    bytes_total: 2,
                    result: Ok(vec![b'b']),
                },
                Expectation {
                    offset: 2,
                    bytes_total: 1,
                    result: Ok(vec![]),
                },
            ]),
        }));
        let (_buf, res) = read_exact_at_impl(buf, 0, |buf, offset| {
            let mock_read_at = Arc::clone(&mock_read_at);
            async move { mock_read_at.lock().await.read_at(buf, offset).await }
        })
        .await;
        let Err(err) = res else {
            panic!("should return an error");
        };
        assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
        assert_eq!(format!("{err}"), "failed to fill whole buffer");
        // buffer contents on error are unspecified
    }
}

struct FileGuard {
    slot_guard: RwLockReadGuard<'static, SlotInner>,
}

impl AsRef<OwnedFd> for FileGuard {
    fn as_ref(&self) -> &OwnedFd {
        // This unwrap is safe because we only create `FileGuard`s
        // if we know that the file is Some.
        self.slot_guard.file.as_ref().unwrap()
    }
}

impl FileGuard {
    /// Soft deprecation: we'll move VirtualFile to async APIs and remove this function eventually.
    fn with_std_file<F, R>(&self, with: F) -> R
    where
        F: FnOnce(&File) -> R,
    {
        // SAFETY:
        // - lifetime of the fd: `file` doesn't outlive the OwnedFd stored in `self`.
        // - `&` usage below: `self` is `&`, hence Rust typesystem guarantees there are is no `&mut`
        let file = unsafe { File::from_raw_fd(self.as_ref().as_raw_fd()) };
        let res = with(&file);
        let _ = file.into_raw_fd();
        res
    }
}

impl tokio_epoll_uring::IoFd for FileGuard {
    unsafe fn as_fd(&self) -> RawFd {
        let owned_fd: &OwnedFd = self.as_ref();
        owned_fd.as_raw_fd()
    }
}

#[cfg(test)]
impl VirtualFile {
    pub(crate) async fn read_blk(
        &self,
        blknum: u32,
        ctx: &RequestContext,
    ) -> Result<crate::tenant::block_io::BlockLease<'_>, std::io::Error> {
        self.inner.read_blk(blknum, ctx).await
    }
}

#[cfg(test)]
impl VirtualFileInner {
    pub(crate) async fn read_blk(
        &self,
        blknum: u32,
        ctx: &RequestContext,
    ) -> Result<crate::tenant::block_io::BlockLease<'_>, std::io::Error> {
        use crate::page_cache::PAGE_SZ;
        let slice = IoBufferMut::with_capacity(PAGE_SZ).slice_full();
        assert_eq!(slice.bytes_total(), PAGE_SZ);
        let slice = self
            .read_exact_at(slice, blknum as u64 * (PAGE_SZ as u64), ctx)
            .await?;
        Ok(crate::tenant::block_io::BlockLease::IoBufferMut(
            slice.into_inner(),
        ))
    }
}

impl Drop for VirtualFileInner {
    /// If a VirtualFile is dropped, close the underlying file if it was open.
    fn drop(&mut self) {
        let handle = self.handle.get_mut();

        fn clean_slot(slot: &Slot, mut slot_guard: RwLockWriteGuard<'_, SlotInner>, tag: u64) {
            if slot_guard.tag == tag {
                slot.recently_used.store(false, Ordering::Relaxed);
                // there is also operation "close-by-replace" for closes done on eviction for
                // comparison.
                if let Some(fd) = slot_guard.file.take() {
                    STORAGE_IO_TIME_METRIC
                        .get(StorageIoOperation::Close)
                        .observe_closure_duration(|| drop(fd));
                }
            }
        }

        // We don't have async drop so we cannot directly await the lock here.
        // Instead, first do a best-effort attempt at closing the underlying
        // file descriptor by using `try_write`, and if that fails, spawn
        // a tokio task to do it asynchronously: we just want it to be
        // cleaned up eventually.
        // Most of the time, the `try_lock` should succeed though,
        // as we have `&mut self` access. In other words, if the slot
        // is still occupied by our file, there should be no access from
        // other I/O operations; the only other possible place to lock
        // the slot is the lock algorithm looking for free slots.
        let slot = &get_open_files().slots[handle.index];
        if let Ok(slot_guard) = slot.inner.try_write() {
            clean_slot(slot, slot_guard, handle.tag);
        } else {
            let tag = handle.tag;
            tokio::spawn(async move {
                let slot_guard = slot.inner.write().await;
                clean_slot(slot, slot_guard, tag);
            });
        };
    }
}

impl OwnedAsyncWriter for VirtualFile {
    async fn write_all_at<Buf: IoBufAligned + Send>(
        &self,
        buf: FullSlice<Buf>,
        offset: u64,
        ctx: &RequestContext,
    ) -> (FullSlice<Buf>, std::io::Result<()>) {
        VirtualFile::write_all_at(self, buf, offset, ctx).await
    }
    async fn set_len(&self, len: u64, ctx: &RequestContext) -> std::io::Result<()> {
        VirtualFile::set_len(self, len, ctx).await
    }
}

impl OpenFiles {
    fn new(num_slots: usize) -> OpenFiles {
        let mut slots = Box::new(Vec::with_capacity(num_slots));
        for _ in 0..num_slots {
            let slot = Slot {
                recently_used: AtomicBool::new(false),
                inner: RwLock::new(SlotInner { tag: 0, file: None }),
            };
            slots.push(slot);
        }

        OpenFiles {
            next: AtomicUsize::new(0),
            slots: Box::leak(slots),
        }
    }
}

///
/// Initialize the virtual file module. This must be called once at page
/// server startup.
///
#[cfg(not(test))]
pub fn init(num_slots: usize, engine: IoEngineKind, mode: IoMode, sync_mode: SyncMode) {
    if OPEN_FILES.set(OpenFiles::new(num_slots)).is_err() {
        panic!("virtual_file::init called twice");
    }
    set_io_mode(mode);
    io_engine::init(engine);
    SYNC_MODE.store(sync_mode as u8, std::sync::atomic::Ordering::Relaxed);
    crate::metrics::virtual_file_descriptor_cache::SIZE_MAX.set(num_slots as u64);
}

const TEST_MAX_FILE_DESCRIPTORS: usize = 10;

// Get a handle to the global slots array.
fn get_open_files() -> &'static OpenFiles {
    //
    // In unit tests, page server startup doesn't happen and no one calls
    // virtual_file::init(). Initialize it here, with a small array.
    //
    // This applies to the virtual file tests below, but all other unit
    // tests too, so the virtual file facility is always usable in
    // unit tests.
    //
    if cfg!(test) {
        OPEN_FILES.get_or_init(|| OpenFiles::new(TEST_MAX_FILE_DESCRIPTORS))
    } else {
        OPEN_FILES.get().expect("virtual_file::init not called yet")
    }
}

/// Gets the io buffer alignment.
pub(crate) const fn get_io_buffer_alignment() -> usize {
    DEFAULT_IO_BUFFER_ALIGNMENT
}

pub(crate) type IoBufferMut = AlignedBufferMut<ConstAlign<{ get_io_buffer_alignment() }>>;
pub(crate) type IoBuffer = AlignedBuffer<ConstAlign<{ get_io_buffer_alignment() }>>;
pub(crate) type IoPageSlice<'a> =
    AlignedSlice<'a, PAGE_SZ, ConstAlign<{ get_io_buffer_alignment() }>>;

static IO_MODE: LazyLock<AtomicU8> = LazyLock::new(|| AtomicU8::new(IoMode::preferred() as u8));

pub fn set_io_mode(mode: IoMode) {
    IO_MODE.store(mode as u8, std::sync::atomic::Ordering::Relaxed);
}

pub(crate) fn get_io_mode() -> IoMode {
    IoMode::try_from(IO_MODE.load(Ordering::Relaxed)).unwrap()
}

static SYNC_MODE: AtomicU8 = AtomicU8::new(SyncMode::Sync as u8);

#[cfg(test)]
mod tests {
    use std::os::unix::fs::FileExt;
    use std::sync::Arc;

    use owned_buffers_io::io_buf_ext::IoBufExt;
    use rand::seq::SliceRandom;
    use rand::{Rng, thread_rng};

    use super::*;
    use crate::context::DownloadBehavior;
    use crate::task_mgr::TaskKind;

    #[tokio::test]
    async fn test_virtual_files() -> anyhow::Result<()> {
        let ctx =
            RequestContext::new(TaskKind::UnitTest, DownloadBehavior::Error).with_scope_unit_test();
        let testdir = crate::config::PageServerConf::test_repo_dir("test_virtual_files");
        std::fs::create_dir_all(&testdir)?;

        let zeropad512 = |content: &[u8]| {
            let mut buf = IoBufferMut::with_capacity_zeroed(512);
            buf[..content.len()].copy_from_slice(content);
            buf.freeze().slice_len()
        };

        let path_a = testdir.join("file_a");
        let file_a = VirtualFile::open_with_options_v2(
            path_a.clone(),
            OpenOptions::new()
                .read(true)
                .write(true)
                // set create & truncate flags to ensure when we trigger a reopen later in this test,
                // the reopen_options must have masked out those flags; if they don't, then
                // the after reopen we will fail to read the `content_a` that we write here.
                .create(true)
                .truncate(true),
            &ctx,
        )
        .await?;
        let (_, res) = file_a.write_all_at(zeropad512(b"content_a"), 0, &ctx).await;
        res?;

        let path_b = testdir.join("file_b");
        let file_b = VirtualFile::open_with_options_v2(
            path_b.clone(),
            OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true),
            &ctx,
        )
        .await?;
        let (_, res) = file_b.write_all_at(zeropad512(b"content_b"), 0, &ctx).await;
        res?;

        let assert_first_512_eq = async |vfile: &VirtualFile, expect: &[u8]| {
            let buf = vfile
                .read_exact_at(IoBufferMut::with_capacity_zeroed(512).slice_full(), 0, &ctx)
                .await
                .unwrap();
            assert_eq!(&buf[..], &zeropad512(expect)[..]);
        };

        // Open a lot of file descriptors / VirtualFile instances.
        // Enough to cause some evictions in the fd cache.

        let mut file_b_dupes = Vec::new();
        for _ in 0..100 {
            let vfile = VirtualFile::open_with_options_v2(
                path_b.clone(),
                OpenOptions::new().read(true),
                &ctx,
            )
            .await?;
            assert_first_512_eq(&vfile, b"content_b").await;
            file_b_dupes.push(vfile);
        }

        // make sure we opened enough files to definitely cause evictions.
        assert!(file_b_dupes.len() > TEST_MAX_FILE_DESCRIPTORS * 2);

        // The underlying file descriptor for 'file_a' should be closed now. Try to read
        // from it again. The VirtualFile reopens the file internally.
        assert_first_512_eq(&file_a, b"content_a").await;

        // Check that all the other FDs still work too. Use them in random order for
        // good measure.
        file_b_dupes.as_mut_slice().shuffle(&mut thread_rng());
        for vfile in file_b_dupes.iter_mut() {
            assert_first_512_eq(vfile, b"content_b").await;
        }

        Ok(())
    }

    /// Test using VirtualFiles from many threads concurrently. This tests both using
    /// a lot of VirtualFiles concurrently, causing evictions, and also using the same
    /// VirtualFile from multiple threads concurrently.
    #[tokio::test]
    async fn test_vfile_concurrency() -> Result<(), Error> {
        const SIZE: usize = 8 * 1024;
        const VIRTUAL_FILES: usize = 100;
        const THREADS: usize = 100;
        const SAMPLE: [u8; SIZE] = [0xADu8; SIZE];

        let ctx =
            RequestContext::new(TaskKind::UnitTest, DownloadBehavior::Error).with_scope_unit_test();
        let testdir = crate::config::PageServerConf::test_repo_dir("vfile_concurrency");
        std::fs::create_dir_all(&testdir)?;

        // Create a test file.
        let test_file_path = testdir.join("concurrency_test_file");
        {
            let file = File::create(&test_file_path)?;
            file.write_all_at(&SAMPLE, 0)?;
        }

        // Open the file many times.
        let mut files = Vec::new();
        for _ in 0..VIRTUAL_FILES {
            let f = VirtualFile::open_with_options_v2(
                &test_file_path,
                OpenOptions::new().read(true),
                &ctx,
            )
            .await?;
            files.push(f);
        }
        let files = Arc::new(files);

        // Launch many threads, and use the virtual files concurrently in random order.
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(THREADS)
            .thread_name("test_vfile_concurrency thread")
            .build()
            .unwrap();
        let mut hdls = Vec::new();
        for _threadno in 0..THREADS {
            let files = files.clone();
            let ctx = ctx.detached_child(TaskKind::UnitTest, DownloadBehavior::Error);
            let hdl = rt.spawn(async move {
                let mut buf = IoBufferMut::with_capacity_zeroed(SIZE);
                let mut rng = rand::rngs::OsRng;
                for _ in 1..1000 {
                    let f = &files[rng.gen_range(0..files.len())];
                    buf = f
                        .read_exact_at(buf.slice_full(), 0, &ctx)
                        .await
                        .unwrap()
                        .into_inner();
                    assert!(buf[..] == SAMPLE);
                }
            });
            hdls.push(hdl);
        }
        for hdl in hdls {
            hdl.await?;
        }
        std::mem::forget(rt);

        Ok(())
    }

    #[tokio::test]
    async fn test_atomic_overwrite_basic() {
        let testdir = crate::config::PageServerConf::test_repo_dir("test_atomic_overwrite_basic");
        std::fs::create_dir_all(&testdir).unwrap();

        let path = testdir.join("myfile");
        let tmp_path = testdir.join("myfile.tmp");

        VirtualFileInner::crashsafe_overwrite(path.clone(), tmp_path.clone(), b"foo".to_vec())
            .await
            .unwrap();

        let post = std::fs::read_to_string(&path).unwrap();
        assert_eq!(post, "foo");
        assert!(!tmp_path.exists());

        VirtualFileInner::crashsafe_overwrite(path.clone(), tmp_path.clone(), b"bar".to_vec())
            .await
            .unwrap();

        let post = std::fs::read_to_string(&path).unwrap();
        assert_eq!(post, "bar");
        assert!(!tmp_path.exists());
    }

    #[tokio::test]
    async fn test_atomic_overwrite_preexisting_tmp() {
        let testdir =
            crate::config::PageServerConf::test_repo_dir("test_atomic_overwrite_preexisting_tmp");
        std::fs::create_dir_all(&testdir).unwrap();

        let path = testdir.join("myfile");
        let tmp_path = testdir.join("myfile.tmp");

        std::fs::write(&tmp_path, "some preexisting junk that should be removed").unwrap();
        assert!(tmp_path.exists());

        VirtualFileInner::crashsafe_overwrite(path.clone(), tmp_path.clone(), b"foo".to_vec())
            .await
            .unwrap();

        let post = std::fs::read_to_string(&path).unwrap();
        assert_eq!(post, "foo");
        assert!(!tmp_path.exists());
    }
}
