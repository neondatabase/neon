//!
//! VirtualFile is like a normal File, but it's not bound directly to
//! a file descriptor. Instead, the file is opened when it's read from,
//! and if too many files are open globally in the system, least-recently
//! used ones are closed.
//!
//! To track which files have been recently used, we use the clock algorithm
//! with a 'recently_used' flag on each slot.
//!
//! This is similar to PostgreSQL's virtual file descriptor facility in
//! src/backend/storage/file/fd.c
//!
use crate::metrics::{STORAGE_IO_SIZE, STORAGE_IO_TIME};
use crate::page_cache::PageWriteGuard;

use std::fs::{self, File, OpenOptions};
use std::io::{Error, ErrorKind, Seek, SeekFrom, Write};

use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};

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
pub struct VirtualFile {
    /// Lazy handle to the global file descriptor cache. The slot that this points to
    /// might contain our File, or it may be empty, or it may contain a File that
    /// belongs to a different VirtualFile.
    handle: Arc<Mutex<Option<File>>>, // only transiently None

    /// Current file position
    pos: u64,

    /// File path and options to use to open it.
    ///
    /// Note: this only contains the options needed to re-open it. For example,
    /// if a new file is created, we only pass the create flag when it's initially
    /// opened, in the VirtualFile::create() function, and strip the flag before
    /// storing it here.
    pub path: PathBuf,

    // These are strings becase we only use them for metrics, and those expect strings.
    // It makes no sense for us to constantly turn the `TimelineId` and `TenantId` into
    // strings.
    tenant_id: String,
    timeline_id: String,
}

impl VirtualFile {
    /// Open a file in read-only mode. Like File::open.
    pub fn open(path: &Path) -> Result<VirtualFile, std::io::Error> {
        Self::open_with_options(path, OpenOptions::new().read(true))
    }

    /// Create a new file for writing. If the file exists, it will be truncated.
    /// Like File::create.
    pub fn create(path: &Path) -> Result<VirtualFile, std::io::Error> {
        Self::open_with_options(
            path,
            OpenOptions::new().write(true).create(true).truncate(true),
        )
    }

    /// Open a file with given options.
    ///
    /// Note: If any custom flags were set in 'open_options' through OpenOptionsExt,
    /// they will be applied also when the file is subsequently re-opened, not only
    /// on the first time. Make sure that's sane!
    pub fn open_with_options(
        path: &Path,
        open_options: &OpenOptions,
    ) -> Result<VirtualFile, std::io::Error> {
        let path_str = path.to_string_lossy();
        let parts = path_str.split('/').collect::<Vec<&str>>();
        let tenant_id;
        let timeline_id;
        if parts.len() > 5 && parts[parts.len() - 5] == "tenants" {
            tenant_id = parts[parts.len() - 4].to_string();
            timeline_id = parts[parts.len() - 2].to_string();
        } else {
            tenant_id = "*".to_string();
            timeline_id = "*".to_string();
        }
        let file = STORAGE_IO_TIME
            .with_label_values(&["open"])
            .observe_closure_duration(|| open_options.open(path))?;

        // Strip all options other than read and write.
        //
        // It would perhaps be nicer to check just for the read and write flags
        // explicitly, but OpenOptions doesn't contain any functions to read flags,
        // only to set them.
        let mut reopen_options = open_options.clone();
        reopen_options.create(false);
        reopen_options.create_new(false);
        reopen_options.truncate(false);

        let vfile = VirtualFile {
            handle: Arc::new(Mutex::new(Some(file))),
            pos: 0,
            path: path.to_path_buf(),
            tenant_id,
            timeline_id,
        };

        Ok(vfile)
    }

    /// Call File::sync_all() on the underlying File.
    pub fn sync_all(&self) -> Result<(), Error> {
        self.with_file("fsync", |file| file.sync_all())?
    }

    pub fn metadata(&self) -> Result<fs::Metadata, Error> {
        self.with_file("metadata", |file| file.metadata())?
    }
}

impl VirtualFile {
    /// Helper function that looks up the underlying File for this VirtualFile,
    /// opening it and evicting some other File if necessary. It calls 'func'
    /// with the physical File.
    fn with_file<F, R>(&self, op: &str, mut func: F) -> Result<R, Error>
    where
        F: FnMut(&File) -> R,
    {
        return Ok(STORAGE_IO_TIME
            .with_label_values(&[op])
            .observe_closure_duration(|| func(&*self.handle.lock().unwrap().as_ref().unwrap())));
    }

    pub fn remove(self) {
        let path = self.path.clone();
        drop(self);
        std::fs::remove_file(path).expect("failed to remove the virtual file");
    }
}

impl Write for VirtualFile {
    fn write(&mut self, buf: &[u8]) -> Result<usize, std::io::Error> {
        let pos = self.pos;
        let n = self.write_at(buf, pos)?;
        self.pos += n as u64;
        Ok(n)
    }

    fn flush(&mut self) -> Result<(), std::io::Error> {
        // flush is no-op for File (at least on unix), so we don't need to do
        // anything here either.
        Ok(())
    }
}

impl VirtualFile {
    pub fn seek(&mut self, pos: SeekFrom) -> Result<u64, Error> {
        match pos {
            SeekFrom::Start(offset) => {
                self.pos = offset;
            }
            SeekFrom::End(offset) => {
                self.pos = self.with_file("seek", |mut file| file.seek(SeekFrom::End(offset)))??
            }
            SeekFrom::Current(offset) => {
                let pos = self.pos as i128 + offset as i128;
                if pos < 0 {
                    return Err(Error::new(
                        ErrorKind::InvalidInput,
                        "offset would be negative",
                    ));
                }
                if pos > u64::MAX as i128 {
                    return Err(Error::new(ErrorKind::InvalidInput, "offset overflow"));
                }
                self.pos = pos as u64;
            }
        }
        Ok(self.pos)
    }

    // Copied from https://doc.rust-lang.org/1.72.0/src/std/os/unix/fs.rs.html#117-135
    pub fn read_exact_at(&self, mut buf: &mut [u8], mut offset: u64) -> Result<(), Error> {
        while !buf.is_empty() {
            match self.read_at(buf, offset) {
                Ok(0) => {
                    return Err(Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "failed to fill whole buffer",
                    ))
                }
                Ok(n) => {
                    buf = &mut buf[n..];
                    offset += n as u64;
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    // Copied from https://doc.rust-lang.org/1.72.0/src/std/os/unix/fs.rs.html#117-135
    pub async fn read_exact_at_async(
        &self,
        mut write_guard: PageWriteGuard<'static>,
        offset: u64,
    ) -> Result<PageWriteGuard<'static>, Error> {
        let file = self.handle.lock().unwrap().take().unwrap();
        let put_back = AtomicBool::new(false);
        let put_back_ref = &put_back;
        scopeguard::defer! {
            if !put_back_ref.load(std::sync::atomic::Ordering::Relaxed) {
                panic!("mut put self.handle back")
            }
        };
        let ((file, write_guard), res) = tokio::task::spawn_blocking(move || {
            let res = file.read_exact_at(write_guard.as_mut(), offset);
            ((file, write_guard), res)
        })
        .await
        .expect("spawn_blocking");
        let replaced = self.handle.lock().unwrap().replace(file);
        assert!(replaced.is_none());
        put_back.store(true, std::sync::atomic::Ordering::Relaxed);
        res.map(|()| write_guard)
    }

    // Copied from https://doc.rust-lang.org/1.72.0/src/std/os/unix/fs.rs.html#219-235
    pub fn write_all_at(&self, mut buf: &[u8], mut offset: u64) -> Result<(), Error> {
        while !buf.is_empty() {
            match self.write_at(buf, offset) {
                Ok(0) => {
                    return Err(Error::new(
                        std::io::ErrorKind::WriteZero,
                        "failed to write whole buffer",
                    ));
                }
                Ok(n) => {
                    buf = &buf[n..];
                    offset += n as u64;
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize, Error> {
        let result = self.with_file("read", |file| {
            tracing::info!("sync read\n{}", std::backtrace::Backtrace::force_capture());
            file.read_at(buf, offset)
        })?;
        if let Ok(size) = result {
            STORAGE_IO_SIZE
                .with_label_values(&["read", &self.tenant_id, &self.timeline_id])
                .add(size as i64);
        }
        result
    }

    pub fn write_at(&self, buf: &[u8], offset: u64) -> Result<usize, Error> {
        let result = self.with_file("write", |file| file.write_at(buf, offset))?;
        if let Ok(size) = result {
            STORAGE_IO_SIZE
                .with_label_values(&["write", &self.tenant_id, &self.timeline_id])
                .add(size as i64);
        }
        result
    }
}
