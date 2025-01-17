//! A module to create and read lock files.
//!
//! File locking is done using [`fcntl::flock`] exclusive locks.
//! The only consumer of this module is currently
//! [`pid_file`](crate::pid_file). See the module-level comment
//! there for potential pitfalls with lock files that are used
//! to store PIDs (pidfiles).

use std::{
    fs,
    io::{Read, Write},
    ops::Deref,
    os::unix::prelude::AsRawFd,
};

use anyhow::Context;
use camino::{Utf8Path, Utf8PathBuf};
use nix::{errno::Errno::EAGAIN, fcntl};

use crate::crashsafe;

/// A handle to an open and unlocked, but not-yet-written lock file.
/// Returned by [`create_exclusive`].
#[must_use]
pub struct UnwrittenLockFile {
    path: Utf8PathBuf,
    file: fs::File,
}

/// Returned by [`UnwrittenLockFile::write_content`].
#[must_use]
pub struct LockFileGuard(fs::File);

impl Deref for LockFileGuard {
    type Target = fs::File;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl UnwrittenLockFile {
    /// Replace the content of this lock file with the byte representation of `contents`.
    pub fn write_content(mut self, contents: String) -> anyhow::Result<LockFileGuard> {
        self.file
            .set_len(0)
            .context("Failed to truncate lockfile")?;
        self.file
            .write_all(contents.as_bytes())
            .with_context(|| format!("Failed to write '{contents}' contents into lockfile"))?;
        crashsafe::fsync_file_and_parent(&self.path).context("fsync lockfile")?;
        Ok(LockFileGuard(self.file))
    }
}

/// Creates and opens a lock file in the path, grabs an exclusive flock on it, and returns
/// a handle that allows overwriting the locked file's content.
///
/// The exclusive lock is released when dropping the returned handle.
///
/// It is not an error if the file already exists.
/// It is an error if the file is already locked.
pub fn create_exclusive(lock_file_path: &Utf8Path) -> anyhow::Result<UnwrittenLockFile> {
    let lock_file = fs::OpenOptions::new()
        .create(true) // O_CREAT
        .truncate(true)
        .write(true)
        .open(lock_file_path)
        .context("open lock file")?;

    let res = fcntl::flock(
        lock_file.as_raw_fd(),
        fcntl::FlockArg::LockExclusiveNonblock,
    );
    match res {
        Ok(()) => Ok(UnwrittenLockFile {
            path: lock_file_path.to_owned(),
            file: lock_file,
        }),
        Err(EAGAIN) => anyhow::bail!("file is already locked"),
        Err(e) => Err(e).context("flock error"),
    }
}

/// Returned by [`read_and_hold_lock_file`].
/// Check out the [`pid_file`](crate::pid_file) module for what the variants mean
/// and potential caveats if the lock files that are used to store PIDs.
pub enum LockFileRead {
    /// No file exists at the given path.
    NotExist,
    /// No other process held the lock file, so we grabbed an flock
    /// on it and read its contents.
    /// Release the flock by dropping the [`LockFileGuard`].
    NotHeldByAnyProcess(LockFileGuard, String),
    /// The file exists but another process was holding an flock on it.
    LockedByOtherProcess {
        not_locked_file: fs::File,
        content: String,
    },
}

/// Open & try to lock the lock file at the given `path`, returning a [handle][`LockFileRead`] to
/// inspect its content.
///
/// It is not an `Err(...)` if the file does not exist or is already locked.
/// Check the [`LockFileRead`] variants for details.
pub fn read_and_hold_lock_file(path: &Utf8Path) -> anyhow::Result<LockFileRead> {
    let res = fs::OpenOptions::new().read(true).open(path);
    let mut lock_file = match res {
        Ok(f) => f,
        Err(e) => match e.kind() {
            std::io::ErrorKind::NotFound => return Ok(LockFileRead::NotExist),
            _ => return Err(e).context("open lock file"),
        },
    };
    let res = fcntl::flock(
        lock_file.as_raw_fd(),
        fcntl::FlockArg::LockExclusiveNonblock,
    );
    // We need the content regardless of lock success / failure.
    // But, read it after flock so that, if it succeeded, the content is consistent.
    let mut content = String::new();
    lock_file
        .read_to_string(&mut content)
        .context("read lock file")?;
    match res {
        Ok(()) => Ok(LockFileRead::NotHeldByAnyProcess(
            LockFileGuard(lock_file),
            content,
        )),
        Err(EAGAIN) => Ok(LockFileRead::LockedByOtherProcess {
            not_locked_file: lock_file,
            content,
        }),
        Err(e) => Err(e).context("flock error"),
    }
}
