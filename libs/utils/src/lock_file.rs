//! A module to create and read lock files. A lock file ensures that only one
//! process is running at a time, in a particular directory.
//!
//! File locking is done using [`fcntl::flock`], which means that holding the
//! lock on file only prevents acquiring another lock on it; all other
//! operations are still possible on files. Other process can still open, read,
//! write, or remove the file, for example.
//! If the file is removed while a process is holding a lock on it,
//! the process that holds the lock does not get any error or notification.
//! Furthermore, you can create a new file with the same name and lock the new file,
//! while the old process is still running.
//! Deleting the lock file while the locking process is still running is a bad idea!

use std::{fs, os::unix::prelude::AsRawFd, path::Path};

use anyhow::Context;
use nix::fcntl;

use crate::crashsafe;

pub enum LockCreationResult {
    Created {
        new_lock_contents: String,
        file: fs::File,
    },
    AlreadyLocked {
        existing_lock_contents: String,
    },
    CreationFailed(anyhow::Error),
}

/// Creates a lock file in the path given and writes the given contents into the file.
/// Note: The lock is automatically released when the file closed. You might want to use Box::leak to make sure it lives until the end of the program.
pub fn create_lock_file(lock_file_path: &Path, contents: String) -> LockCreationResult {
    let lock_file = match fs::OpenOptions::new()
        .create(true) // O_CREAT
        .write(true)
        .open(lock_file_path)
        .context("Failed to open lock file")
    {
        Ok(file) => file,
        Err(e) => return LockCreationResult::CreationFailed(e),
    };

    match fcntl::flock(
        lock_file.as_raw_fd(),
        fcntl::FlockArg::LockExclusiveNonblock,
    ) {
        Ok(()) => {
            match lock_file
                .set_len(0)
                .context("Failed to truncate lockfile")
                .and_then(|()| {
                    fs::write(lock_file_path, &contents).with_context(|| {
                        format!("Failed to write '{contents}' contents into lockfile")
                    })
                })
                .and_then(|()| {
                    crashsafe::fsync_file_and_parent(lock_file_path)
                        .context("Failed to fsync lockfile")
                }) {
                Ok(()) => LockCreationResult::Created {
                    new_lock_contents: contents,
                    file: lock_file,
                },
                Err(e) => LockCreationResult::CreationFailed(e),
            }
        }
        Err(nix::errno::Errno::EAGAIN) => {
            match fs::read_to_string(lock_file_path).context("Failed to read lockfile contents") {
                Ok(existing_lock_contents) => LockCreationResult::AlreadyLocked {
                    existing_lock_contents,
                },
                Err(e) => LockCreationResult::CreationFailed(e),
            }
        }
        Err(e) => {
            LockCreationResult::CreationFailed(anyhow::anyhow!("Failed to lock lockfile: {e}"))
        }
    }
}
