//! Abstraction to create & read pidfiles.
//!
//! A pidfile is a file in the filesystem that stores a process's PID.
//! Its purpose is to implement a singleton behavior where only
//! one process of some "kind" is supposed to be running at a given time.
//! The "kind" is identified by the pidfile.
//!
//! During process startup, the process that is supposed to be a singleton
//! must [claim][`claim_for_current_process`] the pidfile first.
//! If that is unsuccessful, the process must not act as the singleton, i.e.,
//! it must not access any of the resources that only the singleton may access.
//!
//! A common need is to signal a running singleton process, e.g., to make
//! it shut down and exit.
//! For that, we have to [`read`] the pidfile. The result of the `read` operation
//! tells us if there is any singleton process, and if so, what PID it has.
//! We can then proceed to signal it, although some caveats still apply.
//! Read the function-level documentation of [`read`] for that.
//!
//! ## Never Remove Pidfiles
//!
//! It would be natural to assume that the process who claimed the pidfile
//! should remove it upon exit to avoid leaving a stale pidfile in place.
//! However, we already have a reliable way to detect staleness of the pidfile,
//! i.e., the `flock` that [claiming][`claim_for_current_process`] puts on it.
//!
//! And further, removing pidfiles would introduce a **catastrophic race condition**
//! where two processes are running that are supposed to be singletons.
//! Suppose we were to remove our pidfile during process shutdown.
//! Here is how the race plays out:
//! - Suppose we have a service called `myservice` with pidfile `myservice.pidfile`.
//! - Process `A` starts to shut down.
//! - Process `B` is just starting up
//!     - It `open("myservice.pid", O_WRONLY|O_CREAT)` the file
//!     - It blocks on `flock`
//! - Process `A` removes the pidfile as the last step of its shutdown procedure
//!     - `unlink("myservice.pid")
//! - Process `A` exits
//!     - This releases its `flock` and unblocks `B`
//! - Process `B` still has the file descriptor for `myservice.pid` open
//! - Process `B` writes its PID into `myservice.pid`.
//! - But the `myservice.pid` file has been unlinked, so, there is `myservice.pid`
//!   in the directory.
//! - Process `C` starts
//!     - It `open("myservice.pid", O_WRONLY|O_CREAT)` which creates a new file (new inode)
//!     - It `flock`s the file, which, since it's a different file, does not block
//!     - It writes its PID into the file
//!
//! At this point, `B` and `C` are running, which is hazardous.
//! Morale of the story: don't unlink pidfiles, ever.

use std::ops::Deref;

use anyhow::Context;
use camino::Utf8Path;
use nix::unistd::Pid;

use crate::lock_file::{self, LockFileRead};

/// Keeps a claim on a pidfile alive until it is dropped.
/// Returned by [`claim_for_current_process`].
#[must_use]
pub struct PidFileGuard(lock_file::LockFileGuard);

impl Deref for PidFileGuard {
    type Target = lock_file::LockFileGuard;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Try to claim `path` as a pidfile for the current process.
///
/// If another process has already claimed the pidfile, and it is still running,
/// this function returns ane error.
/// Otherwise, the function `flock`s the file and updates its contents to the
/// current process's PID.
/// If the update fails, the flock is released and an error returned.
/// On success, the function returns a [`PidFileGuard`] to keep the flock alive.
///
/// ### Maintaining A Claim
///
/// It is the caller's responsibility to maintain the claim.
/// The claim ends as soon as the returned guard object is dropped.
/// To maintain the claim for the remaining lifetime of the current process,
/// use [`std::mem::forget`] or similar.
pub fn claim_for_current_process(path: &Utf8Path) -> anyhow::Result<PidFileGuard> {
    let unwritten_lock_file = lock_file::create_exclusive(path).context("lock file")?;
    // if any of the next steps fail, we drop the file descriptor and thereby release the lock
    let guard = unwritten_lock_file
        .write_content(Pid::this().to_string())
        .context("write pid to lock file")?;
    Ok(PidFileGuard(guard))
}

/// Returned by [`read`].
pub enum PidFileRead {
    /// No file exists at the given path.
    NotExist,
    /// The given pidfile is currently not claimed by any process.
    /// To determine this, the [`read`] operation acquired
    /// an exclusive flock on the file. The lock is still held and responsibility
    /// to release it is returned through the guard object.
    /// Before releasing it, other [`claim_for_current_process`] or [`read`] calls
    /// will fail.
    ///
    /// ### Caveats
    ///
    /// Do not unlink the pidfile from the filesystem. See module-comment for why.
    NotHeldByAnyProcess(PidFileGuard),
    /// The given pidfile is still claimed by another process whose PID is given
    /// as part of this variant.
    ///
    /// ### Caveats
    ///
    /// 1. The other process might exit at any time, turning the given PID stale.
    /// 2. There is a small window in which `claim_for_current_process` has already
    ///    locked the file but not yet updates its contents. [`read`] will return
    ///    this variant here, but with the old file contents, i.e., a stale PID.
    ///
    /// The kernel is free to recycle PID once it has been `wait(2)`ed upon by
    /// its creator. Thus, acting upon a stale PID, e.g., by issuing a `kill`
    /// system call on it, bears the risk of killing an unrelated process.
    /// This is an inherent limitation of using pidfiles.
    /// The only race-free solution is to have a supervisor-process with a lifetime
    /// that exceeds that of all of its child-processes (e.g., `runit`, `supervisord`).
    LockedByOtherProcess(Pid),
}

/// Try to read the file at the given path as a pidfile that was previously created
/// through [`claim_for_current_process`].
///
/// On success, this function returns a [`PidFileRead`].
/// Check its docs for a description of the meaning of its different variants.
pub fn read(pidfile: &Utf8Path) -> anyhow::Result<PidFileRead> {
    let res = lock_file::read_and_hold_lock_file(pidfile).context("read and hold pid file")?;
    let ret = match res {
        LockFileRead::NotExist => PidFileRead::NotExist,
        LockFileRead::NotHeldByAnyProcess(guard, _) => {
            PidFileRead::NotHeldByAnyProcess(PidFileGuard(guard))
        }
        LockFileRead::LockedByOtherProcess {
            not_locked_file: _not_locked_file,
            content,
        } => {
            // XXX the read races with the write in claim_pid_file_for_pid().
            // But pids are smaller than a page, so the kernel page cache will lock for us.
            // The only problem is that we might get the old contents here.
            // Can only fix that by implementing some scheme that downgrades the
            // exclusive lock to shared lock in claim_pid_file_for_pid().
            PidFileRead::LockedByOtherProcess(parse_pidfile_content(&content)?)
        }
    };
    Ok(ret)
}

fn parse_pidfile_content(content: &str) -> anyhow::Result<Pid> {
    let pid: i32 = content
        .parse()
        .map_err(|_| anyhow::anyhow!("parse pidfile content to PID"))?;
    if pid < 1 {
        anyhow::bail!("bad value in pidfile '{pid}'");
    }
    Ok(Pid::from_raw(pid))
}
