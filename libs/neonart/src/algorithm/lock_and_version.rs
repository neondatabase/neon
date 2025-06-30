//! Each node in the tree has contains one atomic word that stores three things:
//!
//! Bit 0: set if the node is "obsolete". An obsolete node has been removed from the tree,
//!        but might still be accessed by concurrent readers until the epoch expires.
//! Bit 1: set if the node is currently write-locked. Used as a spinlock.
//! Bits 2-63: Version number, incremented every time the node is modified.
//!
//! AtomicLockAndVersion represents that.

use std::sync::atomic::{AtomicU64, Ordering};

pub(crate) struct ConcurrentUpdateError();

pub(crate) struct AtomicLockAndVersion {
    inner: AtomicU64,
}

impl AtomicLockAndVersion {
    pub(crate) fn new() -> AtomicLockAndVersion {
        AtomicLockAndVersion {
            inner: AtomicU64::new(0),
        }
    }
}

impl AtomicLockAndVersion {
    pub(crate) fn read_lock_or_restart(&self) -> Result<u64, ConcurrentUpdateError> {
        let version = self.await_node_unlocked();
        if is_obsolete(version) {
            return Err(ConcurrentUpdateError());
        }
        Ok(version)
    }

    pub(crate) fn check_or_restart(&self, version: u64) -> Result<(), ConcurrentUpdateError> {
        self.read_unlock_or_restart(version)
    }

    pub(crate) fn read_unlock_or_restart(&self, version: u64) -> Result<(), ConcurrentUpdateError> {
        if self.inner.load(Ordering::Acquire) != version {
            return Err(ConcurrentUpdateError());
        }
        Ok(())
    }

    pub(crate) fn upgrade_to_write_lock_or_restart(
        &self,
        version: u64,
    ) -> Result<(), ConcurrentUpdateError> {
        if self
            .inner
            .compare_exchange(
                version,
                set_locked_bit(version),
                Ordering::Acquire,
                Ordering::Relaxed,
            )
            .is_err()
        {
            return Err(ConcurrentUpdateError());
        }
        Ok(())
    }

    pub(crate) fn write_lock_or_restart(&self) -> Result<(), ConcurrentUpdateError> {
        let old = self.inner.load(Ordering::Relaxed);
        if is_obsolete(old) || is_locked(old) {
            return Err(ConcurrentUpdateError());
        }
        if self
            .inner
            .compare_exchange(
                old,
                set_locked_bit(old),
                Ordering::Acquire,
                Ordering::Relaxed,
            )
            .is_err()
        {
            return Err(ConcurrentUpdateError());
        }
        Ok(())
    }

    pub(crate) fn write_unlock(&self) {
        // reset locked bit and overflow into version
        self.inner.fetch_add(2, Ordering::Release);
    }

    pub(crate) fn write_unlock_obsolete(&self) {
        // set obsolete, reset locked, overflow into version
        self.inner.fetch_add(3, Ordering::Release);
    }

    // Helper functions
    fn await_node_unlocked(&self) -> u64 {
        let mut version = self.inner.load(Ordering::Acquire);
        while is_locked(version) {
            // spinlock
            std::thread::yield_now();
            version = self.inner.load(Ordering::Acquire)
        }
        version
    }
}

fn set_locked_bit(version: u64) -> u64 {
    version + 2
}

fn is_obsolete(version: u64) -> bool {
    (version & 1) == 1
}

fn is_locked(version: u64) -> bool {
    (version & 2) == 2
}
